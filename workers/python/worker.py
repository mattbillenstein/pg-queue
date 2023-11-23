#!/usr/bin/env python3

import argparse
import datetime
import importlib
import multiprocessing
import select
import socket
import sys
import time
import traceback
import uuid
from collections import deque

import db

NOT_DONE = str(uuid.uuid4())


class Job:
    def __init__(self, row):
        self._row = row
        self._pipe, pipe = multiprocessing.Pipe(duplex=False)
        self._process = multiprocessing.Process(target=self.run, args=(row, pipe), daemon=True)
        self._process.start()
        self._result = NOT_DONE

    def run(self, row, result):
        payload = row["payload"]
        try:
            mod, func = payload["func"].rsplit(".", 1)
            mod = importlib.import_module(mod)
            func = getattr(mod, func)
            args = payload.get("args", [])
            kw = payload.get("kwargs", {})
            res = func(*args, **kw)
            res = {"result": res, "exc": None}
        except Exception as e:
            exc = "".join(traceback.format_exception(e))
            res = {"result": None, "exc": exc}

        result.send(res)

    @property
    def elapsed(self):
        return (datetime.datetime.utcnow() - self._row["started_at"]).total_seconds()

    @property
    def ready(self):
        return self._process.exitcode is not None

    @property
    def timed_out(self):
        return self.elapsed > self._row["timeout"]

    @property
    def result(self):
        assert self.ready
        if self._result == NOT_DONE:
            self._result = self._pipe.recv()
        return self._result

    # mirrors Process interface
    def is_alive(self):
        return self._process.is_alive()

    def terminate(self):
        return self._process.terminate()

    def kill(self):
        return self._process.kill()

    def close(self):
        return self._process.close()

    @property
    def pid(self):
        return self._process.pid

    @property
    def exitcode(self):
        return self._process.exitcode


def complete_jobs(jobs):
    for job_id, job in list(jobs.items()):
        if job.ready:
            jobs.pop(job_id)
            result = job.result
            result["exitcode"] = job.exitcode if not result["exc"] else 1
            func = "fail_job" if result["exc"] else "finish_job"
            db.fetchone(f"select * from {func}(%s, %s)", (job_id, result))
            db.commit()
        elif job.timed_out:
            jobs.pop(job_id)

            job.terminate()
            time.sleep(3)
            if job.is_alive():
                job.kill()
                time.sleep(1)

            result = {"result": None, "exc": "timed out", "exitcode": job.exitcode}

            try:
                job.close()
            except ValueError:
                print(f"Process {job.pid} not dead?")

            db.fetchone("select * from fail_job(%s, %s)", (job_id, result))
            db.commit()


def main(args):
    multiprocessing.set_start_method("spawn")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-q",
        "--queues",
        type=str,
        help="Queues to consume, comma separated list",
        default="default",
    )
    parser.add_argument(
        "-p",
        "--processes",
        type=int,
        help="Max number of subprocesses running jobs",
        default=4,
    )
    parser.add_argument(
        "-i",
        "--worker_id",
        type=str,
        help="Worker id",
        default=socket.gethostname(),
    )
    args = parser.parse_args()

    queues = args.queues.split(',')
    procs = args.processes
    worker_id = args.worker_id

    jobs = {}

    # there are two postgres connections, one here for listen, and one held in
    # a thread-local in db.py
    with db.connect() as conn:
        conn.notifies = deque()
        with conn.cursor() as cursor:
            cursor.execute("LISTEN job;")
            conn.commit()

            it = 0
            while 1:
                it += 1

                if it % 10 == 1:
                    # check for "lost" jobs, started with our worker_id, but
                    # not in our jobs registry
                    rows = db.fetchall("select * from release_lost_jobs(%s, %s, %s)", (worker_id, queues, list(jobs)))
                    if rows:
                        db.commit()
                        for row in rows:
                            print(f"Releasing lost job: {dict(row)}")

                complete_jobs(jobs)

                if select.select([conn], [], [], 1) != ([], [], []):
                    conn.poll()
                    while conn.notifies:
                        notify = conn.notifies.popleft()
                        print("Notify:", notify.pid, notify.channel, notify.payload)

                        queue, job_id = notify.payload.split()

                        if queue in queues and job_id not in jobs and len(jobs) < procs:
                            row = db.fetchone("select * from reserve_job(%s, %s)", (job_id, worker_id))
                            if row:
                                print(f"Starting {dict(row)}")
                                jobs[job_id] = Job(row)
                                db.commit()
                else:
                    n = procs - len(jobs)
                    if n > 0:
                        q = "select * from reserve_jobs_from_queues(%s, %s, %s)"
                        for row in db.fetchall(q, (queues, worker_id, n)):
                            assert not row["id"] in jobs, row
                            print(f"Starting {dict(row)}")
                            jobs[row["id"]] = Job(row)
                        db.commit()


if __name__ == "__main__":
    main(sys.argv[1:])
