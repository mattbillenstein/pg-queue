-- Prototype task queue implemented on Postgres using listen/notify and FOR
-- UPDATE SKIP LOCKED
--
-- Attempting to roughly follow the semantics of beanstalkd, see:
--   https://github.com/beanstalkd/beanstalkd/blob/master/doc/protocol.txt
--
-- Job states therefore derived from various fields on the job:
--   DELAYED:   started_at is null and now() <  delayed_until and tries > 0
--   READY:     started_at is null and now() >= delayed_until and tries > 0
--   RESERVED:  started_at not null and result null
--   FINISHED:  ended_at != null and result != null
--   FAILED:    started_at is null and tries == 0  (this is BURIED in beanstalkd)
--
--   Jobs can also be LOST (ie RESERVED) having started on a worker, but the
--   worker crashed. From a new worker process with the same id, we can release
--   these jobs back to the queue.
--
-- Some other interesting things I found along the way for inspiration:
--   https://news.ycombinator.com/item?id=20020501
--   https://chbussler.medium.com/implementing-queues-in-postgresql-3f6e9ab724fa
--   https://adriano.fyi/posts/2023-09-24-choose-postgres-queue-technology/
--
-- TODO: cron job to cleanup buried and older succeeded jobs? Indexes?

DROP TABLE IF EXISTS job CASCADE;

CREATE TABLE job (
    id TEXT PRIMARY KEY DEFAULT gen_random_uuid(),  -- job id
    queue TEXT NOT NULL DEFAULT 'default',          -- job queue
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),    -- job created time
    delayed_until TIMESTAMP NOT NULL DEFAULT NOW(), -- job is delayed to start until this time
    started_at TIMESTAMP,                           -- job started time
    ended_at TIMESTAMP,                             -- job ended time, set on success or exception
    tries INTEGER DEFAULT 1,                        -- number of tries, including first run
    retry_delay INTEGER DEFAULT 10,                 -- retry delay in seconds
    timeout INTEGER DEFAULT 3600,                   -- job timeout in seconds
    worker_id TEXT,                                 -- worker-id used to detect "lost" jobs
    payload jsonb,                                  -- job payload {"func": "mod.func", "args": [...], "kwargs": {...}}
    result jsonb                                    -- job result {"result": ..., "exc": ...} only one will be set
);

DROP FUNCTION IF EXISTS notify_job_insert;
CREATE FUNCTION notify_job_insert() RETURNS TRIGGER AS $$
  DECLARE
  output TEXT;
BEGIN
  output = NEW.queue || ' ' || NEW.id;
  PERFORM pg_notify('job', output);
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER on_job_insert AFTER INSERT ON job
  FOR EACH ROW EXECUTE PROCEDURE notify_job_insert();

CREATE FUNCTION reserve_jobs_from_queues(queues TEXT[], worker_id TEXT, num INT) RETURNS SETOF job AS $$
UPDATE job set started_at=now(), worker_id=$2, result=null
where id in (
  SELECT ID FROM job
  WHERE queue=ANY($1) and now() >= delayed_until and started_at is null and tries > 0
  FOR UPDATE SKIP LOCKED
  LIMIT $3
)
RETURNING *
$$ LANGUAGE SQL;

CREATE FUNCTION reserve_job(id TEXT, worker_id TEXT) RETURNS SETOF job AS $$
UPDATE job set started_at=now(), worker_id=$2, result=null
where id=(
  SELECT id FROM job
  WHERE id=$1 and now() > delayed_until and started_at is null and tries > 0
  FOR UPDATE SKIP LOCKED
  LIMIT 1
)
RETURNING *
$$ LANGUAGE SQL;

CREATE FUNCTION finish_job(id TEXT, result jsonb) RETURNS SETOF job AS $$
UPDATE job set result=$2, ended_at=now()
WHERE id=$1
RETURNING *
$$ LANGUAGE SQL;

CREATE FUNCTION fail_job(id TEXT, result jsonb) RETURNS SETOF job AS $$
UPDATE job set result=$2, started_at=null, tries=tries-1, delayed_until=now() + interval '1 second' * retry_delay, worker_id=null
WHERE id=$1
RETURNING *
$$ LANGUAGE SQL;

CREATE FUNCTION release_job(id TEXT, delay INT = 0) RETURNS SETOF job AS $$
UPDATE job set result=null, started_at=null, worker_id=null, delayed_until = now() + interval '1 second' * $2
WHERE id=$1
RETURNING *
$$ LANGUAGE SQL;

CREATE FUNCTION release_lost_jobs(worker_id TEXT, queues TEXT[], claimed_job_ids TEXT[]) RETURNS SETOF job AS $$
UPDATE job set result=null, started_at=null, worker_id=null
WHERE worker_id=$1 and queue=ANY($2) and not id=ANY($3)
RETURNING *
$$ LANGUAGE SQL;

DROP FUNCTION IF EXISTS queue_size;
CREATE FUNCTION queue_size(queue TEXT)
RETURNS TABLE (state TEXT, cnt BIGINT) AS $$
SELECT
  CASE
    WHEN started_at is null and now() <  delayed_until and tries > 0 THEN 'DELAYED'
    WHEN started_at is null and now() >= delayed_until and tries > 0 THEN 'READY'
    WHEN started_at is not null and result is null THEN 'RESERVED'
    WHEN ended_at   is not null and result is not null THEN 'FINISHED'
    WHEN started_at is null and tries <= 0 THEN 'FAILED'
    ELSE 'UNKNOWN'
  END,
  COUNT(*)
FROM job
WHERE queue=$1
GROUP BY 1
ORDER BY 1
$$ LANGUAGE SQL;
