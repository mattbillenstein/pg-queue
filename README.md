# pg-queue

This is a simple task queue built on postgres listen/notify and "FOR UPDATE
SKIP LOCKED"

See comments in pg-queue.sql

## Quickstart

```
# install / setup postgresql
cp pg-example.env pg.env
# edit pg.env
source pg.env
psql < pg-queue.sql

cd workers/python
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
./worker.py

# Then you can put jobs in the queue by running:
./test.py

# monitor the queue
while true; do
  clear
  psql -c 'select id, created_at, started_at, tries, timeout, worker_id, result from job order by created_at desc limit 10' | cat
  psql -c "select * from queue_size('default')" | cat
  sleep 2
done
```
