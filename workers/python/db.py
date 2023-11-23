import json
import logging
import os
import threading

import psycopg2
import psycopg2.extensions
import psycopg2.extras
from psycopg2 import OperationalError, ProgrammingError
from psycopg2.errors import UniqueViolation  # noqa

ctx = threading.local()

psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)

log = logging.getLogger(__name__)


UNSET = "__UNSET__"

def connect(**opts):
    conf = {}
    conf["host"] = os.environ['PGHOST']
    conf["port"] = int(os.environ['PGPORT'])
    conf["database"] = os.environ['PGDATABASE']
    conf["user"] = os.environ['PGUSER']
    conf["password"] = os.environ['PGPASSWORD']
    conf["cursor_factory"] = psycopg2.extras.RealDictCursor
    conf.update(**opts)
    return psycopg2.connect(**conf)


def _conn_key(connect_opts=None):
    connect_opts = connect_opts or {}
    return hash(tuple(sorted(connect_opts)))


def _get_ctx_connection(connect_opts=None, create=True):
    connect_opts = connect_opts or {}
    key = _conn_key(connect_opts)
    if not hasattr(ctx, "pgconns"):
        ctx.pgconns = {}
    conn = ctx.pgconns.get(key)
    if (not conn and create) or (conn and conn.closed):
        ctx.pgconns[key] = conn = connect(**connect_opts)
    return conn


def query(sql, params=None, conn=None, connect_opts=None, cursor_name=None):
    if not conn:
        conn = _get_ctx_connection(connect_opts)

    with conn.cursor(cursor_name) as cursor:
        cursor.execute(sql, params)
        try:
            for row in cursor:
                yield row
        except ProgrammingError:
            return

def fetchall(sql, params=None, conn=None, connect_opts=None):
    return list(query(sql, params, conn, connect_opts))


def fetchone(*args, **kwargs):
    L = fetchall(*args, **kwargs)
    assert len(L) <= 1, L
    return L[0] if L else None


def commit(connect_opts=None):
    """commit on the current thread-local connection"""
    conn = _get_ctx_connection(connect_opts, create=False)
    if conn:
        log.debug("COMMIT %r", conn)
        conn.commit()


def rollback(connect_opts=None):
    """rollback on the current thread-local connection"""
    conn = _get_ctx_connection(connect_opts, create=False)
    if conn:
        log.debug("ROLLBACK %r", conn)
        conn.rollback()
