"""The exporter's /ready probe: 503 until every pool has opened its
min_pool_size connections, 200 after, and 503 once SIGINT starts draining."""

import os
import signal
import time
import urllib.error
import urllib.request

import psycopg

import utils

PORT = 6443
METRICS_PORT = 9943
# The Postgres the pools connect to (the repo fixtures use 5432).
PG_PORT = int(os.environ.get("PGCAT_TEST_PG_PORT", "5432"))
USER = "sharding_user"


def config(pg_port):
    return f"""
[general]
host = "127.0.0.1"
port = {PORT}
admin_username = "admin_user"
admin_password = "admin_pass"
enable_prometheus_exporter = true
prometheus_exporter_port = {METRICS_PORT}
# Start even when the server is unreachable.
validate_config = false

[pools.shard0.users.0]
username = "{USER}"
password = "{USER}"
pool_size = 5
min_pool_size = 2

[pools.shard0.shards.0]
servers = [["127.0.0.1", {pg_port}, "primary"]]
database = "shard0"
"""


def ready_status():
    """/ready's status code, retried while the exporter starts listening."""
    deadline = time.time() + 10
    while True:
        try:
            url = f"http://127.0.0.1:{METRICS_PORT}/ready"
            return urllib.request.urlopen(url, timeout=1).status
        except urllib.error.HTTPError as error:
            return error.code
        except OSError:
            if time.time() > deadline:
                raise
            time.sleep(0.1)


def wait_for_status(expected):
    deadline = time.time() + 10
    while ready_status() != expected and time.time() < deadline:
        time.sleep(0.1)
    assert ready_status() == expected


def test_not_ready_while_pools_lack_min_connections():
    # Nothing listens on port 1, so the pool never opens a connection.
    pgcat = utils.PgcatProcess(config(pg_port=1), PORT).start()
    try:
        time.sleep(1)
        assert ready_status() == 503
    finally:
        pgcat.stop()


def test_ready_once_warm_and_not_ready_once_draining():
    pgcat = utils.PgcatProcess(config(PG_PORT), PORT).start()
    try:
        wait_for_status(200)

        # Shutdown closes idle clients but waits for open transactions, so
        # this keeps pgcat draining instead of exiting.
        with psycopg.connect(pgcat.dsn("shard0"), autocommit=True) as conn:
            conn.execute("BEGIN")
            pgcat.process.send_signal(signal.SIGINT)
            wait_for_status(503)
    finally:
        pgcat.stop()
