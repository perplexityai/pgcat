"""The prepared statement cache answers a Parse the server already has, and a
Close of a named statement, without the server. Those replies must reach the
client where Postgres would have sent them: after the replies to the batch's
earlier messages, and not at all once an earlier message failed. pgx's
pipelined batches match replies to messages by order."""

import os
import struct
import uuid

import pytest

import utils
from utils import frontend

PORT = 6454
PG_PORT = int(os.environ.get("PGCAT_TEST_PG_PORT", "5432"))
DB = "reply_order_db"

CONFIG = f"""
[general]
host = "127.0.0.1"
port = {PORT}
admin_username = "admin_user"
admin_password = "admin_pass"

[pools.{DB}]
pool_mode = "transaction"
prepared_statements_cache_size = 100

[pools.{DB}.users.0]
username = "sharding_user"
password = "sharding_user"
pool_size = 1
statement_timeout = 0

[pools.{DB}.shards.0]
servers = [["127.0.0.1", {PG_PORT}, "primary"]]
database = "shard0"
"""


@pytest.fixture(scope="module")
def pgcat():
    process = utils.PgcatProcess(CONFIG, PORT).start()
    yield process
    process.stop()


def connect():
    sock, message = utils.raw_connection(PORT, DB)
    sock.settimeout(10)
    return sock, message


def parse(name, sql):
    return frontend(b"P", name.encode() + b"\0" + sql.encode() + b"\0" + struct.pack("!h", 0))


def bind(statement):
    return frontend(b"B", b"\0" + statement.encode() + b"\0" + struct.pack("!hhh", 0, 0, 0))


def execute():
    return frontend(b"E", b"\0" + struct.pack("!i", 0))


def close_statement(name):
    return frontend(b"C", b"S" + name.encode() + b"\0")


SYNC = frontend(b"S", b"")
FLUSH = frontend(b"H", b"")


def codes(message, count=None):
    """Reply codes up to and including ReadyForQuery, or `count` of them."""
    seen = []
    while count is None or len(seen) < count:
        code, _ = message()
        seen.append(code.decode())
        if code == b"Z":
            break
    return "".join(seen)


def unique_sql():
    return f"SELECT '{uuid.uuid4()}'"


def cached(sql):
    """Prepare `sql` from another client, so the server connection has it."""
    sock, message = connect()
    sock.sendall(parse("warm", sql) + SYNC)
    assert codes(message) == "1Z"
    sock.close()


def test_cached_parse_answered_after_earlier_replies(pgcat):
    old = unique_sql()
    cached(old)
    sock, message = connect()
    sock.sendall(
        parse("new", unique_sql())
        + bind("new")
        + execute()
        + parse("old", old)
        + bind("old")
        + execute()
        + SYNC
    )
    assert codes(message) == "12DC12DCZ"
    sock.close()


def test_cached_parse_after_error_not_answered(pgcat):
    old = unique_sql()
    cached(old)
    sock, message = connect()
    # Postgres folds 1/0 when it plans the statement: the Bind fails.
    sock.sendall(parse("fails", "SELECT 1/0") + bind("fails") + execute() + parse("old", old) + SYNC)
    assert codes(message) == "1EZ"

    # The connection is in step for the next batch.
    sock.sendall(parse("next", unique_sql()) + bind("next") + execute() + SYNC)
    assert codes(message) == "12DCZ"
    sock.close()


def test_cached_parse_answered_in_order_at_flush(pgcat):
    old = unique_sql()
    cached(old)
    sock, message = connect()
    sock.sendall(parse("new", unique_sql()) + bind("new") + execute() + parse("old", old) + FLUSH)
    assert codes(message, 5) == "12DC1"
    sock.sendall(bind("old") + execute() + SYNC)
    assert codes(message) == "2DCZ"
    sock.close()


def test_close_answered_after_earlier_replies(pgcat):
    sock, message = connect()
    sock.sendall(parse("s", unique_sql()) + SYNC)
    assert codes(message) == "1Z"
    sock.sendall(bind("s") + execute() + close_statement("s") + SYNC)
    assert codes(message) == "2DC3Z"
    sock.close()
