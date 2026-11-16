"""A batch that closes a named prepared statement and parses the name again,
[Close s, Parse s, ..., Sync], leaves s as the new statement, as on Postgres.
Drivers that pipeline do this when they replace a statement. With the
prepared statement cache on, the client's map of statements takes the
batch's Parses and Closes in the batch's order; a refused batch takes back
both. Each case runs with the cache on and, as the control, off (in a
session pool, which keeps the server's own statements between batches)."""

import os
import struct

import pytest

import utils
from utils import frontend

PORT = 6471
PG_PORT = int(os.environ.get("PGCAT_TEST_PG_PORT", "5432"))
CACHED = "close_reparse_db"
UNCACHED = "close_reparse_no_cache_db"


def pool(name, mode, cache_size):
    return f"""
[pools.{name}]
pool_mode = "{mode}"
prepared_statements_cache_size = {cache_size}

[pools.{name}.users.0]
username = "sharding_user"
password = "sharding_user"
pool_size = 1
statement_timeout = 0

[pools.{name}.shards.0]
servers = [["127.0.0.1", {PG_PORT}, "primary"]]
database = "shard0"
"""


CONFIG = (
    f"""
[general]
host = "127.0.0.1"
port = {PORT}
admin_username = "admin_user"
admin_password = "admin_pass"
"""
    + pool(CACHED, "transaction", 100)
    # The control: Postgres's own statements, kept between transactions.
    + pool(UNCACHED, "session", 0)
)

POOLS = pytest.mark.parametrize("db", [CACHED, UNCACHED], ids=["cache", "no-cache"])


@pytest.fixture(scope="module")
def pgcat():
    process = utils.PgcatProcess(CONFIG, PORT).start()
    yield process
    process.stop()


def connect(db):
    sock, message = utils.raw_connection(PORT, db)
    sock.settimeout(10)
    return sock, message


def parse(name, sql):
    return frontend(b"P", name.encode() + b"\0" + sql.encode() + b"\0" + struct.pack("!h", 0))


def bind(statement):
    return frontend(b"B", b"\0" + statement.encode() + b"\0" + struct.pack("!hhh", 0, 0, 0))


def execute():
    return frontend(b"E", b"\0" + struct.pack("!i", 0))


def describe_statement(name):
    return frontend(b"D", b"S" + name.encode() + b"\0")


def close_statement(name):
    return frontend(b"C", b"S" + name.encode() + b"\0")


SYNC = frontend(b"S", b"")

A = "SELECT 'A'::text"
B = "SELECT 'B'::text"
# Answered by pgcat, so a batch with server statements in it is refused.
LOCAL = "SHOW PGCAT_IDENTITY"


def answer(message):
    """The errors and the first column of the rows, up to ReadyForQuery.
    Not the order of the other replies: the cache answers some itself."""
    errors, rows = [], []
    while True:
        code, body = message()
        if code == b"E":
            errors.append(body)
        elif code == b"D":
            (length,) = struct.unpack("!i", body[2:6])
            rows.append(body[6 : 6 + length].decode())
        elif code == b"Z":
            return errors, rows


def ok(message):
    errors, rows = answer(message)
    assert errors == [], errors
    return rows


def runs(sock, message, name):
    """What `name` runs now, by a Bind and Execute in a batch of their own."""
    sock.sendall(bind(name) + execute() + SYNC)
    return ok(message)


def prepared(db, sql=A, name="s"):
    sock, message = connect(db)
    sock.sendall(parse(name, sql) + SYNC)
    ok(message)
    return sock, message


@POOLS
def test_close_then_parse_in_one_batch(pgcat, db):
    sock, message = prepared(db)
    try:
        sock.sendall(close_statement("s") + parse("s", B) + SYNC)
        ok(message)
        assert runs(sock, message, "s") == ["B"]
        # Still the client's: the name holds through more batches.
        assert runs(sock, message, "s") == ["B"]
    finally:
        sock.close()


@POOLS
def test_close_parse_bind_in_one_batch(pgcat, db):
    sock, message = prepared(db)
    try:
        sock.sendall(close_statement("s") + parse("s", B) + bind("s") + execute() + SYNC)
        assert ok(message) == ["B"]
        assert runs(sock, message, "s") == ["B"]
    finally:
        sock.close()


@POOLS
def test_a_bind_before_the_close_runs_the_statement_it_bound(pgcat, db):
    sock, message = prepared(db)
    try:
        sock.sendall(bind("s") + execute() + close_statement("s") + SYNC)
        assert ok(message) == ["A"]
        sock.sendall(parse("s", B) + SYNC)
        ok(message)
        sock.sendall(
            bind("s")
            + execute()
            + close_statement("s")
            + parse("s", A)
            + bind("s")
            + execute()
            + SYNC
        )
        assert ok(message) == ["B", "A"]
        assert runs(sock, message, "s") == ["A"]
    finally:
        sock.close()


@POOLS
def test_a_describe_before_the_close_describes_the_statement(pgcat, db):
    sock, message = prepared(db)
    try:
        sock.sendall(describe_statement("s") + close_statement("s") + SYNC)
        codes = []
        while not codes or codes[-1] != b"Z":
            code, body = message()
            assert code != b"E", body
            codes.append(code)
        assert b"t" in codes and b"T" in codes, codes
    finally:
        sock.close()


@POOLS
@pytest.mark.parametrize(
    "batch",
    [close_statement("s"), close_statement("s") + parse("s", B)],
    ids=["close", "close-parse"],
)
def test_a_refused_batch_leaves_the_statement(pgcat, db, batch):
    sock, message = prepared(db)
    try:
        sock.sendall(batch + parse("local", LOCAL) + SYNC)
        errors, _ = answer(message)
        assert len(errors) == 1 and b"own Sync batch" in errors[0], errors
        assert runs(sock, message, "s") == ["A"]
    finally:
        sock.close()


@POOLS
def test_a_batch_failing_after_the_close_and_parse_keeps_them(pgcat, db):
    sock, message = prepared(db)
    try:
        # Postgres folds 1/0 when it plans the statement: the Bind fails.
        sock.sendall(
            close_statement("s")
            + parse("s", B)
            + parse("fails", "SELECT 1/0")
            + bind("fails")
            + execute()
            + SYNC
        )
        errors, _ = answer(message)
        assert len(errors) == 1 and b"22012" in errors[0], errors
        assert runs(sock, message, "s") == ["B"]
    finally:
        sock.close()


@POOLS
def test_a_batch_failing_before_the_close_and_parse_treats_them_as_a_parse(pgcat, db):
    # Postgres skips what follows the error. Whatever a pool makes of a Parse
    # it sent after one, a Close before that Parse doesn't change it.
    def after_a_failure(batch):
        sock, message = prepared(db)
        try:
            sock.sendall(parse("fails", "SELECT 1/0") + bind("fails") + execute() + batch + SYNC)
            errors, _ = answer(message)
            assert len(errors) == 1 and b"22012" in errors[0], errors
            return runs(sock, message, "s")
        finally:
            sock.close()

    assert after_a_failure(close_statement("s") + parse("s", B)) == after_a_failure(parse("s", B))


@POOLS
def test_a_close_removes_the_statement(pgcat, db):
    sock, message = prepared(db)
    try:
        sock.sendall(close_statement("s") + SYNC)
        ok(message)
        sock.sendall(bind("s") + execute() + SYNC)
        code, body = message()
        assert code == b"E" and b'prepared statement "s" does not exist' in body, (code, body)
    finally:
        sock.close()


@POOLS
def test_a_close_removes_the_unnamed_statement(pgcat, db):
    sock, message = prepared(db, name="")
    try:
        sock.sendall(close_statement("") + SYNC)
        ok(message)
        sock.sendall(bind("") + execute() + SYNC)
        code, body = message()
        assert code == b"E" and b"does not exist" in body, (code, body)
    finally:
        sock.close()
