"""COPY through pgcat with the query parser and plugins on.

The psql scenario tests/psql/copy.sql covers COPY's sub-protocol with no
plugins: errors before and after CopyDone, transactions, COPY TO. Here: the
plugins see COPY statements and must refuse them as they refuse any other
statement on a forbidden table; COPY by extended protocol, including next to
a statement the prepared statement cache answers; CopyFail, and a client gone
mid-COPY; binary COPY from asyncpg; and the server connection after each, for
the next client."""

import asyncio
import io
import os
import struct

import asyncpg
import psycopg2
import pytest

import utils
from utils import frontend

pytestmark = pytest.mark.skipif(
    os.environ.get("PGCAT_TEST_PLUGINS") != "1",
    reason="needs pgcat built with --features test-plugins and PGCAT_TEST_PLUGINS=1",
)

PORT = 6456
PG_PORT = int(os.environ.get("PGCAT_TEST_PG_PORT", "5432"))
DB = "copy_db"
ROWS = "copy_rows"
FORBIDDEN = "copy_forbidden"

CONFIG = f"""
[general]
host = "127.0.0.1"
port = {PORT}
admin_username = "admin_user"
admin_password = "admin_pass"

[pools.{DB}]
pool_mode = "transaction"
prepared_statements_cache_size = 100
query_parser_enabled = true
plugins_require_parse = true

[pools.{DB}.plugins.testing]
enabled = true

[pools.{DB}.plugins.table_access]
enabled = true
tables = ["{FORBIDDEN}"]

[pools.{DB}.users.0]
username = "sharding_user"
password = "sharding_user"
pool_size = 1
statement_timeout = 0

[pools.{DB}.shards.0]
servers = [["127.0.0.1", {PG_PORT}, "primary"]]
database = "shard0"
"""


def postgres():
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=PG_PORT,
        user="sharding_user",
        password="sharding_user",
        dbname="shard0",
        # A server connection pgcat left mid-COPY holds its table's lock.
        options="-c lock_timeout=5000",
    )
    conn.autocommit = True
    return conn


@pytest.fixture(scope="module")
def pgcat():
    with postgres() as conn, conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {ROWS}, {FORBIDDEN}")
        cur.execute(f"CREATE TABLE {ROWS} (id int, note text)")
        cur.execute(f"CREATE TABLE {FORBIDDEN} (secret text)")
        cur.execute(f"INSERT INTO {FORBIDDEN} VALUES ('do not copy')")
    process = utils.PgcatProcess(CONFIG, PORT).start()
    yield process
    process.stop()
    with postgres() as conn, conn.cursor() as cur:
        cur.execute(f"DROP TABLE {ROWS}, {FORBIDDEN}")


@pytest.fixture
def rows():
    """An empty copy_rows, emptied again after the test."""
    with postgres() as conn, conn.cursor() as cur:
        cur.execute(f"TRUNCATE {ROWS}")
    yield
    with postgres() as conn, conn.cursor() as cur:
        cur.execute(f"TRUNCATE {ROWS}")


def stored():
    with postgres() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT id, note FROM {ROWS} ORDER BY id")
        return cur.fetchall()


def raw():
    sock, message = utils.raw_connection(PORT, DB)
    sock.settimeout(10)
    return sock, message


def query(sql):
    return frontend(b"Q", sql.encode() + b"\0")


def parse(name, sql):
    return frontend(b"P", name.encode() + b"\0" + sql.encode() + b"\0" + struct.pack("!h", 0))


def bind(statement):
    return frontend(b"B", b"\0" + statement.encode() + b"\0" + struct.pack("!hhh", 0, 0, 0))


EXECUTE = frontend(b"E", b"\0" + struct.pack("!i", 0))
SYNC = frontend(b"S", b"")
COPY_DONE = frontend(b"c", b"")


def copy_data(data):
    return frontend(b"d", data)


def replies(message):
    seen = []
    while not seen or seen[-1][0] != b"Z":
        seen.append(message())
    return seen


def codes(seen):
    return "".join(code.decode() for code, _ in seen)


def sqlstate(body):
    fields = dict((field[:1], field[1:]) for field in body.split(b"\0") if field)
    return fields[b"C"].decode()


def still_works(message, sock):
    sock.sendall(query("SELECT 'next'"))
    assert codes(replies(message)) == "TDCZ"


@pytest.mark.parametrize(
    "sql, refused_with",
    [
        (f"COPY {FORBIDDEN} TO STDOUT", "42501"),
        (f"COPY public.{FORBIDDEN} (secret) TO STDOUT", "42501"),
        # Postgres folds unquoted names to lower case.
        (f"COPY {FORBIDDEN.upper()} TO STDOUT", "42501"),
        (f'COPY "{FORBIDDEN}" TO STDOUT', "42501"),
        (f"COPY (SELECT secret FROM {FORBIDDEN}) TO STDOUT", "42501"),
        # The parser can't read COPY ... FROM STDIN. Its text names a table a
        # plugin checks, so it's refused unread.
        (f"COPY {FORBIDDEN} FROM STDIN", "0A000"),
    ],
)
def test_copy_of_a_forbidden_table_is_refused(pgcat, sql, refused_with):
    sock, message = raw()
    sock.sendall(query(sql))
    seen = replies(message)
    assert codes(seen) == "EZ", seen
    assert sqlstate(seen[0][1]) == refused_with
    still_works(message, sock)
    sock.close()


def test_copy_in_and_out_across_server_chunks(pgcat, rows):
    # Rows cut across CopyData messages, and far more than a server chunk back.
    data = "".join(f"{i}\t{'x' * (i % 97)}\n" for i in range(1, 3001))
    with psycopg2.connect(pgcat.dsn(DB)) as conn, conn.cursor() as cur:
        cur.copy_expert(f"COPY {ROWS} FROM STDIN", io.StringIO(data), size=1000)
        through_pgcat = io.StringIO()
        cur.copy_expert(f"COPY {ROWS} TO STDOUT", through_pgcat)
    with postgres() as conn, conn.cursor() as cur:
        direct = io.StringIO()
        cur.copy_expert(f"COPY {ROWS} TO STDOUT", direct)
    assert through_pgcat.getvalue() == direct.getvalue()
    assert sorted(direct.getvalue().splitlines()) == sorted(data.splitlines())


def test_copy_fail_leaves_the_server_for_the_next_client(pgcat, rows):
    sock, message = raw()
    sock.sendall(query(f"COPY {ROWS} FROM STDIN"))
    assert message()[0] == b"G"
    sock.sendall(copy_data(b"1\tkept?\n") + frontend(b"f", b"client gave up\0"))
    seen = replies(message)
    assert codes(seen) == "EZ"
    assert sqlstate(seen[0][1]) == "57014"
    assert seen[-1][1] == b"I"
    sock.close()

    # The pool's only server connection, in step, with nothing copied.
    sock, message = raw()
    still_works(message, sock)
    sock.close()
    assert stored() == []


def test_client_gone_mid_copy_leaves_the_server_for_the_next_client(pgcat, rows):
    sock, message = raw()
    sock.sendall(query(f"COPY {ROWS} FROM STDIN"))
    assert message()[0] == b"G"
    sock.sendall(copy_data(b"1\tleft behind\n"))
    sock.close()

    # The server connection was dropped, not handed over still in COPY.
    sock, message = raw()
    still_works(message, sock)
    sock.close()
    assert stored() == []


def test_copy_in_by_extended_protocol(pgcat, rows):
    sock, message = raw()
    # The server ignores a Sync in copy-in mode: as libpq does, the client
    # syncs again after CopyDone, and the server answers that Sync.
    sock.sendall(parse("", f"COPY {ROWS} FROM STDIN") + bind("") + EXECUTE + SYNC)
    assert codes([message(), message(), message()]) == "12G"
    sock.sendall(copy_data(b"1\tone\n2\tt") + copy_data(b"wo\n") + COPY_DONE + SYNC)
    seen = replies(message)
    assert codes(seen) == "CZ"
    assert seen[0][1] == b"COPY 2\0"
    still_works(message, sock)
    sock.close()
    assert stored() == [(1, "one"), (2, "two")]


def test_copy_by_simple_query_after_an_extended_one(pgcat, rows):
    sock, message = raw()
    sock.sendall(parse("", f"COPY {ROWS} FROM STDIN") + bind("") + EXECUTE + SYNC)
    assert codes([message(), message(), message()]) == "12G"
    sock.sendall(copy_data(b"1\tone\n") + COPY_DONE + SYNC)
    assert codes(replies(message)) == "CZ"

    sock.sendall(query(f"COPY {ROWS} FROM STDIN"))
    assert message()[0] == b"G"
    sock.sendall(copy_data(b"2\ttwo\n") + COPY_DONE)
    assert codes(replies(message)) == "CZ"
    sock.close()
    assert stored() == [(1, "one"), (2, "two")]


@pytest.mark.parametrize(
    "end, then",
    [
        (COPY_DONE, query("SELECT 1")),
        (COPY_DONE, frontend(b"H", b"")),
        (COPY_DONE, query(f"SELECT * FROM {FORBIDDEN}")),
        # Postgres runs it in the COPY's batch, and rolls the rows back.
        (COPY_DONE, parse("", "SELECT 1/0") + bind("") + EXECUTE),
        # Postgres skips it, as it skips anything after an error until Sync.
        (
            frontend(b"f", b"client gave up\0"),
            parse("", f"INSERT INTO {ROWS} VALUES (2, 'after CopyFail')") + bind("") + EXECUTE,
        ),
    ],
    ids=["query", "flush", "refused-query", "failing-statement", "insert-after-copy-fail"],
)
def test_extended_copy_must_end_with_sync(pgcat, rows, end, then):
    sock, message = raw()
    sock.sendall(parse("", f"COPY {ROWS} FROM STDIN") + bind("") + EXECUTE + SYNC)
    assert codes([message(), message(), message()]) == "12G"
    sock.sendall(copy_data(b"1\tone\n") + end + then + SYNC)
    code, body = message()
    assert code == b"E", (code, body)
    assert sqlstate(body) == "0A000"
    with pytest.raises((ConnectionError, OSError)):
        while True:
            message()
    sock.close()

    # Nothing of the batch stands, and the next client gets a server in step.
    sock, message = raw()
    still_works(message, sock)
    sock.close()
    assert stored() == []


def test_copy_fail_by_extended_protocol(pgcat, rows):
    sock, message = raw()
    sock.sendall(parse("", f"COPY {ROWS} FROM STDIN") + bind("") + EXECUTE + SYNC)
    assert codes([message(), message(), message()]) == "12G"
    sock.sendall(copy_data(b"1\tone\n") + frontend(b"f", b"client gave up\0") + SYNC)
    seen = replies(message)
    assert codes(seen) == "EZ"
    assert sqlstate(seen[0][1]) == "57014"
    still_works(message, sock)
    sock.close()
    assert stored() == []


def test_copy_out_by_extended_protocol_beside_a_cached_statement(pgcat):
    cached = "SELECT 'cached by another client'"
    sock, message = raw()
    sock.sendall(parse("warm", cached) + SYNC)
    assert codes(replies(message)) == "1Z"
    sock.close()

    sock, message = raw()
    sock.sendall(
        parse("out", "COPY (SELECT g FROM generate_series(1, 3) g) TO STDOUT")
        + bind("out")
        + EXECUTE
        + parse("cached", cached)
        + SYNC
    )
    seen = replies(message)
    assert codes(seen) == "12HdddcC1Z"
    assert [body for code, body in seen if code == b"d"] == [b"1\n", b"2\n", b"3\n"]
    sock.close()


def test_binary_copy_from_asyncpg(pgcat, rows):
    async def run():
        conn = await asyncpg.connect(pgcat.dsn(DB))
        try:
            await conn.copy_records_to_table(ROWS, records=[(i, f"row {i}") for i in range(500)])
            return await conn.fetchval(f"SELECT count(*) FROM {ROWS}")
        finally:
            await conn.close()

    assert asyncio.run(run()) == 500
    assert stored()[:2] == [(0, "row 0"), (1, "row 1")]


STALL_PORT = 6461


def stall_config(idle_timeout, statement_timeout):
    return f"""
[general]
host = "127.0.0.1"
port = {STALL_PORT}
admin_username = "admin_user"
admin_password = "admin_pass"
idle_client_in_transaction_timeout = {idle_timeout}

[pools.{DB}]
pool_mode = "transaction"

[pools.{DB}.users.0]
username = "sharding_user"
password = "sharding_user"
pool_size = 1
statement_timeout = {statement_timeout}

[pools.{DB}.shards.0]
servers = [["127.0.0.1", {PG_PORT}, "primary"]]
database = "shard0"
"""


@pytest.fixture(scope="module", params=[(300, 0), (0, 300)], ids=["idle-timeout", "statement-timeout"])
def stalling(request, pgcat):
    # The tables are made by the pgcat fixture.
    process = utils.PgcatProcess(stall_config(*request.param), STALL_PORT).start()
    yield process
    process.stop()


@pytest.mark.parametrize("sync", [b"", SYNC[:3]], ids=["no-sync", "partial-sync"])
@pytest.mark.parametrize("end", [COPY_DONE, frontend(b"f", b"client gave up\0")], ids=["copy-done", "copy-fail"])
def test_stalled_end_of_extended_copy_times_out(stalling, rows, end, sync):
    sock, message = utils.raw_connection(STALL_PORT, DB)
    sock.settimeout(5)
    sock.sendall(parse("", f"COPY {ROWS} FROM STDIN") + bind("") + EXECUTE + SYNC)
    assert codes([message(), message(), message()]) == "12G"
    # The client stays connected, its Sync missing or cut short.
    sock.sendall(copy_data(b"1\tone\n") + end + sync)
    code, body = message()
    assert code == b"E", (code, body)
    assert b"timeout" in body
    with pytest.raises((ConnectionError, OSError)):
        while True:
            message()
    sock.close()

    # The COPY was rolled back, and the next client gets a server in step.
    sock, message = utils.raw_connection(STALL_PORT, DB)
    sock.settimeout(5)
    still_works(message, sock)
    sock.close()
    assert stored() == []
