"""A client's messages can reach pgcat in any number of reads, and COPY data
can cut rows anywhere between CopyData messages. What pgcat forwards, and what
its plugin hooks decide, must not depend on where the cuts fall.

Here every byte goes in its own send, through a real pgcat. The unit tests in
messages.rs, held_replies.rs and plugins/response.rs make the cuts
deterministically."""

import os
import socket
import struct

import psycopg2
import pytest

import utils
from utils import frontend

pytestmark = pytest.mark.skipif(
    os.environ.get("PGCAT_TEST_PLUGINS") != "1",
    reason="needs pgcat built with --features test-plugins and PGCAT_TEST_PLUGINS=1",
)

PORT = 6455
PG_PORT = int(os.environ.get("PGCAT_TEST_PG_PORT", "5432"))
DB = "split_db"
# Rows read from it lose their last column (the testing plugin's response hook).
ROWS = "split_rows"
COPIED = "split_copied"

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
rewrites = {{ "select 'split'" = "SELECT 'rewritten'" }}
drop_last_column = "{ROWS}"

[pools.{DB}.plugins.table_access]
enabled = true
tables = ["split_forbidden"]

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
        host="127.0.0.1", port=PG_PORT, user="sharding_user", password="sharding_user", dbname="shard0"
    )
    conn.autocommit = True
    return conn


@pytest.fixture(scope="module")
def pgcat():
    with postgres() as conn, conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {ROWS}, {COPIED}")
        cur.execute(f"CREATE TABLE {ROWS} (id int, kept text, dropped text)")
        cur.execute(f"INSERT INTO {ROWS} VALUES (1, 'a', 'x'), (2, 'b', 'y')")
        cur.execute(f"CREATE TABLE {COPIED} (id int, note text)")
    process = utils.PgcatProcess(CONFIG, PORT).start()
    yield process
    process.stop()
    with postgres() as conn, conn.cursor() as cur:
        cur.execute(f"DROP TABLE {ROWS}, {COPIED}")


class ByteAtATime:
    """A socket that sends each byte on its own."""

    def __init__(self, sock):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sock = sock

    def sendall(self, data):
        for i in range(len(data)):
            self.sock.sendall(data[i : i + 1])

    def __getattr__(self, name):
        return getattr(self.sock, name)


def connect(wrap=ByteAtATime):
    sock, message = utils.raw_connection(PORT, DB, wrap)
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


def replies(message):
    """The replies up to ReadyForQuery, as (code, body) pairs."""
    seen = []
    while not seen or seen[-1][0] != b"Z":
        seen.append(message())
    return seen


def codes(seen):
    return "".join(code.decode() for code, _ in seen)


def columns(seen):
    """The values of each DataRow, as text."""
    rows = []
    for code, body in seen:
        if code != b"D":
            continue
        (count,) = struct.unpack("!h", body[:2])
        values, at = [], 2
        for _ in range(count):
            (length,) = struct.unpack("!i", body[at : at + 4])
            values.append(body[at + 4 : at + 4 + length].decode())
            at += 4 + length
        rows.append(values)
    return rows


def sqlstate(body):
    fields = dict((field[:1], field[1:]) for field in body.split(b"\0") if field)
    return fields[b"C"].decode()


def test_simple_query_is_read_whole(pgcat):
    sock, message = connect()
    sock.sendall(query("select 'split'"))
    assert columns(replies(message)) == [["rewritten"]]

    sock.sendall(query("SELECT * FROM split_forbidden"))
    seen = replies(message)
    assert codes(seen) == "EZ"
    assert sqlstate(seen[0][1]) == "42501"
    sock.close()


def test_pipelined_batch_with_planned_and_cached_statements(pgcat):
    cached = "SELECT 'cached by another client'"
    warm, warm_message = connect(wrap=lambda sock: sock)
    warm.sendall(parse("warm", cached) + SYNC)
    assert codes(replies(warm_message)) == "1Z"
    warm.close()

    sock, message = connect()
    sock.sendall(
        parse("rows", f"SELECT id, kept, dropped FROM {ROWS} ORDER BY id")
        + bind("rows")
        + EXECUTE
        + parse("cached", cached)
        + bind("cached")
        + EXECUTE
        + SYNC
    )
    seen = replies(message)
    assert codes(seen) == "12DDC12DCZ"
    assert columns(seen) == [["1", "a"], ["2", "b"], ["cached by another client"]]
    sock.close()


def test_copy_rows_cut_across_copy_data(pgcat):
    sock, message = connect()
    sock.sendall(query("BEGIN"))
    assert codes(replies(message)) == "CZ"

    sock.sendall(query(f"COPY {COPIED} FROM STDIN"))
    assert message()[0] == b"G"
    data = b"1\tone\n2\ttwo\n3\tthree\n"
    for at in range(0, len(data), 4):
        sock.sendall(frontend(b"d", data[at : at + 4]))
    sock.sendall(frontend(b"c", b""))
    assert [code for code, _ in replies(message)] == [b"C", b"Z"]

    sock.sendall(query(f"SELECT id, note FROM {COPIED} ORDER BY id"))
    assert columns(replies(message)) == [["1", "one"], ["2", "two"], ["3", "three"]]
    sock.sendall(query("ROLLBACK"))
    assert codes(replies(message)) == "CZ"
    sock.close()
