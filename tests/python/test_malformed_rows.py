"""A server row that doesn't match its RowDescription, for a statement a
plugin rewrites the rows of. Postgres never sends one, so a small fake
server does. The client gets an error instead of the row, and the server
connection is closed, not handed to the next client."""

import os
import socketserver
import struct
import threading

import pytest

import utils
from utils import frontend

pytestmark = pytest.mark.skipif(
    os.environ.get("PGCAT_TEST_PLUGINS") != "1",
    reason="needs pgcat built with --features test-plugins and PGCAT_TEST_PLUGINS=1",
)

PORT = 6458
FAKE_PORT = 6459
DB = "malformed_db"
# The testing plugin drops the last column of rows read from it.
TABLE = "malformed_rows"

CONFIG = f"""
[general]
host = "127.0.0.1"
port = {PORT}
admin_username = "admin_user"
admin_password = "admin_pass"

[pools.{DB}]
pool_mode = "transaction"
query_parser_enabled = true
plugins_require_parse = true

[pools.{DB}.plugins.testing]
enabled = true
drop_last_column = "{TABLE}"

[pools.{DB}.users.0]
username = "sharding_user"
password = "sharding_user"
pool_size = 1
statement_timeout = 0

[pools.{DB}.shards.0]
servers = [["127.0.0.1", {FAKE_PORT}, "primary"]]
database = "shard0"
"""


def text_columns(*names):
    body = struct.pack("!h", len(names))
    for name in names:
        body += name.encode() + b"\0" + struct.pack("!ihihih", 0, 0, 25, -1, -1, 0)
    return frontend(b"T", body)


def row(*values):
    body = struct.pack("!h", len(values))
    for value in values:
        body += struct.pack("!i", len(value)) + value
    return frontend(b"D", body)


def complete(tag):
    return frontend(b"C", tag.encode() + b"\0")


READY = frontend(b"Z", b"I")


class FakeServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self):
        super().__init__(("127.0.0.1", FAKE_PORT), FakeConnection)
        self.lock = threading.Condition()
        self.connections = 0
        # (connection number, query) for each simple query, and the
        # connections that were closed.
        self.queries = []
        self.closed = set()


class FakeConnection(socketserver.BaseRequestHandler):
    def recv(self, n):
        data = b""
        while len(data) < n:
            chunk = self.request.recv(n - len(data))
            if not chunk:
                raise ConnectionError("closed")
            data += chunk
        return data

    def handle(self):
        server = self.server
        with server.lock:
            server.connections += 1
            number = server.connections
        try:
            (length,) = struct.unpack("!i", self.recv(4))
            self.recv(length - 4)
            self.request.sendall(
                frontend(b"R", struct.pack("!i", 0))
                + frontend(b"S", b"server_version\x0014.0\0")
                + frontend(b"S", b"standard_conforming_strings\0on\0")
                + frontend(b"K", struct.pack("!ii", number, 0))
                + READY
            )
            while True:
                code = self.recv(1)
                (length,) = struct.unpack("!i", self.recv(4))
                body = self.recv(length - 4)
                if code == b"X":
                    break
                if code != b"Q":
                    continue
                sql = body.rstrip(b"\0").decode()
                with server.lock:
                    server.queries.append((number, sql))
                if TABLE in sql:
                    # Two columns described, one sent.
                    reply = text_columns("a", "b") + row(b"1") + complete("SELECT 1")
                elif sql.lower().startswith("select"):
                    reply = text_columns("a") + row(b"1") + complete("SELECT 1")
                else:
                    reply = complete("SET")
                self.request.sendall(reply + READY)
        except (ConnectionError, OSError):
            pass
        with server.lock:
            server.closed.add(number)
            server.lock.notify_all()


@pytest.fixture(scope="module")
def fake():
    server = FakeServer()
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    yield server
    server.shutdown()
    server.server_close()
    thread.join()


@pytest.fixture(scope="module")
def pgcat(fake):
    process = utils.PgcatProcess(CONFIG, PORT).start()
    yield process
    process.stop()


def served(fake, text):
    with fake.lock:
        return [number for number, sql in fake.queries if text in sql]


def test_row_that_does_not_match_its_description(fake, pgcat):
    sock, message = utils.raw_connection(PORT, DB)
    sock.settimeout(10)
    sock.sendall(frontend(b"Q", f"SELECT a, b FROM {TABLE}\0".encode()))
    code, body = message()
    # No description or row first: the error, then pgcat closes the connection.
    assert code == b"E", (code, body)
    assert b"pgcat could not follow the server's response" in body
    with pytest.raises((ConnectionError, OSError)):
        while True:
            message()
    sock.close()

    # The next client gets another server connection, and its answer.
    sock, message = utils.raw_connection(PORT, DB)
    sock.settimeout(10)
    sock.sendall(frontend(b"Q", b"SELECT 'next'\0"))
    seen = []
    while not seen or seen[-1] != b"Z":
        seen.append(message()[0])
    assert b"".join(seen) == b"TDCZ"
    sock.close()

    [bad] = served(fake, TABLE)
    [good] = served(fake, "'next'")
    assert good != bad
    with fake.lock:
        assert fake.lock.wait_for(lambda: bad in fake.closed, timeout=10)
