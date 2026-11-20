"""shutdown_drain_spread: on SIGINT, idle clients are closed at random times
over the window, not all at once, so they don't all reconnect elsewhere at once."""

import os
import selectors
import signal
import socket
import struct
import time

import psycopg

import utils

PORT = 6485
PG_PORT = int(os.environ.get("PGCAT_TEST_PG_PORT", "5432"))
SPREAD = 5.0
TIMEOUT = 10.0
CLIENTS = 50
# What a client may retry on a new connection: pgcat sent nothing it got on
# this one to a server.
SHUTDOWN = {
    "S": "FATAL",
    "V": "FATAL",
    "C": "PS001",
    "M": "terminating connection due to administrator command",
}

CONFIG = f"""
[general]
host = "127.0.0.1"
port = {PORT}
admin_username = "admin_user"
admin_password = "admin_pass"
shutdown_drain_spread = {int(SPREAD * 1000)}
shutdown_timeout = {int(TIMEOUT * 1000)}

[pools.shard0.users.0]
username = "sharding_user"
password = "sharding_user"
pool_size = 5

[pools.shard0.shards.0]
servers = [["127.0.0.1", {PG_PORT}, "primary"]]
database = "shard0"
"""


def test_idle_clients_disconnect_spread_over_the_window():
    pgcat = utils.PgcatProcess(CONFIG, PORT).start()
    try:
        socks = [utils.raw_connection(PORT, "shard0")[0] for _ in range(CLIENTS)]
        selector = selectors.DefaultSelector()
        for sock in socks:
            selector.register(sock, selectors.EVENT_READ, bytearray())

        pgcat.process.send_signal(signal.SIGINT)
        start = time.monotonic()

        closed_after = []
        while len(closed_after) < CLIENTS and time.monotonic() - start < TIMEOUT + 2:
            for key, _ in selector.select(timeout=0.1):
                data = key.fileobj.recv(4096)
                key.data.extend(data)
                if not data:
                    closed_after.append(time.monotonic() - start)
                    selector.unregister(key.fileobj)
                    # pgcat closed it, not the hard exit at shutdown_timeout.
                    assert key.data.startswith(b"E"), bytes(key.data)
                    assert b"administrator command" in key.data, bytes(key.data)

        assert len(closed_after) == CLIENTS
        # Closing everyone at once, or everyone at the same delay, fails this.
        assert max(closed_after) - min(closed_after) > SPREAD / 2, closed_after
        assert max(closed_after) < SPREAD + 1, closed_after
    finally:
        pgcat.stop()


def test_a_message_split_across_sigint_is_answered():
    # The shutdown notice arrives while pgcat has read only part of a
    # message. Dropping that read would lose the bytes and break the client.
    pgcat = utils.PgcatProcess(CONFIG, PORT).start()
    try:
        clients = [utils.raw_connection(PORT, "shard0") for _ in range(10)]
        query = utils.frontend(b"Q", b"SELECT 1\0")
        for sock, _ in clients:
            sock.sendall(query[:3])
        time.sleep(0.2)

        pgcat.process.send_signal(signal.SIGINT)
        time.sleep(0.2)

        answered = 0
        for sock, message in clients:
            sock.sendall(query[3:])
            sock.settimeout(SPREAD + 1)
            # A client can draw a deadline before the rest arrives; it's
            # then closed instead, which is fine.
            codes = []
            while not codes or codes[-1] not in (b"Z", b"E"):
                codes.append(message()[0])
            answered += codes == [b"T", b"D", b"C", b"Z"]

        assert answered >= 5, answered
    finally:
        pgcat.stop()


def test_a_reload_raising_both_settings_moves_the_hard_exit():
    # Startup allows a short window; a reload raises the spread and the
    # timeout together. The hard exit must use the reloaded timeout, or it
    # closes clients still waiting out the reloaded spread.
    short = CONFIG.replace(
        f"shutdown_drain_spread = {int(SPREAD * 1000)}", "shutdown_drain_spread = 500"
    ).replace(f"shutdown_timeout = {int(TIMEOUT * 1000)}", "shutdown_timeout = 1000")
    # Both replacements matched. The newline keeps "shutdown_timeout = 10000"
    # from passing for 1000.
    assert "shutdown_drain_spread = 500\n" in short, short
    assert "shutdown_timeout = 1000\n" in short, short
    pgcat = utils.PgcatProcess(short, PORT).start()
    try:
        pgcat.write_config(CONFIG)
        # Returns once the reloaded config is stored.
        utils.admin_reload(PORT)

        socks = [utils.raw_connection(PORT, "shard0")[0] for _ in range(CLIENTS)]
        selector = selectors.DefaultSelector()
        for sock in socks:
            selector.register(sock, selectors.EVENT_READ, bytearray())

        pgcat.process.send_signal(signal.SIGINT)
        start = time.monotonic()

        closed_after = []
        while len(closed_after) < CLIENTS and time.monotonic() - start < TIMEOUT + 2:
            for key, _ in selector.select(timeout=0.1):
                data = key.fileobj.recv(4096)
                key.data.extend(data)
                if not data:
                    closed_after.append(time.monotonic() - start)
                    selector.unregister(key.fileobj)
                    # The exit at the startup timeout closes without a notice.
                    assert b"administrator command" in key.data, (
                        time.monotonic() - start,
                        bytes(key.data),
                    )

        assert len(closed_after) == CLIENTS
        assert max(closed_after) > 2, closed_after
    finally:
        pgcat.stop()


def test_a_drained_client_gets_the_shutdown_code_and_is_closed():
    now = CONFIG.replace(
        f"shutdown_drain_spread = {int(SPREAD * 1000)}", "shutdown_drain_spread = 0"
    )
    assert "shutdown_drain_spread = 0\n" in now, now
    pgcat = utils.PgcatProcess(now, PORT).start()
    try:
        sock, message = utils.raw_connection(PORT, "shard0", timeout=TIMEOUT)
        pgcat.process.send_signal(signal.SIGINT)

        code, body = message()
        assert code == b"E", (code, body)
        assert utils.error_fields(body) == SHUTDOWN
        assert utils.closed(sock)
    finally:
        pgcat.stop()


def startup(port, db):
    """Send a StartupMessage as sharding_user; return the socket and pgcat's
    first reply."""
    sock = socket.create_connection(("127.0.0.1", port), timeout=TIMEOUT)
    params = f"user\0sharding_user\0database\0{db}\0\0".encode()
    sock.sendall(struct.pack("!ii", 8 + len(params), 196608) + params)
    code, length = struct.unpack("!ci", utils.recv_exactly(sock, 5))
    return sock, code, utils.recv_exactly(sock, length - 4)


def test_a_client_connecting_during_shutdown_gets_the_shutdown_code():
    pgcat = utils.PgcatProcess(CONFIG, PORT).start()
    # pgcat exits once no non-admin client is left, and drains a client only
    # between transactions, so this one keeps it running.
    holder = psycopg.connect(
        f"postgres://sharding_user:sharding_user@127.0.0.1:{PORT}/shard0"
    )
    try:
        holder.execute("SELECT 1")
        pgcat.process.send_signal(signal.SIGINT)

        # A connection that starts before pgcat handles SIGINT is admitted.
        deadline = time.monotonic() + TIMEOUT
        while True:
            sock, code, body = startup(PORT, "shard0")
            if code != b"R":
                break
            sock.close()
            assert time.monotonic() < deadline, "pgcat admitted clients after SIGINT"

        assert code == b"E", (code, body)
        assert utils.error_fields(body) == SHUTDOWN
        assert utils.closed(sock)
        sock.close()
    finally:
        holder.close()
        pgcat.stop()


def test_a_drain_after_a_copy_waits_for_the_rest_of_its_query():
    # After CopyDone, the rest of a simple Query's reply can take more than
    # one read from the server. Checking the server in after the first one
    # leaves the client without its ReadyForQuery, so a drain then closes it
    # with the shutdown code after its COPY ran.
    table = "drain_copy_rows"
    direct = f"postgres://sharding_user:sharding_user@127.0.0.1:{PG_PORT}/shard0"
    with psycopg.connect(direct, autocommit=True) as conn:
        conn.execute(f"DROP TABLE IF EXISTS {table}")
        conn.execute(f"CREATE TABLE {table} (n int)")
    now = CONFIG.replace(
        f"shutdown_drain_spread = {int(SPREAD * 1000)}", "shutdown_drain_spread = 0"
    )
    assert "shutdown_drain_spread = 0\n" in now, now
    pgcat = utils.PgcatProcess(now, PORT).start()
    try:
        sock, message = utils.raw_connection(PORT, "shard0", timeout=TIMEOUT)
        query = f"COPY {table} FROM STDIN; SELECT repeat('x', 10000)\0".encode()
        sock.sendall(utils.frontend(b"Q", query))
        assert message()[0] == b"G"
        sock.sendall(utils.frontend(b"d", b"1\n") + utils.frontend(b"c", b""))

        # The COPY ran.
        replies = [message()]
        assert replies == [(b"C", b"COPY 1\0")], replies
        pgcat.process.send_signal(signal.SIGINT)
        while True:
            try:
                replies.append(message())
            except ConnectionError:
                break

        codes = [code for code, _ in replies]
        assert codes == [b"C", b"T", b"D", b"C", b"Z", b"E"], codes
        assert replies[2][1].endswith(b"x" * 10000), replies[2]
        assert replies[4] == (b"Z", b"I"), replies[4]
        assert utils.error_fields(replies[5][1]) == SHUTDOWN
    finally:
        pgcat.stop()
    with psycopg.connect(direct, autocommit=True) as conn:
        assert conn.execute(f"SELECT count(*) FROM {table}").fetchone() == (1,)
        conn.execute(f"DROP TABLE {table}")
