"""shutdown_drain_spread: on SIGINT, idle clients are closed at random times
over the window, not all at once, so they don't all reconnect elsewhere at once."""

import os
import selectors
import signal
import time

import utils

PORT = 6485
PG_PORT = int(os.environ.get("PGCAT_TEST_PG_PORT", "5432"))
SPREAD = 5.0
TIMEOUT = 10.0
CLIENTS = 50

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
