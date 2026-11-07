import asyncio
import contextlib
import json
import os
import signal
import socket
import ssl
import subprocess
import time
import uuid

import asyncpg
import pytest


@pytest.fixture(params=[False, True], ids=["ssl_declined_prefer", "tls_verify_full"])
def cancel_listener(request, tmp_path):
    binary = os.environ.get("PGCAT_BINARY", "./target/debug/pgcat")
    backend_port = int(os.environ.get("PGCAT_TEST_POSTGRES_PORT", "5432"))
    backend_password = os.environ.get("PGCAT_TEST_POSTGRES_PASSWORD", "postgres")
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]

    tls_config = ""
    context = None
    if request.param:
        certificate = tmp_path / "server.crt"
        key = tmp_path / "server.key"
        subprocess.run(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-keyout",
                str(key),
                "-out",
                str(certificate),
                "-days",
                "1",
                "-subj",
                "/CN=localhost",
                "-addext",
                "subjectAltName=DNS:localhost",
            ],
            check=True,
            capture_output=True,
        )
        context = ssl.create_default_context(cafile=certificate)
        tls_config = (
            f"tls_certificate = {json.dumps(str(certificate))}\n"
            f"tls_private_key = {json.dumps(str(key))}\n"
        )
    config = tmp_path / "pgcat.toml"
    config.write_text(
        f"""[general]
host = "127.0.0.1"
port = {port}
worker_threads = 2
shutdown_timeout = 1000
admin_username = "admin"
admin_password = "fixture"
{tls_config}
[pools.postgres]
pool_mode = "transaction"
default_role = "primary"
[pools.postgres.users.0]
username = "postgres"
password = {json.dumps(backend_password)}
pool_size = 2
min_pool_size = 0
[pools.postgres.shards.0]
database = "postgres"
servers = [["127.0.0.1", {backend_port}, "primary"]]
"""
    )
    with (tmp_path / "pgcat.log").open("w") as log:
        process = subprocess.Popen([binary, str(config)], stdout=log, stderr=log)
        try:
            deadline = time.monotonic() + 5
            while True:
                assert process.poll() is None, "fixture PgCat exited before startup"
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                        break
                except OSError:
                    assert time.monotonic() < deadline, "fixture PgCat did not start"
                    time.sleep(0.01)
            yield port, context, backend_port, backend_password
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


def test_asyncpg_cancel_stops_backend_after_ssl_request(cancel_listener):
    port, ssl_context, backend_port, password = cancel_listener

    async def run():
        observer = await asyncpg.connect(
            host="127.0.0.1",
            port=backend_port,
            user="postgres",
            password=password,
            database="postgres",
            ssl=False,
        )
        conn = None
        task = None
        query = f"SELECT pg_sleep(20) /* ssl_cancel_{uuid.uuid4().hex} */"
        try:
            conn = await asyncpg.connect(
                host="localhost",
                port=port,
                user="postgres",
                password=password,
                database="postgres",
                ssl=ssl_context or "prefer",
            )
            task = asyncio.create_task(conn.execute(query))

            async def active_pid():
                return await observer.fetchval(
                    "SELECT pid FROM pg_stat_activity WHERE query=$1 AND state='active'",
                    query,
                )

            deadline = time.monotonic() + 5
            while await active_pid() is None:
                assert time.monotonic() < deadline, "query never reached the backend"
                await asyncio.sleep(0.01)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

            deadline = time.monotonic() + 2
            while await active_pid() is not None:
                assert time.monotonic() < deadline, (
                    "canceled query still active in PostgreSQL"
                )
                await asyncio.sleep(0.01)
            assert await conn.fetchval("SELECT 1") == 1
        finally:
            # A failed regression test must not leave its long query consuming a backend.
            await observer.execute(
                "SELECT pg_cancel_backend(pid) FROM pg_stat_activity "
                "WHERE query=$1 AND state='active'",
                query,
            )
            if task and not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            if conn:
                await conn.close(timeout=3)
            await observer.close()

    asyncio.run(run())
