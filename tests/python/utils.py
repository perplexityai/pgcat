import hashlib
import os
import signal
import socket
import struct
import subprocess
import time
from typing import Tuple
import tempfile

import psutil
import psycopg2

PGCAT_HOST = "127.0.0.1"
PGCAT_PORT = "6432"


def _pgcat_start(config_path: str):
    pg_cat_send_signal(signal.SIGTERM)
    os.system(f"./target/debug/pgcat {config_path} &")
    time.sleep(2)


def pgcat_start():
    _pgcat_start(config_path='.circleci/pgcat.toml')


def pgcat_generic_start(config: str):
    tmp = tempfile.NamedTemporaryFile()
    with open(tmp.name, 'w') as f:
        f.write(config)
    _pgcat_start(config_path=tmp.name)


def glauth_send_signal(signal: signal.Signals):
    try:
        for proc in psutil.process_iter(["pid", "name"]):
            if proc.name() == "glauth":
                os.kill(proc.pid, signal)
    except Exception as e:
        # The process can be gone when we send this signal
        print(e)

    if signal == signal.SIGTERM:
        # Returns 0 if pgcat process exists
        time.sleep(2)
        if not os.system('pgrep glauth'):
            raise Exception("glauth not closed after SIGTERM")


def pg_cat_send_signal(signal: signal.Signals):
    try:
        for proc in psutil.process_iter(["pid", "name"]):
            if "pgcat" == proc.name():
                os.kill(proc.pid, signal)
    except Exception as e:
        # The process can be gone when we send this signal
        print(e)

    if signal == signal.SIGTERM:
        # Returns 0 if pgcat process exists
        time.sleep(2)
        if not os.system('pgrep pgcat'):
            raise Exception("pgcat not closed after SIGTERM")


def connect_db(
    autocommit: bool = True,
    admin: bool = False,
) -> Tuple[psycopg2.extensions.connection, psycopg2.extensions.cursor]:

    if admin:
        user = "admin_user"
        password = "admin_pass"
        db = "pgcat"
    else:
        user = "sharding_user"
        password = "sharding_user"
        db = "sharded_db"

    conn = psycopg2.connect(
        f"postgres://{user}:{password}@{PGCAT_HOST}:{PGCAT_PORT}/{db}?application_name=testing_pgcat",
        connect_timeout=2,
    )
    conn.autocommit = autocommit
    cur = conn.cursor()

    return (conn, cur)

def connect_db_trust(
    autocommit: bool = True,
    admin: bool = False,
) -> Tuple[psycopg2.extensions.connection, psycopg2.extensions.cursor]:

    if admin:
        user = "admin_user"
        db = "pgcat"
    else:
        user = "sharding_user"
        db = "sharded_db"

    conn = psycopg2.connect(
        f"postgres://{user}@{PGCAT_HOST}:{PGCAT_PORT}/{db}?application_name=testing_pgcat",
        connect_timeout=2,
    )
    conn.autocommit = autocommit
    cur = conn.cursor()

    return (conn, cur)


def cleanup_conn(conn: psycopg2.extensions.connection, cur: psycopg2.extensions.cursor):
    cur.close()
    conn.close()


# PGCAT_BINARY runs PgcatProcess tests against another build of the server,
# such as an embedding executable, as test_cancellation.py already allows. The
# shared pgcat helpers on 6432 above always run ./target/debug/pgcat.
PGCAT_BINARY = os.environ.get("PGCAT_BINARY", "./target/debug/pgcat")


class PgcatProcess:
    """A pgcat process with its own config and port, for tests that need
    plugins configured. It doesn't touch the shared pgcat on 6432."""

    def __init__(
        self,
        config: str,
        port: int,
        log_level: str = "info",
        binary: str = PGCAT_BINARY,
        env: dict = None,
    ):
        self.port = port
        self.env = env or {}
        self.config_file = tempfile.NamedTemporaryFile("w", suffix=".toml", delete=False)
        self.log_file = tempfile.NamedTemporaryFile("w+", suffix=".log", delete=False)
        self.log_level = log_level
        self.binary = binary
        self.write_config(config)
        self.process = None

    def write_config(self, config: str):
        with open(self.config_file.name, "w") as f:
            f.write(config)

    def start(self):
        env = dict(os.environ, LOG_LEVEL=self.log_level, NO_COLOR="true", **self.env)
        self.process = subprocess.Popen(
            [self.binary, self.config_file.name],
            stdout=self.log_file,
            stderr=subprocess.STDOUT,
            env=env,
        )
        deadline = time.time() + 10
        while time.time() < deadline:
            if self.process.poll() is not None:
                raise Exception("pgcat exited early:\n" + self.logs())
            try:
                socket.create_connection((PGCAT_HOST, self.port), timeout=0.2).close()
                return self
            except OSError:
                time.sleep(0.1)
        raise Exception("pgcat did not start:\n" + self.logs())

    def reload(self):
        self.process.send_signal(signal.SIGHUP)
        time.sleep(0.5)

    def stop(self):
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
        self.process = None

    def logs(self) -> str:
        with open(self.log_file.name) as f:
            return f.read()

    def dsn(self, db: str, user: str = "sharding_user", password: str = "sharding_user") -> str:
        return f"postgres://{user}:{password}@{PGCAT_HOST}:{self.port}/{db}?application_name=testing_pgcat"


def raw_connection(port: int, db: str, wrap=lambda sock: sock, timeout=None):
    """A protocol-level connection to pgcat as sharding_user, authenticated,
    ready for queries. Returns the socket and a function reading one message.
    `wrap` can replace the socket, for example to change how it sends.
    `timeout` bounds each socket operation, from the startup on."""
    sock = wrap(socket.create_connection((PGCAT_HOST, port), timeout=timeout))
    params = f"user\0sharding_user\0database\0{db}\0\0".encode()
    sock.sendall(struct.pack("!ii", 8 + len(params), 196608) + params)

    def message():
        code, length = struct.unpack("!ci", recv_exactly(sock, 5))
        return code, recv_exactly(sock, length - 4)

    code, body = message()
    assert code == b"R" and struct.unpack("!i", body[:4])[0] == 5, (code, body)
    inner = hashlib.md5(b"sharding_usersharding_user").hexdigest().encode()
    password = b"md5" + hashlib.md5(inner + body[4:8]).hexdigest().encode() + b"\0"
    sock.sendall(b"p" + struct.pack("!i", 4 + len(password)) + password)
    while message()[0] != b"Z":
        pass
    return sock, message


def admin_reload(port: int, timeout: float = 10.0):
    """Run RELOAD on a separate admin connection (simple protocol) and return
    once pgcat reports the command complete and ReadyForQuery. The whole
    exchange (connect, every send, every receive) shares one monotonic
    deadline of `timeout` seconds."""
    deadline = time.monotonic() + timeout

    def remaining():
        left = deadline - time.monotonic()
        if left <= 0:
            raise TimeoutError(f"admin RELOAD exceeded {timeout}s")
        return left

    sock = socket.create_connection((PGCAT_HOST, port), timeout=remaining())
    try:

        def send(data):
            sock.settimeout(remaining())
            sock.sendall(data)

        def read(n):
            data = b""
            while len(data) < n:
                sock.settimeout(remaining())
                chunk = sock.recv(n - len(data))
                if not chunk:
                    raise ConnectionError("closed")
                data += chunk
            return data

        def message():
            code, length = struct.unpack("!ci", read(5))
            return code, read(length - 4)

        params = b"user\0admin_user\0database\0pgcat\0\0"
        send(struct.pack("!ii", 8 + len(params), 196608) + params)
        code, body = message()
        assert code == b"R" and struct.unpack("!i", body[:4])[0] == 5, (code, body)
        inner = hashlib.md5(b"admin_passadmin_user").hexdigest().encode()
        password = b"md5" + hashlib.md5(inner + body[4:8]).hexdigest().encode() + b"\0"
        send(b"p" + struct.pack("!i", 4 + len(password)) + password)
        while True:
            code, body = message()
            assert code != b"E", body
            if code == b"Z":
                break
        send(frontend(b"Q", b"RELOAD\0"))
        completed = False
        while True:
            code, body = message()
            assert code != b"E", body
            if code == b"C":
                assert body == b"RELOAD\0", body
                completed = True
            if code == b"Z":
                break
        assert completed, "RELOAD did not report CommandComplete"
    finally:
        sock.close()


def recv_exactly(sock, n):
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError("closed")
        data += chunk
    return data


def frontend(code, body):
    return code + struct.pack("!i", 4 + len(body)) + body
