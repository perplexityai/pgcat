"""
Pool behavior a client can observe: waiting for and timing out on a server
connection, PAUSE / RESUME, server_lifetime, prepared statements shared across
clients and servers, per-client parameters, the admin console as psql sees
it, and the idle-in-transaction and statement timeouts.

Ported in spirit from PgBouncer's test suite (test/test_operations.py,
test_prepared.py, test_timeouts.py, test_admin.py) where pgcat has the same
feature. Tests marked xfail are known pgcat bugs and are strict: they fail
when the bug is fixed, so the marker goes with the fix.
"""

import re
import subprocess
import time

import psycopg
import psycopg2
import pytest

import utils
from test_psql_scenarios import PSQL

PORT = 6451
PG = "postgres://postgres:postgres@127.0.0.1:5432/shard0"
CONNECT_TIMEOUT_MS = 500
IDLE_IN_TRANSACTION_MS = 1500
SERVER_LIFETIME_MS = 300
STATEMENT_TIMEOUT_MS = 300


def pool(name, pool_size=1, cache_size=100, statement_timeout=0, extra=""):
    return f"""
[pools.{name}]
pool_mode = "transaction"
prepared_statements_cache_size = {cache_size}
{extra}

[pools.{name}.users.0]
username = "sharding_user"
password = "sharding_user"
pool_size = {pool_size}
statement_timeout = {statement_timeout}

[pools.{name}.shards.0]
servers = [["127.0.0.1", 5432, "primary"]]
database = "shard0"
"""


CONFIG = (
    f"""
[general]
host = "127.0.0.1"
port = {PORT}
admin_username = "admin_user"
admin_password = "admin_pass"
connect_timeout = {CONNECT_TIMEOUT_MS}
idle_client_in_transaction_timeout = {IDLE_IN_TRANSACTION_MS}
"""
    + pool("sem_one")
    + pool("sem_two", pool_size=2)
    + pool("sem_timeout", statement_timeout=STATEMENT_TIMEOUT_MS)
    + pool("sem_lifetime", extra=f"server_lifetime = {SERVER_LIFETIME_MS}")
)


@pytest.fixture(scope="module")
def pgcat():
    process = utils.PgcatProcess(CONFIG, PORT).start()
    yield process
    process.stop()


@pytest.fixture
def table():
    """A fresh table sharding_user can write, read back as the superuser."""
    su = psycopg2.connect(PG)
    su.autocommit = True
    cur = su.cursor()
    cur.execute("DROP TABLE IF EXISTS pooler_semantics; CREATE TABLE pooler_semantics (v text)")
    cur.execute("GRANT ALL ON pooler_semantics TO sharding_user")

    def rows():
        cur.execute("SELECT v FROM pooler_semantics ORDER BY v")
        return [r[0] for r in cur.fetchall()]

    yield rows
    cur.execute("DROP TABLE pooler_semantics")
    su.close()


class KnownBug(Exception):
    """Raised only on a known bug's exact outcome. Known-bug tests are
    xfail(raises=KnownBug, strict=True): any other failure fails, and the
    test passing once the bug is fixed fails too, so the marker goes with
    the fix."""


def known_bug(condition, what):
    if condition:
        raise KnownBug(what)


def connect(pgcat, db, **kwargs):
    return psycopg.connect(pgcat.dsn(db), autocommit=True, **kwargs)


def admin(pgcat):
    # The admin console speaks only the simple protocol; psycopg would
    # otherwise switch a repeated query to a prepared statement.
    return psycopg.connect(
        f"postgres://admin_user:admin_pass@127.0.0.1:{pgcat.port}/pgcat",
        autocommit=True,
        prepare_threshold=None,
    )


def waiting_clients(console, db):
    cur = console.execute("SHOW POOLS")
    columns = [column.name for column in cur.description]
    for row in cur.fetchall():
        if row[columns.index("database")] == db:
            return row[columns.index("cl_waiting")]
    raise AssertionError(f"no pool {db} in SHOW POOLS")


def wait_until(condition, what, timeout=5):
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            pytest.fail(f"timed out waiting for {what}")
        time.sleep(0.02)


def send(conn, sql):
    """Send a query without waiting for its result. send_query returns once
    the query is written, so the pooler has it before the caller goes on."""
    conn.pgconn.send_query(sql.encode())


def finished(conn):
    try:
        conn.pgconn.consume_input()
    except psycopg.OperationalError:
        # The server side closed (pgcat's statement timeout does this);
        # get_result() hands back what arrived first and can't block.
        return True
    return not conn.pgconn.is_busy()


def results(conn, what, timeout=10):
    """The results of a query sent with send(). get_result() blocks while
    the connection is busy, so each call waits for input first, under one
    deadline: a pooler that never answers fails the test instead of hanging it."""
    deadline = time.monotonic() + timeout
    out = []
    while True:
        wait_until(lambda: finished(conn), what, timeout=deadline - time.monotonic())
        res = conn.pgconn.get_result()
        if res is None:
            return out
        out.append(res)


def result(conn, what):
    res = results(conn, what)[0]
    assert res.status == psycopg.pq.ExecStatus.TUPLES_OK, res.error_message
    return res.get_value(0, 0).decode()


# Regression: a client waiting for an exhausted pool hangs forever, or fails
# at once, instead of waiting connect_timeout (PgBouncer's query_wait_timeout).
def test_checkout_times_out_while_pool_is_busy(pgcat):
    holder = connect(pgcat, "sem_one")
    holder.execute("BEGIN")
    holder.execute("SELECT 1")
    waiter = connect(pgcat, "sem_one")

    started = time.monotonic()
    with pytest.raises(psycopg.Error, match="could not get connection from the pool"):
        waiter.execute("SELECT 1")
    waited = time.monotonic() - started
    assert CONNECT_TIMEOUT_MS / 1000 * 0.8 <= waited < 5, waited

    holder.execute("COMMIT")
    assert connect(pgcat, "sem_one").execute("SELECT 2").fetchone() == (2,)


# Regression: a server connection released by one client doesn't wake a
# client already waiting for it.
def test_waiting_client_gets_released_connection(pgcat):
    holder = connect(pgcat, "sem_one")
    holder.execute("BEGIN")
    holder.execute("SELECT 1")
    waiter = connect(pgcat, "sem_one")
    console = admin(pgcat)

    send(waiter, "SELECT 'got it'")
    wait_until(lambda: waiting_clients(console, "sem_one") >= 1, "the waiter to queue")
    holder.execute("COMMIT")
    assert result(waiter, "the waiter's result after COMMIT") == "got it"


# Regression: PAUSE lets queries through, or RESUME doesn't release the
# clients that queued while paused (PgBouncer test_pause_resume).
def test_pause_holds_queries_until_resume(pgcat):
    client = connect(pgcat, "sem_one")
    client.execute("SELECT 1")
    console = admin(pgcat)
    console.execute("PAUSE sem_one,sharding_user")
    try:
        send(client, "SELECT 'resumed'")
        # A client held by PAUSE shows as idle in SHOW POOLS and SHOW CLIENTS,
        # so nothing to wait on: proving the query didn't run needs a window.
        # Twice connect_timeout, because a paused pool is not a busy pool and
        # must not time the client out.
        time.sleep(CONNECT_TIMEOUT_MS / 1000 * 2)
        assert not finished(client), "query ran while the pool was paused"
    finally:
        console.execute("RESUME sem_one,sharding_user")
    assert result(client, "the paused query's result after RESUME") == "resumed"


# Regression: server_lifetime is not enforced, so backends live forever
# (memory bloat, no rebalancing after failover), or enforcing it breaks the
# client using the pool.
def test_server_lifetime_replaces_backend(pgcat):
    client = connect(pgcat, "sem_lifetime", prepare_threshold=None)
    first = client.execute("SELECT pg_backend_pid()").fetchone()[0]
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        pid = client.execute("SELECT pg_backend_pid()").fetchone()[0]
        if pid != first:
            return
        time.sleep(0.05)
    pytest.fail(f"backend {first} outlived server_lifetime = {SERVER_LIFETIME_MS} ms")


# Regression: the statement cache maps a client's statement name to another
# client's statement on the same server (PgBouncer
# test_discard_or_deallocate_all, first half).
def test_same_statement_name_is_per_client(pgcat):
    one = connect(pgcat, "sem_one")
    two = connect(pgcat, "sem_one")
    one.pgconn.prepare(b"stmt", b"SELECT 'one'")
    two.pgconn.prepare(b"stmt", b"SELECT 'two'")
    for _ in range(3):
        assert one.pgconn.exec_prepared(b"stmt", []).get_value(0, 0) == b"one"
        assert two.pgconn.exec_prepared(b"stmt", []).get_value(0, 0) == b"two"


# Regression: a statement a client prepared on one server is not prepared on
# another server before that client's Bind lands there (PgBouncer
# test_prepared_statement).
def test_prepared_statement_follows_client_across_servers(pgcat):
    client = connect(pgcat, "sem_two")
    query = "SELECT pg_backend_pid(), %s::text"
    prepared_on = client.execute(query, ["first"], prepare=True).fetchone()[0]

    # Hold both servers, then free only the one the statement isn't on.
    hogs = [connect(pgcat, "sem_two") for _ in range(2)]
    held = {}
    for hog in hogs:
        hog.execute("BEGIN")
        held[hog.execute("SELECT pg_backend_pid()").fetchone()[0]] = hog
    assert prepared_on in held and len(held) == 2, held
    other = next(pid for pid in held if pid != prepared_on)
    held[other].execute("COMMIT")

    assert client.execute(query, ["second"], prepare=True).fetchone() == (other, "second")
    held[prepared_on].execute("COMMIT")
    assert client.execute(query, ["third"], prepare=True).fetchone()[1] == "third"


# Regression: parameters a client SET are lost or swapped when another
# client's transaction runs on the same server in between.
def test_client_parameters_survive_interleaving(pgcat):
    tokyo = connect(pgcat, "sem_one")
    chicago = connect(pgcat, "sem_one")
    tokyo.execute("SET TimeZone = 'Asia/Tokyo'")
    chicago.execute("SET TimeZone = 'America/Chicago'")
    tokyo.execute("SET application_name = 'tokyo_app'")
    for _ in range(2):
        assert tokyo.execute("SHOW TimeZone").fetchone() == ("Asia/Tokyo",)
        assert chicago.execute("SHOW TimeZone").fetchone() == ("America/Chicago",)
        assert tokyo.execute("SHOW application_name").fetchone() == ("tokyo_app",)
        assert chicago.execute("SHOW application_name").fetchone() != ("tokyo_app",)


# Regression: the admin console misreports to a stock psql client, or an
# unknown command drops the console connection (PgBouncer test_admin).
@pytest.mark.skipif(PSQL is None, reason="needs psql")
def test_admin_console_in_psql(pgcat):
    script = "\n".join(
        [
            "SHOW VERSION;",
            "SHOW DATABASES;",
            "SHOW POOLS;",
            "SHOW CONFIG;",
            "NOT A COMMAND;",
            "SHOW VERSION;",
        ]
    )
    result = subprocess.run(
        [PSQL, "-X", "--csv", "-v", "ON_ERROR_STOP=0", "-h", "127.0.0.1",
         "-p", str(pgcat.port), "-U", "admin_user", "-d", "pgcat"],
        input=script, capture_output=True, text=True, timeout=30,
        env={"PGPASSWORD": "admin_pass", "PATH": "/usr/bin:/bin"},
    )
    out = result.stdout
    assert len(re.findall(r"^PgCat \d+\.\d+\.\d+$", out, re.M)) == 2, out
    pools = [line for line in out.splitlines() if line.startswith("sem_")]
    assert {line.split(",")[0] for line in pools} >= {"sem_one", "sem_two", "sem_timeout", "sem_lifetime"}
    assert re.search(rf"^connect_timeout,{CONNECT_TIMEOUT_MS},", out, re.M), out
    assert "Unsupported query against the admin database" in result.stderr, result.stderr


# The console answers an unknown command with severity FATAL yet keeps the
# connection; a client trusting the severity treats the session as dead.
@pytest.mark.xfail(
    raises=KnownBug, strict=True,
    reason="admin console reports unknown commands as FATAL instead of ERROR",
)
def test_admin_console_unknown_command_is_error(pgcat):
    console = admin(pgcat)
    with pytest.raises(psycopg.Error, match="Unsupported query against the admin database") as raised:
        console.execute("NOT A COMMAND")
    assert console.execute("SHOW VERSION").fetchone()[0].startswith("PgCat")
    severity = raised.value.diag.severity
    known_bug(severity == "FATAL", "unknown admin command reported as FATAL")
    assert severity == "ERROR"


# Pgcat answers the idle timeout with an ERROR and keeps the session, but
# rolls the transaction back underneath the client, so the client's next
# statement is reported failed yet runs in autocommit, and the client's
# COMMIT succeeds. Postgres ends the session instead (FATAL), so nothing commits.
@pytest.mark.xfail(
    raises=KnownBug, strict=True,
    reason="after idle_client_in_transaction_timeout the next statement is reported failed but commits",
)
def test_idle_in_transaction_timeout_commits_nothing(pgcat, table):
    client = psycopg2.connect(pgcat.dsn("sem_one"))
    cur = client.cursor()
    cur.execute("INSERT INTO pooler_semantics VALUES ('first')")
    time.sleep(IDLE_IN_TRANSACTION_MS / 1000 + 0.5)
    # Any error: a fix may close the session instead, as Postgres does.
    with pytest.raises(psycopg2.Error):
        cur.execute("INSERT INTO pooler_semantics VALUES ('second')")
    # In Postgres a COMMIT after an error in the transaction rolls back.
    try:
        client.commit()
    except psycopg2.Error:
        pass
    client.close()
    rows = table()
    known_bug(rows == ["second"], "statement reported failed after the timeout committed")
    # The transaction timed out, and the statement after it was reported
    # failed: neither may commit.
    assert rows == []


def backend_running(su, marker):
    return su.execute(
        "SELECT count(*) FROM pg_stat_activity"
        " WHERE state = 'active' AND query LIKE %s AND pid <> pg_backend_pid()",
        [f"%{marker}%"],
    ).fetchone()[0] > 0


TIMEOUT_ERRORS = {
    # Pgcat's own timeout, which closes the client connection.
    (b"FATAL", b"58000", b"pool statement timeout"),
    # What Postgres reports if a fix cancels the query instead.
    (b"ERROR", b"57014", b"canceling statement due to statement timeout"),
}


# statement_timeout makes pgcat give up on the server connection, but it
# doesn't cancel the query, which keeps running and commits after the client
# was told the connection closed.
@pytest.mark.xfail(
    raises=KnownBug, strict=True,
    reason="pool statement_timeout doesn't cancel the server query",
)
def test_statement_timeout_cancels_query(pgcat, table):
    su = psycopg.connect(PG, autocommit=True)
    marker = "statement_timeout_probe"
    client = connect(pgcat, "sem_timeout")
    send(client, f"INSERT INTO pooler_semantics SELECT 'late' FROM pg_sleep(1) /* {marker} */")
    wait_until(lambda: backend_running(su, marker), "the query to reach Postgres")

    res = results(client, "the statement timeout")[0]
    field = psycopg.pq.DiagnosticField
    error = (
        res.error_field(field.SEVERITY_NONLOCALIZED),
        res.error_field(field.SQLSTATE),
        res.error_field(field.MESSAGE_PRIMARY),
    )
    assert error in TIMEOUT_ERRORS, error

    wait_until(lambda: not backend_running(su, marker), "the backend query to end")
    rows = table()
    su.close()
    known_bug(rows == ["late"], "query committed after the client saw the timeout")
    assert rows == []
