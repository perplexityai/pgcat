"""
End-to-end tests for the SQL observer (`sql_observer_enabled`): the SQL
statements clients send, as pgcat exports them to Prometheus.

The pools use the test-only `testing` plugin, so these tests are skipped unless
PGCAT_TEST_PLUGINS=1 (see test_plugin_hooks.py).
"""

import os
import re
import signal
import struct
import threading
import time
import urllib.request
import uuid

import asyncio

import asyncpg
import psycopg
import psycopg2
import pytest

import utils

pytestmark = pytest.mark.skipif(
    os.environ.get("PGCAT_TEST_PLUGINS") != "1",
    reason="needs pgcat built with --features test-plugins and PGCAT_TEST_PLUGINS=1",
)

PORT = 6442
METRICS_PORT = 9942
# The Postgres the pools connect to (the repo fixtures use 5432).
PG_PORT = int(os.environ.get("PGCAT_TEST_PG_PORT", "5432"))
DB = "observed_db"
# Observing, without the query parser.
UNPARSED_DB = "observed_unparsed_db"
# The parser and plugins, but not observing.
UNOBSERVED_DB = "unobserved_db"
# The parser and plugins, observing, with the prepared statement cache on.
CACHED_DB = "observed_cached_db"
USER = "sharding_user"


def pool(name, settings, observed=True):
    return f"""
[pools.{name}]
pool_mode = "transaction"
sql_observer_enabled = {"true" if observed else "false"}
{settings}

[pools.{name}.users.0]
username = "{USER}"
password = "{USER}"
pool_size = 1
statement_timeout = 0

[pools.{name}.shards.0]
servers = [["127.0.0.1", {PG_PORT}, "primary"]]
database = "shard0"
"""


PLUGINS = """
query_parser_enabled = true
plugins_require_parse = true
identity_tenant_parameter = "app.tenant"

[pools.{name}.plugins.testing]
enabled = true
verify_identity = true
rewrites = {{ "select 'one'" = "SELECT 'one'; SELECT 'two'", "select * from pg_catalog.pg_class limit 0" = "SELECT * FROM pg_catalog.pg_namespace LIMIT 0", "select 'tbl'" = "SELECT relname FROM pg_catalog.pg_class LIMIT 1" }}

[pools.{name}.plugins.table_access]
enabled = true
tables = ["plugin_forbidden"]

[pools.{name}.plugins.intercept]
enabled = true

[pools.{name}.plugins.intercept.queries.0]
query = "select 'intercept me'"
schema = [["answer", "text"]]
result = [["from pgcat"]]
"""


COPY_PARAM = """
[pools.{name}.plugins.testing.copy_param]
table = "plugin_hooks_test"
from = "a"
to = "b"
"""


def general(port, metrics_port):
    return f"""
[general]
host = "127.0.0.1"
port = {port}
admin_username = "admin_user"
admin_password = "admin_pass"
enable_prometheus_exporter = true
prometheus_exporter_port = {metrics_port}
"""


CONFIG = (
    general(PORT, METRICS_PORT)
    + pool(DB, PLUGINS.format(name=DB))
    + pool(
        CACHED_DB,
        "prepared_statements_cache_size = 100\n"
        + PLUGINS.format(name=CACHED_DB)
        + COPY_PARAM.format(name=CACHED_DB),
    )
    + pool(UNPARSED_DB, "query_parser_enabled = false")
    + pool(UNOBSERVED_DB, PLUGINS.format(name=UNOBSERVED_DB), observed=False)
)


@pytest.fixture(scope="module")
def pgcat():
    process = utils.PgcatProcess(CONFIG, PORT).start()
    yield process
    process.stop()


RELATION_METRIC = "pgcat_sql_observer_relation_attempts_total"


def metrics_text(metrics_port):
    """The exporter's page. Each request is bounded, and a failed one is
    retried until a deadline, since the exporter starts listening after the
    pooler does."""
    deadline = time.time() + 10
    while True:
        try:
            url = f"http://127.0.0.1:{metrics_port}/metrics"
            with urllib.request.urlopen(url, timeout=5) as response:
                return response.read().decode()
        except OSError as error:
            if time.time() > deadline:
                raise AssertionError(f"no metrics on port {metrics_port}: {error}") from error
            time.sleep(0.1)


def scrape(metrics_port):
    """Every sample of the observer's metrics: {(name, labels): value}."""
    text = metrics_text(metrics_port)
    samples = {}
    for line in text.splitlines():
        if not line.startswith("pgcat_sql_observer_"):
            continue
        # Relation samples have labels this decoder can't split: see `relation_samples`.
        if line.startswith(RELATION_METRIC):
            continue
        series, value = line.rsplit(" ", 1)
        name, labels = series.rstrip("}").split("{", 1)
        samples[(name, frozenset(label.replace('"', "") for label in labels.split(",")))] = int(
            value
        )
    return samples


def observed(db, metrics_port=METRICS_PORT):
    """The pool's counts, or None if it doesn't export them."""
    samples = scrape(metrics_port)
    pool_labels = {f"pool={db}", f"user={USER}"}

    def sample(name, *labels):
        return samples.get((name, frozenset(pool_labels | set(labels))))

    observing = sample("pgcat_sql_observer_observing")
    counts = {
        "present": sample("pgcat_sql_observer_attempts_total", "identity=present"),
        "absent": sample("pgcat_sql_observer_attempts_total", "identity=absent"),
        "expired": sample("pgcat_sql_observer_attempts_total", "identity=expired"),
        "refused": sample("pgcat_sql_observer_attempts_total", "identity=refused"),
    }
    for reason in [
        "unparsed_query",
        "refused_unparsed",
        "unparsed_statement",
        "unknown_portal",
        "metadata_invalidated",
    ]:
        counts[reason] = sample("pgcat_sql_observer_gap_events_total", f"reason={reason}")
    return observing, counts


def delta(db, run, metrics_port=METRICS_PORT):
    """How much each count moved while `run` ran."""
    observing, before = observed(db, metrics_port)
    assert observing == 1
    run()
    observing, after = observed(db, metrics_port)
    assert observing == 1
    return {key: after[key] - before[key] for key in before if after[key] != before[key]}


def simple_conn(pgcat, db=DB):
    conn = psycopg2.connect(pgcat.dsn(db))
    conn.autocommit = True
    return conn, conn.cursor()


def tenant_token(name):
    return f"tenant:{name}#{uuid.uuid4()}"


def test_simple_queries_count_the_statements_sent(pgcat):
    conn, cur = simple_conn(pgcat)

    def run():
        cur.execute("SELECT 1; SELECT 2; SELECT 3")
        # Rewritten into two: counted as sent.
        cur.execute("SELECT 'one'")
        assert cur.fetchall() == [("two",)]
        # Answered by pgcat.
        cur.execute("SELECT 'intercept me'")
        # Refused by a plugin, or by the server: attempted all the same.
        with pytest.raises(psycopg2.Error, match="plugin_forbidden"):
            cur.execute("SELECT * FROM plugin_forbidden")
        with pytest.raises(psycopg2.errors.DivisionByZero):
            cur.execute("SELECT 1 / 0")

    assert delta(DB, run) == {"absent": 7}
    conn.close()


def test_identity_and_pgcat_own_sql_are_not_counted(pgcat):
    conn, cur = simple_conn(pgcat)

    def run():
        # pgcat sets app.tenant on the server, in transactions of its own.
        cur.execute(f"SET PGCAT_IDENTITY TO '{tenant_token('observed')}'")
        cur.execute("SHOW PGCAT_IDENTITY")
        cur.execute("SELECT current_setting('app.tenant')")
        assert cur.fetchone() == ("observed",)
        cur.execute("BEGIN")
        cur.execute("SELECT 1")
        cur.execute("COMMIT")
        cur.execute("RESET PGCAT_IDENTITY")
        cur.execute("SELECT 1")

    assert delta(DB, run) == {"present": 4, "absent": 1}
    conn.close()


def test_queries_pgcat_did_not_parse_are_gaps(pgcat):
    conn, cur = simple_conn(pgcat)

    def run():
        # Forwarded: the server answers.
        with pytest.raises(psycopg2.errors.SyntaxError):
            cur.execute("SELEKT 1")
        # Refused: they might touch a table a plugin checks, or set the tenant.
        with pytest.raises(psycopg2.Error, match="could not parse"):
            cur.execute("SELEKT * FROM plugin_forbidden")
        with pytest.raises(psycopg2.Error, match="set by pgcat"):
            cur.execute("SET app.tenant TO 'intruder'")

    assert delta(DB, run) == {"unparsed_query": 1, "refused_unparsed": 2}
    conn.close()


# Statement names live as long as the server connection, which the tests
# share: each test gets names of its own.
TEST_TOKEN = [b""]

# Valid SQL the parser doesn't parse.
UNPARSEABLE = b"SELECT 1 WINDOW w AS ()"


RAWS = []


@pytest.fixture(autouse=True)
def fresh_names():
    TEST_TOKEN[0] = b"_" + uuid.uuid4().hex[:8].encode()
    yield
    # A failed test must not keep the pools' one server connection.
    while RAWS:
        RAWS.pop().close()


def named(name):
    return name + TEST_TOKEN[0] if name else name


def msg_parse(name, query, types=()):
    name = named(name)
    return utils.frontend(
        b"P",
        name
        + b"\0"
        + query
        + b"\0"
        + struct.pack("!h", len(types))
        + b"".join(struct.pack("!i", oid) for oid in types),
    )


def msg_bind(portal, statement, params=()):
    portal, statement = named(portal), named(statement)
    body = portal + b"\0" + statement + b"\0" + struct.pack("!h", 0)
    body += struct.pack("!h", len(params))
    for param in params:
        body += struct.pack("!i", len(param)) + param
    return utils.frontend(b"B", body + struct.pack("!h", 0))


def msg_execute(portal, rows=0):
    portal = named(portal)
    return utils.frontend(b"E", portal + b"\0" + struct.pack("!i", rows))


def msg_close(kind, name):
    name = named(name)
    return utils.frontend(b"C", kind + name + b"\0")


def msg_describe(kind, name):
    return utils.frontend(b"D", kind + named(name) + b"\0")


SYNC = utils.frontend(b"S", b"")
FLUSH = utils.frontend(b"H", b"")


def first_column(row):
    """The text of the first column of a DataRow body."""
    (length,) = struct.unpack("!i", row[2:6])
    return row[6 : 6 + length].decode()


class Raw:
    """A protocol-level client to one pool. Every reply is checked: a send
    expects no ErrorResponse unless `error` says otherwise (True: any;
    a string: one containing it), so counts can't hide a broken reply."""

    def __init__(self, db, port=PORT):
        self.db = db
        self.sock, self.message = utils.raw_connection(port, db)
        self.sock.settimeout(8)
        RAWS.append(self)
        self.rows = []
        self.errors = []

    def read_to_ready(self, error=False):
        codes, self.rows, self.errors = [], [], []
        while True:
            code, body = self.message()
            codes.append(code)
            if code == b"D":
                self.rows.append(first_column(body))
            if code == b"E":
                self.errors.append(body)
            if code == b"Z":
                break
        if error is False:
            assert not self.errors, self.errors
        elif error is True:
            assert self.errors, codes
        else:
            assert any(error.encode() in e for e in self.errors), (error, self.errors)
        return codes, body

    def batch(self, *messages, sync=True, error=False):
        """Send messages and a Sync; read to ReadyForQuery. Returns the reply
        codes and the transaction status."""
        self.sock.sendall(b"".join(messages) + (SYNC if sync else b""))
        return self.read_to_ready(error)

    def simple(self, query, error=False):
        self.sock.sendall(utils.frontend(b"Q", query.encode() + b"\0"))
        return self.read_to_ready(error)

    def read(self, count):
        """The next `count` replies (after a Flush)."""
        return [self.message()[0] for _ in range(count)]

    def identity_set(self):
        self.simple("SHOW PGCAT_IDENTITY")
        return self.rows == ["t"]

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


@pytest.mark.parametrize("db", [DB, CACHED_DB])
def test_extended_protocol_counts_the_first_execute_of_a_portal(pgcat, db):
    raw = Raw(db)

    # Rows fetched in two Executes: one statement.
    def run():
        codes, _ = raw.batch(
            msg_parse(b"", b"SELECT generate_series(1, 2)"),
            msg_bind(b"", b""),
            msg_execute(b"", 1),
            msg_execute(b"", 1),
        )
        assert codes == [b"1", b"2", b"D", b"s", b"D", b"s", b"Z"]
        assert raw.rows == ["1", "2"]

    assert delta(db, run) == {"absent": 1}

    # A transaction keeps the server (and the portal) across batches.
    raw.simple("BEGIN")
    stages = []

    def parse_and_bind():
        raw.batch(
            msg_parse(b"s", b"SELECT generate_series(1, 3)"),
            msg_bind(b"p", b"s"),
            msg_describe(b"P", b"p"),
        )

    stages.append(delta(db, parse_and_bind))

    def first_execute():
        codes, _ = raw.batch(msg_execute(b"p", 1))
        assert (codes, raw.rows) == ([b"D", b"s", b"Z"], ["1"])

    stages.append(delta(db, first_execute))

    def continuation():
        codes, _ = raw.batch(msg_execute(b"p", 1), msg_execute(b"p", 1))
        assert (codes, raw.rows) == ([b"D", b"s", b"D", b"s", b"Z"], ["2", "3"])

    stages.append(delta(db, continuation))

    def rebind():
        codes, _ = raw.batch(msg_close(b"P", b"p"), msg_bind(b"p", b"s"), msg_execute(b"p"))
        assert (codes, raw.rows) == ([b"3", b"2", b"D", b"D", b"D", b"C", b"Z"], ["1", "2", "3"])

    stages.append(delta(db, rebind))
    # P/B/Describe alone: zero. First E: one. More Executes: zero. A fresh Bind: one.
    assert stages == [{}, {"absent": 1}, {}, {"absent": 1}]

    # The unnamed statement replaced in the batch cannot retarget the earlier
    # portal's snapshot: p1 stays SELECT 1 (one), p2 is the empty query (none).
    def retarget():
        codes, _ = raw.batch(
            msg_parse(b"", b"SELECT 1"),
            msg_bind(b"p1", b""),
            msg_parse(b"", b""),
            msg_bind(b"p2", b""),
            msg_execute(b"p1"),
            msg_execute(b"p2"),
        )
        assert codes == [b"1", b"2", b"1", b"2", b"D", b"C", b"I", b"Z"], codes

    assert delta(db, retarget) == {"absent": 1}
    raw.simple("ROLLBACK")
    raw.close()


def test_psycopg_prepared_and_pgcat_answered_statements(pgcat):
    with psycopg.connect(pgcat.dsn(DB), autocommit=True) as conn:

        def run():
            assert conn.execute("SELECT %s::int", [1]).fetchone() == (1,)
            # Answered by pgcat, but SQL the client sent.
            assert conn.execute("SELECT 'intercept me'", prepare=True).fetchone() == (
                "from pgcat",
            )
            # Not SQL.
            assert conn.execute("SHOW PGCAT_IDENTITY", prepare=True).fetchone()[0] is False

        assert delta(DB, run) == {"absent": 2}


def test_unparsed_unknown_and_closed_executes_are_gaps(pgcat):
    raw = Raw(UNPARSED_DB)

    def run():
        codes, _ = raw.batch(msg_parse(b"", b"SELECT 1"), msg_bind(b"", b""), msg_execute(b""))
        assert (codes, raw.rows) == ([b"1", b"2", b"D", b"C", b"Z"], ["1"])

    assert delta(UNPARSED_DB, run) == {"unparsed_statement": 1}
    raw.close()

    raw = Raw(CACHED_DB)

    # A portal that was never bound (a SQL cursor's, say): the server refuses.
    def never_bound():
        raw.batch(msg_execute(b"nothing"), error="does not exist")

    assert delta(CACHED_DB, never_bound) == {"unknown_portal": 1}

    # Portals live as long as the transaction.
    raw.simple("BEGIN")

    def closed():
        raw.batch(msg_parse(b"s", b"SELECT 1"), msg_bind(b"p", b"s"))
        raw.batch(msg_close(b"P", b"p"), msg_execute(b"p"), error="does not exist")

    assert delta(CACHED_DB, closed) == {"metadata_invalidated": 1}
    raw.simple("ROLLBACK")
    raw.close()


def test_simple_query_destroys_the_unnamed_statement_and_portal(pgcat):
    raw = Raw(CACHED_DB)
    raw.simple("BEGIN")

    def run():
        raw.batch(msg_parse(b"", b"SELECT 1"), msg_bind(b"", b""))
        raw.simple("SELECT 2")
        assert raw.rows == ["2"]
        # The server destroyed the unnamed portal too.
        raw.batch(msg_execute(b""), error="does not exist")

    assert delta(CACHED_DB, run) == {"absent": 1, "metadata_invalidated": 1}
    raw.simple("ROLLBACK")
    raw.close()


def test_backend_error_traces(pgcat):
    raw = Raw(CACHED_DB)

    # Bad-parameter Bind, then Execute, Sync. Staged Executes stay counted.
    raw.simple("BEGIN")

    def run():
        codes, status = raw.batch(
            msg_parse(b"s", b"SELECT $1::int", [23]),
            msg_bind(b"p", b"s", [b"x"]),
            msg_execute(b"p"),
            error="invalid input syntax",
        )
        assert (codes, status) == ([b"1", b"E", b"Z"], b"E")
        # A backend error forgets nothing: the name the failed batch Parsed
        # still counts, in a state that isn't idle.
        raw.batch(msg_bind(b"p2", b"s", [b"1"]), msg_execute(b"p2"), error="aborted")

    assert delta(CACHED_DB, run) == {"absent": 2}
    raw.simple("ROLLBACK")

    # The same, idle: the statement stays known.
    def run():
        raw.batch(
            msg_parse(b"s", b"SELECT $1::int", [23]),
            msg_bind(b"p", b"s", [b"x"]),
            msg_execute(b"p"),
            error="invalid input syntax",
        )
        codes, _ = raw.batch(msg_bind(b"p2", b"s", [b"1"]), msg_execute(b"p2"))
        assert (codes, raw.rows) == ([b"2", b"D", b"C", b"Z"], ["1"])

    assert delta(CACHED_DB, run) == {"absent": 2}

    # A statement parsed before the failing batch stays known.
    raw.batch(msg_parse(b"t", b"SELECT $1::int", [23]))

    def run():
        raw.batch(msg_bind(b"q", b"t", [b"x"]), msg_execute(b"q"), error="invalid input syntax")
        codes, _ = raw.batch(msg_bind(b"q", b"t", [b"1"]), msg_execute(b"q"))
        assert (codes, raw.rows) == ([b"2", b"D", b"C", b"Z"], ["1"])

    assert delta(CACHED_DB, run) == {"absent": 2}

    # Bind, failing Execute, Bind, Execute, Sync: both Executes were admitted.
    raw.batch(msg_parse(b"d", b"SELECT 1/(g - g) FROM generate_series(1, 1) g"))

    def run():
        codes, _ = raw.batch(
            msg_bind(b"z", b"d"),
            msg_execute(b"z"),
            msg_bind(b"z", b"d"),
            msg_execute(b"z"),
            error="division by zero",
        )
        assert codes == [b"2", b"E", b"Z"]

    assert delta(CACHED_DB, run) == {"absent": 2}

    # A batch of only Parse, Bind and Close fails: what it staged stands.
    raw.simple("BEGIN")

    def run():
        raw.batch(msg_parse(b"a", b"SELECT 1"), msg_bind(b"pa", b"a"))
        raw.batch(
            msg_parse(b"b", b"SELECT 1"),
            msg_bind(b"pb", b"b", [b"surplus"]),
            msg_close(b"S", b"a"),
            error="bind message supplies",
        )
        # The portal the failed batch Bound counts as an attempt, though the
        # server never made it; the one it didn't touch counts too.
        raw.batch(msg_execute(b"pb"), error="does not exist")
        raw.batch(msg_execute(b"pa"), error="aborted")

    assert delta(CACHED_DB, run) == {"absent": 2}
    raw.simple("ROLLBACK")

    # Simple Query batches count each statement; the error is the server's.
    def run():
        raw.simple("SELECT 1; SELECT 1/0; SELECT 2", error="division by zero")

    assert delta(CACHED_DB, run) == {"absent": 3}
    raw.close()


# (statement, server status after the batch, the server's answer to the second
# portal: an error text, or the row it returns). Observed on PG 18: the portal
# the lifecycle statement ended is the server's business; the observer's
# attribution is what is checked. DEALLOCATE doesn't destroy a bound portal, so
# the server answers (the observer still treats it as lost: conservative).
GONE = "does not exist"
ENDERS = [
    (b"COMMIT", b"I", GONE),
    (b"ROLLBACK", b"I", GONE),
    (b"ROLLBACK TO SAVEPOINT sp", b"E", GONE),
    (b"COMMIT AND CHAIN", b"E", GONE),
    (b"ROLLBACK AND CHAIN", b"E", GONE),
    (b"CLOSE ALL", b"E", GONE),
    (b"DEALLOCATE ALL", b"T", ["1"]),
    (b"DISCARD ALL", b"E", "cannot run inside a transaction block"),
]
# DEALLOCATE ALL and DISCARD ALL also destroy the statements pgcat's own cache
# prepared on the server, so they run in a pool without the cache.
UNCACHED_ENDERS = {b"DEALLOCATE ALL", b"DISCARD ALL"}


@pytest.mark.parametrize("ender,status,answer", ENDERS)
def test_statements_that_end_portals_drop_them(pgcat, ender, status, answer):
    db = DB if ender in UNCACHED_ENDERS else CACHED_DB
    raw = Raw(db)
    raw.simple("BEGIN")
    raw.simple("SAVEPOINT sp")
    raw.batch(msg_parse(b"c", ender), msg_parse(b"s", b"SELECT 1"))

    def run():
        # The lifecycle statement is counted, then the second portal is lost.
        codes, final = raw.batch(
            msg_bind(b"pc", b"c"),
            msg_bind(b"ps", b"s"),
            msg_execute(b"pc"),
            msg_execute(b"ps"),
            error=answer if isinstance(answer, str) else False,
        )
        assert final == status
        if isinstance(answer, str):
            assert codes == [b"2", b"2", b"C", b"E", b"Z"] or (
                codes == [b"2", b"2", b"E", b"Z"] and ender == b"DISCARD ALL"
            )
            assert raw.rows == []
        else:
            assert codes == [b"2", b"2", b"C", b"D", b"C", b"Z"]
            assert raw.rows == answer

    assert delta(db, run) == {"absent": 1, "metadata_invalidated": 1}

    # Forward, on the same client: a fresh Parse, Bind and Execute count again
    # and return their row (a failed transaction is rolled back first).
    if status == b"E":
        raw.simple("ROLLBACK")

    def fresh():
        codes, _ = raw.batch(
            msg_parse(b"s2", b"SELECT 1"), msg_bind(b"p2", b"s2"), msg_execute(b"p2")
        )
        assert (codes, raw.rows) == ([b"1", b"2", b"D", b"C", b"Z"], ["1"])

    assert delta(db, fresh) == {"absent": 1}
    raw.close()


def test_release_savepoint_keeps_the_portal(pgcat):
    raw = Raw(CACHED_DB)
    raw.simple("BEGIN")
    raw.simple("SAVEPOINT sp")

    def run():
        raw.batch(msg_parse(b"r", b"RELEASE SAVEPOINT sp"), msg_parse(b"s", b"SELECT 1"))
        codes, _ = raw.batch(
            msg_bind(b"pr", b"r"),
            msg_bind(b"ps", b"s"),
            msg_execute(b"pr"),
            msg_execute(b"ps"),
        )
        assert (codes, raw.rows) == ([b"2", b"2", b"C", b"D", b"C", b"Z"], ["1"])
        # A named continuation after it: zero, and no gap.
        raw.batch(msg_execute(b"ps"))

    assert delta(CACHED_DB, run) == {"absent": 2}
    raw.simple("ROLLBACK")
    raw.close()


def test_unparsed_simple_query_drops_the_metadata(pgcat):
    raw = Raw(CACHED_DB)
    raw.simple("BEGIN")

    def run():
        raw.batch(msg_parse(b"s", b"SELECT 1"), msg_bind(b"p", b"s"))
        raw.simple("SELEKT nonsense", error="syntax error")
        raw.batch(msg_execute(b"p"), error="aborted")

    assert delta(CACHED_DB, run) == {"unparsed_query": 1, "metadata_invalidated": 1}
    raw.simple("ROLLBACK")
    raw.close()


def test_unparsed_statement_prepared_keeps_portals_until_executed(pgcat):
    raw = Raw(CACHED_DB)
    raw.simple("BEGIN")

    def run():
        raw.batch(msg_parse(b"a", b"SELECT 1"), msg_bind(b"pa", b"a"))
        # Prepared, described, bound: nothing counted, nothing dropped.
        codes, _ = raw.batch(
            msg_parse(b"u", UNPARSEABLE), msg_describe(b"S", b"u"), msg_bind(b"pu", b"u")
        )
        assert codes[0] == b"1"
        # The named portal is still counted at its first Execute.
        raw.batch(msg_execute(b"pa"))
        assert raw.rows == ["1"]
        # The unparsed one: a gap, and everything else is cleared.
        raw.batch(msg_execute(b"pu"))
        assert raw.rows == ["1"]
        raw.batch(msg_execute(b"pa"))
        assert raw.rows == []

    counts = delta(CACHED_DB, run)
    assert counts == {"absent": 1, "unparsed_statement": 1, "metadata_invalidated": 1}
    raw.simple("ROLLBACK")
    raw.close()


def test_idle_drops_portals_but_not_statements(pgcat):
    raw = Raw(CACHED_DB)

    def run():
        raw.batch(msg_parse(b"s", b"SELECT 1"), msg_bind(b"p", b"s"))
        # The batch ended the implicit transaction: the portal is gone.
        raw.batch(msg_execute(b"p"), error="does not exist")
        codes, _ = raw.batch(msg_bind(b"p", b"s"), msg_execute(b"p"))
        assert (codes, raw.rows) == ([b"2", b"D", b"C", b"Z"], ["1"])

    # Idle is a plain end: the lookup finds nothing, and nothing was forgotten.
    assert delta(CACHED_DB, run) == {"absent": 1, "unknown_portal": 1}
    raw.close()


def test_flush_boundaries_for_continuing_clients(pgcat):
    raw = Raw(CACHED_DB)

    # Parse, Describe, Flush: zero.
    def prepare():
        raw.sock.sendall(msg_parse(b"s", b"SELECT 1") + msg_describe(b"S", b"s") + FLUSH)
        assert raw.read(3) == [b"1", b"t", b"T"]

    assert delta(CACHED_DB, prepare) == {}

    # Bind and Flush in a transaction: zero; the first Execute counts.
    raw.simple("BEGIN")

    def bound():
        raw.sock.sendall(msg_bind(b"p", b"s") + FLUSH)
        assert raw.read(1) == [b"2"]

    assert delta(CACHED_DB, bound) == {}

    def executed():
        codes, _ = raw.batch(msg_execute(b"p"))
        assert (codes, raw.rows) == ([b"D", b"C", b"Z"], ["1"])

    assert delta(CACHED_DB, executed) == {"absent": 1}
    raw.simple("COMMIT")

    # Outside a transaction the Flush's Sync ends the implicit one, and the
    # RFQ it strips still removes the portal.
    def idle_by_flush():
        raw.sock.sendall(msg_bind(b"p", b"s") + FLUSH)
        assert raw.read(1) == [b"2"]
        raw.batch(msg_execute(b"p"), error="does not exist")

    assert delta(CACHED_DB, idle_by_flush) == {"unknown_portal": 1}
    raw.close()


def test_continuation_is_not_reclassified_by_later_identity_changes(pgcat):
    raw = Raw(CACHED_DB)
    raw.simple("BEGIN")

    def run():
        raw.batch(
            msg_parse(b"s", b"SELECT generate_series(1, 5)"),
            msg_bind(b"p", b"s"),
            msg_execute(b"p", 2),
        )
        assert raw.rows == ["1", "2"]
        # Identity changes between fetches (a pgcat command, not SQL).
        raw.simple(f"SET PGCAT_IDENTITY TO '{tenant_token('fetcher')}'")
        raw.batch(msg_execute(b"p", 2))
        assert raw.rows == ["3", "4"]
        raw.simple("RESET PGCAT_IDENTITY")
        raw.batch(msg_execute(b"p", 2))
        assert raw.rows == ["5"]

    # Counted once, as absent: the identity was taken at the first Execute.
    assert delta(CACHED_DB, run) == {"absent": 1}
    raw.simple("ROLLBACK")
    raw.close()


def test_reset_all_and_identity_with_the_observer_on(pgcat):
    raw = Raw(CACHED_DB)
    raw.simple(f"SET PGCAT_IDENTITY TO '{tenant_token('resetter')}'")
    assert raw.identity_set()

    # Identity commands, in either protocol, are not SQL attempts.
    def commands():
        raw.simple("SHOW PGCAT_IDENTITY")
        raw.batch(
            msg_parse(b"i", b"SHOW PGCAT_IDENTITY"),
            msg_bind(b"pi", b"i"),
            msg_execute(b"pi"),
        )
        assert raw.rows == ["t"]

    assert delta(CACHED_DB, commands) == {}

    # RESET ALL is SQL the parser reads. Bound, not executed: nothing is
    # counted and the identity stays.
    def bound():
        raw.batch(msg_parse(b"r", b"RESET ALL"), msg_bind(b"pr", b"r"))

    assert delta(CACHED_DB, bound) == {}
    assert raw.identity_set()

    # Bound in one batch, executed in the next (a transaction keeps the
    # portal).
    raw.simple("BEGIN")
    raw.batch(msg_parse(b"r", b"RESET ALL"))
    raw.sock.sendall(msg_bind(b"pr", b"r") + FLUSH)
    assert raw.read(1) == [b"2"]
    assert raw.identity_set()

    def executed():
        codes, _ = raw.batch(msg_execute(b"pr"))
        assert codes == [b"C", b"Z"]

    # Executed: counted under the identity it ran with, and the identity goes
    # with the session reset.
    assert delta(CACHED_DB, executed) == {"present": 1}
    assert not raw.identity_set()
    raw.simple("COMMIT")

    # After it, statements are counted without an identity.
    def after():
        raw.simple("SELECT 1")
        assert raw.rows == ["1"]

    assert delta(CACHED_DB, after) == {"absent": 1}

    # Simple protocol: counted under the identity it ran with, and the
    # identity goes.
    raw.simple(f"SET PGCAT_IDENTITY TO '{tenant_token('resetter')}'")
    assert delta(CACHED_DB, lambda: raw.simple("RESET ALL")) == {"present": 1}
    assert not raw.identity_set()
    raw.close()


@pytest.fixture(scope="module")
def hook_table():
    conn = psycopg2.connect(f"postgres://{USER}:{USER}@127.0.0.1:{PG_PORT}/shard0")
    conn.autocommit = True
    conn.cursor().execute(
        "DROP TABLE IF EXISTS plugin_hooks_test;"
        "CREATE TABLE plugin_hooks_test (id serial PRIMARY KEY, a text, b text)"
    )
    yield conn
    conn.close()


def rows_with(conn, value):
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM plugin_hooks_test WHERE a = %s", [value])
    return cur.fetchone()[0]


INSERT = b"INSERT INTO plugin_hooks_test (a) VALUES ($1)"


def test_refusal_before_checkout_keeps_the_staged_count(pgcat, hook_table):
    value = uuid.uuid4().hex
    raw = Raw(CACHED_DB)
    raw.batch(msg_parse(b"ins", INSERT, [25]))

    # The hook refuses the Bind before checkout: the Execute was admitted.
    def refused():
        codes, _ = raw.batch(
            msg_bind(b"p", b"ins", [b"no-server"]),
            msg_execute(b"p"),
            error="before checkout",
        )
        assert codes == [b"E", b"Z"]

    assert delta(CACHED_DB, refused) == {"absent": 1}
    assert rows_with(hook_table, "no-server") == 0

    # The statement survived: a retry runs, and counts anew.
    def retried():
        codes, _ = raw.batch(msg_bind(b"p", b"ins", [value.encode()]), msg_execute(b"p"))
        assert codes == [b"2", b"C", b"Z"]

    assert delta(CACHED_DB, retried) == {"absent": 1}
    assert rows_with(hook_table, value) == 1
    raw.close()


def test_flush_refusal_keeps_the_count_and_discards_the_tail(pgcat, hook_table):
    value = uuid.uuid4().hex
    raw = Raw(CACHED_DB)
    raw.batch(msg_parse(b"ins", INSERT, [25]))

    def refused():
        raw.sock.sendall(
            msg_bind(b"p", b"ins", [b"no-server"]) + msg_execute(b"p") + FLUSH
        )
        assert raw.read(1) == [b"E"]
        # Everything until the Sync is ignored: no count, no gap.
        raw.sock.sendall(msg_execute(b"p") + msg_execute(b"p") + SYNC)
        codes = raw.read(1)
        assert codes == [b"Z"]

    assert delta(CACHED_DB, refused) == {"absent": 1}

    def retried():
        codes, _ = raw.batch(msg_bind(b"p", b"ins", [value.encode()]), msg_execute(b"p"))
        assert codes == [b"2", b"C", b"Z"]

    assert delta(CACHED_DB, retried) == {"absent": 1}
    assert rows_with(hook_table, value) == 1
    raw.close()


def test_batch_mixing_local_and_server_statements_is_rejected_as_a_whole(pgcat):
    raw = Raw(CACHED_DB)

    def refused():
        raw.batch(
            msg_parse(b"loc", b"SELECT 'intercept me'"),
            msg_parse(b"srv", b"SELECT 1"),
            msg_bind(b"pl", b"loc"),
            msg_bind(b"ps", b"srv"),
            msg_execute(b"pl"),
            msg_execute(b"ps"),
            error="their own Sync batch",
        )

    # Both Executes were admitted.
    assert delta(CACHED_DB, refused) == {"absent": 2}

    # In a batch of its own, the pgcat-answered statement runs and counts.
    def alone():
        codes, _ = raw.batch(
            msg_parse(b"loc", b"SELECT 'intercept me'"),
            msg_bind(b"pl", b"loc"),
            msg_execute(b"pl"),
        )
        assert (codes, raw.rows) == ([b"1", b"2", b"D", b"C", b"Z"], ["from pgcat"])

    assert delta(CACHED_DB, alone) == {"absent": 1}
    raw.close()


def test_refused_parse_contributes_nothing_but_its_simple_counterpart_counts(pgcat):
    raw = Raw(CACHED_DB)

    def refused_parse():
        codes, _ = raw.batch(
            msg_parse(b"bad", b"SELECT * FROM plugin_forbidden"),
            msg_bind(b"pb", b"bad"),
            msg_execute(b"pb"),
            error="plugin_forbidden",
        )
        assert codes == [b"E", b"Z"]

    assert delta(CACHED_DB, refused_parse) == {}

    def refused_query():
        raw.simple("SELECT * FROM plugin_forbidden", error="plugin_forbidden")

    assert delta(CACHED_DB, refused_query) == {"absent": 1}
    raw.close()


def test_cached_statements_past_the_bounds_are_still_counted(pgcat):
    # With the cache on, a named statement is classified on the pool's shared
    # Parse, so the 128-statement and 128-byte bounds don't apply to it;
    # portals and own entries are still bounded. (With the cache off the
    # bounds apply to every statement: see the next test.)
    raw = Raw(CACHED_DB)

    def run():
        # An Execute counted before the overflow stays counted.
        codes, _ = raw.batch(
            msg_parse(b"first", b"SELECT 1"),
            msg_bind(b"pf", b"first"),
            msg_execute(b"pf"),
            # 129 statements are past the 128 the observer keeps.
            *[msg_parse(b"o%d" % i, b"SELECT 1") for i in range(129)],
        )
        assert codes[-1] == b"Z" and codes.count(b"1") == 130 and codes.count(b"C") == 1
        assert raw.rows == ["1"]
        # An old name is still counted, and the SQL works.
        codes, _ = raw.batch(msg_bind(b"p", b"o0"), msg_execute(b"p"))
        assert (codes, raw.rows) == ([b"2", b"D", b"C", b"Z"], ["1"])

    assert delta(CACHED_DB, run) == {"absent": 2}

    # A fresh Parse and Bind count too.
    def fresh():
        codes, _ = raw.batch(
            msg_parse(b"n", b"SELECT 2"), msg_bind(b"p", b"n"), msg_execute(b"p")
        )
        assert (codes, raw.rows) == ([b"1", b"2", b"D", b"C", b"Z"], ["2"])

    assert delta(CACHED_DB, fresh) == {"absent": 1}

    # A name past 128 bytes: counted from its shared Parse.
    long_name = b"n" * 200

    def long():
        codes, _ = raw.batch(
            msg_parse(long_name, b"SELECT 3"), msg_bind(b"p", long_name), msg_execute(b"p")
        )
        assert (codes, raw.rows) == ([b"1", b"2", b"D", b"C", b"Z"], ["3"])

    assert delta(CACHED_DB, long) == {"absent": 1}
    raw.close()


def test_overflowing_the_bounds_forgets_without_failing_the_client(pgcat):
    # With the cache off the observer keeps every statement itself, so its
    # bounds apply: past them it forgets, and the SQL still works. The
    # transaction keeps the statements on the server between batches.
    raw = Raw(DB)
    raw.simple("BEGIN")

    def run():
        codes, _ = raw.batch(
            msg_parse(b"first", b"SELECT 1"),
            msg_bind(b"pf", b"first"),
            msg_execute(b"pf"),
            # 129 statements are past the 128 the observer keeps.
            *[msg_parse(b"o%d" % i, b"SELECT 1") for i in range(129)],
        )
        assert codes[-1] == b"Z" and codes.count(b"1") == 130 and codes.count(b"C") == 1
        # An old name without metadata is a gap, and the SQL works.
        codes, _ = raw.batch(msg_bind(b"p", b"o0"), msg_execute(b"p"))
        assert (codes, raw.rows) == ([b"2", b"D", b"C", b"Z"], ["1"])

    # Not idle, so the observer still knows why the name is missing.
    assert delta(DB, run) == {"absent": 1, "metadata_invalidated": 1}

    # A name past 128 bytes: forgotten, and its Execute is a gap.
    long_name = b"n" * 200

    def long():
        codes, _ = raw.batch(
            msg_parse(long_name, b"SELECT 3"), msg_bind(b"pl", long_name), msg_execute(b"pl")
        )
        assert (codes, raw.rows) == ([b"1", b"2", b"D", b"C", b"Z"], ["3"])

    assert delta(DB, long) == {"metadata_invalidated": 1}
    raw.simple("ROLLBACK")
    raw.close()


def test_exact_limits_snapshots_and_the_identity_taken_at_execute(pgcat):
    raw = Raw(CACHED_DB)

    # Exactly 128 statements fit: no overflow, nothing forgotten.
    def at_limit():
        raw.batch(*[msg_parse(b"l%d" % i, b"SELECT 1") for i in range(128)])
        codes, _ = raw.batch(msg_bind(b"p", b"l0"), msg_execute(b"p"))
        assert (codes, raw.rows) == ([b"2", b"D", b"C", b"Z"], ["1"])

    assert delta(CACHED_DB, at_limit) == {"absent": 1}
    # (That client's table is full: the rest is on a new client.)
    raw.close()
    raw = Raw(CACHED_DB)

    # An Execute counts the statement the portal was bound to, even after the
    # statement is closed and the name reused.
    raw.simple("BEGIN")

    def replaced():
        raw.batch(msg_parse(b"s", b"SELECT 1"), msg_bind(b"p", b"s"))
        raw.batch(msg_close(b"S", b"s"), msg_parse(b"s", b"SELECT 2"))
        codes, _ = raw.batch(msg_execute(b"p"))
        assert (codes, raw.rows) == ([b"D", b"C", b"Z"], ["1"])

    assert delta(CACHED_DB, replaced) == {"absent": 1}
    raw.simple("ROLLBACK")

    # The identity is the one at the Execute, not at Parse or Bind.
    raw.simple("BEGIN")

    def identity_at_execute():
        raw.batch(msg_parse(b"i", b"SELECT 1"), msg_bind(b"pi", b"i"))
        raw.simple(f"SET PGCAT_IDENTITY TO '{tenant_token('late')}'")
        raw.batch(msg_execute(b"pi"))
        assert raw.rows == ["1"]

    assert delta(CACHED_DB, identity_at_execute) == {"present": 1}
    raw.simple("ROLLBACK")
    raw.close()


def test_asyncpg_prepared_statement_flow_is_counted_per_execute(pgcat):
    async def use():
        conn = await asyncpg.connect(pgcat.dsn(CACHED_DB), statement_cache_size=0)
        try:
            # Parse, Describe and Flush: nothing counted. Then Bind and Execute.
            statement = await conn.prepare("SELECT $1::int")
            assert await statement.fetchval(1) == 1
            assert await statement.fetchval(2) == 2
            async with conn.transaction():
                seen = []
                async for row in conn.cursor("SELECT generate_series(1, 5)", prefetch=2):
                    seen.append(row[0])
                    if len(seen) == 3:
                        # The identity changes between fetches.
                        await conn.execute(f"SET PGCAT_IDENTITY TO '{tenant_token('cursor')}'")
                assert seen == [1, 2, 3, 4, 5]
        finally:
            await conn.close()

    # Two Executes of the statement, the cursor's portal (once), and the simple
    # Queries BEGIN and COMMIT, the last under the identity set mid-cursor.
    assert delta(CACHED_DB, lambda: asyncio.run(use())) == {"absent": 4, "present": 1}


def test_without_the_parser_each_query_is_one_gap(pgcat):
    conn, cur = simple_conn(pgcat, UNPARSED_DB)

    def run():
        cur.execute("SELECT 1; SELECT 2")
        # The ones after the first, inside the transaction, too.
        cur.execute("BEGIN")
        cur.execute("SELECT 1")
        cur.execute("COMMIT")

    assert delta(UNPARSED_DB, run) == {"unparsed_query": 4}
    conn.close()


def test_pools_not_observing_say_so(pgcat):
    conn, cur = simple_conn(pgcat, UNOBSERVED_DB)
    cur.execute("SELECT 1; SELECT 2")
    conn.close()

    observing, counts = observed(UNOBSERVED_DB)
    assert observing == 0
    # No counts: zero would read as no traffic.
    assert set(counts.values()) == {None}


RELOAD_PORT = 6443
RELOAD_METRICS_PORT = 9943
RELOAD_DB = "reloaded_db"


def reload_config(observed):
    return general(RELOAD_PORT, RELOAD_METRICS_PORT) + pool(
        RELOAD_DB, PLUGINS.format(name=RELOAD_DB), observed=observed
    )


def test_reload_turns_observing_on_and_off_and_keeps_counts():
    process = utils.PgcatProcess(reload_config(False), RELOAD_PORT).start()

    def reload(observed):
        process.write_config(reload_config(observed))
        process.process.send_signal(signal.SIGHUP)
        deadline = time.time() + 10
        while observed_now() != int(observed):
            assert time.time() < deadline, process.logs()
            time.sleep(0.05)

    def observed_now():
        return observed(RELOAD_DB, RELOAD_METRICS_PORT)[0]

    def query(sql):
        conn, cur = simple_conn(process, RELOAD_DB)
        cur.execute(sql)
        conn.close()

    def not_observing():
        observing, counts = observed(RELOAD_DB, RELOAD_METRICS_PORT)
        return observing == 0 and set(counts.values()) == {None}

    try:
        query("SELECT 1")
        assert not_observing()

        reload(True)
        query("SELECT 1; SELECT 2")
        _, counts = observed(RELOAD_DB, RELOAD_METRICS_PORT)
        assert counts["absent"] == 2

        reload(False)
        query("SELECT 1")
        assert not_observing()

        # The pool was replaced twice; its counts carried on, without what it
        # wasn't observing.
        reload(True)
        _, counts = observed(RELOAD_DB, RELOAD_METRICS_PORT)
        assert counts["absent"] == 2
    finally:
        process.stop()


RACE_PORT = 6444
RACE_METRICS_PORT = 9944
RACE_DB = "race_db"


def race_config(parser):
    return general(RACE_PORT, RACE_METRICS_PORT) + pool(
        RACE_DB, f"query_parser_enabled = {'true' if parser else 'false'}"
    )


def test_reload_enabling_the_parser_between_queries_records_each_query_once():
    """pgcat refreshes a client's pool settings before it plans each
    message, so the client's next simple Query after a reload that enabled
    the parser is parsed with it. That is one query, so one observation:
    its statements, not a gap as well."""
    process = utils.PgcatProcess(race_config(False), RACE_PORT).start()

    def counts():
        return observed(RACE_DB, RACE_METRICS_PORT)[1]

    try:
        conn, cur = simple_conn(process, RACE_DB)
        cur.execute("SELECT 1")
        assert counts()["unparsed_query"] == 1

        process.write_config(race_config(True))
        utils.admin_reload(RACE_PORT)

        # Planned with the settings the reload brought: statements, no gap.
        cur.execute("SELECT 1; SELECT 2")
        after = counts()
        assert after["absent"] == 2 and after["present"] == 0
        assert after["unparsed_query"] == 1
        assert after["refused_unparsed"] == 0

        # And the same client's next query.
        cur.execute("SELECT 1; SELECT 2")
        after = counts()
        assert after["absent"] == 4 and after["unparsed_query"] == 1
        conn.close()
    finally:
        process.stop()


# ---------------------------------------------------------------------------
# Relations (Stage C): `relation_attempts` by the table names seen.
# ---------------------------------------------------------------------------

PGC = "pg_class"
PGN = "pg_namespace"
HOOK = "plugin_hooks_test"
# A table name that needs quotes, with a quote, a backslash, a comma and a
# newline in it, to pin the metric's escaping.
ODD = '"T,1\\""\n"'
NOEXP = "no_explicit_relation"
FIXED = {"other", "unknown", NOEXP}

LABEL = re.compile(r'(\w+)="((?:[^"\\]|\\.)*)"')
ESCAPES = {"n": "\n", "\\": "\\", '"': '"'}


def unescape(value):
    return re.sub(r"\\(.)", lambda m: ESCAPES.get(m.group(1), m.group(0)), value)


def relation_samples(db, metrics_port=METRICS_PORT):
    """{(relation, identity): value} of the pool's relation series, with the
    label values decoded (escaped quotes, backslashes, commas, newlines)."""
    text = metrics_text(metrics_port)
    samples = {}
    for line in text.splitlines():
        if not line.startswith(RELATION_METRIC + "{"):
            continue
        series, value = line.rsplit(" ", 1)
        labels = {k: unescape(v) for k, v in LABEL.findall(series)}
        assert set(labels) == {"pool", "user", "relation", "identity"}, line
        if labels["pool"] == db:
            assert labels["user"] == USER
            key = (labels["relation"], labels["identity"])
            assert key not in samples, line
            samples[key] = int(value)
    return samples


def rdelta(db, run, metrics_port=METRICS_PORT):
    """(global and gap delta, relation delta by (relation, identity)). A name
    seen for the first time starts at 0; no series ever goes away."""
    before = relation_samples(db, metrics_port)
    counts = delta(db, run, metrics_port)
    after = relation_samples(db, metrics_port)
    assert set(before) <= set(after)
    return counts, {k: v - before.get(k, 0) for k, v in after.items() if v != before.get(k, 0)}


def test_relations_label_simple_queries_from_the_original_statements(pgcat):
    conn, cur = simple_conn(pgcat)

    def run():
        cur.execute("SELECT 1; SELECT * FROM PG_CATALOG.PG_CLASS LIMIT 1")
        cur.execute(
            "SELECT c.oid FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON true LIMIT 1"
        )
        # The schema is dropped: u is pg_class, again.
        cur.execute("SELECT * FROM pg_catalog.pg_class c, pg_class u LIMIT 1")
        cur.execute("SELECT * FROM pg_catalog.pg_am LIMIT 1")
        # Rewritten into two statements, counted as the one sent.
        cur.execute("SELECT 'one'")
        # Original A rewritten to B: labels A only. A constant rewritten to a table: still none.
        cur.execute("SELECT * FROM pg_catalog.pg_class LIMIT 0")
        cur.execute("SELECT 'tbl'")
        with pytest.raises(psycopg2.Error, match="plugin_forbidden"):
            cur.execute("SELECT * FROM plugin_forbidden")
        with pytest.raises(psycopg2.errors.DivisionByZero):
            cur.execute("SELECT 1 / 0")

    counts, relations = rdelta(DB, run)
    assert counts == {"absent": 10}
    assert relations == {
        (PGC, "absent"): 4,
        (PGN, "absent"): 1,
        ("pg_am", "absent"): 1,
        # Refused by a plugin, but admitted as the client wrote it.
        ("plugin_forbidden", "absent"): 1,
        (NOEXP, "absent"): 4,
    }
    conn.close()


@pytest.mark.parametrize("db", [DB, CACHED_DB])
def test_relations_label_the_first_execute_of_a_portal(pgcat, db):
    raw = Raw(db)
    raw.simple("BEGIN")
    sql = b"SELECT * FROM pg_catalog.pg_class WHERE oid > $1 LIMIT 3"
    stages = []

    def prepare_and_bind():
        raw.batch(
            msg_parse(b"s", sql, [23]),
            msg_bind(b"p", b"s", [b"0"]),
            msg_describe(b"P", b"p"),
        )

    stages.append(rdelta(db, prepare_and_bind))

    def first():
        raw.batch(msg_execute(b"p", 1))

    stages.append(rdelta(db, first))

    def more():
        raw.batch(msg_execute(b"p", 1), msg_execute(b"p", 1))

    stages.append(rdelta(db, more))

    def rebind():
        raw.batch(msg_close(b"P", b"p"), msg_bind(b"p", b"s", [b"0"]), msg_execute(b"p", 1))

    stages.append(rdelta(db, rebind))
    assert stages == [
        ({}, {}),
        ({"absent": 1}, {(PGC, "absent"): 1}),
        ({}, {}),
        ({"absent": 1}, {(PGC, "absent"): 1}),
    ]

    # The portal keeps the statement it was bound to, after the name is reused;
    # the identity is the one at its first Execute.
    def reused():
        raw.batch(msg_bind(b"q", b"s", [b"0"]))
        raw.batch(msg_close(b"S", b"s"), msg_parse(b"s", b"SELECT 1"))
        raw.simple(f"SET PGCAT_IDENTITY TO '{tenant_token('late')}'")
        raw.batch(msg_execute(b"q", 1))

    assert rdelta(db, reused) == ({"present": 1}, {(PGC, "present"): 1})

    # An original table query rewritten to another table: the original is labeled.
    def rewritten():
        raw.batch(
            msg_parse(b"w", b"SELECT * FROM pg_catalog.pg_class LIMIT 0"),
            msg_bind(b"pw", b"w"),
            msg_execute(b"pw"),
        )

    assert rdelta(db, rewritten) == ({"present": 1}, {(PGC, "present"): 1})
    raw.simple("ROLLBACK")
    raw.close()


def test_relations_after_refusal_flush_error_and_lifecycle(pgcat, hook_table):
    raw = Raw(CACHED_DB)
    raw.batch(msg_parse(b"ins", INSERT.replace(b"INTO ", b"INTO public."), [25]))

    def refused():
        codes, _ = raw.batch(
            msg_bind(b"p", b"ins", [b"no-server"]),
            msg_execute(b"p"),
            error="before checkout",
        )
        assert codes == [b"E", b"Z"]

    # Admitted, so counted, although never run.
    assert rdelta(CACHED_DB, refused) == ({"absent": 1}, {(HOOK, "absent"): 1})

    def retried():
        raw.batch(msg_bind(b"p", b"ins", [uuid.uuid4().hex.encode()]), msg_execute(b"p"))

    assert rdelta(CACHED_DB, retried) == ({"absent": 1}, {(HOOK, "absent"): 1})

    def flush_refused():
        raw.sock.sendall(msg_bind(b"p", b"ins", [b"no-server"]) + msg_execute(b"p") + FLUSH)
        assert raw.read(1) == [b"E"]
        raw.sock.sendall(msg_execute(b"p") + SYNC)
        assert raw.read(1) == [b"Z"]

    assert rdelta(CACHED_DB, flush_refused) == ({"absent": 1}, {(HOOK, "absent"): 1})
    raw.close()

    # A backend error invalidates what the batch touched: a gap, no relation.
    raw = Raw(CACHED_DB)
    raw.batch(
        msg_parse(b"a", b"SELECT * FROM pg_catalog.pg_class LIMIT 1"),
        msg_parse(b"z", b"SELECT 1 / 0"),
    )

    def errored():
        raw.batch(
            msg_bind(b"pa", b"a"),
            msg_bind(b"pz", b"z"),
            msg_execute(b"pz"),
            error=True,
        )

    assert rdelta(CACHED_DB, errored) == ({"absent": 1}, {(NOEXP, "absent"): 1})

    def invalidated():
        raw.batch(msg_execute(b"pa"), error=True)

    counts, relations = rdelta(CACHED_DB, invalidated)
    assert relations == {}
    assert counts == {"unknown_portal": 1}, counts  # B's gap, unchanged

    # A lifecycle statement is counted, the portal it ended is a gap only.
    raw.simple("ROLLBACK")
    raw.simple("BEGIN")
    raw.batch(msg_parse(b"c", b"COMMIT"), msg_parse(b"s", b"SELECT * FROM pg_catalog.pg_class LIMIT 1"))

    def lifecycle():
        raw.batch(
            msg_bind(b"pc", b"c"),
            msg_bind(b"ps", b"s"),
            msg_execute(b"pc"),
            msg_execute(b"ps"),
            error=GONE,
        )

    assert rdelta(CACHED_DB, lifecycle) == (
        {"absent": 1, "metadata_invalidated": 1},
        {(NOEXP, "absent"): 1},
    )

    def fresh():
        raw.batch(
            msg_parse(b"s2", b"SELECT * FROM pg_catalog.pg_class LIMIT 1"),
            msg_bind(b"p2", b"s2"),
            msg_execute(b"p2"),
        )

    assert rdelta(CACHED_DB, fresh) == ({"absent": 1}, {(PGC, "absent"): 1})
    raw.close()


# The two cache-off error paths below are pinned as documented, not endorsed:
# see docs/sql-observer-design.md#refused-and-failed-batches-stand. A change
# to either must update these tests and that doc on purpose.

FORBIDDEN = b"SELECT * FROM plugin_forbidden"


def test_a_refused_rebind_counts_the_server_portal_again(pgcat):
    # In a transaction, so the server keeps the portal between batches.
    raw = Raw(DB)
    raw.simple("BEGIN")
    stages = []

    def first():
        codes, _ = raw.batch(
            msg_parse(b"s", b"SELECT generate_series(1, 3)"),
            msg_bind(b"p", b"s"),
            msg_execute(b"p", 1),
        )
        assert (codes, raw.rows) == ([b"1", b"2", b"D", b"s", b"Z"], ["1"])

    stages.append(delta(DB, first))

    def continued():
        codes, _ = raw.batch(msg_execute(b"p", 1))
        assert (codes, raw.rows) == ([b"D", b"s", b"Z"], ["2"])

    stages.append(delta(DB, continued))

    # The forbidden Parse makes pgcat refuse the whole batch, so the server
    # never sees the rebind, and the refusal fails the transaction.
    def refused_rebind():
        _, status = raw.batch(
            msg_bind(b"p", b"s"), msg_parse(b"x", FORBIDDEN), error="plugin_forbidden"
        )
        assert status == b"E"

    stages.append(delta(DB, refused_rebind))

    # The observer kept the refused Bind as a fresh portal, so continuing the
    # server's counted portal counts again: a failed attempt, not a second run.
    def executed():
        _, status = raw.batch(msg_execute(b"p", 1), error="current transaction is aborted")
        assert status == b"E"

    stages.append(delta(DB, executed))
    assert stages == [{"absent": 1}, {}, {}, {"absent": 1}]
    raw.simple("ROLLBACK")
    raw.close()


@pytest.mark.parametrize("failure", ["refused", "failed"])
def test_a_redefinition_the_server_never_made_is_attributed_to_the_new_sql(pgcat, failure):
    # In a transaction, so the server keeps the statements between batches.
    # The savepoint lets it run them again after the failed batch.
    raw = Raw(DB)
    raw.simple("BEGIN")
    raw.batch(
        msg_parse(b"s1", b"SELECT 'A' FROM pg_catalog.pg_class LIMIT 1"),
        msg_parse(b"int", b"SELECT $1::int", [23]),
    )
    raw.simple("SAVEPOINT sp")
    redefine = [
        msg_close(b"S", b"s1"),
        msg_parse(b"s1", b"SELECT 'B' FROM pg_catalog.pg_namespace LIMIT 1"),
    ]
    if failure == "refused":
        # pgcat refuses the batch at the forbidden Parse: the server sees none of it.
        raw.batch(*redefine, msg_parse(b"x", FORBIDDEN), error="plugin_forbidden")
    else:
        # The server fails the Bind and skips the rest of the batch.
        raw.batch(msg_bind(b"x", b"int", [b"nan"]), *redefine, error="invalid input syntax")
    raw.simple("ROLLBACK TO SAVEPOINT sp")

    # PostgreSQL kept s1 as A, but the observer counts B's table.
    def executed():
        raw.batch(msg_bind(b"p", b"s1"), msg_execute(b"p"))
        assert raw.rows == ["A"]

    assert rdelta(DB, executed) == ({"absent": 1}, {(PGN, "absent"): 1})

    # Until s1 is defined again.
    def redefined():
        raw.batch(
            msg_close(b"S", b"s1"),
            msg_parse(b"s1", b"SELECT 'A' FROM pg_catalog.pg_class LIMIT 1"),
            msg_bind(b"p2", b"s1"),
            msg_execute(b"p2"),
        )
        assert raw.rows == ["A"]

    assert rdelta(DB, redefined) == ({"absent": 1}, {(PGC, "absent"): 1})
    raw.simple("ROLLBACK")
    raw.close()


# With the cache on, a batch pgcat refuses puts the client's map of
# prepared statements back, and the observer's label for a statement whose
# shared classification isn't the client's (the unnamed one, one a plugin
# rewrote) is kept with it. So after [Close s, Parse s=B, <refused>, Sync] the
# statement runs A and is counted as A, named, unnamed or rewritten.
REFUSED_A = b"SELECT 'A' FROM pg_catalog.pg_namespace LIMIT 1"
REFUSED_B = {
    "plain": b"SELECT 'B' FROM pg_catalog.pg_class LIMIT 1",
    # Rewritten to read pg_namespace: the client's own SQL reads pg_class.
    "rewritten": b"SELECT * FROM pg_catalog.pg_class LIMIT 0",
}


@pytest.mark.parametrize("name", [b"s", b""], ids=["named", "unnamed"])
@pytest.mark.parametrize("b", ["plain", "rewritten"])
def test_cached_refused_redefinition_is_counted_as_what_runs(pgcat, name, b):
    raw = Raw(CACHED_DB)
    raw.batch(msg_parse(name, REFUSED_A))

    def refused():
        closes = [msg_close(b"S", name)] if name else []
        codes, _ = raw.batch(
            *closes,
            msg_parse(name, REFUSED_B[b]),
            msg_parse(b"x", b"SELECT * FROM plugin_forbidden"),
            error="plugin_forbidden",
        )
        assert codes == [b"E", b"Z"]

    assert rdelta(CACHED_DB, refused) == ({}, {})

    def executed():
        raw.batch(msg_bind(b"p", name), msg_execute(b"p"))
        assert raw.rows == ["A"]

    assert rdelta(CACHED_DB, executed) == ({"absent": 1}, {(PGN, "absent"): 1})
    raw.close()


# With the cache on, a batch the server fails was sent: what it parsed stands
# in the client's map, as before. The server skipped the unnamed
# statement's redefinition, and pgcat prepares B again at the next Bind: B
# runs and is counted.
def test_cached_failed_redefinition_is_counted_as_what_runs(pgcat):
    raw = Raw(CACHED_DB)
    raw.batch(msg_parse(b"", REFUSED_A))

    def failed():
        raw.batch(
            msg_parse(b"z", b"SELECT 1 / 0"),
            msg_bind(b"pz", b"z"),
            msg_execute(b"pz"),
            msg_parse(b"", REFUSED_B["plain"]),
            error="division by zero",
        )

    assert rdelta(CACHED_DB, failed) == ({"absent": 1}, {(NOEXP, "absent"): 1})

    def executed():
        raw.batch(msg_bind(b"", b""), msg_execute(b""))
        assert raw.rows == ["B"]

    assert rdelta(CACHED_DB, executed) == ({"absent": 1}, {(PGC, "absent"): 1})
    raw.close()


# With the cache on, the unnamed statement stays in the client's map, and pgcat
# still binds it under its cached name (a simple Query destroys only the
# server's unnamed statement, not pgcat's renamed one): it runs and is counted.
def test_cached_unnamed_statement_survives_a_simple_query(pgcat):
    raw = Raw(CACHED_DB)
    raw.batch(msg_parse(b"", REFUSED_A))
    raw.simple("SELECT 2")

    def executed():
        raw.batch(msg_bind(b"", b""), msg_execute(b""))
        assert raw.rows == ["A"]

    assert rdelta(CACHED_DB, executed) == ({"absent": 1}, {(PGN, "absent"): 1})
    raw.close()


def test_relations_seventeen_names_and_bare_labels(pgcat):
    tables = [
        "pg_class", "pg_namespace", "pg_am", "pg_attribute", "pg_type", "pg_proc",
        "pg_database", "pg_index", "pg_constraint", "pg_trigger", "pg_operator",
        "pg_language", "pg_collation", "pg_extension", "pg_tablespace", "pg_cast",
        "pg_description",
    ]  # fmt: skip
    conn, cur = simple_conn(pgcat)

    # pgcat only reads the FROM list; WHERE false keeps PostgreSQL from running the 17-way join.
    def run():
        cur.execute("SELECT 1 FROM " + ", ".join(f"pg_catalog.{t}" for t in tables) + " WHERE false")

    # Too many relations to name: one unknown, and no gap or error.
    assert rdelta(DB, run) == ({"absent": 1}, {("unknown", "absent"): 1})

    # Names fold as PostgreSQL folds them; one that needs quotes keeps them
    # (decoded exactly here), and one named like a fixed label is quoted.
    def odd():
        with pytest.raises(psycopg2.errors.UndefinedTable):
            cur.execute('SELECT * FROM "Odd""S\\ch"."T,1\\""\n"')
        with pytest.raises(psycopg2.errors.UndefinedTable):
            cur.execute("SELECT * FROM Public.Other")
        cur.execute("SELECT * FROM PG_CATALOG.PG_AM LIMIT 1")

    assert rdelta(DB, odd) == (
        {"absent": 3},
        {(ODD, "absent"): 1, ('"other"', "absent"): 1, ("pg_am", "absent"): 1},
    )
    conn.close()

    # Every label is a bare name, a quoted one or a fixed label, for every
    # identity state; there are none for a pool that doesn't observe.
    samples = relation_samples(DB)
    labels = {relation for relation, _ in samples}
    assert {PGC, PGN, "pg_am", ODD, '"other"'} | FIXED <= labels
    assert all(re.fullmatch(r"[a-z_][a-z0-9_$]*", r) or r.startswith('"') for r in labels)
    assert {identity for _, identity in samples} == {"present", "absent", "expired", "refused"}
    assert relation_samples(UNOBSERVED_DB) == {}


def test_identity_states_label_both_metrics_and_log_callers(pgcat):
    raw = Raw(DB)
    query = "SELECT * FROM pg_catalog.pg_type LIMIT 1"

    def statement():
        raw.simple(query)

    # Expired: a token that is already past its expiry is forgotten before the
    # next statement is counted, and stays expired.
    raw.simple(f"SET PGCAT_IDENTITY TO 'expires:1:tenant:gone#{uuid.uuid4()}'")
    assert rdelta(DB, statement) == ({"expired": 1}, {("pg_type", "expired"): 1})
    assert not raw.identity_set()
    assert rdelta(DB, statement) == ({"expired": 1}, {("pg_type", "expired"): 1})

    # Refused: the last SET was refused, until the next successful one.
    raw.simple("SET PGCAT_IDENTITY TO 'invalid'", error="invalid identity token")
    assert rdelta(DB, statement) == ({"refused": 1}, {("pg_type", "refused"): 1})
    assert not raw.identity_set()
    raw.simple(f"SET PGCAT_IDENTITY TO '{tenant_token('back')}'")
    assert rdelta(DB, statement) == ({"present": 1}, {("pg_type", "present"): 1})

    # A reset makes it absent.
    raw.simple("SET PGCAT_IDENTITY TO 'invalid'", error="invalid identity token")
    raw.simple("RESET PGCAT_IDENTITY")
    assert rdelta(DB, statement) == ({"absent": 1}, {("pg_type", "absent"): 1})
    client = "%s:%d" % raw.sock.getsockname()[:2]
    raw.close()

    # Each (relation, state) other than present is logged once, with the
    # client's application_name (pgcat's default: this client sends none) and address.
    deadline = time.time() + 10
    pattern = re.compile(
        r'relation "pg_type" first counted with identity (\w+): application_name "([^"]*)", client (\S+)'
    )
    while sorted(state for state, _, _ in pattern.findall(pgcat.logs())) != ["absent", "expired", "refused"]:
        assert time.time() < deadline, pattern.findall(pgcat.logs())
        time.sleep(0.05)
    assert {(name, address) for _, name, address in pattern.findall(pgcat.logs())} == {("pgcat", client)}


def test_identity_states_label_extended_executes_until_a_reset(pgcat):
    raw = Raw(DB)

    def execute():
        raw.batch(
            msg_parse(b"", b"SELECT * FROM pg_catalog.pg_range LIMIT 1"),
            msg_bind(b"", b""),
            msg_execute(b""),
        )

    def lapse(state):
        if state == "expired":
            raw.simple(f"SET PGCAT_IDENTITY TO 'expires:1:tenant:gone#{uuid.uuid4()}'")
        else:
            raw.simple("SET PGCAT_IDENTITY TO 'invalid'", error="invalid identity token")

    # An Execute is labeled with the state at its admission, and DISCARD ALL
    # or RESET ALL clears either lapse.
    for state, reset in [
        ("expired", "DISCARD ALL"),
        ("refused", "DISCARD ALL"),
        ("expired", "RESET ALL"),
        ("refused", "RESET ALL"),
    ]:
        lapse(state)
        assert rdelta(DB, execute) == ({state: 1}, {("pg_range", state): 1}), (state, reset)
        assert rdelta(DB, execute) == ({state: 1}, {("pg_range", state): 1}), (state, reset)
        raw.simple(reset)
        assert rdelta(DB, execute) == ({"absent": 1}, {("pg_range", "absent"): 1}), (state, reset)
    raw.close()


def test_relations_are_not_counted_without_the_parser_or_the_observer(pgcat):
    conn, cur = simple_conn(pgcat, UNPARSED_DB)

    def run():
        cur.execute("SELECT * FROM pg_catalog.pg_class LIMIT 1")

    assert rdelta(UNPARSED_DB, run) == ({"unparsed_query": 1}, {})
    conn.close()
    conn, cur = simple_conn(pgcat, UNOBSERVED_DB)
    cur.execute("SELECT * FROM pg_catalog.pg_class LIMIT 1")
    assert relation_samples(UNOBSERVED_DB) == {}
    conn.close()


CHANGE_PORT = 6445
CHANGE_METRICS_PORT = 9945


def change_config(cache_size):
    return general(CHANGE_PORT, CHANGE_METRICS_PORT) + pool(
        RELOAD_DB,
        f"prepared_statements_cache_size = {cache_size}\n" + PLUGINS.format(name=RELOAD_DB),
    )


def test_reload_keeps_the_names_their_slots_and_old_metadata():
    process = utils.PgcatProcess(change_config(100), CHANGE_PORT).start()
    raw = None

    def rd(run):
        return rdelta(RELOAD_DB, run, CHANGE_METRICS_PORT)

    try:
        raw = Raw(RELOAD_DB, CHANGE_PORT)
        raw.batch(msg_parse(b"old", b"SELECT * FROM pg_catalog.pg_class LIMIT 1"))
        assert rd(lambda: raw.batch(msg_bind(b"p", b"old"), msg_execute(b"p"))) == (
            {"absent": 1},
            {(PGC, "absent"): 1},
        )
        before = relation_samples(RELOAD_DB, CHANGE_METRICS_PORT)

        # A reload that changes the pool keeps every name and count.
        process.write_config(change_config(50))
        utils.admin_reload(CHANGE_PORT)
        assert relation_samples(RELOAD_DB, CHANGE_METRICS_PORT) == before

        # A statement prepared before the reload still counts by name.
        assert rd(lambda: raw.batch(msg_bind(b"p2", b"old"), msg_execute(b"p2"))) == (
            {"absent": 1},
            {(PGC, "absent"): 1},
        )
        raw.batch(msg_parse(b"new", b"SELECT * FROM pg_catalog.pg_namespace LIMIT 1"))
        assert rd(lambda: raw.batch(msg_bind(b"p3", b"new"), msg_execute(b"p3"))) == (
            {"absent": 1},
            {(PGN, "absent"): 1},
        )
    finally:
        if raw:
            raw.close()
        process.stop()
