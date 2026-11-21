"""A separate executable registers a plugin that pools turn on and configure in
their own `[plugins.<name>]` section, through the public library API."""
import os
import signal
import struct
import subprocess
import tempfile
import threading
import time
import urllib.request

import psycopg
import psycopg2
import pytest
from psycopg import pq

import utils
# no_inherited_otel is autouse: no pgcat started here inherits OTEL_ variables.
from test_trace_context import (  # noqa: F401
    ENV,
    STATUS_ERROR,
    Collector,
    comment,
    new_trace,
    no_inherited_otel,
    only,
)

PORT = 6457
RELOAD_PORT = 6473
BINARY = os.environ.get(
    "PGCAT_CONFIGURED_PLUGIN_BINARY", "./target/debug/examples/configured_plugin"
)
STANDALONE = os.environ.get("PGCAT_BINARY", "./target/debug/pgcat")
# Core's own build, whichever binary PGCAT_BINARY runs the other tests
# against: a build that adds its own plugins can run this file with its own.
CORE = os.environ.get("PGCAT_CORE_BINARY", "./target/debug/pgcat")
SENTINEL = "configured-plugin-sentinel-5f0c"
pytestmark = pytest.mark.skipif(
    os.environ.get("PGCAT_TEST_PLUGINS") != "1",
    reason="requires the test-only configured_plugin example",
)


def pool(name, settings, section=None):
    config = f"""
[pools.{name}]
pool_mode = "transaction"
prepared_statements_cache_size = 100
plugin_hook_timeout = 1000
{settings}

[pools.{name}.users.0]
username = "sharding_user"
password = "sharding_user"
pool_size = 1

[pools.{name}.shards.0]
servers = [["127.0.0.1", 5432, "primary"]]
database = "shard0"
"""
    if section is not None:
        config += f"\n[pools.{name}.plugins.synthetic]\n{section}\n"
    return config


def config(port, answer=5, section=None):
    if section is None:
        section = f"""enabled = true
answer = {answer}
tables = ["configured_gated"]
note = "{SENTINEL}"
"""
    return (
        f"""
[general]
host = "127.0.0.1"
port = {port}
admin_username = "admin_user"
admin_password = "admin_pass"
"""
        + pool(
            "configured",
            "query_parser_enabled = true\nplugins_require_parse = true",
            section,
        )
        + pool("plain", "query_parser_enabled = false")
    )


@pytest.fixture(scope="module")
def pgcat():
    process = utils.PgcatProcess(config(PORT), PORT, log_level="trace", binary=BINARY)
    try:
        process.start()
        yield process
    finally:
        process.stop()


def run(dsn, sql, extended):
    if extended:
        with psycopg.connect(dsn, autocommit=True) as connection:
            return connection.execute(sql, prepare=True).fetchone()[0]
    connection = psycopg2.connect(dsn)
    try:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchone()[0]
    finally:
        connection.close()


def run_without_rows(dsn, sql, extended):
    """Run a statement that returns no rows."""
    if extended:
        with psycopg.connect(dsn, autocommit=True) as connection:
            connection.execute(sql, prepare=True)
        return
    connection = psycopg2.connect(dsn)
    try:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute(sql)
    finally:
        connection.close()


def sqlstate(dsn, sql, extended):
    error = psycopg.Error if extended else psycopg2.Error
    with pytest.raises(error) as raised:
        run(dsn, sql, extended)
    exception = raised.value
    return exception.sqlstate if extended else exception.pgcode


@pytest.mark.parametrize("extended", [False, True])
def test_runs_only_on_the_pool_that_turns_it_on(pgcat, extended):
    configured = pgcat.dsn("configured")
    plain = pgcat.dsn("plain")

    assert run(configured, "SELECT 8001", extended) == 5
    assert sqlstate(configured, "SELECT 8002", extended) == "42501"

    assert run(plain, "SELECT 8001", extended) == 8001
    assert run(plain, "SELECT 8002", extended) == 8002
    # Not every pool parses every statement: the server answers this one.
    assert sqlstate(plain, "not valid sql", extended) == "42601"


@pytest.mark.parametrize("extended", [False, True])
def test_unparsed_statements_are_refused(pgcat, extended):
    configured = pgcat.dsn("configured")
    # Whatever they name: the plugin sees every statement the server runs.
    assert sqlstate(configured, "selec * from configured_gated", extended) == "0A000"
    assert sqlstate(configured, "selec * from somewhere_else", extended) == "0A000"
    # RESET can't touch a table: it's sent, as before.
    run_without_rows(configured, "RESET statement_timeout", extended)
    # Other pools' are the server's to answer.
    assert sqlstate(pgcat.dsn("plain"), "selec * from somewhere_else", extended) == "42601"


UNPARSED_PORT = 6470
# Past this length pgcat doesn't parse a statement.
PADDED = "SELECT 8002" + " " * 100


@pytest.mark.parametrize("extended", [False, True])
def test_only_a_table_scoped_plugin_lets_unparsed_statements_through(extended):
    long_queries = "query_parser_enabled = true\nquery_parser_max_length = 64"
    section = 'enabled = true\nanswer = 5\ntables = ["configured_gated"]'
    toml = (
        f"""
[general]
host = "127.0.0.1"
port = {UNPARSED_PORT}
admin_username = "admin_user"
admin_password = "admin_pass"
"""
        + pool("refusing", long_queries + "\nplugins_require_parse = true", section)
        # The refusal doesn't depend on plugins_require_parse.
        + pool("unrequired", long_queries, section)
        + pool(
            "scoped",
            long_queries + "\nplugins_require_parse = true",
            section + "\ntable_scoped = true",
        )
    )
    process = utils.PgcatProcess(toml, UNPARSED_PORT, binary=BINARY)
    try:
        process.start()
        for name in ("refusing", "unrequired"):
            dsn = process.dsn(name)
            # Parsed, the plugin refuses it; unparsed, so does pgcat.
            assert sqlstate(dsn, "SELECT 8002", extended) == "42501"
            assert sqlstate(dsn, PADDED, extended) == "0A000"
            assert sqlstate(dsn, "SELEKT 1", extended) == "0A000"
            run_without_rows(dsn, "RESET statement_timeout", extended)

        # A table-scoped plugin refuses only those naming its tables: others
        # reach the server without its hooks.
        scoped = process.dsn("scoped")
        assert sqlstate(scoped, "SELECT 8002", extended) == "42501"
        assert run(scoped, PADDED, extended) == 8002
        assert sqlstate(scoped, "SELEKT 1", extended) == "42601"
        assert sqlstate(scoped, "selec * from configured_gated", extended) == "0A000"
    finally:
        process.stop()


def test_logs_show_whether_it_is_on_never_its_settings(pgcat):
    run(pgcat.dsn("configured"), "SELECT 8001", False)
    logs = pgcat.logs()
    assert '"synthetic": true' in logs
    assert SENTINEL not in logs


SENSITIVE_PORT = 6477
# Routing that reads sharding keys from the query: its literals and a comment.
SHARDED = r"""query_parser_enabled = true
query_parser_read_write_splitting = true
primary_reads_enabled = true
automatic_sharding_key = "configured_sharded.id"
sharding_key_regex = '/\* sharding_key: (\d+) \*/'"""


def test_sensitive_queries_keep_their_sharding_keys_out_of_the_logs():
    toml = (
        f"""
[general]
host = "127.0.0.1"
port = {SENSITIVE_PORT}
admin_username = "admin_user"
admin_password = "admin_pass"
"""
        + pool("sensitive", SHARDED, "enabled = true\nanswer = 5\nsensitive = true")
        + pool("routed", SHARDED, "enabled = true\nanswer = 5")
    )
    process = utils.PgcatProcess(toml, SENSITIVE_PORT, log_level="trace", binary=BINARY)
    try:
        process.start()
        sensitive = process.dsn("sensitive")
        with psycopg.connect(sensitive, autocommit=True) as connection:
            connection.execute("CREATE TABLE IF NOT EXISTS configured_sharded (id bigint)")
        for extended in (False, True):
            run(sensitive, "SELECT count(*) FROM configured_sharded WHERE id = 123456789", extended)
            run(sensitive, "/* sharding_key: 987654321 */ SELECT 1", extended)
        with psycopg.connect(sensitive, autocommit=True) as connection:
            connection.execute(
                "SELECT count(*) FROM configured_sharded WHERE id = %s", (555666777,)
            ).fetchone()
        logs = process.logs()
        # Routing read the keys, and logged none of them.
        assert "Automatically using shard" in logs
        assert "Found one sharding key" in logs
        for value in ("123456789", "987654321", "555666777"):
            assert value not in logs

        # A pool whose queries aren't sensitive logs them, as upstream does.
        routed = process.dsn("routed")
        run(routed, "SELECT count(*) FROM configured_sharded WHERE id = 111222333", False)
        run(routed, "/* sharding_key: 444555666 */ SELECT 1", False)
        logs = process.logs()
        assert "Sharding keys found: [Value(111222333)]" in logs
        assert "Setting sharding_key to 444555666" in logs
        with psycopg.connect(routed, autocommit=True) as connection:
            connection.execute("DROP TABLE configured_sharded")
    finally:
        process.stop()


def test_reload_applies_the_new_section_and_keeps_it_on_errors():
    process = utils.PgcatProcess(config(RELOAD_PORT), RELOAD_PORT, binary=BINARY)
    try:
        process.start()
        with psycopg.connect(process.dsn("configured"), autocommit=True) as connection:
            assert connection.execute("SELECT 8001", prepare=True).fetchone()[0] == 5

            process.write_config(config(RELOAD_PORT, answer=6))
            utils.admin_reload(RELOAD_PORT)
            # The statement prepared before is planned again with the new section.
            assert connection.execute("SELECT 8001", prepare=True).fetchone()[0] == 6
            assert connection.execute("SELECT 8001").fetchone()[0] == 6

            # A section the plugin refuses keeps the previous config.
            process.write_config(
                config(RELOAD_PORT, section="enabled = true\nanswer = 7\nansw = 8")
            )
            with pytest.raises((AssertionError, ConnectionError)):
                utils.admin_reload(RELOAD_PORT)
            assert connection.execute("SELECT 8001", prepare=True).fetchone()[0] == 6

            # Off, it no longer rewrites, prepared or not.
            process.write_config(
                config(RELOAD_PORT, section="enabled = false\nanswer = 7")
            )
            utils.admin_reload(RELOAD_PORT)
            assert connection.execute("SELECT 8001", prepare=True).fetchone()[0] == 8001
            assert connection.execute("SELECT 8001").fetchone()[0] == 8001
        assert "Config reload error" in process.logs()
    finally:
        process.stop()


ANSWERED_PORT = 6467


def test_reload_refuses_a_statement_the_plugin_answered():
    process = utils.PgcatProcess(config(ANSWERED_PORT), ANSWERED_PORT, binary=BINARY)

    def assert_refused(pgconn, name):
        """An ERROR, not a FATAL a driver such as pgx closes the connection on,
        and the connection is ready for the next statement."""
        result = pgconn.exec_prepared(name, [])
        assert result.status == pq.ExecStatus.FATAL_ERROR
        severity = result.error_field(pq.DiagnosticField.SEVERITY_NONLOCALIZED)
        assert severity == b"ERROR"
        assert result.error_field(pq.DiagnosticField.SQLSTATE) == b"XX000"
        assert b"prepared before the plugin configuration changed" in (
            result.error_message
        )
        assert pgconn.status == pq.ConnStatus.OK
        assert pgconn.transaction_status == pq.TransactionStatus.IDLE
    try:
        process.start()
        with psycopg.connect(process.dsn("configured"), autocommit=True) as connection:
            # Parse, then Bind, Describe, Execute and Sync: pgcat answers both
            # with what the plugin returned at Parse.
            pgconn = connection.pgconn
            pgconn.prepare(b"answered", b"SELECT 8009")
            assert pgconn.exec_prepared(b"answered", []).get_value(0, 0) == b"5"
            assert pgconn.exec_prepared(b"answered", []).get_value(0, 0) == b"5"

            process.write_config(config(ANSWERED_PORT, answer=6))
            utils.admin_reload(ANSWERED_PORT)
            # The answer came from the old section: refused, not replayed.
            assert_refused(pgconn, b"answered")
            # Prepared again, the plugin answers with the new section.
            pgconn.prepare(b"again", b"SELECT 8009")
            assert pgconn.exec_prepared(b"again", []).get_value(0, 0) == b"6"

            # Off, the statements it answered stay refused, and the server
            # answers what is prepared since.
            process.write_config(
                config(ANSWERED_PORT, section="enabled = false\nanswer = 7")
            )
            utils.admin_reload(ANSWERED_PORT)
            for name in (b"answered", b"again"):
                assert_refused(pgconn, name)
            pgconn.prepare(b"server", b"SELECT 8009")
            assert pgconn.exec_prepared(b"server", []).get_value(0, 0) == b"8009"
    finally:
        process.stop()


ANSWERED_PORTAL_PORT = 6472


def test_reload_refuses_a_portal_the_plugin_answered():
    """A portal of a statement the plugin answered, bound and flushed before a
    reload that changes the plugin's section, isn't executed with that answer:
    not as the first message after the reload, nor after another message
    picked up the reload, nor once the plugin is off."""
    port = ANSWERED_PORTAL_PORT
    process = utils.PgcatProcess(config(port), port, binary=BINARY)

    def frame(code, *fields, tail=b""):
        return utils.frontend(code, b"".join(field + b"\0" for field in fields) + tail)

    def parse(name, sql):
        return frame(b"P", name, sql, tail=struct.pack("!h", 0))

    def bind(portal, statement):
        return frame(b"B", portal, statement, tail=struct.pack("!hhh", 0, 0, 0))

    def execute(portal):
        return frame(b"E", portal, tail=struct.pack("!i", 0))

    flush, sync = utils.frontend(b"H", b""), utils.frontend(b"S", b"")
    stale = b"the plugin configuration changed after the statement was planned"

    try:
        process.start()
        sock, message = utils.raw_connection(port, "configured")
        sock.settimeout(10)

        def replies(*messages, count=None):
            """The replies up to ReadyForQuery, or the first `count`."""
            sock.sendall(b"".join(messages))
            received = [message()]
            while (count is None and received[-1][0] != b"Z") or (
                count is not None and len(received) < count
            ):
                received.append(message())
            return received

        def bound(statement, portal):
            """Prepared and bound, answered by pgcat at a Flush."""
            sent = [parse(statement, b"SELECT 8009"), bind(portal, statement), flush]
            assert [code for code, _ in replies(*sent, count=2)] == [b"1", b"2"]

        def refused(received):
            assert [code for code, _ in received] == [b"E", b"Z"], received
            assert stale in received[0][1], received

        def reload(**changes):
            process.write_config(config(port, **changes))
            utils.admin_reload(port)

        # The Execute is the first message after the reload.
        bound(b"first", b"first")
        reload(answer=6)
        refused(replies(execute(b"first"), sync))

        # A Parse after the reload planned with the new section, which answers
        # it; the portal bound before keeps being refused.
        bound(b"second", b"second")
        reload(answer=7)
        received = replies(parse(b"third", b"SELECT 8009"), execute(b"second"), sync)
        refused(received)

        # Turned off, the portal it answered isn't executed either.
        bound(b"fourth", b"fourth")
        reload(section="enabled = false\nanswer = 8")
        refused(replies(execute(b"fourth"), sync))

        # What's prepared and bound since runs: the server answers it.
        received = replies(
            parse(b"", b"SELECT 8009"), bind(b"", b""), execute(b""), sync
        )
        assert [code for code, _ in received] == [b"1", b"2", b"D", b"C", b"Z"]
        assert b"8009" in received[2][1]
        sock.close()
    finally:
        process.stop()


INHERIT_PORT = 6474


def inheriting_config(port, section):
    """A pool without its own `plugins`, running the global section."""
    return (
        f"""
[general]
host = "127.0.0.1"
port = {port}
admin_username = "admin_user"
admin_password = "admin_pass"

[plugins.synthetic]
{section}
"""
        + pool("inherits", "query_parser_enabled = true")
    )


def test_reload_applies_a_changed_global_section_to_inheriting_pools():
    process = utils.PgcatProcess(
        inheriting_config(INHERIT_PORT, "enabled = true\nanswer = 5"), INHERIT_PORT, binary=BINARY
    )
    try:
        process.start()
        with psycopg.connect(process.dsn("inherits"), autocommit=True) as connection:
            assert connection.execute("SELECT 8001", prepare=True).fetchone()[0] == 5

            process.write_config(inheriting_config(INHERIT_PORT, "enabled = true\nanswer = 6"))
            utils.admin_reload(INHERIT_PORT)
            assert connection.execute("SELECT 8001", prepare=True).fetchone()[0] == 6
            assert connection.execute("SELECT 8001").fetchone()[0] == 6

            process.write_config(inheriting_config(INHERIT_PORT, "enabled = false\nanswer = 6"))
            utils.admin_reload(INHERIT_PORT)
            assert connection.execute("SELECT 8001", prepare=True).fetchone()[0] == 8001
            assert connection.execute("SELECT 8001").fetchone()[0] == 8001
    finally:
        process.stop()


def start_fails(binary, toml, env=None):
    process = utils.PgcatProcess(toml, 0, binary=binary)
    result = subprocess.run(
        [binary, process.config_file.name],
        env=dict(os.environ, LOG_LEVEL="info", NO_COLOR="true", **(env or {})),
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    output = result.stdout + result.stderr
    assert result.returncode == 78, output
    assert SENTINEL not in output
    return output


@pytest.mark.parametrize("binary", [BINARY, STANDALONE])
def test_a_section_without_its_plugin_fails_the_config(binary):
    toml = config(0).replace("plugins.synthetic]", "plugins.unregistered]")
    output = start_fails(binary, toml)
    assert 'plugins."unregistered": no plugin of that name is registered' in output


def test_the_standalone_binary_refuses_the_plugin_section():
    output = start_fails(STANDALONE, config(0))
    assert 'plugins."synthetic": no plugin of that name is registered' in output


CORE_UNKNOWN_PORT = 6478
UNKNOWN_SECTION = "\n[pools.plain.plugins.unregistered]\nenabled = true\n"
UNKNOWN_REFUSED = 'plugins."unregistered": no plugin of that name is registered'


def core_config(port):
    """A pool with no section the standalone binary lacks a plugin for."""
    return f"""
[general]
host = "127.0.0.1"
port = {port}
admin_username = "admin_user"
admin_password = "admin_pass"
""" + pool("plain", "query_parser_enabled = true")


def test_core_refuses_an_unregistered_plugin_section():
    """Core's binary (the one core's images and deb ship) refuses a config that
    turns on a plugin it does not register, at startup and on reload, rather
    than run the pool without it."""
    assert UNKNOWN_REFUSED in start_fails(CORE, core_config(0) + UNKNOWN_SECTION)

    port = CORE_UNKNOWN_PORT
    process = utils.PgcatProcess(core_config(port), port, binary=CORE)
    try:
        process.start()
        with psycopg.connect(process.dsn("plain"), autocommit=True) as connection:
            process.write_config(core_config(port) + UNKNOWN_SECTION)
            with pytest.raises((AssertionError, ConnectionError)):
                utils.admin_reload(port)
            # The config in force stays, without the section.
            assert connection.execute("SELECT 1").fetchone()[0] == 1
        with psycopg.connect(process.dsn("plain"), autocommit=True) as connection:
            assert connection.execute("SELECT 1").fetchone()[0] == 1
        logs = process.logs()
        assert UNKNOWN_REFUSED in logs
        assert "Config reload error" in logs
    finally:
        process.stop()


@pytest.mark.parametrize(
    "section, message",
    [
        (f'enabled = true\nanswer = "five"\nnote = "{SENTINEL}"', 'plugins."synthetic":'),
        (f'enabled = true\nanswer = 5\nanswr = 5\nnote = "{SENTINEL}"', "unknown field"),
        (
            f'enabled = true\nanswer = 5\nneeds_parse = true\nnote = "{SENTINEL}"',
            'plugins."synthetic": needs plugins_require_parse',
        ),
    ],
)
def test_a_section_the_plugin_refuses_fails_the_config(section, message):
    toml = config(0, section=section).replace(
        "plugins_require_parse = true", "plugins_require_parse = false"
    )
    assert message in start_fails(BINARY, toml)


def test_an_enabled_section_needs_the_query_parser():
    toml = config(0) + f"""
[plugins.synthetic]
enabled = true
answer = 5
note = "{SENTINEL}"
"""
    output = start_fails(BINARY, toml)
    assert "query_parser_enabled = true" in output


IDENTITY_PORT = 6475
IDENTITY_METRICS_PORT = 9962
VERIFIER_RELOAD_PORT = 6462
REFUSED_TOKEN = "configured-plugin-token-sentinel-91d2"
# A second verifier for the "verified" pool.
TESTING_VERIFIER = """
[pools.verified.plugins.testing]
enabled = true
verify_identity = true
"""


def traced(config, collector):
    """`config`, its statements with a sampled traceparent exported to `collector`."""
    return config.replace(
        'admin_password = "admin_pass"\n',
        'admin_password = "admin_pass"\ntrace_sample_ratio = 1\n'
        f'trace_otlp_endpoint = "{collector.endpoint}"\n',
        1,
    )


def assert_refused_spans(spans, ended, sqlstate, unspanned=()):
    """Each statement of `ended`, at which pgcat ended the session, has one
    span, refused with `sqlstate`, and no other span is refused. Those of
    `unspanned` have none: the session ended at their Parse, before pgcat
    read the Execute that would get the span."""
    assert not {span["trace_id"] for span in spans} & set(unspanned)
    for trace in ended:
        span = only(spans, trace)
        assert span["status"] == STATUS_ERROR
        assert span["attributes"]["pgcat.refused"] is True
        assert span["attributes"]["error.type"] == sqlstate
        assert "pgcat.server.role" not in span["attributes"]
    refused = [span for span in spans if span["attributes"].get("pgcat.refused")]
    assert len(refused) == len(ended), refused


def identity_config(port, extra="", metrics_port=None):
    metrics = (
        f"enable_prometheus_exporter = true\nprometheus_exporter_port = {metrics_port}\n"
        if metrics_port
        else ""
    )
    return (
        f"""
[general]
host = "127.0.0.1"
port = {port}
admin_username = "admin_user"
admin_password = "admin_pass"
{metrics}"""
        + pool(
            "verified",
            'query_parser_enabled = true\nplugins_require_parse = true\n'
            'identity_tenant_parameter = "app.tenant"',
            f"enabled = true\nanswer = 1\nverify_identity = true\n{extra}",
        )
        + pool("plain", "query_parser_enabled = false")
    )


@pytest.fixture(scope="module")
def verifying_pgcat():
    process = utils.PgcatProcess(
        identity_config(IDENTITY_PORT, metrics_port=IDENTITY_METRICS_PORT),
        IDENTITY_PORT,
        log_level="trace",
        binary=BINARY,
    )
    try:
        process.start()
        yield process
    finally:
        process.stop()


def assert_absent(process, value):
    """`value` isn't in pgcat's log, admin `SHOW` output or `/metrics`."""
    assert value not in process.logs()
    # The admin console speaks only the simple protocol.
    with psycopg.connect(
        process.dsn("pgcat", "admin_user", "admin_pass"), autocommit=True, prepare_threshold=None
    ) as admin:
        for command in ("SHOW CLIENTS", "SHOW SERVERS", "SHOW POOLS", "SHOW STATS"):
            assert value not in repr(admin.execute(command).fetchall()), command
    url = f"http://127.0.0.1:{IDENTITY_METRICS_PORT}/metrics"
    with urllib.request.urlopen(url, timeout=5) as response:
        assert value not in response.read().decode()


def hook_errors(sqlstate, port=IDENTITY_METRICS_PORT):
    """`pgcat_hook_errors_total` for the synthetic verifier."""
    text = urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5).read().decode()
    labels = f'{{plugin="synthetic",hook="verify_identity",sqlstate="{sqlstate}"}} '
    values = [line.split()[1] for line in text.splitlines()
              if line.startswith("pgcat_hook_errors_total" + labels)]
    return int(values[0]) if values else 0


class Session:
    """One client connection: the simple protocol (psycopg2), or the extended
    one (psycopg, every statement prepared: Parse, then Bind and Execute)."""

    def __init__(self, dsn, extended):
        self.extended = extended
        self.statements = 0
        if extended:
            self.connection = psycopg.connect(dsn, autocommit=True)
        else:
            self.connection = psycopg2.connect(dsn)
            self.connection.autocommit = True

    def prepared(self, sql):
        """`sql`, parsed afresh. psycopg would run a statement it prepared
        before again, and with it the plugin's answer at its Parse."""
        self.statements += 1
        return self.connection.execute(f"{sql} -- {self.statements}", prepare=True)

    def value(self, sql):
        if self.extended:
            return self.prepared(sql).fetchone()[0]
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchone()[0]

    def run(self, sql):
        if self.extended:
            self.prepared(sql)
        else:
            with self.connection.cursor() as cursor:
                cursor.execute(sql)

    def error(self, sql):
        error = psycopg.Error if self.extended else psycopg2.Error
        with pytest.raises(error) as raised:
            self.run(sql)
        exception = raised.value
        code = exception.sqlstate if self.extended else exception.pgcode
        return code, str(exception)

    def row(self, sql):
        if self.extended:
            return tuple(self.prepared(sql).fetchone())
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            return tuple(cursor.fetchone())

    def close(self):
        self.connection.close()


def sent_by(connection, call):
    """The messages libpq sends during `call()`, by name, from its trace."""
    with tempfile.TemporaryFile("w+") as trace:
        connection.pgconn.trace(trace.fileno())
        connection.pgconn.set_trace_flags(pq.Trace.SUPPRESS_TIMESTAMPS)
        try:
            call()
        finally:
            connection.pgconn.untrace()
        trace.seek(0)
        return [line.split("\t")[2].strip() for line in trace if line.startswith("F\t")]


def test_extended_sessions_send_parse_bind_and_execute(verifying_pgcat):
    # psycopg sends a statement with no parameters as a simple Query unless
    # it's prepared.
    session = Session(verifying_pgcat.dsn("verified"), extended=True)
    try:
        sent = sent_by(
            session.connection, lambda: session.run("SET PGCAT_IDENTITY = 'synthetic:acme'")
        )
        assert sent == ["Parse", "Sync", "Bind", "Describe", "Execute", "Sync"]
        control = sent_by(session.connection, lambda: session.connection.execute("SELECT 8003"))
        assert control == ["Query"]
    finally:
        session.close()


@pytest.mark.parametrize("extended", [False, True])
def test_the_plugin_verifies_identities_on_its_pool(verifying_pgcat, extended):
    session = Session(verifying_pgcat.dsn("verified"), extended)
    try:
        assert session.value("SELECT 8003") == "none"
        session.run("SET PGCAT_IDENTITY = 'synthetic:acme'")
        # The plugin's claims reach its callbacks, and its tenant the server.
        assert session.value("SELECT 8003") == "acme"
        assert session.value("SELECT current_setting('app.tenant', true)") == "acme"

        code, message = session.error(f"SET PGCAT_IDENTITY = '{REFUSED_TOKEN}'")
        assert code == "28000"
        assert "PGCAT_IDENTITY refused: synthetic plugin refused the identity" in message
        assert REFUSED_TOKEN not in message
        # A refused token clears the identity the client had.
        assert session.value("SELECT 8003") == "none"
        assert session.value("SELECT current_setting('app.tenant', true)") in ("", None)
        assert_absent(verifying_pgcat, REFUSED_TOKEN)
    finally:
        session.close()
    logs = verifying_pgcat.logs()
    assert "Caller identity refused" in logs
    assert REFUSED_TOKEN not in logs


@pytest.mark.parametrize("extended", [False, True])
def test_pools_it_is_not_on_do_not_verify(verifying_pgcat, extended):
    session = Session(verifying_pgcat.dsn("plain"), extended)
    try:
        session.run(f"SET PGCAT_IDENTITY = '{REFUSED_TOKEN}'")
        # Kept as it is, shown by its fingerprint.
        identity_set, fingerprint = session.row("SHOW PGCAT_IDENTITY")
        assert identity_set is True
        assert fingerprint and REFUSED_TOKEN not in fingerprint
        assert session.value("SELECT 8003") == 8003
        assert_absent(verifying_pgcat, REFUSED_TOKEN)
    finally:
        session.close()
    assert REFUSED_TOKEN not in verifying_pgcat.logs()


def test_a_pool_with_two_verifiers_fails_the_config():
    output = start_fails(BINARY, identity_config(0) + TESTING_VERIFIER)
    assert 'plugins ["testing", "synthetic"] all verify identities on the pool' in output
    assert "Error in pool { verified }" in output


def test_a_verifier_without_the_tenant_parameter_fails_the_config():
    toml = identity_config(0).replace('\nidentity_tenant_parameter = "app.tenant"', "")
    assert "identity_tenant_parameter" not in toml
    output = start_fails(BINARY, toml)
    assert (
        'plugins."synthetic" verifies identities, so the pool needs identity_tenant_parameter'
        in output
    )
    assert "Error in pool { verified }" in output


def bounded(call, timeout=10):
    """`call()`'s result, failing the test if it takes longer than `timeout`
    seconds. The thread is a daemon: stopping pgcat ends a call left hanging."""
    result = {}

    def run():
        try:
            result["value"] = call()
        except BaseException as error:  # noqa: BLE001, re-raised below
            result["error"] = error

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    thread.join(timeout)
    assert not thread.is_alive(), f"no answer within {timeout}s"
    if "error" in result:
        raise result["error"]
    return result["value"]


@pytest.mark.parametrize("extended", [False, True])
def test_a_verifier_that_does_not_answer_fails_at_the_hook_timeout(
    verifying_pgcat, extended
):
    session = Session(verifying_pgcat.dsn("verified"), extended)
    try:
        session.run("SET PGCAT_IDENTITY = 'synthetic:acme'")
        assert session.value("SELECT 8003") == "acme"
        timeouts, refusals = hook_errors("PH001"), hook_errors("28000")

        # The pool's plugin_hook_timeout is 1 s.
        code, message = bounded(
            lambda: session.error("SET PGCAT_IDENTITY = 'synthetic-hangs'")
        )
        # The hook's SQLSTATE, not a refusal's 28000.
        assert code == "PH001"
        assert "PGCAT_IDENTITY refused: a plugin hook took too long" in message
        # Counted for the verifier before the client got it, as a timeout.
        assert (hook_errors("PH001"), hook_errors("28000")) == (timeouts + 1, refusals)
        # Like a refusal, it clears the identity the client had.
        assert session.row("SHOW PGCAT_IDENTITY") == (False, None)
        assert session.value("SELECT 8003") == "none"
        assert session.value("SELECT current_setting('app.tenant', true)") in ("", None)
    finally:
        session.close()


ECHOED_TOKEN = "synthetic-echo-token-sentinel-3b7e"


@pytest.mark.parametrize("extended", [False, True])
def test_a_refusal_that_quotes_the_token_is_replaced(verifying_pgcat, extended):
    session = Session(verifying_pgcat.dsn("verified"), extended)
    try:
        session.run("SET PGCAT_IDENTITY = 'synthetic:acme'")
        # The fixture's verifier puts the token in its message; pgcat doesn't
        # pass it on.
        code, message = session.error(f"SET PGCAT_IDENTITY = '{ECHOED_TOKEN}'")
        assert code == "28000"
        assert "PGCAT_IDENTITY refused: the plugin refused the identity" in message
        assert ECHOED_TOKEN not in message
        assert session.row("SHOW PGCAT_IDENTITY") == (False, None)
        assert_absent(verifying_pgcat, ECHOED_TOKEN)
    finally:
        session.close()
    assert ECHOED_TOKEN not in verifying_pgcat.logs()


def test_reload_keeps_the_verifier_and_can_turn_it_off():
    port = VERIFIER_RELOAD_PORT
    process = utils.PgcatProcess(identity_config(port), port, binary=BINARY)

    def tenant_of(token):
        """What the plugin sees on a new client that sets `token`."""
        session = Session(process.dsn("verified"), extended=False)
        try:
            session.run(f"SET PGCAT_IDENTITY = '{token}'")
            return session.value("SELECT 8003")
        finally:
            session.close()

    try:
        process.start()
        assert tenant_of("synthetic:acme") == "acme"

        # A second verifier on the pool: the reload keeps the previous config.
        process.write_config(identity_config(port) + TESTING_VERIFIER)
        with pytest.raises((AssertionError, ConnectionError)):
            utils.admin_reload(port)
        assert "all verify identities on the pool" in process.logs()
        assert tenant_of("synthetic:acme") == "acme"

        # Not verifying, the plugin keeps tokens without a tenant or claims.
        process.write_config(
            identity_config(port).replace("verify_identity = true", "verify_identity = false")
        )
        utils.admin_reload(port)
        assert tenant_of("synthetic:acme") == "none"
        assert tenant_of(REFUSED_TOKEN) == "none"
    finally:
        process.stop()


STALE_IDENTITY_PORT = 6463
SYNC = utils.frontend(b"S", b"")
FLUSH = utils.frontend(b"H", b"")


def parse(name, sql):
    return utils.frontend(b"P", name + b"\0" + sql.encode() + b"\0" + struct.pack("!h", 0))


def bind(portal, statement):
    return utils.frontend(b"B", portal + b"\0" + statement + b"\0" + struct.pack("!hhh", 0, 0, 0))


def execute(portal, rows=0):
    return utils.frontend(b"E", portal + b"\0" + struct.pack("!i", rows))


def query(sql):
    return utils.frontend(b"Q", sql.encode() + b"\0")


def close_portal(portal):
    return utils.frontend(b"C", b"P" + portal + b"\0")


def error_of(replies):
    """The SQLSTATE and message of the replies' ErrorResponse."""
    (body,) = [body for code, body in replies if code == b"E"]
    fields = {field[:1]: field[1:].decode() for field in body.split(b"\0") if field}
    return fields[b"C"], fields[b"M"]


def exchanges(sock, message):
    """Sends messages on a raw connection, returning the replies up to
    ReadyForQuery. Each is awaited for no longer than the socket's timeout."""

    def replies(*messages):
        sock.sendall(b"".join(messages))
        received = [message()]
        while received[-1][0] != b"Z":
            received.append(message())
        return received

    return replies


def first_row(replies):
    """The first DataRow's columns, as text, None for NULL."""
    body = next(body for code, body in replies if code == b"D")
    (count,) = struct.unpack("!h", body[:2])
    columns, offset = [], 2
    for _ in range(count):
        (length,) = struct.unpack("!i", body[offset : offset + 4])
        offset += 4
        columns.append(None if length < 0 else body[offset : offset + length].decode())
        offset += max(length, 0)
    return columns


def test_a_set_prepared_before_a_reload_is_verified_again():
    """A SET PGCAT_IDENTITY prepared, or bound, before a reload that turns
    verification on is verified when it's executed, as a SET would be now."""
    port = STALE_IDENTITY_PORT
    verifying = identity_config(port)
    process = utils.PgcatProcess(
        verifying.replace("verify_identity = true", "verify_identity = false"),
        port,
        binary=BINARY,
    )
    refused = f"SET PGCAT_IDENTITY = '{REFUSED_TOKEN}'"
    try:
        process.start()
        sock, message = bounded(lambda: utils.raw_connection(port, "verified"))
        # Every reply is awaited, never for longer than this.
        sock.settimeout(10)
        replies = exchanges(sock, message)

        def codes(*messages):
            return [code for code, _ in replies(*messages)]

        # Accepted: the plugin doesn't verify tokens yet.
        assert codes(parse(b"refused", refused), SYNC) == [b"1", b"Z"]
        assert codes(parse(b"valid", "SET PGCAT_IDENTITY = 'synthetic:acme'"), SYNC) == [
            b"1",
            b"Z",
        ]
        assert codes(parse(b"bound", refused), bind(b"bound", b"bound"), SYNC) == [
            b"1",
            b"2",
            b"Z",
        ]

        process.write_config(verifying)
        utils.admin_reload(port)

        # The portal bound before the reload, executed first after it.
        code, error = error_of(replies(execute(b"bound"), SYNC))
        assert code == "28000"
        assert "PGCAT_IDENTITY refused: synthetic plugin refused the identity" in error
        assert first_row(replies(query("SHOW PGCAT_IDENTITY"))) == ["f", None]

        # The control: a SET of the token is refused now.
        assert error_of(replies(query(refused)))[0] == "28000"

        # So is the statement prepared before the reload, which clears the
        # identity set before it, as a refused SET does.
        assert codes(query("SET PGCAT_IDENTITY = 'synthetic:acme'")) == [b"C", b"Z"]
        assert first_row(replies(query("SHOW PGCAT_IDENTITY")))[0] == "t"
        assert first_row(replies(query("SELECT 8003"))) == ["acme"]
        code, error = error_of(replies(bind(b"", b"refused"), execute(b""), SYNC))
        assert code == "28000"
        assert "PGCAT_IDENTITY refused: synthetic plugin refused the identity" in error
        assert first_row(replies(query("SHOW PGCAT_IDENTITY"))) == ["f", None]
        assert first_row(replies(query("SELECT 8003"))) == ["none"]
        tenant = first_row(replies(query("SELECT current_setting('app.tenant', true)")))
        assert tenant in ([""], [None])

        # A valid token's gets the verifier's claims and tenant.
        assert codes(bind(b"", b"valid"), execute(b""), SYNC) == [b"2", b"C", b"Z"]
        assert first_row(replies(query("SELECT 8003"))) == ["acme"]
        tenant = first_row(replies(query("SELECT current_setting('app.tenant', true)")))
        assert tenant == ["acme"]
        sock.close()
    finally:
        process.stop()
    assert REFUSED_TOKEN not in process.logs()


STALE_PORTAL_PORT = 6464
STALE_PLAN = (
    "XX000",
    "the plugin configuration changed after the statement was planned, send it again",
)


def test_a_portal_bound_before_a_reload_is_refused():
    """A portal bound in a transaction before a reload that changed the plugin
    configuration isn't executed with the old plan: the batch is refused at its
    Sync, the first after the reload or a later one, including after a local
    portal of the same name is closed, a Bind of its name failed, or one was
    still buffered when a simple Query ran. Portals bound since run, and an
    identity SET bound before is verified again, once."""
    port = STALE_PORTAL_PORT
    process = utils.PgcatProcess(identity_config(port), port, binary=BINARY)
    series = parse(b"", "SELECT generate_series(1, 3)")
    try:
        process.start()
        sock, message = bounded(lambda: utils.raw_connection(port, "verified"))
        # Every reply is awaited, never for longer than this.
        sock.settimeout(10)
        replies = exchanges(sock, message)

        def codes(*messages):
            return [code for code, _ in replies(*messages)]

        def reload(answer):
            process.write_config(identity_config(port).replace("answer = 1", f"answer = {answer}"))
            utils.admin_reload(port)

        def refused(*messages):
            received = replies(*messages)
            assert [code for code, _ in received] == [b"E", b"Z"]
            assert error_of(received) == STALE_PLAN

        suspended = [b"1", b"2", b"D", b"s", b"Z"]

        # Its first Execute after the reload.
        assert codes(query("BEGIN")) == [b"C", b"Z"]
        assert codes(series, bind(b"p", b""), execute(b"p", 1), SYNC) == suspended
        reload(2)
        refused(execute(b"p", 1), SYNC)
        assert codes(query("ROLLBACK")) == [b"C", b"Z"]

        # A later one, after an identity SET's portal bound before it too.
        assert codes(query("BEGIN")) == [b"C", b"Z"]
        assert codes(series, bind(b"p", b""), execute(b"p", 1), SYNC) == suspended
        set_acme = parse(b"set", "SET PGCAT_IDENTITY = 'synthetic:acme'")
        assert codes(set_acme, bind(b"set", b"set"), SYNC) == [b"1", b"2", b"Z"]
        verified = int(first_row(replies(query("SELECT 8004")))[0])
        reload(3)
        assert codes(execute(b"set"), SYNC) == [b"C", b"Z"]
        assert first_row(replies(query("SELECT 8003"))) == ["acme"]
        # The statement keeps that verification.
        assert codes(bind(b"again", b"set"), execute(b"again"), SYNC) == [b"2", b"C", b"Z"]
        assert first_row(replies(query("SELECT 8004"))) == [str(verified + 1)]

        assert codes(series, bind(b"q", b""), execute(b"q", 1), SYNC) == suspended
        assert codes(execute(b"q", 1), SYNC) == [b"D", b"s", b"Z"]
        refused(execute(b"p", 1), SYNC)
        assert codes(query("ROLLBACK")) == [b"C", b"Z"]

        # Its name bound to a local statement, then closed: pgcat answers
        # both, and the server's portal stays open.
        for answer, portal in ((4, b"p"), (5, b"")):
            assert codes(query("BEGIN")) == [b"C", b"Z"]
            assert codes(series, bind(portal, b""), execute(portal, 1), SYNC) == suspended
            reload(answer)
            assert codes(bind(portal, b"set"), SYNC) == [b"2", b"Z"]
            assert codes(close_portal(portal), SYNC) == [b"3", b"Z"]
            refused(execute(portal, 1), SYNC)
            assert codes(query("ROLLBACK")) == [b"C", b"Z"]

        # Bound again in a batch that failed, then rolled back to a savepoint
        # taken before it: the server refused that Bind, as the portal is
        # open, also after a local Bind of the name; or pgcat refused the
        # batch. The old portal is still refused, and runs once bound again.
        local = [bind(b"p", b"set"), SYNC]
        smuggled = parse(b"smuggled", "SET LOCAL PGCAT_IDENTITY = 'synthetic:acme'")
        for answer, before, rebind, failure in (
            (6, [], [series, bind(b"p", b""), SYNC], ([b"1", b"E", b"Z"], "42P03")),
            (7, local, [series, bind(b"p", b""), SYNC], ([b"1", b"E", b"Z"], "42P03")),
            (8, [], [series, bind(b"p", b""), smuggled, SYNC], ([b"E", b"Z"], "0A000")),
        ):
            assert codes(query("BEGIN")) == [b"C", b"Z"]
            assert codes(series, bind(b"p", b""), execute(b"p", 1), SYNC) == suspended
            assert codes(query("SAVEPOINT before_rebind")) == [b"C", b"Z"]
            reload(answer)
            assert first_row(replies(query("SELECT 8001"))) == [str(answer)]
            if before:
                assert codes(*before) == [b"2", b"Z"]
            received = replies(*rebind)
            assert ([code for code, _ in received], error_of(received)[0]) == failure
            assert codes(query("ROLLBACK TO SAVEPOINT before_rebind")) == [b"C", b"Z"]
            refused(execute(b"p", 1), SYNC)
            # The control: a Bind the server accepted.
            assert codes(query("ROLLBACK TO SAVEPOINT before_rebind")) == [b"C", b"Z"]
            rebound = codes(series, close_portal(b"p"), bind(b"p", b""), execute(b"p", 1), SYNC)
            assert rebound == [b"1", b"3", b"2", b"D", b"s", b"Z"]
            assert codes(execute(b"p", 1), SYNC) == [b"D", b"s", b"Z"]
            assert codes(query("ROLLBACK")) == [b"C", b"Z"]

        # A simple Query sent while a Bind of its name is buffered runs alone:
        # its ReadyForQuery doesn't confirm that Bind, which the refused batch
        # then drops.
        named = parse(b"series", "SELECT generate_series(1, 3)")
        assert codes(query("BEGIN")) == [b"C", b"Z"]
        assert codes(named, bind(b"p", b"series"), execute(b"p", 1), SYNC) == suspended
        assert codes(query("SAVEPOINT before_rebind")) == [b"C", b"Z"]
        reload(9)
        assert first_row(replies(query("SELECT 8001"))) == ["9"]
        ran = replies(bind(b"p", b"series"), query("SELECT 1"))
        assert ([code for code, _ in ran], first_row(ran)) == ([b"T", b"D", b"C", b"Z"], ["1"])
        not_a_token = parse(b"not_a_token", "SET LOCAL PGCAT_IDENTITY = 'not-a-token'")
        received = replies(not_a_token, SYNC)
        assert ([code for code, _ in received], error_of(received)[0]) == ([b"E", b"Z"], "0A000")
        assert codes(query("ROLLBACK TO SAVEPOINT before_rebind")) == [b"C", b"Z"]
        refused(execute(b"p", 1), SYNC)
        # The control: the Bind sent with its batch's Sync after such a Query.
        assert codes(query("ROLLBACK TO SAVEPOINT before_rebind")) == [b"C", b"Z"]
        ran = replies(close_portal(b"p"), bind(b"p", b"series"), query("SELECT 1"))
        assert [code for code, _ in ran] == [b"T", b"D", b"C", b"Z"]
        assert codes(execute(b"p", 1), SYNC) == [b"3", b"2", b"D", b"s", b"Z"]
        assert codes(execute(b"p", 1), SYNC) == [b"D", b"s", b"Z"]
        assert codes(query("ROLLBACK")) == [b"C", b"Z"]

        # The control: a Bind sent at a Flush, answered without ReadyForQuery.
        assert codes(query("BEGIN")) == [b"C", b"Z"]
        assert codes(bind(b"p", b"series"), execute(b"p", 1), SYNC) == [b"2", b"D", b"s", b"Z"]
        reload(10)
        assert first_row(replies(query("SELECT 8001"))) == ["10"]
        sock.sendall(close_portal(b"p") + bind(b"p", b"series") + execute(b"p", 1) + FLUSH)
        assert [message()[0] for _ in range(4)] == [b"3", b"2", b"D", b"s"]
        assert codes(execute(b"p", 1), SYNC) == [b"D", b"s", b"Z"]
        assert codes(query("ROLLBACK")) == [b"C", b"Z"]
        sock.close()
    finally:
        process.stop()


LIFECYCLE_PORT = 6476
LIFECYCLE_METRICS_PORT = 9961
START_SECRET = "configured-plugin-start-sentinel-77ab"


def with_metrics(toml, port):
    return toml.replace(
        'admin_password = "admin_pass"\n',
        f'admin_password = "admin_pass"\nenable_prometheus_exporter = true\n'
        f"prometheus_exporter_port = {port}\n",
        1,
    )


def task_running(port):
    text = urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5).read().decode()
    values = [line.split()[1] for line in text.splitlines()
              if line.startswith("pgcat_synthetic_task_running ")]
    assert len(values) == 1, text
    return values[0]


@pytest.mark.parametrize("stop", [signal.SIGTERM, signal.SIGINT])
def test_the_plugin_starts_before_clients_and_stops_with_pgcat(stop):
    toml = with_metrics(config(LIFECYCLE_PORT), LIFECYCLE_METRICS_PORT)
    process = utils.PgcatProcess(toml, LIFECYCLE_PORT, binary=BINARY)
    try:
        process.start()
        # A statement is answered once the accept loop runs, after `start`.
        assert run(process.dsn("configured"), "SELECT 8001", extended=False) == 5
        assert task_running(LIFECYCLE_METRICS_PORT) == "1"
        logs = process.logs()
        assert logs.index("synthetic plugin started, enabled: true") < logs.index(
            "Waiting for clients"
        )

        process.process.send_signal(stop)
        assert process.process.wait(timeout=10) == 0
        logs = process.logs()
        assert logs.index("synthetic plugin stopped its task, joined: true") < logs.index(
            "Shutting down..."
        )
        assert "didn't shut down" not in logs
    finally:
        process.stop()


def test_the_plugin_starts_when_no_pool_turns_it_on():
    toml = config(LIFECYCLE_PORT, section="enabled = false\nanswer = 1")
    process = utils.PgcatProcess(toml, LIFECYCLE_PORT, binary=BINARY)
    try:
        process.start()
        assert run(process.dsn("configured"), "SELECT 8001", extended=False) == 8001
        assert "synthetic plugin started, enabled: false" in process.logs()
    finally:
        process.stop()


def test_a_plugin_that_fails_to_start_stops_pgcat():
    output = start_fails(BINARY, config(0), env={"PGCAT_SYNTHETIC_START_FAILURE": START_SECRET})
    assert "synthetic plugin can't start" in output
    assert 'Plugin "synthetic" failed to start' in output
    # pgcat logs that the plugin failed, never the plugin's error.
    assert START_SECRET not in output
    assert "Waiting for clients" not in output


WITHHOLD_PORT = 6465
WITHHOLD_RELOAD_PORT = 6466
WITHHELD = "the server's message is withheld: it can quote protected values"
ROW_SENTINEL = "configured-plugin-row-sentinel-7a3e"
NOTICE_TABLE = "configured_withheld_notice_3c1f"
HELD_SETTINGS_CHANGED = "the pool's plugin settings changed in a reload: connect again"


def withholding(port, withhold):
    return config(
        port,
        section=f"enabled = true\nanswer = 5\nwithhold_server_text = {str(withhold).lower()}\n",
    )


def server_diagnostics(dsn, extended):
    """In a transaction, a DROP that draws a notice naming its table and an
    INSERT that breaks a check constraint, whose DETAIL quotes the row: the
    error's diagnostics and the notices' messages."""
    notices = []
    with psycopg.connect(dsn) as connection:
        connection.add_notice_handler(lambda notice: notices.append(notice.message_primary))
        connection.execute(
            "CREATE TEMP TABLE withheld_rows (secret text CHECK (secret = '')) ON COMMIT DROP"
        )
        connection.execute(f"DROP TABLE IF EXISTS {NOTICE_TABLE}", prepare=extended)
        with pytest.raises(psycopg.errors.CheckViolation) as raised:
            if extended:
                connection.execute("INSERT INTO withheld_rows VALUES (%s)", [ROW_SENTINEL])
            else:
                connection.execute(f"INSERT INTO withheld_rows VALUES ('{ROW_SENTINEL}')")
    return raised.value.diag, notices


@pytest.mark.parametrize("extended", [False, True])
def test_the_server_text_is_shown_by_default(pgcat, extended):
    diag, notices = server_diagnostics(pgcat.dsn("configured"), extended)
    assert "violates check constraint" in diag.message_primary
    assert ROW_SENTINEL in diag.message_detail
    assert [notice for notice in notices if NOTICE_TABLE in notice], notices


@pytest.mark.parametrize("extended", [False, True])
def test_a_plugin_can_withhold_the_server_text(extended):
    """Only the severity, SQLSTATE and object names of the server's errors and
    notices reach the client: Postgres quotes rows in their text."""
    process = utils.PgcatProcess(withholding(WITHHOLD_PORT, True), WITHHOLD_PORT, binary=BINARY)
    try:
        process.start()
        diag, notices = server_diagnostics(process.dsn("configured"), extended)
        assert diag.sqlstate == "23514"
        assert diag.message_primary == WITHHELD
        assert diag.message_detail is None
        assert diag.table_name == "withheld_rows"
        assert diag.constraint_name == "withheld_rows_secret_check"
        assert notices == [WITHHELD]
        # The other pool doesn't withhold.
        diag, _ = server_diagnostics(process.dsn("plain"), extended)
        assert ROW_SENTINEL in diag.message_detail
    finally:
        process.stop()


@pytest.mark.parametrize("extended", [False, True])
@pytest.mark.parametrize("turns_on", [True, False])
def test_a_reload_that_changes_withholding_ends_held_sessions(extended, turns_on):
    """A client holding its server keeps whether the server's text is withheld
    from the pool it was checked out from. A reload that changes it ends the
    session at its next statement, whose reply would be sanitized for another."""
    port = WITHHOLD_RELOAD_PORT
    collector = Collector()
    process = utils.PgcatProcess(
        traced(withholding(port, not turns_on), collector), port, binary=BINARY, env=ENV
    )
    try:
        process.start()
        sock, message = bounded(lambda: utils.raw_connection(port, "configured"))
        sock.settimeout(10)
        replies = exchanges(sock, message)
        assert [code for code, _ in replies(query("BEGIN"))] == [b"C", b"Z"]
        ended = new_trace()
        sql = f"{comment(ended)} SELECT 1"
        if extended:
            # Bound before the reload, so that its Execute is the next message.
            sent = replies(parse(b"held", sql), bind(b"held", b"held"), SYNC)
            assert [code for code, _ in sent] == [b"1", b"2", b"Z"]

        process.write_config(traced(withholding(port, turns_on), collector))
        utils.admin_reload(port)

        # The statement is refused before it runs: its span says so.
        sock.sendall(execute(b"held") + SYNC if extended else query(sql))
        code, body = message()
        assert code == b"E", body
        assert error_of([(code, body)]) == ("0A000", HELD_SETTINGS_CHANGED)
        with pytest.raises(ConnectionError):
            message()
        sock.close()

        # A new client gets the pool's new setting.
        diag, _ = server_diagnostics(process.dsn("configured"), extended)
        assert (diag.message_primary == WITHHELD) == turns_on

        # Spans export in the order statements end: this one's comes last.
        marker = new_trace()
        with psycopg.connect(process.dsn("configured"), autocommit=True) as connection:
            connection.execute(f"{comment(marker)} SELECT 1", prepare=False)
        assert_refused_spans(collector.spans_until(marker), [ended], "0A000")
    finally:
        process.stop()
        collector.close()


WITHHOLD_COPY_PORT = 6482


def test_a_reload_that_ends_a_session_mid_copy_leaves_the_copy_no_span():
    """A reload that changes withholding ends a session holding a server
    mid-COPY at its next Sync. The session's end closes the server before the
    COPY's ReadyForQuery, and pgcat didn't refuse the COPY: it has no span."""
    port = WITHHOLD_COPY_PORT
    collector = Collector()
    process = utils.PgcatProcess(
        traced(withholding(port, False), collector), port, binary=BINARY, env=ENV
    )
    try:
        process.start()
        sock, message = bounded(lambda: utils.raw_connection(port, "configured"))
        sock.settimeout(10)
        copy = new_trace()
        sock.sendall(
            query(f"{comment(copy)} CREATE TEMP TABLE copied (n int) ON COMMIT DROP; COPY copied FROM STDIN")
        )
        assert [message()[0], message()[0]] == [b"C", b"G"]

        process.write_config(traced(withholding(port, True), collector))
        utils.admin_reload(port)

        sock.sendall(utils.frontend(b"d", b"41\n") + SYNC)
        code, body = message()
        assert code == b"E", body
        assert error_of([(code, body)]) == ("0A000", HELD_SETTINGS_CHANGED)
        with pytest.raises(ConnectionError):
            message()
        sock.close()

        marker = new_trace()
        with psycopg.connect(process.dsn("configured"), autocommit=True) as connection:
            connection.execute(f"{comment(marker)} SELECT 1", prepare=False)
        spans = collector.spans_until(marker)
        assert [span for span in spans if span["trace_id"] == copy] == []
    finally:
        process.stop()
        collector.close()


WITHHOLD_WAIT_PORT = 6481


@pytest.mark.parametrize("extended", [False, True])
@pytest.mark.parametrize("turns_on", [True, False])
def test_a_reload_that_changes_withholding_refuses_a_statement_waiting_for_a_server(
    extended, turns_on
):
    """A statement that waited for a server across a reload changing whether
    the server's text is withheld is refused before it runs (its plan is the
    old section's), so its reply can't go out under either setting; the
    session goes on, with the pool's new setting."""
    port = WITHHOLD_WAIT_PORT

    def config_of(withhold):
        # One server in the pool; the wait outlasts the 1 s default.
        return withholding(port, withhold).replace(
            'admin_password = "admin_pass"\n',
            'admin_password = "admin_pass"\nconnect_timeout = 10000\n',
            1,
        )

    def waiting():
        with psycopg.connect(
            process.dsn("pgcat", "admin_user", "admin_pass"), autocommit=True, prepare_threshold=None
        ) as admin:
            cursor = admin.execute("SHOW POOLS")
            columns = [column.name for column in cursor.description]
            return sum(
                row[columns.index("cl_waiting")]
                for row in cursor.fetchall()
                if row[columns.index("database")] == "configured"
            )

    process = utils.PgcatProcess(config_of(not turns_on), port, binary=BINARY)
    try:
        process.start()
        holder, holder_message = bounded(lambda: utils.raw_connection(port, "configured"))
        holder.settimeout(10)
        assert [code for code, _ in exchanges(holder, holder_message)(query("BEGIN"))] == [
            b"C",
            b"Z",
        ]

        sock, message = bounded(lambda: utils.raw_connection(port, "configured"))
        sock.settimeout(10)
        replies = exchanges(sock, message)
        if extended:
            sock.sendall(parse(b"", "SELECT 1") + bind(b"", b"") + execute(b"") + SYNC)
        else:
            sock.sendall(query("SELECT 1"))
        deadline = time.monotonic() + 5
        # Each look is bounded too, its connection included: a stalled admin
        # fails the test here, not at the CI job's timeout.
        while bounded(waiting, timeout=max(deadline - time.monotonic(), 1)) != 1:
            assert time.monotonic() < deadline, "the client didn't wait for the server"
            time.sleep(0.05)

        process.write_config(config_of(turns_on))
        utils.admin_reload(port)
        # The holder's next statement ends its session, and the server it
        # held isn't used again: the waiting client gets another.
        holder.sendall(query("COMMIT"))
        code, body = holder_message()
        assert error_of([(code, body)]) == ("0A000", HELD_SETTINGS_CHANGED)
        holder.close()

        received = []
        while not received or received[-1][0] != b"Z":
            received.append(message())
        assert error_of(received) == STALE_PLAN
        assert [code for code, _ in received] == [b"E", b"Z"]

        # Sent again, it runs, with the pool's new setting: a server error
        # quoting a value is withheld only if the reload turned it on.
        assert first_row(replies(query("SELECT 8001"))) == ["5"]
        code, text = error_of(replies(query(f"SELECT CAST('{ROW_SENTINEL}' AS int)")))
        assert code == "22P02"
        assert (text == WITHHELD) == turns_on
        assert (ROW_SENTINEL in text) != turns_on
        sock.close()
        diag, _ = server_diagnostics(process.dsn("configured"), extended)
        assert (diag.message_primary == WITHHELD) == turns_on
    finally:
        process.stop()


RECHECK_PORT = 6468
# The pool's settings for the testing plugin, which doesn't verify identities.
TESTING_OFF = """
[pools.verified.plugins.testing]
enabled = false
"""
CONNECT_AGAIN = "the identity's verifier changed in a reload: connect again"


class RawSession:
    """A raw connection to the verifying pool: statements sent as a simple
    Query, or as Parse, Bind, Execute and Sync."""

    def __init__(self, port, extended):
        self.sock, self.message = bounded(lambda: utils.raw_connection(port, "verified"))
        # Every reply is awaited, never for longer than this.
        self.sock.settimeout(10)
        self.replies = exchanges(self.sock, self.message)
        self.extended = extended

    def messages(self, sql):
        if self.extended:
            return [parse(b"", sql), bind(b"", b""), execute(b""), SYNC]
        return [query(sql)]

    def run(self, sql):
        return self.replies(*self.messages(sql))

    def ok(self, sql):
        received = self.run(sql)
        assert b"E" not in [code for code, _ in received], error_of(received)
        return received

    def value(self, sql):
        return first_row(self.ok(sql))[0]

    def error(self, sql):
        """The statement's error, and the transaction status after it."""
        received = self.run(sql)
        status = received[-1][1]
        return error_of(received), status

    def ended(self, sql):
        """The statement ends the session, with 0A000."""
        self.ended_at(*self.messages(sql))

    def ended_at(self, *messages):
        self.sock.sendall(b"".join(messages))
        code, body = self.message()
        assert code == b"E" and b"C0A000\0" in body, body
        assert CONNECT_AGAIN.encode() in body, body
        with pytest.raises(ConnectionError):
            self.message()
        self.sock.close()

    def close(self):
        self.sock.close()


@pytest.mark.parametrize("extended", [False, True])
def test_a_reload_of_the_verifier_verifies_identities_again(extended):
    """A reload that changes the pool's verifier, or its section, has the
    token of a client's identity verified again before its next statement,
    once. The same tenant and claims keep the identity, in a transaction too;
    anything else ends the session."""
    port = RECHECK_PORT
    collector = Collector()
    process = utils.PgcatProcess(
        traced(identity_config(port), collector), port, binary=BINARY, env=ENV
    )

    def reload(extra="", testing=""):
        process.write_config(traced(identity_config(port, extra) + testing, collector))
        utils.admin_reload(port)

    def verifications(session):
        return int(session.value("SELECT 8004"))

    def identified():
        session = RawSession(port, extended)
        session.ok("SET PGCAT_IDENTITY = 'synthetic:acme'")
        assert session.value("SELECT 8003") == "acme"
        return session

    try:
        process.start()

        # The section changes, not the result: the session goes on, with one
        # verification for the reload, none for the statements after it.
        session = identified()
        before = verifications(session)
        reload('note = "changed"\n')
        assert session.value("SELECT 8003") == "acme"
        assert session.value("SELECT current_setting('app.tenant', true)") == "acme"
        assert verifications(session) == before + 1
        assert session.value("SELECT 8003") == "acme"
        assert verifications(session) == before + 1

        # A reload of the pool's plugins that leaves the verifier as it was:
        # no verification.
        reload('note = "changed"\n', TESTING_OFF)
        assert session.value("SELECT 8003") == "acme"
        assert verifications(session) == before + 1

        # In a transaction, holding a server: it goes on too.
        session.ok("BEGIN")
        assert session.value("SELECT 8003") == "acme"
        reload('note = "again"\n')
        assert session.value("SELECT 8003") == "acme"
        assert session.value("SELECT current_setting('app.tenant', true)") == "acme"
        session.ok("COMMIT")
        assert verifications(session) == before + 2
        session.close()

        # Other claims, the tenant the same; verification turned off; the
        # verifier removed: the session ends, in a transaction too, and the
        # statement it ends at gets a refused span. In the extended protocol,
        # that's an Execute of a portal bound before the reload; a new
        # statement's batch ends the session at its Parse, with no span.
        ended, unspanned = [], []
        for changed in (
            identity_config(port, 'claim = "other"\n'),
            identity_config(port).replace("verify_identity = true", "verify_identity = false"),
            identity_config(port).replace("enabled = true\nanswer = 1", "answer = 1"),
        ):
            for transaction in (False, True):
                reload()
                session = identified()
                trace = new_trace()
                held = extended and transaction
                if transaction:
                    session.ok("BEGIN")
                if held:
                    sql = f"{comment(trace)} SELECT current_setting('app.tenant', true)"
                    sent = session.replies(parse(b"held", sql), bind(b"held", b"held"), SYNC)
                    assert [code for code, _ in sent] == [b"1", b"2", b"Z"]
                process.write_config(traced(changed, collector))
                utils.admin_reload(port)
                if held:
                    session.ended_at(execute(b"held"), SYNC)
                else:
                    session.ended(f"{comment(trace)} SELECT 8003")
                (unspanned if extended and not held else ended).append(trace)

        # A new client gets the server, in the pool of one, and the verifier.
        # Spans export in the order statements end: its statement's comes last.
        reload('claim = "other"\n')
        session = identified()
        marker = new_trace()
        session.ok(f"{comment(marker)} SELECT 1")
        session.close()
        spans = collector.spans_until(marker)
        assert_refused_spans(spans, ended, "0A000", unspanned)
        # The client as pgcat read the statement: its identity verified.
        for trace in ended:
            attributes = only(spans, trace)["attributes"]
            assert (attributes["pgcat.identity"], attributes["pgcat.identity.verified"]) == (
                "present",
                True,
            )
    finally:
        process.stop()
        collector.close()


@pytest.mark.parametrize("extended", [False, True])
def test_a_verifier_unavailable_after_a_reload_fails_statements_fast(extended):
    """A verifier that doesn't answer after a reload fails the client's
    statements at the hook timeout with PH001, never running them with the old
    identity, and is asked again at each; once it answers the same, the
    session goes on, in a transaction too."""
    port = RECHECK_PORT + 1
    process = utils.PgcatProcess(identity_config(port), port, binary=BINARY)

    def reload(extra=""):
        process.write_config(identity_config(port, extra))
        utils.admin_reload(port)

    def unavailable(session, status):
        (code, message), after = bounded(lambda: session.error("SELECT 8003"))
        assert code == "PH001"
        assert "PGCAT_IDENTITY could not be verified again" in message
        assert after == status

    try:
        process.start()
        session = RawSession(port, extended)
        session.ok("SET PGCAT_IDENTITY = 'synthetic:acme'")
        before = int(session.value("SELECT 8004"))

        reload("hang = true\n")
        unavailable(session, b"I")
        unavailable(session, b"I")
        reload()
        assert session.value("SELECT 8003") == "acme"
        # Each statement asked the verifier, the first after the reload once.
        assert int(session.value("SELECT 8004")) == before + 3

        # In a transaction: the refused statement fails it.
        session.ok("BEGIN")
        reload("hang = true\n")
        unavailable(session, b"E")
        reload()
        session.ok("ROLLBACK")
        assert session.value("SELECT 8003") == "acme"
        assert session.value("SELECT current_setting('app.tenant', true)") == "acme"
        session.close()
    finally:
        process.stop()


REFUSED_RECHECK_PORT = 6480
REFUSED_RECHECK_METRICS_PORT = 9963


@pytest.mark.parametrize("extended", [False, True])
def test_a_refusal_after_a_reload_is_counted_as_the_0a000_the_client_gets(extended):
    """A reload's verifier that refuses the identity a client holds ends the
    session with 0A000, and `pgcat_hook_errors_total` counts it once, as that
    0A000: not as 28000, which a refused SET PGCAT_IDENTITY still is."""
    port, metrics = REFUSED_RECHECK_PORT, REFUSED_RECHECK_METRICS_PORT
    verifying = identity_config(port, metrics_port=metrics)
    process = utils.PgcatProcess(
        verifying.replace("verify_identity = true", "verify_identity = false"),
        port,
        binary=BINARY,
    )

    def counts():
        return hook_errors("0A000", metrics), hook_errors("28000", metrics)

    try:
        process.start()
        # Nothing verifies it yet: the token is kept as it is.
        session = RawSession(port, extended)
        session.ok("SET PGCAT_IDENTITY = 'unverifiable'")
        assert session.value("SELECT 8003") == "none"
        before = counts()

        # The verifier the reload adds refuses it.
        process.write_config(verifying)
        utils.admin_reload(port)
        session.ended("SELECT 8003")
        assert counts() == (before[0] + 1, before[1])

        # The control: refused at a SET, the same token is a 28000.
        session = RawSession(port, extended)
        (code, _), _ = session.error("SET PGCAT_IDENTITY = 'unverifiable'")
        assert code == "28000"
        assert counts() == (before[0] + 1, before[1] + 1)
        session.close()
    finally:
        process.stop()


@pytest.mark.parametrize("after", ["execute", "sync", "flush"])
def test_an_identity_set_buffered_across_a_verifier_reload_is_refused(after):
    """A batch whose SET PGCAT_IDENTITY was verified, at its Execute, before
    a reload that changed only what verifies identities is refused at its
    Sync, or at a Flush: the SET would keep that verification. Whether
    another Execute, the Sync or a Flush is the first message after the
    reload."""
    port = RECHECK_PORT + 2
    unverified = identity_config(port).replace("verify_identity = true", "verify_identity = false")
    process = utils.PgcatProcess(unverified, port, binary=BINARY)
    set_acme = parse(b"set", "SET PGCAT_IDENTITY = 'synthetic:acme'")
    try:
        process.start()
        session = RawSession(port, extended=True)
        session.sock.sendall(set_acme + bind(b"", b"set") + execute(b""))
        # A simple Query runs alone, the batch staying buffered: its reply
        # means pgcat has taken the Execute, and verified the SET, before the
        # reload.
        ran = session.replies(query("SELECT 1"))
        assert ([code for code, _ in ran], first_row(ran)) == ([b"T", b"D", b"C", b"Z"], ["1"])

        # The testing verifier: the plugins' plans don't change.
        process.write_config(unverified + TESTING_VERIFIER)
        utils.admin_reload(port)

        if after == "flush":
            # Answered at the Flush, without ReadyForQuery; the Sync ends it.
            session.sock.sendall(FLUSH)
            received = [session.message()]
            received += session.replies(SYNC)
        else:
            received = session.replies(*([execute(b"")] if after == "execute" else []), SYNC)
        assert [code for code, _ in received] == [b"E", b"Z"]
        assert error_of(received) == STALE_PLAN
        assert first_row(session.replies(query("SHOW PGCAT_IDENTITY"))) == ["f", None]
        session.close()
    finally:
        process.stop()


EXPIRY_PORT = 6479


@pytest.mark.parametrize("extended", [False, True])
def test_a_kept_identity_expires_when_its_verifier_says_so(extended):
    """A reload whose verifier keeps a client's identity, the same tenant and
    claims, gives it the new verification's expiry without ending the
    session: one already past forgets the identity before the next
    statement, as a new SET of the token would be."""
    port = EXPIRY_PORT
    unverified = identity_config(port).replace("verify_identity = true", "verify_identity = false")
    process = utils.PgcatProcess(unverified, port, binary=BINARY)
    try:
        process.start()
        session = RawSession(port, extended)
        # Nothing verifies it: kept, without an expiry.
        session.ok("SET PGCAT_IDENTITY = 'expires:1:x'")
        assert session.value("SHOW PGCAT_IDENTITY") == "t"

        # The testing verifier names no tenant and no claims for the token,
        # as the identity has, and says it expired in 1970.
        process.write_config(unverified + TESTING_VERIFIER)
        utils.admin_reload(port)
        assert session.value("SHOW PGCAT_IDENTITY") == "f"
        assert session.value("SELECT 1") == "1"
        session.close()
    finally:
        process.stop()
