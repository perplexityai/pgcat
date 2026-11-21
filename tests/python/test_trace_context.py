"""Statement spans: a statement whose leading sqlcommenter comment carries a
sampled W3C traceparent gets one span in the caller's trace, exported over
OTLP/HTTP. pgcat exports here to a fake collector that records what it gets.

Spans export in the order statements end, so a test that runs statements it
expects no span for, then a sampled one, waits for the sampled one's span and
checks that nothing else came before it: no fixed sleeps."""

import http.server
import os
import queue
import socket
import struct
import threading
import time

import psycopg
import psycopg2
import pytest

import utils

PORT = 6486
PG_PORT = int(os.environ.get("PGCAT_TEST_PG_PORT", "5432"))
SPAN = "b7ad6b7169203331"
# OTEL_BSP_SCHEDULE_DELAY is the OpenTelemetry batch processor's export
# interval, honored as the spec says: export quickly in tests.
ENV = {"OTEL_BSP_SCHEDULE_DELAY": "50"}


def config(ratio, endpoint, statement_timeout=0, pool="", sections="", general="", pool_size=5):
    endpoint = "" if endpoint is None else f'trace_otlp_endpoint = "{endpoint}"'
    return f"""
[general]
host = "127.0.0.1"
port = {PORT}
admin_username = "admin_user"
admin_password = "admin_pass"
trace_sample_ratio = {ratio}
{endpoint}
{general}

[pools.shard0]
pool_mode = "transaction"
prepared_statements_cache_size = 100
{pool}

[pools.shard0.users.0]
username = "sharding_user"
password = "sharding_user"
pool_size = {pool_size}
statement_timeout = {statement_timeout}

[pools.shard0.shards.0]
servers = [["127.0.0.1", {PG_PORT}, "primary"]]
database = "shard0"
{sections}
"""


def new_trace():
    return os.urandom(16).hex()


def comment(trace, flags="01", span=SPAN):
    # As sqlcommenter clients write it: sorted keys, URL-encoded values.
    return f"/*application='api',route='%2Fv1%2Fitems',traceparent='00-{trace}-{span}-{flags}'*/"


class Collector:
    """A fake OTLP/HTTP collector: queues each export request it receives."""

    def __init__(self):
        self.requests = queue.Queue()
        self.status = 200
        self.spans = []
        self.headers = []
        collector = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers["Content-Length"]))
                headers = {name.lower(): value for name, value in self.headers.items()}
                collector.headers.append(headers)
                collector.requests.put((self.path, headers.get("content-type"), body))
                self.send_response(collector.status)
                self.send_header("Content-Type", "application/x-protobuf")
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, *args):
                pass

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    @property
    def endpoint(self):
        return f"http://127.0.0.1:{self.server.server_port}/v1/traces"

    def request(self, timeout=10):
        path, content_type, body = self.requests.get(timeout=timeout)
        assert path == "/v1/traces", path
        assert content_type == "application/x-protobuf", content_type
        return list(export_spans(body))

    def spans_until(self, trace, timeout=10, count=1):
        """Every span received until `count` in `trace` have arrived."""
        deadline = time.monotonic() + timeout
        while sum(span["trace_id"] == trace for span in self.spans) < count:
            self.spans.extend(self.request(timeout=max(0, deadline - time.monotonic())))
        return self.spans

    def close(self):
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture(autouse=True)
def no_inherited_otel(monkeypatch):
    # pgcat inherits the runner's environment, where an OTEL_SERVICE_NAME
    # would change the spans checked here. A test's own go through `env`.
    for name in [name for name in os.environ if name.startswith("OTEL_")]:
        monkeypatch.delenv(name)


@pytest.fixture
def collector():
    collector = Collector()
    yield collector
    collector.close()


@pytest.fixture
def pgcat(collector):
    process = utils.PgcatProcess(config(1, collector.endpoint), PORT, log_level="debug", env=ENV).start()
    yield process
    process.stop()


def connect(pgcat):
    conn = psycopg2.connect(pgcat.dsn("shard0"))
    conn.autocommit = True
    return conn


def run(pgcat, *statements):
    conn = connect(pgcat)
    try:
        cur = conn.cursor()
        for statement in statements:
            cur.execute(statement)
    finally:
        conn.close()


def reconfigure(pgcat, ratio, endpoint):
    pgcat.write_config(config(ratio, endpoint))
    utils.admin_reload(PORT)


# OTLP protobuf, decoded just enough to read spans
# (opentelemetry/proto/collector/trace/v1 and trace/v1).


def varint(data, i):
    value = shift = 0
    while True:
        byte = data[i]
        i += 1
        value |= (byte & 0x7F) << shift
        shift += 7
        if not byte & 0x80:
            return value, i


def fields(data):
    i = 0
    while i < len(data):
        key, i = varint(data, i)
        number, wire = key >> 3, key & 7
        if wire == 0:
            value, i = varint(data, i)
        elif wire == 1:
            value, i = data[i : i + 8], i + 8
        elif wire == 2:
            length, i = varint(data, i)
            value, i = data[i : i + length], i + length
        elif wire == 5:
            value, i = data[i : i + 4], i + 4
        else:
            raise ValueError(f"wire type {wire}")
        yield number, value


def any_value(data):
    for number, value in fields(data):
        if number == 1:
            return value.decode()
        if number == 2:
            return bool(value)
        if number == 3:
            return value - (1 << 64) if value >= 1 << 63 else value
        if number == 4:
            return struct.unpack("<d", value)[0]
    return None


def key_values(messages):
    result = {}
    for message in messages:
        parts = dict(fields(message))
        result[parts[1].decode()] = any_value(parts.get(2, b""))
    return result


def export_spans(request):
    for number, resource_spans in fields(request):
        if number != 1:
            continue
        resource = {}
        scopes = []
        for kind, value in fields(resource_spans):
            if kind == 1:
                resource = key_values(v for n, v in fields(value) if n == 1)
            elif kind == 2:
                scopes.append(value)
        for scope_spans in scopes:
            scope = {}
            for kind, value in fields(scope_spans):
                if kind == 1:
                    scope = {n: v.decode() for n, v in fields(value) if n in (1, 2)}
                elif kind == 2:
                    yield span(value, resource, scope)


def span(data, resource, scope):
    result = {"resource": resource, "scope": scope, "status": 0, "trace_state": "", "attrs": []}
    for number, value in fields(data):
        if number == 1:
            result["trace_id"] = value.hex()
        elif number == 2:
            result["span_id"] = value.hex()
        elif number == 3:
            result["trace_state"] = value.decode()
        elif number == 4:
            result["parent_span_id"] = value.hex()
        elif number == 5:
            result["name"] = value.decode()
        elif number == 6:
            result["kind"] = value
        elif number == 7:
            result["start"] = struct.unpack("<Q", value)[0]
        elif number == 8:
            result["end"] = struct.unpack("<Q", value)[0]
        elif number == 9:
            result["attrs"].append(value)
        elif number == 15:
            result["status"] = dict(fields(value)).get(3, 0)
    result["attributes"] = key_values(result.pop("attrs"))
    return result


SPAN_KIND_SERVER = 2
STATUS_ERROR = 2


def only(spans, trace):
    matching = [span for span in spans if span["trace_id"] == trace]
    assert len(matching) == 1, matching
    return matching[0]


def test_a_sampled_statement_joins_the_callers_trace(pgcat, collector):
    trace = new_trace()
    run(pgcat, f"{comment(trace)} SELECT pg_sleep(0.2)")

    span = only(collector.spans_until(trace), trace)
    assert span["parent_span_id"] == SPAN
    assert span["span_id"] not in ("", "0" * 16, SPAN)
    assert span["name"] == "pgcat.statement"
    assert span["kind"] == SPAN_KIND_SERVER
    assert span["trace_state"] == ""
    assert span["status"] == 0
    # The span covers the statement, not just its admission.
    assert span["end"] - span["start"] >= 200_000_000, span
    assert span["attributes"] == {
        "db.system.name": "postgresql",
        "pgcat.pool.name": "shard0",
        "pgcat.pool.user": "sharding_user",
        "pgcat.server.role": "primary",
        "pgcat.shard": 0,
        "pgcat.protocol": "simple",
        "pgcat.identity": "absent",
        "pgcat.identity.verified": False,
        "pgcat.client": "other",
    }
    assert span["resource"]["service.name"] == "pgcat"
    assert span["scope"][1] == "pgcat"

    # Neither the statement nor its comment are logged, even at debug.
    logs = pgcat.logs()
    for secret in (trace, "pg_sleep", "traceparent", "%2Fv1"):
        assert secret not in logs, secret


def test_extended_protocol_statements_keep_their_parent(pgcat, collector):
    unnamed, prepared = new_trace(), new_trace()
    # With parameters, psycopg reads % as a placeholder.
    with psycopg.connect(pgcat.dsn("shard0"), autocommit=True) as conn:
        conn.execute(comment(unnamed).replace("%", "%%") + " SELECT %s::int", (1,))
        # A prepared statement keeps the traceparent it was parsed with: its
        # second execution is a Bind and Execute only.
        for _ in range(2):
            conn.execute(comment(prepared).replace("%", "%%") + " SELECT %s::int", (1,), prepare=True)

    spans = collector.spans_until(prepared, count=2)
    assert only(spans, unnamed)["attributes"]["pgcat.protocol"] == "extended"
    executions = [span for span in spans if span["trace_id"] == prepared]
    assert len(executions) == 2, executions
    for span in executions + [only(spans, unnamed)]:
        assert span["parent_span_id"] == SPAN
        assert span["attributes"]["pgcat.protocol"] == "extended"


def test_statements_without_a_sampled_traceparent_get_no_span(pgcat, collector):
    untraced = new_trace()
    tp = f"00-{untraced}-{SPAN}"
    run(
        pgcat,
        # The caller didn't sample the trace.
        f"{comment(untraced, flags='00')} SELECT 1",
        "SELECT 1",
        # Not a leading comment, not version 00, not lowercase, not alone.
        f"SELECT 1 /*traceparent='{tp}-01'*/",
        f" /*traceparent='{tp}-01'*/ SELECT 1",
        f"/*traceparent='01-{untraced}-{SPAN}-01'*/ SELECT 1",
        f"/*traceparent='{tp.upper()}-01'*/ SELECT 1",
        f"/*traceparent='{tp}-01',traceparent='{tp}-01'*/ SELECT 1",
        # tracestate alone isn't read.
        "/*tracestate='congo=t61rcWkgMzE'*/ SELECT 1",
    )
    marker = new_trace()
    run(pgcat, f"{comment(marker)} SELECT 1")
    assert [span["trace_id"] for span in collector.spans_until(marker)] == [marker]


def test_the_ratio_keeps_traces_by_trace_id(pgcat, collector):
    reconfigure(pgcat, 0.5, collector.endpoint)
    # TraceIdRatioBased keeps a trace when its id's low 63 bits, shifted, fall
    # under the ratio: every service sampling at 0.5 keeps the same traces.
    dropped = "4bf92f3577b34da6" + "ffffffffffffffff"
    kept = "4bf92f3577b34da6" + "0000000000000001"
    run(pgcat, f"{comment(dropped)} SELECT 1", f"{comment(kept)} SELECT 1")
    assert [span["trace_id"] for span in collector.spans_until(kept)] == [kept]


def test_an_endpoint_change_takes_effect_at_restart(pgcat, collector):
    second = Collector()
    try:
        reconfigure(pgcat, 1, second.endpoint)
        trace = new_trace()
        run(pgcat, f"{comment(trace)} SELECT 1")
        only(collector.spans_until(trace), trace)
        assert second.requests.empty()
        assert "trace_otlp_endpoint changes take effect at restart" in pgcat.logs()
    finally:
        second.close()


def test_ratio_zero_records_nothing_and_reloads(collector):
    pgcat = utils.PgcatProcess(config(0, collector.endpoint), PORT, env=ENV).start()
    try:
        off = new_trace()
        run(pgcat, f"{comment(off)} SELECT 1")
        reconfigure(pgcat, 1, collector.endpoint)
        on = new_trace()
        run(pgcat, f"{comment(on)} SELECT 1")
        assert [span["trace_id"] for span in collector.spans_until(on)] == [on]

        # Turning it back off stops new spans, also on an open connection.
        conn = connect(pgcat)
        try:
            cur = conn.cursor()
            reconfigure(pgcat, 0, collector.endpoint)
            off_again = new_trace()
            cur.execute(f"{comment(off_again)} SELECT 1")
            reconfigure(pgcat, 1, collector.endpoint)
            on_again = new_trace()
            cur.execute(f"{comment(on_again)} SELECT 1")
        finally:
            conn.close()
        assert [span["trace_id"] for span in collector.spans_until(on_again)] == [on, on_again]
    finally:
        pgcat.stop()


def test_a_flush_ends_the_span(pgcat, collector):
    # A driver's Flush gets the batch's replies without a Sync: pgcat syncs
    # the server for it, and that ReadyForQuery ends the span.
    trace = new_trace()
    sock, message = utils.raw_connection(PORT, "shard0", timeout=10)
    try:
        sql = f"{comment(trace)} SELECT 1".encode()
        sock.sendall(
            utils.frontend(b"P", b"\0" + sql + b"\0" + struct.pack("!h", 0))
            + utils.frontend(b"B", b"\0\0" + struct.pack("!hhh", 0, 0, 0))
            + utils.frontend(b"E", b"\0" + struct.pack("!i", 0))
            + utils.frontend(b"H", b"")
        )
        assert [message()[0] for _ in range(4)] == [b"1", b"2", b"D", b"C"]
        span = only(collector.spans_until(trace), trace)
        assert span["parent_span_id"] == SPAN
        assert span["attributes"]["pgcat.protocol"] == "extended"

        sock.sendall(utils.frontend(b"S", b""))
        assert message()[0] == b"Z"
    finally:
        sock.close()
    # The Sync's ReadyForQuery doesn't end it again.
    marker = new_trace()
    run(pgcat, f"{comment(marker)} SELECT 1")
    only(collector.spans_until(marker), trace)


def test_statements_in_a_transaction_are_traced(pgcat, collector):
    trace = new_trace()
    conn = connect(pgcat)
    try:
        conn.autocommit = False
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.execute(f"{comment(trace)} SELECT 2")
        conn.commit()
    finally:
        conn.close()
    assert only(collector.spans_until(trace), trace)["parent_span_id"] == SPAN


def test_a_failed_statement_has_an_error_status(pgcat, collector):
    trace = new_trace()
    with pytest.raises(psycopg2.errors.DivisionByZero):
        run(pgcat, f"{comment(trace)} SELECT 1/0")

    span = only(collector.spans_until(trace), trace)
    assert span["status"] == STATUS_ERROR
    assert span["attributes"]["db.response.status_code"] == "22012"
    assert span["attributes"]["error.type"] == "22012"


def run_as(pgcat, application_name, *statements):
    conn = psycopg2.connect(pgcat.dsn("shard0").replace("testing_pgcat", application_name))
    conn.autocommit = True
    try:
        cur = conn.cursor()
        for statement in statements:
            cur.execute(statement)
    finally:
        conn.close()


def test_spans_name_only_configured_clients(collector):
    pool = 'sql_observer_clients = ["api", "worker"]'
    pgcat = utils.PgcatProcess(config(1, collector.endpoint, pool=pool), PORT, env=ENV).start()
    try:
        traces = {name: new_trace() for name in ("api", "worker", "API", "web", "")}
        for name, trace in traces.items():
            run_as(pgcat, name, f"{comment(trace)} SELECT 1")
        spans = collector.spans_until(traces[""])
    finally:
        pgcat.stop()
    clients = {name: only(spans, trace)["attributes"]["pgcat.client"] for name, trace in traces.items()}
    # Only an exact match: any other name, the empty one too, is `other`.
    assert clients == {"api": "api", "worker": "worker", "API": "other", "web": "other", "": "other"}


def test_spans_record_the_identity_state_never_the_token(pgcat, collector):
    # Nothing on this pool verifies identities: pgcat keeps the token as sent.
    token = f"secret-{new_trace()}"
    unverified, reset = new_trace(), new_trace()
    run(
        pgcat,
        f"SET PGCAT_IDENTITY TO '{token}'",
        f"{comment(unverified)} SELECT 1",
        "RESET PGCAT_IDENTITY",
        f"{comment(reset)} SELECT 1",
    )
    spans = collector.spans_until(reset)
    attributes = only(spans, unverified)["attributes"]
    assert (attributes["pgcat.identity"], attributes["pgcat.identity.verified"]) == ("present", False)
    attributes = only(spans, reset)["attributes"]
    assert (attributes["pgcat.identity"], attributes["pgcat.identity.verified"]) == ("absent", False)
    assert token not in str(spans) and token not in pgcat.logs()


def test_a_refused_statement_gets_a_span_with_pgcats_sqlstate(pgcat, collector):
    # An identity command pgcat can't forward without sending its token: it
    # refuses it without a server, with 0A000.
    token = f"secret-{new_trace()}"
    trace = new_trace()
    with pytest.raises(psycopg2.errors.FeatureNotSupported):
        run(pgcat, f"{comment(trace)} SET PGCAT_IDENTITY TO '{token}'; SELECT 1")

    span = only(collector.spans_until(trace), trace)
    assert span["parent_span_id"] == SPAN
    assert span["status"] == STATUS_ERROR
    assert span["attributes"] == {
        "db.system.name": "postgresql",
        "pgcat.pool.name": "shard0",
        "pgcat.pool.user": "sharding_user",
        "pgcat.protocol": "simple",
        "pgcat.identity": "absent",
        "pgcat.identity.verified": False,
        "pgcat.client": "other",
        "pgcat.refused": True,
        "error.type": "0A000",
    }
    assert token not in str(span) and token not in pgcat.logs()


@pytest.mark.parametrize("protocol", ["simple", "extended"])
def test_a_statement_no_server_could_be_checked_out_for_gets_a_span(collector, protocol):
    pgcat = utils.PgcatProcess(
        config(1, collector.endpoint, general="connect_timeout = 300", pool_size=1), PORT, env=ENV
    ).start()
    try:
        holder = connect(pgcat)
        try:
            holder.cursor().execute("BEGIN")
            trace = new_trace()
            if protocol == "simple":
                with pytest.raises(psycopg2.errors.TooManyConnections):
                    run(pgcat, f"{comment(trace)} SELECT 1")
            else:
                with psycopg.connect(pgcat.dsn("shard0"), autocommit=True) as conn:
                    with pytest.raises(psycopg.errors.TooManyConnections):
                        # With parameters, psycopg sends it in the extended protocol.
                        conn.execute(comment(trace).replace("%", "%%") + " SELECT %s::int", (1,))
        finally:
            holder.close()
        span = only(collector.spans_until(trace), trace)
    finally:
        pgcat.stop()
    assert span["status"] == STATUS_ERROR
    attributes = span["attributes"]
    assert (attributes["pgcat.protocol"], attributes["pgcat.refused"]) == (protocol, True)
    assert attributes["error.type"] == "53300"
    assert "pgcat.server.role" not in attributes and "db.response.status_code" not in attributes


def unsynced_execute(sql):
    """Parse, Bind and Execute of `sql`, without the Sync that runs them."""
    sql = sql.encode()
    return (
        utils.frontend(b"P", b"\0" + sql + b"\0" + struct.pack("!h", 0))
        + utils.frontend(b"B", b"\0\0" + struct.pack("!hhh", 0, 0, 0))
        + utils.frontend(b"E", b"\0" + struct.pack("!i", 0))
    )


def until_ready(message):
    replies = [message()]
    while replies[-1][0] != b"Z":
        replies.append(message())
    return replies


@pytest.mark.parametrize("traced_query", [True, False])
@pytest.mark.parametrize("refusal", ["identity", "checkout"])
def test_a_refused_query_ends_only_its_own_span(collector, refusal, traced_query):
    # A simple Query pgcat refuses while an Execute waits for its Sync: the
    # Execute runs at the Sync, and the server's reply ends its own span.
    pgcat = utils.PgcatProcess(
        config(1, collector.endpoint, general="connect_timeout = 300", pool_size=1), PORT, env=ENV
    ).start()
    try:
        executed, refused = new_trace(), new_trace()
        # A multi-statement identity command, or no server to check out.
        query, sqlstate = "SET PGCAT_IDENTITY TO 'x'; SELECT 1", b"0A000"
        holder = connect(pgcat)
        if refusal == "checkout":
            query, sqlstate = "SELECT 1", b"53300"
            holder.cursor().execute("BEGIN")
        if traced_query:
            query = f"{comment(refused)} {query}"
        sock, message = utils.raw_connection(PORT, "shard0", timeout=10)
        try:
            sock.sendall(
                unsynced_execute(f"{comment(executed)} SELECT 41")
                + utils.frontend(b"Q", query.encode() + b"\0")
            )
            replies = until_ready(message)
            assert [code for code, _ in replies] == [b"E", b"Z"], replies
            assert b"C" + sqlstate + b"\0" in replies[0][1], replies
            holder.close()

            sock.sendall(utils.frontend(b"S", b""))
            replies = until_ready(message)
            assert [code for code, _ in replies] == [b"1", b"2", b"D", b"C", b"Z"], replies
            assert replies[2][1].endswith(b"41"), replies
        finally:
            sock.close()
        marker = new_trace()
        run(pgcat, f"{comment(marker)} SELECT 1")
        spans = collector.spans_until(marker)
    finally:
        pgcat.stop()

    span = only(spans, executed)
    assert span["status"] == 0
    assert span["attributes"] == {
        "db.system.name": "postgresql",
        "pgcat.pool.name": "shard0",
        "pgcat.pool.user": "sharding_user",
        "pgcat.server.role": "primary",
        "pgcat.shard": 0,
        "pgcat.protocol": "extended",
        "pgcat.identity": "absent",
        "pgcat.identity.verified": False,
        "pgcat.client": "other",
    }
    refusals = [span for span in spans if span["attributes"].get("pgcat.refused")]
    if traced_query:
        assert refusals == [only(spans, refused)]
        attributes = refusals[0]["attributes"]
        assert (attributes["pgcat.protocol"], attributes["error.type"]) == ("simple", sqlstate.decode())
        assert "pgcat.server.role" not in attributes
    else:
        assert refusals == []


def test_the_checkout_failure_limit_ends_the_spans_waiting_for_a_sync(collector):
    # The limit ends the session at a Query no server could be checked out
    # for: the Execute that waited for its Sync never runs, and its span ends
    # as refused too, with the FATAL's SQLSTATE.
    pgcat = utils.PgcatProcess(
        config(
            1,
            collector.endpoint,
            pool="checkout_failure_limit = 1",
            general="connect_timeout = 300",
            pool_size=1,
        ),
        PORT,
        env=ENV,
    ).start()
    try:
        executed, refused = new_trace(), new_trace()
        holder = connect(pgcat)
        try:
            holder.cursor().execute("BEGIN")
            sock, message = utils.raw_connection(PORT, "shard0", timeout=10)
            try:
                sock.sendall(
                    unsynced_execute(f"{comment(executed)} SELECT 41")
                    + utils.frontend(b"Q", f"{comment(refused)} SELECT 1".encode() + b"\0")
                )
                replies = [message() for _ in range(3)]
                assert [code for code, _ in replies] == [b"E", b"Z", b"E"], replies
                assert utils.error_fields(replies[2][1])["S"] == "FATAL", replies
                assert utils.closed(sock)
            finally:
                sock.close()
        finally:
            holder.close()
        marker = new_trace()
        run(pgcat, f"{comment(marker)} SELECT 1")
        spans = collector.spans_until(marker)
    finally:
        pgcat.stop()

    for trace, protocol in ((refused, "simple"), (executed, "extended")):
        span = only(spans, trace)
        assert span["status"] == STATUS_ERROR
        attributes = span["attributes"]
        assert (attributes["pgcat.protocol"], attributes["pgcat.refused"]) == (protocol, True)
        assert attributes["error.type"] == "53300"
        assert "pgcat.server.role" not in attributes


def one_server(collector, pool=""):
    """pgcat with one server, for a test to hold so checkouts fail."""
    return utils.PgcatProcess(
        config(1, collector.endpoint, pool=pool, general="connect_timeout = 300", pool_size=1),
        PORT,
        env=ENV,
    ).start()


def held(pgcat):
    holder = connect(pgcat)
    holder.cursor().execute("BEGIN")
    return holder


def spans_after(pgcat, collector):
    """Every span exported so far, once a marker statement's span arrives."""
    marker = new_trace()
    run(pgcat, f"{comment(marker)} SELECT 1")
    return collector.spans_until(marker)


def test_an_identity_command_pgcat_answered_in_a_pipeline_gets_no_span(collector):
    # Psycopg sends a pipelined statement as Parse, Bind, Describe, Execute,
    # Sync and Flush. pgcat answers the identity command at the Sync, so the
    # Flush that finds no server has nothing left to refuse.
    pgcat = one_server(collector)
    try:
        answered = new_trace()
        holder = held(pgcat)
        conn = psycopg.connect(pgcat.dsn("shard0"), autocommit=True)
        try:
            with pytest.raises(psycopg.Error):
                with conn.pipeline():
                    cur = conn.execute(f"{comment(answered)} SHOW PGCAT_IDENTITY")
                    assert cur.fetchone() == (False, None)
        finally:
            conn.close()
            holder.close()
        spans = spans_after(pgcat, collector)
    finally:
        pgcat.stop()
    assert [span for span in spans if span["trace_id"] == answered] == []


@pytest.mark.parametrize("end", ["sync", "flush", "query"])
def test_a_batch_pgcat_answered_gets_no_span_when_the_next_sync_is_refused(collector, end):
    pgcat = one_server(collector)
    try:
        answered = new_trace()
        sql = f"{comment(answered)} SHOW PGCAT_IDENTITY"
        holder = held(pgcat)
        sock, message = utils.raw_connection(PORT, "shard0", timeout=10)
        try:
            if end == "query":
                sock.sendall(utils.frontend(b"Q", sql.encode() + b"\0"))
                replies = until_ready(message)
                assert [code for code, _ in replies] == [b"T", b"D", b"C", b"Z"], replies
            elif end == "sync":
                sock.sendall(unsynced_execute(sql) + utils.frontend(b"S", b""))
                replies = until_ready(message)
                assert [code for code, _ in replies] == [b"1", b"2", b"D", b"C", b"Z"], replies
            else:
                sock.sendall(unsynced_execute(sql) + utils.frontend(b"H", b""))
                replies = [message() for _ in range(4)]
                assert [code for code, _ in replies] == [b"1", b"2", b"D", b"C"], replies

            # A Sync on its own, with no server to check out.
            sock.sendall(utils.frontend(b"S", b""))
            replies = until_ready(message)
            assert [code for code, _ in replies] == [b"E", b"Z"], replies
            assert utils.error_fields(replies[0][1])["C"] == "53300", replies
        finally:
            sock.close()
            holder.close()
        spans = spans_after(pgcat, collector)
    finally:
        pgcat.stop()
    assert [span for span in spans if span["trace_id"] == answered] == []


def test_each_execute_of_a_refused_batch_gets_one_span(collector):
    # After a batch pgcat answered, a batch no server could be checked out for
    # still ends each Execute's span as refused, once.
    pgcat = one_server(collector)
    try:
        answered, first, second = new_trace(), new_trace(), new_trace()
        holder = held(pgcat)
        sock, message = utils.raw_connection(PORT, "shard0", timeout=10)
        try:
            sock.sendall(unsynced_execute(f"{comment(answered)} SHOW PGCAT_IDENTITY") + utils.frontend(b"S", b""))
            replies = until_ready(message)
            assert [code for code, _ in replies] == [b"1", b"2", b"D", b"C", b"Z"], replies
            sock.sendall(
                unsynced_execute(f"{comment(first)} SELECT 1")
                + unsynced_execute(f"{comment(second)} SELECT 2")
                + utils.frontend(b"S", b"")
            )
            for _ in range(2):
                replies = until_ready(message)
                assert [code for code, _ in replies] == [b"E", b"Z"], replies
                assert utils.error_fields(replies[0][1])["C"] == "53300", replies
                sock.sendall(utils.frontend(b"S", b""))
        finally:
            sock.close()
            holder.close()
        spans = spans_after(pgcat, collector)
    finally:
        pgcat.stop()
    assert [span for span in spans if span["trace_id"] == answered] == []
    for trace in (first, second):
        span = only(spans, trace)
        assert span["status"] == STATUS_ERROR
        attributes = span["attributes"]
        assert (attributes["pgcat.protocol"], attributes["pgcat.refused"]) == ("extended", True)
        assert attributes["error.type"] == "53300"


def function_call():
    """A FunctionCall of no function, no arguments: never reaches a server here."""
    return utils.frontend(b"F", struct.pack("!ihhh", 0, 0, 0, 0))


@pytest.mark.parametrize("limit", [False, True])
def test_a_refused_function_call_leaves_the_execute_waiting_for_its_sync(collector, limit):
    # An Execute waits for its Sync, and pgcat answers a Query in between.
    # A function call no server could be checked out for refuses only itself.
    pgcat = one_server(collector, pool="checkout_failure_limit = 1" if limit else "")
    try:
        executed, answered = new_trace(), new_trace()
        holder = held(pgcat)
        sock, message = utils.raw_connection(PORT, "shard0", timeout=10)
        try:
            sock.sendall(
                unsynced_execute(f"{comment(executed)} SELECT 41")
                + utils.frontend(b"Q", f"{comment(answered)} SHOW PGCAT_IDENTITY".encode() + b"\0")
            )
            replies = until_ready(message)
            assert [code for code, _ in replies] == [b"T", b"D", b"C", b"Z"], replies

            sock.sendall(function_call())
            replies = until_ready(message)
            assert [code for code, _ in replies] == [b"E", b"Z"], replies
            assert utils.error_fields(replies[0][1])["C"] == "53300", replies
            if limit:
                # The limit ends the session: the Execute never runs.
                code, body = message()
                assert code == b"E" and utils.error_fields(body)["S"] == "FATAL", (code, body)
                assert utils.closed(sock)
            else:
                holder.close()
                sock.sendall(utils.frontend(b"S", b""))
                replies = until_ready(message)
                assert [code for code, _ in replies] == [b"1", b"2", b"D", b"C", b"Z"], replies
                assert replies[2][1].endswith(b"41"), replies
        finally:
            sock.close()
            holder.close()
        spans = spans_after(pgcat, collector)
    finally:
        pgcat.stop()

    assert [span for span in spans if span["trace_id"] == answered] == []
    attributes = only(spans, executed)["attributes"]
    if limit:
        assert (attributes["pgcat.refused"], attributes["error.type"]) == (True, "53300")
    else:
        assert "pgcat.refused" not in attributes, attributes
        assert (attributes["pgcat.server.role"], attributes["pgcat.shard"]) == ("primary", 0)


def simple_query(sql):
    return utils.frontend(b"Q", sql.encode() + b"\0")


def queries_while_an_execute_waits(sock, message, executed, waiting, answer, local, failed):
    """An Execute of `waiting` without its Sync, a Query pgcat answers or
    refuses, then one a server runs and fails with 22012."""
    sock.sendall(unsynced_execute(f"{comment(executed)} {waiting}"))
    if answer == "answered":
        sock.sendall(simple_query(f"{comment(local)} SHOW PGCAT_IDENTITY"))
        expected = [b"T", b"D", b"C", b"Z"]
    else:
        sock.sendall(simple_query(f"{comment(local)} SET PGCAT_IDENTITY TO 'x'; SELECT 1"))
        expected = [b"E", b"Z"]
    replies = until_ready(message)
    assert [code for code, _ in replies] == expected, replies
    sock.sendall(simple_query(f"{comment(failed)} SELECT 1/0"))
    replies = until_ready(message)
    assert [code for code, _ in replies] == [b"E", b"Z"], replies
    assert utils.error_fields(replies[0][1])["C"] == "22012", replies


def server_errors(spans):
    return [span for span in spans if span["attributes"].get("db.response.status_code") == "22012"]


@pytest.mark.parametrize("answer", ["answered", "refused"])
@pytest.mark.parametrize("waiting", ["SHOW PGCAT_IDENTITY", "SELECT 41"])
def test_a_query_a_server_runs_ends_only_its_own_span(pgcat, collector, waiting, answer):
    # The server's reply to a Query is the Query's, even with an Execute
    # waiting for its Sync: the Execute's span ends at that Sync's reply.
    executed, local, failed = new_trace(), new_trace(), new_trace()
    sock, message = utils.raw_connection(PORT, "shard0", timeout=10)
    try:
        queries_while_an_execute_waits(sock, message, executed, waiting, answer, local, failed)
        before = spans_after(pgcat, collector)
        assert [span for span in before if span["trace_id"] == executed] == []

        sock.sendall(utils.frontend(b"S", b""))
        replies = until_ready(message)
        assert [code for code, _ in replies] == [b"1", b"2", b"D", b"C", b"Z"], replies
    finally:
        sock.close()
    spans = spans_after(pgcat, collector)

    assert server_errors(spans) == [only(spans, failed)]
    attributes = only(spans, failed)["attributes"]
    assert (attributes["pgcat.protocol"], attributes["pgcat.server.role"]) == ("simple", "primary")
    if waiting == "SELECT 41":
        span = only(spans, executed)
        assert span["status"] == 0
        attributes = span["attributes"]
        assert (attributes["pgcat.protocol"], attributes["pgcat.server.role"]) == ("extended", "primary")
        assert "pgcat.refused" not in attributes and "error.type" not in attributes
    else:
        assert [span for span in spans if span["trace_id"] == executed] == []
    if answer == "refused":
        attributes = only(spans, local)["attributes"]
        assert (attributes["pgcat.refused"], attributes["error.type"]) == (True, "0A000")
    else:
        assert [span for span in spans if span["trace_id"] == local] == []


def test_an_execute_waiting_when_the_client_disconnects_gets_no_span(pgcat, collector):
    executed, local, failed = new_trace(), new_trace(), new_trace()
    sock, message = utils.raw_connection(PORT, "shard0", timeout=10)
    try:
        queries_while_an_execute_waits(sock, message, executed, "SELECT 41", "answered", local, failed)
    finally:
        sock.close()
    spans = spans_after(pgcat, collector)
    assert server_errors(spans) == [only(spans, failed)]
    assert [span for span in spans if span["trace_id"] in (executed, local)] == []


@pytest.mark.parametrize("ending", [b"S", b"H"])
def test_a_copy_keeps_its_span_past_a_sync_in_its_data(pgcat, collector, ending):
    # The server ignores a Sync or Flush during COPY IN; here a bad row before
    # it has failed the COPY, whose span ends at the COPY's ReadyForQuery.
    copy = new_trace()
    sock, message = utils.raw_connection(PORT, "shard0", timeout=10)
    try:
        sock.sendall(simple_query("BEGIN; CREATE TEMP TABLE copied (n int) ON COMMIT DROP"))
        assert [code for code, _ in until_ready(message)] == [b"C", b"C", b"Z"]
        sock.sendall(simple_query(f"{comment(copy)} COPY copied FROM STDIN"))
        assert message()[0] == b"G"
        sock.sendall(utils.frontend(b"d", b"x\n") + utils.frontend(ending, b""))
        code, body = message()
        assert (code, utils.error_fields(body)["C"]) == (b"E", "22P02"), body
        if ending == b"S":
            assert message()[0] == b"Z"
        sock.sendall(utils.frontend(b"c", b""))
        assert message()[0] == b"Z"
        sock.sendall(simple_query("ROLLBACK"))
        assert [code for code, _ in until_ready(message)] == [b"C", b"Z"]
    finally:
        sock.close()
    span = only(spans_after(pgcat, collector), copy)
    assert span["status"] == STATUS_ERROR
    attributes = span["attributes"]
    assert (attributes["error.type"], attributes["db.response.status_code"]) == ("22P02", "22P02")
    assert (attributes["pgcat.protocol"], attributes["pgcat.server.role"]) == ("simple", "primary")



@pytest.mark.parametrize("row", [b"41\n", b"x\n"])
def test_a_batch_refused_during_a_copy_leaves_the_copy_its_span(pgcat, collector, row):
    # A Flush during COPY IN hands pgcat the batch buffered before the COPY,
    # which it refuses for mixing in an identity command. The COPY goes on, and
    # its own ReadyForQuery ends its span.
    executed, copy = new_trace(), new_trace()
    sock, message = utils.raw_connection(PORT, "shard0", timeout=10)
    try:
        sock.sendall(
            unsynced_execute(f"{comment(executed)} SELECT 41")
            + unsynced_execute("SHOW PGCAT_IDENTITY")
            + simple_query(
                f"{comment(copy)} CREATE TEMP TABLE copied (n int) ON COMMIT DROP; COPY copied FROM STDIN"
            )
        )
        assert [message()[0], message()[0]] == [b"C", b"G"]
        sock.sendall(utils.frontend(b"H", b""))
        code, body = message()
        assert (code, utils.error_fields(body)["C"]) == (b"E", "0A000"), body
        before = spans_after(pgcat, collector)
        attributes = only(before, executed)["attributes"]
        assert (attributes["pgcat.refused"], attributes["error.type"]) == (True, "0A000")
        assert [span for span in before if span["trace_id"] == copy] == []

        sock.sendall(utils.frontend(b"d", row) + utils.frontend(b"c", b""))
        (code, body), ready = until_ready(message)
        if row == b"41\n":
            assert (code, body) == (b"C", b"COPY 1\x00")
        else:
            assert (code, utils.error_fields(body)["C"]) == (b"E", "22P02"), body
        assert ready[0] == b"Z"
        # The Sync ends the discard the refused Flush began.
        sock.sendall(utils.frontend(b"S", b""))
        assert [code for code, _ in until_ready(message)] == [b"Z"]
    finally:
        sock.close()
    spans = spans_after(pgcat, collector)
    assert only(spans, executed)["attributes"]["pgcat.refused"] is True
    span = only(spans, copy)
    attributes = span["attributes"]
    assert "pgcat.refused" not in attributes
    assert (attributes["pgcat.protocol"], attributes["pgcat.server.role"]) == ("simple", "primary")
    if row == b"41\n":
        assert span["status"] == 0
        assert "error.type" not in attributes
    else:
        assert span["status"] == STATUS_ERROR
        assert (attributes["error.type"], attributes["db.response.status_code"]) == ("22P02", "22P02")


def test_a_copy_whose_server_pgcat_let_go_gets_no_span(collector):
    # In transaction mode, a Sync pgcat answers itself outside a transaction
    # gives the server back to the pool, which closes it mid-COPY. The COPY's
    # ReadyForQuery never comes, and a later refusal mustn't end its span.
    pgcat = utils.PgcatProcess(
        config(
            1,
            collector.endpoint,
            pool="checkout_failure_limit = 1",
            general="connect_timeout = 300",
            pool_size=1,
        ),
        PORT,
        env=ENV,
    ).start()
    try:
        executed, copy = new_trace(), new_trace()
        sock, message = utils.raw_connection(PORT, "shard0", timeout=10)
        try:
            sock.sendall(
                unsynced_execute(f"{comment(executed)} SELECT 41")
                + unsynced_execute("SHOW PGCAT_IDENTITY")
                + simple_query(
                    f"{comment(copy)} CREATE TEMP TABLE copied (n int) ON COMMIT DROP; COPY copied FROM STDIN"
                )
            )
            assert [message()[0], message()[0]] == [b"C", b"G"]
            sock.sendall(utils.frontend(b"S", b""))
            (code, body), ready = until_ready(message)
            assert (code, utils.error_fields(body)["C"]) == (b"E", "0A000"), body
            assert ready == (b"Z", b"I")
            before = spans_after(pgcat, collector)
            attributes = only(before, executed)["attributes"]
            assert (attributes["pgcat.refused"], attributes["error.type"]) == (True, "0A000")
            assert [span for span in before if span["trace_id"] == copy] == []

            # With the only server held, the next Sync ends the session.
            holder = held(pgcat)
            try:
                sock.sendall(utils.frontend(b"S", b""))
                replies = [message() for _ in range(3)]
                assert [code for code, _ in replies] == [b"E", b"Z", b"E"], replies
                assert utils.error_fields(replies[0][1])["C"] == "53300", replies
                assert utils.error_fields(replies[2][1])["S"] == "FATAL", replies
                assert utils.closed(sock)
            finally:
                holder.close()
        finally:
            sock.close()
        spans = spans_after(pgcat, collector)
    finally:
        pgcat.stop()
    assert only(spans, executed)["attributes"]["error.type"] == "0A000"
    assert [span for span in spans if span["trace_id"] == copy] == []


PLUGINS = """
[pools.shard0.plugins.testing]
enabled = true
verify_identity = true
"""


@pytest.mark.skipif(
    os.environ.get("PGCAT_TEST_PLUGINS") != "1",
    reason="needs pgcat built with --features test-plugins and PGCAT_TEST_PLUGINS=1",
)
def test_spans_record_whether_a_plugin_verified_the_identity(collector):
    pool = """
query_parser_enabled = true
plugins_require_parse = true
identity_tenant_parameter = "app.tenant"
"""
    pgcat = utils.PgcatProcess(
        config(1, collector.endpoint, pool=pool, sections=PLUGINS), PORT, env=ENV
    ).start()
    try:
        # The testing verifier accepts this token and refuses one starting
        # with `invalid`.
        token, invalid = f"tenant:{new_trace()}", f"invalid-{new_trace()}"
        verified, refused, after = new_trace(), new_trace(), new_trace()
        conn = connect(pgcat)
        try:
            cur = conn.cursor()
            cur.execute(f"SET PGCAT_IDENTITY TO '{token}'")
            cur.execute(f"{comment(verified)} SELECT 1")
            with pytest.raises(psycopg2.errors.InvalidAuthorizationSpecification):
                cur.execute(f"{comment(refused)} SET PGCAT_IDENTITY TO '{invalid}'")
            cur.execute(f"{comment(after)} SELECT 1")
        finally:
            conn.close()
        spans = collector.spans_until(after)
        logs = pgcat.logs()
    finally:
        pgcat.stop()

    attributes = only(spans, verified)["attributes"]
    assert (attributes["pgcat.identity"], attributes["pgcat.identity.verified"]) == ("present", True)
    # The refused SET runs with the identity before it, and fails with 28000.
    span = only(spans, refused)
    assert span["status"] == STATUS_ERROR
    assert span["attributes"]["pgcat.identity"] == "present"
    assert span["attributes"]["pgcat.refused"] is True
    assert span["attributes"]["error.type"] == "28000"
    assert "pgcat.server.role" not in span["attributes"]
    attributes = only(spans, after)["attributes"]
    assert (attributes["pgcat.identity"], attributes["pgcat.identity.verified"]) == ("refused", False)
    for secret in (token, invalid):
        assert secret not in str(spans) and secret not in logs, secret


def test_a_statement_timeout_ends_the_span(collector):
    pgcat = utils.PgcatProcess(
        config(1, collector.endpoint, statement_timeout=200), PORT, env=ENV
    ).start()
    try:
        trace = new_trace()
        with pytest.raises(psycopg2.OperationalError):
            run(pgcat, f"{comment(trace)} SELECT pg_sleep(5)")
        span = only(collector.spans_until(trace), trace)
    finally:
        pgcat.stop()
    assert span["status"] == STATUS_ERROR
    assert span["attributes"]["error.type"] == "statement_timeout"
    assert "db.response.status_code" not in span["attributes"]


def test_export_failures_never_reach_statements(pgcat, collector):
    collector.status = 500
    # Each export fails; the statements don't notice, and pgcat warns once,
    # not per export. The warning for an export is logged before the next
    # export is sent, so after the third request the first two are logged.
    for _ in range(3):
        run(pgcat, f"{comment(new_trace())} SELECT 1")
        collector.request()
    assert pgcat.logs().count("Statement span export failed: HTTP 500") == 1

    collector.status = 200
    marker = new_trace()
    run(pgcat, f"{comment(marker)} SELECT 1")
    collector.spans_until(marker)
    run(pgcat, f"{comment(new_trace())} SELECT 1")
    collector.request()
    assert "Statement span export recovered" in pgcat.logs()


def test_an_unreachable_collector_never_reaches_statements():
    with socket.socket() as unused:
        unused.bind(("127.0.0.1", 0))
        closed = f"http://127.0.0.1:{unused.getsockname()[1]}/v1/traces"
    pgcat = utils.PgcatProcess(config(1, closed), PORT, env=ENV).start()
    try:
        deadline = time.monotonic() + 10
        while "Statement span export failed" not in pgcat.logs():
            assert time.monotonic() < deadline, pgcat.logs()
            run(pgcat, f"{comment(new_trace())} SELECT 1")
        run(pgcat, f"{comment(new_trace())} SELECT 1")
    finally:
        pgcat.stop()


@pytest.mark.parametrize("variable", ["OTEL_EXPORTER_OTLP_HEADERS", "OTEL_EXPORTER_OTLP_TRACES_HEADERS"])
def test_otlp_headers_from_the_environment_are_not_sent(collector, variable):
    # The usual place for an OTLP credential; pgcat exports over plain http.
    # The SDK lets them replace its own content headers too.
    headers = "authorization=secret,x-api-key=secret,content-type=sentinel,content-encoding=sentinel"
    env = dict(ENV, **{variable: headers})
    pgcat = utils.PgcatProcess(config(1, collector.endpoint), PORT, env=env).start()
    try:
        trace = new_trace()
        run(pgcat, f"{comment(trace)} SELECT 1")
        only(collector.spans_until(trace), trace)
    finally:
        pgcat.stop()
    for headers in collector.headers:
        assert set(headers) == {"host", "content-type", "content-length"}, headers
        assert headers["content-type"] == "application/x-protobuf", headers


def test_a_bad_endpoint_from_the_environment_isnt_logged(collector):
    # Not a valid URI, so the SDK refuses it, quoting the value.
    env = dict(ENV, OTEL_EXPORTER_OTLP_TRACES_ENDPOINT="http://collector:4318/v1/traces?key=xq7 zv9")
    pgcat = utils.PgcatProcess(config(1, None), PORT, env=env).start()
    try:
        # Tracing stays off; statements are unaffected.
        run(pgcat, f"{comment(new_trace())} SELECT 1")
        logs = pgcat.logs()
    finally:
        pgcat.stop()
    assert "Statement tracing stays off" in logs, logs
    assert "xq7" not in logs and "zv9" not in logs, logs


def unused_endpoint():
    with socket.socket() as unused:
        unused.bind(("127.0.0.1", 0))
        return f"http://127.0.0.1:{unused.getsockname()[1]}"


def collector_base(collector):
    return collector.endpoint.removesuffix("/v1/traces")


@pytest.mark.parametrize(
    "variables",
    [
        # The generic endpoint gets the signal path, with or without a slash.
        lambda collector: {"OTEL_EXPORTER_OTLP_ENDPOINT": collector_base(collector)},
        lambda collector: {"OTEL_EXPORTER_OTLP_ENDPOINT": collector_base(collector) + "/"},
        # The traces endpoint is used as is and wins over the generic one;
        # an empty one counts as unset.
        lambda collector: {
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": collector.endpoint,
            "OTEL_EXPORTER_OTLP_ENDPOINT": unused_endpoint(),
        },
        lambda collector: {
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "",
            "OTEL_EXPORTER_OTLP_ENDPOINT": collector_base(collector),
        },
    ],
)
def test_the_environment_endpoint_resolves_in_protocol_order(collector, variables):
    env = dict(ENV, **variables(collector))
    pgcat = utils.PgcatProcess(config(1, None), PORT, env=env).start()
    try:
        trace = new_trace()
        run(pgcat, f"{comment(trace)} SELECT 1")
        # The collector checks the request path is /v1/traces.
        only(collector.spans_until(trace), trace)
    finally:
        pgcat.stop()


@pytest.mark.skipif(not os.supports_bytes_environ, reason="needs a bytes environment")
def test_a_non_unicode_traces_endpoint_isnt_skipped(collector):
    # The generic endpoint would work, but the traces one is set: tracing stays off.
    env = dict(ENV, OTEL_EXPORTER_OTLP_ENDPOINT=collector_base(collector))
    pgcat = utils.PgcatProcess(config(1, None), PORT, env=env)
    os.environb[b"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] = b"http://127.0.0.1/\xff"
    try:
        pgcat.start()
        try:
            run(pgcat, f"{comment(new_trace())} SELECT 1")
            logs = pgcat.logs()
        finally:
            pgcat.stop()
    finally:
        del os.environb[b"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"]
    assert "Statement tracing stays off" in logs, logs
    assert collector.requests.empty()


def test_shutdown_flushes_queued_spans(collector):
    # A long export interval: only the shutdown flush sends this span.
    env = {"OTEL_BSP_SCHEDULE_DELAY": "60000"}
    pgcat = utils.PgcatProcess(config(1, collector.endpoint), PORT, env=env).start()
    try:
        trace = new_trace()
        run(pgcat, f"{comment(trace)} SELECT 1")
        assert collector.requests.empty()
    finally:
        pgcat.stop()
    only(collector.spans_until(trace), trace)
