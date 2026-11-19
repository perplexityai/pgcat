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


def config(ratio, endpoint, statement_timeout=0):
    endpoint = "" if endpoint is None else f'trace_otlp_endpoint = "{endpoint}"'
    return f"""
[general]
host = "127.0.0.1"
port = {PORT}
admin_username = "admin_user"
admin_password = "admin_pass"
trace_sample_ratio = {ratio}
{endpoint}

[pools.shard0]
pool_mode = "transaction"
prepared_statements_cache_size = 100

[pools.shard0.users.0]
username = "sharding_user"
password = "sharding_user"
pool_size = 5
statement_timeout = {statement_timeout}

[pools.shard0.shards.0]
servers = [["127.0.0.1", {PG_PORT}, "primary"]]
database = "shard0"
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
