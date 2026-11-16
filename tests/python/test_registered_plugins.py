"""A separate executable registers a plugin through the public library API."""
import asyncio
import os
import struct
import subprocess

import asyncpg
import psycopg
import psycopg2
import pytest

import utils

PORT = 6447
BINARY = os.environ.get(
    "PGCAT_REGISTERED_PLUGIN_BINARY", "./target/debug/examples/registered_plugin"
)
pytestmark = pytest.mark.skipif(
    os.environ.get("PGCAT_TEST_PLUGINS") != "1",
    reason="requires the test-only registered_plugin example",
)

CONFIG = f"""
[general]
host = "127.0.0.1"
port = {PORT}
admin_username = "admin_user"
admin_password = "admin_pass"

[pools.registration]
pool_mode = "transaction"
query_parser_enabled = false
prepared_statements_cache_size = 100
plugin_hook_timeout = 1000

[pools.registration.users.0]
username = "sharding_user"
password = "sharding_user"
pool_size = 1

[pools.registration.shards.0]
servers = [["127.0.0.1", 5432, "primary"]]
database = "shard0"
"""


@pytest.fixture(scope="module")
def pgcat():
    process = utils.PgcatProcess(CONFIG, PORT, binary=BINARY)
    try:
        process.start()
        yield process
    finally:
        process.stop()


@pytest.fixture(params=["text", "structured", "debug"])
def logging_pgcat(request):
    port = 6448
    process = utils.PgcatProcess(
        CONFIG.replace(f"port = {PORT}", f"port = {port}"),
        port,
        log_level="trace",
        binary=BINARY,
    )
    try:
        with pytest.MonkeyPatch.context() as environment:
            environment.setenv("RUST_LOG", "sqlparser::parser=trace")
            environment.setenv("LOG_FORMAT", request.param)
            process.start()
        yield process
    finally:
        process.stop()


@pytest.mark.parametrize("extended", [False, True])
def test_rewrite_deny_timeout_and_error_recovery(pgcat, extended):
    connect = psycopg.connect if extended else psycopg2.connect
    connection = connect(pgcat.dsn("registration"))
    try:
        connection.autocommit = True
        if extended:
            connection.prepare_threshold = 0
        with connection.cursor() as cursor:
            cursor.execute("SELECT 7001")
            assert cursor.fetchone() == (7002,)
            for query, sqlstate in [
                ("SELECT 7003", "42501"),
                ("SELECT 7004", "PH001"),
                ("SELECT 7005", "XX000"),
                ("SELECT 7006", "42501"),
                ("SELECT 7008", "42501"),
                ("not valid sql", "0A000"),
            ]:
                with pytest.raises((psycopg.Error, psycopg2.Error)) as error:
                    cursor.execute(query)
                observed = error.value.sqlstate if extended else error.value.pgcode
                assert observed == sqlstate
                cursor.execute("SELECT 7")
                assert cursor.fetchone() == (7,)
    finally:
        connection.close()


@pytest.mark.parametrize("extended", [False, True])
def test_password_literals_and_parse_errors_are_not_disclosed(logging_pgcat, extended):
    password = f"synthetic_password_{extended}_not_a_credential"
    invalid_literal = f"synthetic_invalid_{extended}_not_a_credential"
    connect = psycopg.connect if extended else psycopg2.connect
    connection = connect(logging_pgcat.dsn("registration"))
    try:
        if extended:
            # Unnamed extended queries exercise Parse without coupling this
            # logging test to driver-managed prepared-statement invalidation.
            connection.prepare_threshold = None
        with (connection.cursor(binary=True) if extended else connection.cursor()) as cursor:
            # Exercise a valid password statement without changing even the
            # scratch role's credentials: Postgres must reject the write.
            cursor.execute("SET TRANSACTION READ ONLY")
            with pytest.raises((psycopg.Error, psycopg2.Error)) as read_only:
                cursor.execute(f"ALTER ROLE sharding_user PASSWORD '{password}'")
            observed = read_only.value.sqlstate if extended else read_only.value.pgcode
            assert observed == "25006"
            assert password not in str(read_only.value)
            connection.rollback()
            connection.autocommit = True
            with pytest.raises((psycopg.Error, psycopg2.Error)) as error:
                cursor.execute(
                    f"ALTER ROLE sharding_user PASSWORD '{password}' '{invalid_literal}'"
                )
            observed = error.value.sqlstate if extended else error.value.pgcode
            assert observed == "0A000"
            assert password not in str(error.value)
            assert invalid_literal not in str(error.value)
            cursor.execute("SELECT 7")
            assert cursor.fetchone() == (7,)
    finally:
        connection.rollback()
        connection.close()
    logs = logging_pgcat.logs()
    expected_event = "Parsing prepared statement" if extended else "Parsing simple query"
    assert expected_event in logs
    assert "SQL parsing failed" in logs
    assert password not in logs
    assert invalid_literal not in logs


def test_registered_runtime_rejects_legacy_plugins_without_pool_parser():
    config = CONFIG.replace(f"port = {PORT}", "port = 0") + """
[pools.registration.plugins.table_access]
enabled = true
tables = ["blocked"]
"""
    process = utils.PgcatProcess(config, 0, binary=BINARY)
    result = subprocess.run(
        [BINARY, process.config_file.name],
        env=dict(os.environ, LOG_LEVEL="trace", NO_COLOR="true"),
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 78
    assert "plugins are only valid when query_parser_enabled is true" in (
        result.stdout + result.stderr
    )


def test_bind_response_and_cached_plan(pgcat):
    with psycopg.connect(pgcat.dsn("registration"), autocommit=True) as connection:
        with connection.cursor() as cursor:
            for value in (1, 2, 3):
                cursor.execute(
                    "SELECT %s::int AS registration_bind, 99 AS registration_hidden",
                    (value,),
                    prepare=True,
                )
                assert cursor.fetchone() == (42,)
                assert len(cursor.description) == 1


def test_simple_batch_is_not_silently_truncated(pgcat):
    connection = psycopg2.connect(pgcat.dsn("registration"))
    try:
        connection.autocommit = True
        with connection.cursor() as cursor:
            with pytest.raises(psycopg2.Error) as error:
                cursor.execute("SELECT 7001; SELECT 7003")
            assert error.value.pgcode == "42501"
            cursor.execute("SELECT 7")
            assert cursor.fetchone() == (7,)
    finally:
        connection.close()


def test_simple_query_response_hook(pgcat):
    connection = psycopg2.connect(pgcat.dsn("registration"))
    try:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute("SELECT 11 AS registration_result, 99 AS registration_hidden")
            assert cursor.fetchone() == (11,)
            assert len(cursor.description) == 1
    finally:
        connection.close()


def test_pipelined_registered_plans_remain_separate(pgcat):
    with psycopg.connect(pgcat.dsn("registration"), autocommit=True) as connection:
        with connection.pipeline():
            first = connection.execute("SELECT 7001")
            second = connection.execute(
                "SELECT %s::int AS registration_bind, 99 AS registration_hidden", (1,)
            )
            third = connection.execute("SELECT 13")
        assert first.fetchone() == (7002,)
        assert second.fetchone() == (42,)
        assert third.fetchone() == (13,)


def test_flush_client_and_pool_reuse(pgcat):
    async def check():
        for value in (8, 9):
            connection = await asyncpg.connect(pgcat.dsn("registration"))
            try:
                row = await connection.fetchrow(
                    "SELECT $1::int AS registration_bind, 99 AS registration_hidden", value
                )
                assert tuple(row) == (42,)
                with pytest.raises(asyncpg.PostgresError) as error:
                    await connection.fetchval("SELECT 7003")
                assert error.value.sqlstate == "42501"
                assert await connection.fetchval("SELECT 17") == 17
            finally:
                await connection.close()
    asyncio.run(check())


def test_pipeline_denial_skips_later_work_and_recovers(pgcat):
    with psycopg.connect(pgcat.dsn("registration"), autocommit=True) as connection:
        connection.execute("CREATE TEMP TABLE registration_pipeline (value int)")
        with pytest.raises(psycopg.Error) as error:
            with connection.pipeline():
                connection.execute("SELECT 7001")
                connection.execute("SELECT 7003")
                connection.execute("INSERT INTO registration_pipeline VALUES (1)")
        assert error.value.sqlstate == "42501"
        assert connection.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
        assert connection.execute("SELECT count(*) FROM registration_pipeline").fetchone() == (0,)
        assert connection.execute("SELECT 7001").fetchone() == (7002,)


def test_denial_aborts_explicit_transaction(pgcat):
    with psycopg.connect(pgcat.dsn("registration")) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            with pytest.raises(psycopg.Error) as error:
                cursor.execute("SELECT 7003")
            assert error.value.sqlstate == "42501"
            with pytest.raises(psycopg.Error) as aborted:
                cursor.execute("SELECT 1")
            assert aborted.value.sqlstate == "25P02"
            connection.rollback()
            cursor.execute("SELECT 1")
            assert cursor.fetchone() == (1,)


POLICY_PORT = 6449
POLICY_CONFIG = CONFIG.replace(f"port = {PORT}", f"port = {POLICY_PORT}").replace(
    "query_parser_enabled = false", "query_parser_enabled = true"
) + """
[pools.registration.plugins.table_access]
enabled = true
tables = ["registration_policy_blocked"]
"""
ALLOWED = "SELECT value FROM registration_policy_allowed"
BLOCKED = "SELECT value FROM registration_policy_blocked"
ALLOWED_ROWS = [("allowed-row",)]
TABLE_DENIED = 'permission for table "registration_policy_blocked" denied'
REWRITE_DENIED = "permission denied for rewritten statement"
SYNC = utils.frontend(b"S", b"")


@pytest.fixture(scope="module")
def policy_pgcat():
    # Created directly on the server: table_access refuses the blocked table.
    connection = psycopg2.connect("postgres://sharding_user:sharding_user@127.0.0.1:5432/shard0")
    try:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute(
                """
                DROP TABLE IF EXISTS registration_policy_allowed, registration_policy_blocked;
                CREATE TABLE registration_policy_allowed (value text);
                CREATE TABLE registration_policy_blocked (value text);
                INSERT INTO registration_policy_allowed VALUES ('allowed-row');
                INSERT INTO registration_policy_blocked VALUES ('blocked-row');
                """
            )
    finally:
        connection.close()
    # The debug log names the parse path, so tests can check the protocol used.
    process = utils.PgcatProcess(POLICY_CONFIG, POLICY_PORT, log_level="debug", binary=BINARY)
    try:
        process.start()
        yield process
    finally:
        process.stop()


def parse_events_since(pgcat, offset):
    with open(pgcat.log_file.name) as log:
        log.seek(offset)
        logs = log.read()
    return logs.count("Parsing simple query"), logs.count("Parsing prepared statement")


def parse_frame(statement, sql):
    return utils.frontend(b"P", statement + sql.encode() + b"\0" + struct.pack("!h", 0))


def execute_frames(statement):
    """Bind `statement` to the unnamed portal, then describe and run it."""
    return (
        utils.frontend(b"B", b"\0" + statement + struct.pack("!hhh", 0, 0, 0))
        + utils.frontend(b"D", b"P\0")
        + utils.frontend(b"E", b"\0" + struct.pack("!i", 0))
    )


def until_ready(message):
    frames = []
    while True:
        frames.append(message())
        if frames[-1][0] == b"Z":
            return frames


def data_rows(frames):
    rows = []
    for code, body in frames:
        if code != b"D":
            continue
        (count,), offset, row = struct.unpack("!h", body[:2]), 2, []
        for _ in range(count):
            (length,) = struct.unpack("!i", body[offset : offset + 4])
            row.append(body[offset + 4 : offset + 4 + length].decode())
            offset += 4 + length
        rows.append(tuple(row))
    return rows


def error_fields(body):
    return {field[:1]: field[1:].decode() for field in body.split(b"\0") if field}


@pytest.mark.parametrize("protocol", ["simple", "unnamed", "named"])
def test_table_access_checks_original_and_rewritten_sql(policy_pgcat, protocol):
    # Raw frames fix the protocol: a Query, or Parse/Bind/Describe/Execute/Sync
    # on the unnamed or a named statement, with the default statement cache.
    sock, message = utils.raw_connection(POLICY_PORT, "registration")
    names = iter(range(100))

    def send(frames, parses):
        offset = os.path.getsize(policy_pgcat.log_file.name)
        sock.sendall(frames)
        response = until_ready(message)
        # pgcat's log shows which path parsed it, independently of the frames sent.
        assert parse_events_since(policy_pgcat, offset) == parses
        assert b"blocked-row" not in b"".join(body for _, body in response)
        return response

    def run(sql):
        if protocol == "simple":
            return send(utils.frontend(b"Q", sql.encode() + b"\0"), (1, 0)), None
        name = f"registration_policy_{next(names)}" if protocol == "named" else ""
        statement = name.encode() + b"\0"
        return send(parse_frame(statement, sql) + execute_frames(statement) + SYNC, (0, 1)), name

    def allowed(sql):
        response, name = run(sql)
        codes = [b"T", b"D", b"C", b"Z"] if protocol == "simple" else [b"1", b"2", b"T", b"D", b"C", b"Z"]
        assert [code for code, _ in response] == codes
        assert data_rows(response) == ALLOWED_ROWS
        assert response[-1] == (b"Z", b"I")
        return name

    def refused(sql, error):
        response, name = run(sql)
        assert [code for code, _ in response] == [b"E", b"Z"]
        fields = error_fields(response[0][1])
        assert (fields[b"S"], fields[b"C"], fields[b"M"]) == ("ERROR", "42501", error)
        assert response[-1] == (b"Z", b"I")
        # The same connection keeps working after the refusal.
        allowed(ALLOWED)
        return name

    try:
        allowed_statement = allowed(ALLOWED)
        refused(BLOCKED, TABLE_DENIED)
        # The rewrite reaches the blocked table: the final SQL is checked, and the
        # error doesn't name the table, which the client didn't write.
        refused_statement = refused(f"{ALLOWED} WHERE value = 'to-blocked'", REWRITE_DENIED)
        # The rewrite leaves the blocked table: the original SQL is still checked.
        refused(f"{BLOCKED} WHERE value = 'to-allowed'", TABLE_DENIED)
        # An allowed rewrite runs; the original filter matches no row.
        allowed(f"{ALLOWED} WHERE value = 'to-allowed'")

        if protocol == "named":
            # The allowed statement runs again from the cache, without a Parse.
            response = send(execute_frames(allowed_statement.encode() + b"\0") + SYNC, (0, 0))
            assert [code for code, _ in response] == [b"2", b"T", b"D", b"C", b"Z"]
            assert data_rows(response) == ALLOWED_ROWS
            # The refused rewrite was never prepared. pgcat closes the connection
            # on an unknown statement, so this check comes last.
            response = send(execute_frames(refused_statement.encode() + b"\0") + SYNC, (0, 0))
            assert b"D" not in [code for code, _ in response]
            assert "does not exist" in error_fields(response[0][1])[b"M"]
    finally:
        sock.close()


def test_rewrite_refusal_rejects_its_pipelined_batch(policy_pgcat):
    sock, message = utils.raw_connection(POLICY_PORT, "registration")
    try:
        batch = [ALLOWED, f"{ALLOWED} WHERE value = 'to-blocked'", ALLOWED]
        sock.sendall(b"".join(parse_frame(b"\0", sql) + execute_frames(b"\0") for sql in batch) + SYNC)
        response = until_ready(message)
        assert [code for code, _ in response] == [b"E", b"Z"]
        fields = error_fields(response[0][1])
        assert (fields[b"C"], fields[b"M"]) == ("42501", REWRITE_DENIED)
        assert response[-1] == (b"Z", b"I")

        sock.sendall(parse_frame(b"\0", ALLOWED) + execute_frames(b"\0") + SYNC)
        assert data_rows(until_ready(message)) == ALLOWED_ROWS
    finally:
        sock.close()


def test_rewrite_refusal_with_flush_client(policy_pgcat):
    async def check():
        offset = os.path.getsize(policy_pgcat.log_file.name)
        connection = await asyncpg.connect(policy_pgcat.dsn("registration"))
        try:
            for _ in range(2):
                with pytest.raises(asyncpg.PostgresError) as error:
                    await connection.fetch(f"{ALLOWED} WHERE value = 'to-blocked'")
                assert (error.value.sqlstate, error.value.message) == ("42501", REWRITE_DENIED)
                rows = await connection.fetch(f"{ALLOWED} WHERE value = 'to-allowed'")
                assert [tuple(row) for row in rows] == ALLOWED_ROWS
        finally:
            await connection.close()
        # asyncpg sent only Parse messages (with Flush), never a simple Query.
        simple, prepared = parse_events_since(policy_pgcat, offset)
        assert simple == 0 and prepared >= 2

    asyncio.run(check())
