"""
End-to-end tests for the plugin hooks, using the test-only `testing` plugin.

The plugin is only compiled with `cargo build --features test-plugins`, so these
tests are skipped unless PGCAT_TEST_PLUGINS=1.
"""

import asyncio
import os
import struct
import time

import asyncpg
import psycopg
import psycopg2
import pytest

import utils

pytestmark = pytest.mark.skipif(
    os.environ.get("PGCAT_TEST_PLUGINS") != "1",
    reason="needs pgcat built with --features test-plugins and PGCAT_TEST_PLUGINS=1",
)

PORT = 6440
DB = "plugin_db"

CONFIG = f"""
[general]
host = "127.0.0.1"
port = {PORT}
admin_username = "admin_user"
admin_password = "admin_pass"

[pools.{DB}]
pool_mode = "transaction"
prepared_statements_cache_size = 100
query_parser_enabled = true
plugins_require_parse = true
plugin_hook_timeout = 300

[pools.{DB}.plugins.testing]
enabled = true
rewrites = {{ "select 1" = "SELECT 2" }}
delays = {{ "select 'slow'" = 2000, "select 'slow' where $1" = 2000 }}
drop_last_column = "plugin_hooks_rows"

[pools.{DB}.plugins.testing.copy_param]
table = "plugin_hooks_test"
from = "a"
to = "b"

[pools.{DB}.plugins.table_access]
enabled = true
tables = ["plugin_forbidden"]

[pools.{DB}.plugins.intercept]
enabled = true

[pools.{DB}.plugins.intercept.queries.0]
query = "select 'intercept me'"
schema = [["answer", "text"]]
result = [["from pgcat"]]

[pools.{DB}.users.0]
username = "sharding_user"
password = "sharding_user"
pool_size = 1
statement_timeout = 0

[pools.{DB}.shards.0]
servers = [["127.0.0.1", 5432, "primary"]]
database = "shard0"
"""


def superuser():
    conn = psycopg2.connect("postgres://postgres:postgres@127.0.0.1:5432/shard0")
    conn.autocommit = True
    return conn


@pytest.fixture(scope="module")
def pgcat():
    conn = superuser()
    conn.cursor().execute("""
        DROP TABLE IF EXISTS plugin_rows;
        CREATE TABLE plugin_rows (id int);
        GRANT ALL ON plugin_rows TO sharding_user;
    """)
    conn.close()

    # Created directly on the server, so the plugin doesn't see it.
    conn = psycopg2.connect("postgres://sharding_user:sharding_user@127.0.0.1:5432/shard0")
    conn.autocommit = True
    conn.cursor().execute(
        """
        DROP TABLE IF EXISTS plugin_hooks_test;
        CREATE TABLE plugin_hooks_test (id serial PRIMARY KEY, a text, b text);

        DROP TABLE IF EXISTS plugin_hooks_rows;
        CREATE TABLE plugin_hooks_rows (n int, a text, b text);
        -- About 100 KB of rows, so results span many server chunks.
        INSERT INTO plugin_hooks_rows
            SELECT n, 'a' || n, repeat('b', 40) FROM generate_series(1, 2000) n;
        INSERT INTO plugin_hooks_rows VALUES (0, 'fail', 'b');
        """
    )
    conn.close()

    process = utils.PgcatProcess(CONFIG, PORT).start()
    yield process
    process.stop()


def committed_rows():
    conn = superuser()
    cur = conn.cursor()
    cur.execute("SELECT id FROM plugin_rows ORDER BY id")
    rows = [row[0] for row in cur.fetchall()]
    conn.close()
    return rows


def count_rows(pgcat, a):
    conn = psycopg2.connect(pgcat.dsn(DB))
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM plugin_hooks_test WHERE a = %s", (a,))
    count = cur.fetchone()[0]
    conn.close()
    return count


def test_rewrite_simple_protocol(pgcat):
    conn = psycopg2.connect(pgcat.dsn(DB))
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("SELECT 1")
    assert cur.fetchone() == (2,)

    # Statements the plugin doesn't rewrite are untouched.
    cur.execute("SELECT 3")
    assert cur.fetchone() == (3,)

    # Inside a transaction too.
    cur.execute("BEGIN")
    cur.execute("SELECT 1")
    assert cur.fetchone() == (2,)
    cur.execute("COMMIT")

    conn.close()


def test_rewrite_extended_protocol(pgcat):
    with psycopg.connect(pgcat.dsn(DB), autocommit=True) as conn:
        # Unnamed statement: Parse, Bind, Describe, Execute, Sync.
        assert conn.execute("SELECT 1").fetchone() == (2,)

        # Named prepared statement, reused across executions.
        for _ in range(3):
            assert conn.execute("SELECT 1", prepare=True).fetchone() == (2,)


def test_rewrite_asyncpg(pgcat):
    async def run():
        conn = await asyncpg.connect(pgcat.dsn(DB))
        try:
            # Parse, Describe, Flush, then Bind, Execute, Sync.
            assert await conn.fetchval("SELECT 1") == 2
            stmt = await conn.prepare("SELECT 1")
            for _ in range(3):
                assert await stmt.fetchval() == 2
        finally:
            await conn.close()

    asyncio.run(run())


def test_rewritten_statement_shares_cache(pgcat):
    # pool_size is 1, so every client uses the same server connection.
    clients = [psycopg.connect(pgcat.dsn(DB), autocommit=True) for _ in range(2)]
    try:
        for conn in clients:
            assert conn.execute("SELECT 1", prepare=True).fetchone() == (2,)
        # Sending the rewritten SQL directly hits the same cache entry.
        assert clients[0].execute("SELECT 2", prepare=True).fetchone() == (2,)
    finally:
        for conn in clients:
            conn.close()

    # Simple protocol, so this query doesn't add a prepared statement itself.
    conn = psycopg2.connect(pgcat.dsn(DB))
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SELECT statement FROM pg_prepared_statements ORDER BY statement")
    statements = [row[0] for row in cur.fetchall()]
    conn.close()

    assert statements.count("SELECT 2") == 1
    assert "SELECT 1" not in statements


def test_require_parse_rejects_unparseable_sql(pgcat):
    conn = psycopg2.connect(pgcat.dsn(DB))
    conn.autocommit = True
    cur = conn.cursor()

    # Statements the parser can't read that name a table a plugin checks, or
    # could spell one with Unicode escapes.
    for query in [
        "SELEKT * FROM plugin_forbidden",
        'SELEKT * FROM U&"plugin\\005fforbidden"',
        # The parser reads `ONLY t` as a table named ONLY: table_access
        # wouldn't have seen plugin_forbidden.
        "SELECT * FROM ONLY plugin_forbidden",
    ]:
        with pytest.raises(psycopg2.Error, match="could not parse") as error:
            cur.execute(query)
        assert error.value.pgcode == "0A000"

    # The connection is still usable.
    cur.execute("SELECT 1")
    assert cur.fetchone() == (2,)

    # Others go to the server, which answers them: nothing to check in them.
    with pytest.raises(psycopg2.errors.SyntaxError):
        cur.execute("SELEKT 1")
    cur.execute("SELECT count(*) FROM ONLY plugin_rows")

    # The parser doesn't know RESET, but it can't touch a table.
    cur.execute("RESET ALL")
    conn.close()

    with psycopg.connect(pgcat.dsn(DB), autocommit=True) as conn:
        with pytest.raises(psycopg.Error, match="could not parse"):
            conn.execute("SELEKT * FROM plugin_forbidden WHERE a = %s", [1])
        with pytest.raises(psycopg.errors.SyntaxError):
            conn.execute("SELEKT %s", [1])
        assert conn.execute("SELECT 1").fetchone() == (2,)

    async def run():
        conn = await asyncpg.connect(pgcat.dsn(DB))
        try:
            with pytest.raises(asyncpg.PostgresError, match="could not parse"):
                await conn.fetchval("SELEKT * FROM plugin_forbidden")
            assert await conn.fetchval("SELECT 1") == 2
        finally:
            await conn.close()

    asyncio.run(run())


def test_intercept_extended_protocol(pgcat):
    conn = psycopg2.connect(pgcat.dsn(DB))
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SELECT 'intercept me'")
    assert cur.fetchall() == [("from pgcat",)]
    conn.close()

    with psycopg.connect(pgcat.dsn(DB), autocommit=True) as conn:
        cur = conn.execute("SELECT 'intercept me'")
        assert cur.description[0].name == "answer"
        assert cur.fetchall() == [("from pgcat",)]
        # Named statement reused across batches.
        for _ in range(3):
            assert conn.execute("SELECT 'intercept me'", prepare=True).fetchall() == [
                ("from pgcat",)
            ]
        # Regular statements still go to the server afterwards.
        assert conn.execute("SELECT 3").fetchone() == (3,)

    async def run():
        conn = await asyncpg.connect(pgcat.dsn(DB))
        try:
            stmt = await conn.prepare("SELECT 'intercept me'")
            assert [a.name for a in stmt.get_attributes()] == ["answer"]
            for _ in range(3):
                assert await stmt.fetchval() == "from pgcat"
            assert await conn.fetchval("SELECT 3") == 3
        finally:
            await conn.close()

    asyncio.run(run())


def test_refused_statements_are_errors(pgcat):
    # ERROR with a SQLSTATE, not FATAL: drivers keep the connection.
    conn = psycopg2.connect(pgcat.dsn(DB))
    conn.autocommit = True
    cur = conn.cursor()
    with pytest.raises(psycopg2.Error) as error:
        cur.execute("SELECT * FROM plugin_forbidden")
    assert error.value.pgcode == "42501"
    assert error.value.diag.severity == "ERROR"
    with pytest.raises(psycopg2.Error) as error:
        cur.execute("SELEKT * FROM plugin_forbidden")
    assert error.value.pgcode == "0A000"
    cur.execute("SELECT 3")
    assert cur.fetchone() == (3,)
    conn.close()

    with psycopg.connect(pgcat.dsn(DB), autocommit=True) as conn:
        with pytest.raises(psycopg.Error) as error:
            conn.execute("SELECT * FROM plugin_forbidden")
        assert error.value.sqlstate == "42501"
        assert error.value.diag.severity == "ERROR"
        assert conn.execute("SELECT 3").fetchone() == (3,)

    async def run():
        conn = await asyncpg.connect(pgcat.dsn(DB))
        try:
            with pytest.raises(asyncpg.PostgresError) as error:
                await conn.fetch("SELECT * FROM plugin_forbidden")
            assert error.value.sqlstate == "42501"
            assert await conn.fetchval("SELECT 3") == 3
        finally:
            await conn.close()

    asyncio.run(run())


def test_refusal_fails_the_transaction(pgcat):
    # Simple protocol: the rest of the transaction can't commit.
    conn = psycopg2.connect(pgcat.dsn(DB))
    cur = conn.cursor()
    cur.execute("INSERT INTO plugin_rows VALUES (1)")
    with pytest.raises(psycopg2.Error) as error:
        cur.execute("SELECT * FROM plugin_forbidden")
    assert error.value.pgcode == "42501"
    assert conn.info.transaction_status == psycopg2.extensions.TRANSACTION_STATUS_INERROR
    with pytest.raises(psycopg2.errors.InFailedSqlTransaction):
        cur.execute("INSERT INTO plugin_rows VALUES (2)")
    conn.commit()
    assert conn.info.transaction_status == psycopg2.extensions.TRANSACTION_STATUS_IDLE
    conn.close()
    assert committed_rows() == []

    # Extended protocol, a refused Parse.
    with psycopg.connect(pgcat.dsn(DB)) as conn:
        conn.execute("INSERT INTO plugin_rows VALUES (%s)", [3])
        with pytest.raises(psycopg.Error) as error:
            conn.execute("SELECT * FROM plugin_forbidden WHERE id = %s", [1])
        assert error.value.sqlstate == "42501"
        assert conn.info.transaction_status == psycopg.pq.TransactionStatus.INERROR
        with pytest.raises(psycopg.errors.InFailedSqlTransaction):
            conn.execute("INSERT INTO plugin_rows VALUES (%s)", [4])
        conn.rollback()
        assert conn.execute("SELECT 3").fetchone() == (3,)
        conn.commit()
    assert committed_rows() == []

    async def run():
        conn = await asyncpg.connect(pgcat.dsn(DB))
        try:
            with pytest.raises(asyncpg.PostgresError):
                async with conn.transaction():
                    await conn.execute("INSERT INTO plugin_rows VALUES (5)")
                    await conn.fetch("SELECT * FROM plugin_forbidden")
            assert await conn.fetchval("SELECT 3") == 3
        finally:
            await conn.close()

    asyncio.run(run())
    assert committed_rows() == []

    # Outside a transaction a refusal leaves nothing behind either.
    conn = psycopg2.connect(pgcat.dsn(DB))
    conn.autocommit = True
    cur = conn.cursor()
    with pytest.raises(psycopg2.Error):
        cur.execute("SELECT * FROM plugin_forbidden")
    assert conn.info.transaction_status == psycopg2.extensions.TRANSACTION_STATUS_IDLE
    cur.execute("INSERT INTO plugin_rows VALUES (6)")
    conn.close()
    assert committed_rows() == [6]


def test_slow_plugins_fail_their_statement(pgcat):
    # A hook that takes longer than plugin_hook_timeout fails the statement with
    # its own SQLSTATE, and the connection goes on.
    conn = psycopg2.connect(pgcat.dsn(DB))
    conn.autocommit = True
    cur = conn.cursor()
    with pytest.raises(psycopg2.Error) as error:
        cur.execute("SELECT 'slow'")
    assert error.value.pgcode == "PH001"
    assert error.value.diag.severity == "ERROR"
    cur.execute("SELECT 3")
    assert cur.fetchone() == (3,)
    conn.close()

    with psycopg.connect(pgcat.dsn(DB), autocommit=True) as conn:
        with pytest.raises(psycopg.Error) as error:
            conn.execute("SELECT 'slow' WHERE %s", [True])
        assert error.value.sqlstate == "PH001"
        assert conn.execute("SELECT 3").fetchone() == (3,)


def test_bind_hook_appends_param_psycopg(pgcat):
    with psycopg.connect(pgcat.dsn(DB), autocommit=True) as conn:
        # Text format parameters.
        row = conn.execute(
            "INSERT INTO plugin_hooks_test (a) VALUES (%s) RETURNING a, b", ["text"]
        ).fetchone()
        assert row == ("text", "text")

        # Binary format parameters.
        row = conn.execute(
            "INSERT INTO plugin_hooks_test (a) VALUES (%b) RETURNING a, b", ["binary"]
        ).fetchone()
        assert row == ("binary", "binary")

        # Several rows, each gets its own appended parameter.
        rows = conn.execute(
            "INSERT INTO plugin_hooks_test (a) VALUES (%s), (%s) RETURNING a, b",
            ["one", "two"],
        ).fetchall()
        assert rows == [("one", "one"), ("two", "two")]

        # A named statement reused across binds.
        for i in range(5):
            row = conn.execute(
                "INSERT INTO plugin_hooks_test (a) VALUES (%s) RETURNING a, b",
                [f"prepared-{i}"],
                prepare=True,
            ).fetchone()
            assert row == (f"prepared-{i}", f"prepared-{i}")

    # The simple protocol has no Bind, the insert is left alone.
    conn = psycopg2.connect(pgcat.dsn(DB))
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("INSERT INTO plugin_hooks_test (a) VALUES ('simple') RETURNING a, b")
    assert cur.fetchone() == ("simple", None)
    conn.close()


def test_bind_hook_appends_param_asyncpg(pgcat):
    async def run():
        conn = await asyncpg.connect(pgcat.dsn(DB))
        try:
            # asyncpg describes the statement first: it must see one parameter.
            stmt = await conn.prepare(
                "INSERT INTO plugin_hooks_test (a) VALUES ($1) RETURNING a, b"
            )
            assert len(stmt.get_parameters()) == 1

            for i in range(5):
                row = await stmt.fetchrow(f"asyncpg-{i}")
                assert tuple(row) == (f"asyncpg-{i}", f"asyncpg-{i}")

            rows = await conn.fetch(
                "INSERT INTO plugin_hooks_test (a) VALUES ($1), ($2) RETURNING a, b",
                "three",
                "four",
            )
            assert [tuple(row) for row in rows] == [("three", "three"), ("four", "four")]
        finally:
            await conn.close()

    asyncio.run(run())


def test_bind_hook_error(pgcat):
    with psycopg.connect(pgcat.dsn(DB), autocommit=True) as conn:
        with pytest.raises(psycopg.Error, match="testing plugin refused the value"):
            conn.execute("INSERT INTO plugin_hooks_test (a) VALUES (%s)", ["fail"])
        # The connection is still usable, in and out of transactions.
        assert conn.execute("SELECT 3").fetchone() == (3,)

        with pytest.raises(psycopg.Error, match="refused"):
            with conn.transaction():
                conn.execute("INSERT INTO plugin_hooks_test (a) VALUES (%s)", ["fail"])
        assert conn.execute("SELECT 3").fetchone() == (3,)

    async def run():
        conn = await asyncpg.connect(pgcat.dsn(DB))
        try:
            with pytest.raises(asyncpg.PostgresError, match="refused"):
                await conn.execute("INSERT INTO plugin_hooks_test (a) VALUES ($1)", "fail")
            assert await conn.fetchval("SELECT 3") == 3
        finally:
            await conn.close()

    asyncio.run(run())

    assert count_rows(pgcat, "fail") == 0


def test_bind_hook_refusal_fails_the_transaction(pgcat):
    with psycopg.connect(pgcat.dsn(DB)) as conn:
        conn.execute("INSERT INTO plugin_hooks_test (a) VALUES (%s)", ["before-refusal"])
        with pytest.raises(psycopg.Error) as error:
            conn.execute("INSERT INTO plugin_hooks_test (a) VALUES (%s)", ["fail"])
        assert error.value.sqlstate == "XX000"
        assert error.value.diag.severity == "ERROR"
        assert conn.info.transaction_status == psycopg.pq.TransactionStatus.INERROR
        with pytest.raises(psycopg.errors.InFailedSqlTransaction):
            conn.execute("INSERT INTO plugin_hooks_test (a) VALUES (%s)", ["after-refusal"])
        conn.commit()
    assert count_rows(pgcat, "before-refusal") == 0

    # A refused Bind rejects its whole batch: nothing in it is written.
    with psycopg.connect(pgcat.dsn(DB), autocommit=True) as conn:
        with pytest.raises(psycopg.Error):
            with conn.pipeline():
                conn.execute("INSERT INTO plugin_hooks_test (a) VALUES (%s)", ["same-batch"])
                conn.execute("INSERT INTO plugin_hooks_test (a) VALUES (%s)", ["fail"])
        assert conn.execute("SELECT 3").fetchone() == (3,)
    assert count_rows(pgcat, "same-batch") == 0


def test_too_many_parameters_are_refused(pgcat):
    # A planned statement naming $4000000000 is refused, not allocated for.
    for placeholder in ["$4000000000", "$65536"]:
        sock, message = raw_connection()
        query = f"INSERT INTO plugin_hooks_test (a) VALUES ({placeholder})\0".encode()
        sock.sendall(frontend(b"P", b"\0" + query + struct.pack("!h", 0)) + frontend(b"S", b""))
        code, body = message()
        assert code == b"E", (code, body)
        assert b"C54000\0" in body
        assert message()[0] == b"Z"
        sock.close()

    async def run():
        conn = await asyncpg.connect(pgcat.dsn(DB))
        try:
            with pytest.raises(asyncpg.PostgresError) as error:
                await conn.prepare("INSERT INTO plugin_hooks_test (a) VALUES ($4000000000)")
            assert error.value.sqlstate == "54000"
            assert await conn.fetchval("SELECT 3") == 3
        finally:
            await conn.close()

    asyncio.run(run())


def test_response_hook_drops_column_simple_protocol(pgcat):
    conn = psycopg2.connect(pgcat.dsn(DB))
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("SELECT n, a, b FROM plugin_hooks_rows WHERE n BETWEEN 1 AND 3 ORDER BY n")
    assert [column.name for column in cur.description] == ["n", "a"]
    assert cur.fetchall() == [(1, "a1"), (2, "a2"), (3, "a3")]

    # A result much larger than a server chunk.
    cur.execute("SELECT a, b FROM plugin_hooks_rows WHERE n > 0 ORDER BY n")
    rows = cur.fetchall()
    assert len(rows) == 2000
    assert rows[-1] == ("a2000",)
    conn.close()


def test_response_hook_drops_column_extended_protocol(pgcat):
    with psycopg.connect(pgcat.dsn(DB), autocommit=True) as conn:
        for binary in (False, True):
            cur = conn.cursor(binary=binary)
            cur.execute(
                "SELECT n, a, b FROM plugin_hooks_rows WHERE n BETWEEN %s AND %s ORDER BY n",
                [1, 3],
            )
            assert [column.name for column in cur.description] == ["n", "a"]
            assert cur.fetchall() == [(1, "a1"), (2, "a2"), (3, "a3")]

            cur.execute("SELECT a, b FROM plugin_hooks_rows WHERE n > 0 ORDER BY n")
            rows = cur.fetchall()
            assert len(rows) == 2000 and rows[-1] == ("a2000",)

        # A named statement reused across executions.
        for i in range(1, 4):
            row = conn.execute(
                "SELECT n, a, b FROM plugin_hooks_rows WHERE n = %s", [i], prepare=True
            ).fetchone()
            assert row == (i, f"a{i}")

        # Other tables are left alone.
        assert conn.execute("SELECT 3, 4").fetchone() == (3, 4)


def test_response_hook_asyncpg(pgcat):
    async def run():
        conn = await asyncpg.connect(pgcat.dsn(DB))
        try:
            stmt = await conn.prepare("SELECT n, a, b FROM plugin_hooks_rows WHERE n = $1")
            assert [a.name for a in stmt.get_attributes()] == ["n", "a"]
            for i in range(1, 4):
                assert tuple(await stmt.fetchrow(i)) == (i, f"a{i}")

            rows = await conn.fetch("SELECT a, b FROM plugin_hooks_rows WHERE n > 0 ORDER BY n")
            assert len(rows) == 2000 and tuple(rows[-1]) == ("a2000",)

            # A cursor executes the portal a few rows at a time (PortalSuspended).
            async with conn.transaction():
                seen = []
                async for record in conn.cursor(
                    "SELECT n, a, b FROM plugin_hooks_rows WHERE n > 0 ORDER BY n", prefetch=7
                ):
                    seen.append(tuple(record))
            assert len(seen) == 2000
            assert seen[0] == (1, "a1") and seen[-1] == (2000, "a2000")
        finally:
            await conn.close()

    asyncio.run(run())


def raw_connection():
    return utils.raw_connection(PORT, DB)


frontend = utils.frontend


def test_malformed_bind_doesnt_crash_pgcat(pgcat):
    sock, message = raw_connection()
    query = b"INSERT INTO plugin_hooks_test (a) VALUES ($1)\0"
    parse = frontend(b"P", b"\0" + query + struct.pack("!h", 0))
    # Claims one parameter format code and value, and carries neither.
    bind = frontend(b"B", b"\0\0" + struct.pack("!h", 1))
    sock.sendall(parse + bind + frontend(b"S", b""))

    # pgcat ends the connection (or answers with an error); either way it
    # doesn't panic, and serves the next client.
    try:
        while True:
            code, _ = message()
            if code in (b"E", b"Z"):
                break
    except ConnectionError:
        pass
    sock.close()

    with psycopg.connect(pgcat.dsn(DB), autocommit=True) as conn:
        assert conn.execute("SELECT 3").fetchone() == (3,)
    assert "panicked" not in pgcat.logs()


def test_rejected_batch_keeps_the_statements_it_closed(pgcat):
    sock, message = raw_connection()
    query = b"INSERT INTO plugin_hooks_test (a) VALUES ($1)\0"

    def bind(value):
        value = value.encode()
        body = b"\0s1\0" + struct.pack("!hhi", 0, 1, len(value)) + value + struct.pack("!h", 0)
        return frontend(b"B", body) + frontend(b"E", b"\0" + struct.pack("!i", 0))

    def until_ready():
        codes = []
        while True:
            code, _ = message()
            codes.append(code)
            if code == b"Z":
                return codes

    sock.sendall(frontend(b"P", b"s1\0" + query + struct.pack("!h", 0)) + frontend(b"S", b""))
    assert until_ready() == [b"1", b"Z"]

    # The hook refuses the Bind, so the batch, Close included, never happens.
    sock.sendall(bind("fail") + frontend(b"C", b"Ss1\0") + frontend(b"S", b""))
    assert until_ready() == [b"E", b"Z"]

    # s1 still exists, and is still planned: the hook copies a into b.
    sock.sendall(bind("journal") + frontend(b"S", b""))
    assert until_ready() == [b"2", b"C", b"Z"]
    sock.close()

    conn = psycopg2.connect(pgcat.dsn(DB))
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SELECT a, b FROM plugin_hooks_test WHERE a = 'journal'")
    assert cur.fetchall() == [("journal", "journal")]
    conn.close()


def test_rejected_batch_keeps_what_its_statements_do(pgcat):
    # A statement the rejected batch would have prepared again still ends a
    # transaction on the server, and pgcat still knows it does.
    sock, message = raw_connection()
    no_types = struct.pack("!h", 0)
    no_params = struct.pack("!hhh", 0, 0, 0)
    execute = frontend(b"E", b"\0" + struct.pack("!i", 0))

    def until_ready():
        codes = []
        while True:
            code, body = message()
            codes.append((code, body))
            if code == b"Z":
                return codes

    sock.sendall(frontend(b"P", b"s2\0COMMIT\0" + no_types) + frontend(b"S", b""))
    assert [code for code, _ in until_ready()] == [b"1", b"Z"]

    # Close s2 and prepare it again as a SELECT, in a batch the hook refuses.
    insert = b"INSERT INTO plugin_hooks_test (a) VALUES ($1)\0"
    value = b"fail"
    refused = frontend(b"B", b"\0\0" + struct.pack("!hhi", 0, 1, len(value)) + value + struct.pack("!h", 0))
    sock.sendall(
        frontend(b"C", b"Ss2\0")
        + frontend(b"P", b"s2\0SELECT 1\0" + no_types)
        + frontend(b"P", b"\0" + insert + no_types)
        + refused
        + execute
        + frontend(b"S", b"")
    )
    assert [code for code, _ in until_ready()] == [b"E", b"Z"]

    # s2 is still COMMIT: after a statement with row hooks, it's refused.
    select = b"SELECT a, b FROM plugin_hooks_rows WHERE n = 1\0"
    sock.sendall(
        frontend(b"P", b"\0" + select + no_types)
        + frontend(b"B", b"\0\0" + no_params)
        + execute
        + frontend(b"B", b"\0s2\0" + no_params)
        + execute
        + frontend(b"S", b"")
    )
    replies = until_ready()
    assert [code for code, _ in replies] == [b"E", b"Z"], replies
    assert b"C0A000\0" in replies[0][1]
    sock.close()


def test_hooks_cut_off_mid_query_let_their_server_go(pgcat):
    # A Bind hook that queries the server and outlives the hook timeout leaves
    # the connection mid-exchange: pgcat lets it go, and the client goes on.
    with psycopg.connect(pgcat.dsn(DB), autocommit=True) as conn:
        with pytest.raises(psycopg.Error) as error:
            conn.execute("INSERT INTO plugin_hooks_test (a) VALUES (%s)", ["slow-query"])
        assert error.value.sqlstate == "PH001"
        for n in range(3):
            assert conn.execute("SELECT %s::int", [n]).fetchone() == (n,)
    assert "a plugin hook was cut off while it queried the server" in pgcat.logs()
    assert count_rows(pgcat, "slow-query") == 0

    # The client's own transaction is lost with the connection: the client
    # can't go on as if it weren't, so its connection ends.
    conn = psycopg.connect(pgcat.dsn(DB))
    conn.execute("SELECT 3")
    with pytest.raises(psycopg.Error) as error:
        conn.execute("INSERT INTO plugin_hooks_test (a) VALUES (%s)", ["slow-query"])
    assert error.value.sqlstate == "PH001"
    with pytest.raises(psycopg.OperationalError):
        conn.execute("SELECT 3")
    conn.close()
    with psycopg.connect(pgcat.dsn(DB), autocommit=True) as conn:
        assert conn.execute("SELECT 3").fetchone() == (3,)
    assert count_rows(pgcat, "slow-query") == 0


def test_bind_refused_before_checkout(pgcat):
    # The pool's one server connection is held by another client's transaction.
    # A Bind a hook refuses before checkout fails at once, without waiting for it.
    holder = psycopg.connect(pgcat.dsn(DB))
    holder.execute("SELECT 1")
    try:
        with psycopg.connect(pgcat.dsn(DB), autocommit=True) as conn:
            start = time.time()
            with pytest.raises(psycopg.Error, match="before checkout") as error:
                conn.execute("INSERT INTO plugin_hooks_test (a) VALUES (%s)", ["no-server"])
            assert error.value.sqlstate == "XX000"
            assert time.time() - start < 0.5
    finally:
        holder.rollback()
        holder.close()
    assert count_rows(pgcat, "no-server") == 0


def test_response_hook_error(pgcat):
    conn = psycopg2.connect(pgcat.dsn(DB))
    conn.autocommit = True
    cur = conn.cursor()
    with pytest.raises(psycopg2.Error, match="testing plugin refused the row"):
        cur.execute("SELECT a, b FROM plugin_hooks_rows ORDER BY n")
    cur.execute("SELECT a, b FROM plugin_hooks_rows WHERE n = 1")
    assert cur.fetchall() == [("a1",)]
    conn.close()

    with psycopg.connect(pgcat.dsn(DB), autocommit=True) as conn:
        with pytest.raises(psycopg.Error, match="refused the row"):
            conn.execute("SELECT a, b FROM plugin_hooks_rows ORDER BY n").fetchall()
        assert conn.execute("SELECT a, b FROM plugin_hooks_rows WHERE n = 1").fetchall() == [
            ("a1",)
        ]

    async def run():
        conn = await asyncpg.connect(pgcat.dsn(DB))
        try:
            with pytest.raises(asyncpg.PostgresError, match="refused the row"):
                await conn.fetch("SELECT a, b FROM plugin_hooks_rows ORDER BY n")
            assert await conn.fetchval("SELECT a, b FROM plugin_hooks_rows WHERE n = 1") == "a1"
        finally:
            await conn.close()

    asyncio.run(run())


def test_response_hook_pipeline(pgcat):
    # One batch: a planned statement, then the unnamed statement replaced by an
    # unplanned one before Sync. The first result must still be rewritten.
    with psycopg.connect(pgcat.dsn(DB), autocommit=True) as conn:
        with conn.pipeline():
            first = conn.execute("SELECT a, b FROM plugin_hooks_rows WHERE n = %s", [1])
            second = conn.execute("SELECT %s::text, 'x'", ["y"])
        assert first.fetchall() == [("a1",)]
        assert second.fetchall() == [("y", "x")]


def test_response_hook_failure_fails_the_transaction(pgcat):
    with psycopg.connect(pgcat.dsn(DB)) as conn:
        conn.execute("INSERT INTO plugin_rows VALUES (10)")
        with pytest.raises(psycopg.Error) as error:
            conn.execute("SELECT a, b FROM plugin_hooks_rows ORDER BY n").fetchall()
        assert error.value.sqlstate == "XX000"
        assert error.value.diag.severity == "ERROR"
        assert conn.info.transaction_status == psycopg.pq.TransactionStatus.INERROR
        conn.commit()

    conn = psycopg2.connect(pgcat.dsn(DB))
    cur = conn.cursor()
    cur.execute("INSERT INTO plugin_rows VALUES (11)")
    with pytest.raises(psycopg2.Error):
        cur.execute("SELECT a, b FROM plugin_hooks_rows ORDER BY n")
    assert conn.info.transaction_status == psycopg2.extensions.TRANSACTION_STATUS_INERROR
    conn.commit()
    conn.close()

    rows = committed_rows()
    assert 10 not in rows and 11 not in rows


def test_response_hook_failure_rolls_back_its_batch(pgcat):
    with psycopg.connect(pgcat.dsn(DB), autocommit=True) as conn:
        # Outside a transaction, the batch runs in one pgcat opens and rolls back.
        with pytest.raises(psycopg.Error):
            with conn.pipeline():
                conn.execute("INSERT INTO plugin_rows VALUES (%s)", [12])
                conn.execute("SELECT a, b FROM plugin_hooks_rows ORDER BY n")
        assert conn.info.transaction_status == psycopg.pq.TransactionStatus.IDLE

        # A lone statement that only reads fails on its own.
        with pytest.raises(psycopg.Error):
            conn.execute("SELECT a, b FROM plugin_hooks_rows ORDER BY n").fetchall()
        assert conn.info.transaction_status == psycopg.pq.TransactionStatus.IDLE

        # A batch whose hooks don't fail commits.
        with conn.pipeline():
            conn.execute("INSERT INTO plugin_rows VALUES (%s)", [13])
            conn.execute("SELECT a, b FROM plugin_hooks_rows WHERE n = %s", [1])
        assert conn.info.transaction_status == psycopg.pq.TransactionStatus.IDLE

    rows = committed_rows()
    assert 12 not in rows and 13 in rows

    # A batch that begins a transaction itself keeps it, as the client asked.
    with psycopg.connect(pgcat.dsn(DB), autocommit=True) as conn:
        with conn.pipeline():
            conn.execute("BEGIN")
            conn.execute("INSERT INTO plugin_rows VALUES (%s)", [14])
            cur = conn.execute("SELECT a, b FROM plugin_hooks_rows WHERE n = %s", [1])
        assert cur.fetchall() == [("a1",)]
        assert conn.info.transaction_status == psycopg.pq.TransactionStatus.INTRANS
        assert 14 not in committed_rows()
        conn.execute("COMMIT")
    assert 14 in committed_rows()


def test_commit_after_a_hooked_statement_is_refused(pgcat):
    # A response hook failing on the first statement couldn't undo what the
    # COMMIT after it, in the same batch, committed: such batches are refused.
    with psycopg.connect(pgcat.dsn(DB), autocommit=True) as conn:
        conn.execute("BEGIN")
        conn.execute("INSERT INTO plugin_rows VALUES (%s)", [20])
        with pytest.raises(psycopg.Error, match="COMMIT or ROLLBACK") as error:
            with conn.pipeline():
                conn.execute("SELECT a, b FROM plugin_hooks_rows WHERE n = %s", [1])
                conn.execute("COMMIT")
                # Reusing the unnamed statement doesn't hide the COMMIT.
                conn.execute("SELECT %s::int", [1])
        assert error.value.sqlstate == "0A000"
        # Refused like any statement: the transaction can only be rolled back.
        assert conn.info.transaction_status == psycopg.pq.TransactionStatus.INERROR
        conn.execute("ROLLBACK")

        # A COMMIT before it, or in its own batch, is fine.
        conn.execute("BEGIN")
        with conn.pipeline():
            conn.execute("INSERT INTO plugin_rows VALUES (%s)", [21])
            conn.execute("COMMIT")
            cur = conn.execute("SELECT a, b FROM plugin_hooks_rows WHERE n = %s", [1])
        assert cur.fetchall() == [("a1",)]
    rows = committed_rows()
    assert 20 not in rows and 21 in rows


def test_writes_without_row_hooks_need_no_transaction(pgcat):
    # Plans that only append parameters have no hook that can fail on the
    # results: pgcat sends them as they are, without a transaction of its own.
    admin = superuser()
    cur = admin.cursor()
    cur.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
    cur.execute("SELECT pg_stat_statements_reset()")
    with psycopg.connect(pgcat.dsn(DB), autocommit=True) as conn:
        for _ in range(3):
            conn.execute("INSERT INTO plugin_hooks_test (a) VALUES (%s)", ["unwrapped"])
    cur.execute("SELECT coalesce(sum(calls), 0) FROM pg_stat_statements WHERE query ILIKE 'BEGIN%'")
    assert cur.fetchone()[0] == 0
    admin.close()
    assert count_rows(pgcat, "unwrapped") == 3


def test_slow_response_hooks_fail_their_statement(pgcat):
    admin = superuser()
    admin.cursor().execute("INSERT INTO plugin_hooks_rows VALUES (-5, 'slow-row', 'b')")
    try:
        with psycopg.connect(pgcat.dsn(DB), autocommit=True) as conn:
            with pytest.raises(psycopg.Error) as error:
                conn.execute("SELECT a, b FROM plugin_hooks_rows WHERE n = %s", [-5]).fetchall()
            assert error.value.sqlstate == "PH001"
            assert conn.execute("SELECT 3").fetchone() == (3,)
    finally:
        admin.cursor().execute("DELETE FROM plugin_hooks_rows WHERE n = -5")
        admin.close()
