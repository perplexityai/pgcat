"""
asyncpg integration tests for pgcat.

asyncpg uses the PostgreSQL extended query protocol differently from psycopg2:
it sends Parse → Describe → Flush (not Sync) during prepared statement
initialization. This exercises pgcat's Flush ('H') message handler.

Expects pgcat to already be running on port 6432 with the .circleci/pgcat.toml config.
"""

import asyncio

import asyncpg
import pytest

PGCAT_HOST = "127.0.0.1"
PGCAT_PORT = 6432
DATABASE = "sharded_db"
USER = "sharding_user"
PASSWORD = "sharding_user"


async def connect():
    return await asyncpg.connect(
        host=PGCAT_HOST,
        port=PGCAT_PORT,
        database=DATABASE,
        user=USER,
        password=PASSWORD,
    )


# ==================== Basic Connectivity ====================


class TestBasicConnectivity:
    def test_simple_query(self):
        async def run():
            conn = await connect()
            try:
                result = await conn.fetchval("SELECT 1")
                assert result == 1
            finally:
                await conn.close()

        asyncio.run(run())

    def test_multiple_simple_queries(self):
        async def run():
            conn = await connect()
            try:
                for i in range(20):
                    result = await conn.fetchval("SELECT $1::int", i)
                    assert result == i
            finally:
                await conn.close()

        asyncio.run(run())

    def test_multiple_connections(self):
        async def run():
            conns = []
            try:
                for _ in range(5):
                    conns.append(await connect())
                for i, conn in enumerate(conns):
                    result = await conn.fetchval("SELECT $1::int", i)
                    assert result == i
            finally:
                for conn in conns:
                    await conn.close()

        asyncio.run(run())


# ==================== Prepared Statements ====================


class TestPreparedStatements:
    def test_prepare_and_execute(self):
        """Basic prepared statement: Parse -> Describe -> Flush, then Bind -> Execute -> Sync."""
        async def run():
            conn = await connect()
            try:
                stmt = await conn.prepare("SELECT $1::int + $2::int")
                result = await stmt.fetchval(3, 4)
                assert result == 7
            finally:
                await conn.close()

        asyncio.run(run())

    def test_prepare_reuse(self):
        """Reuse a prepared statement many times -- tests the cache path."""
        async def run():
            conn = await connect()
            try:
                stmt = await conn.prepare("SELECT $1::int * 2")
                for i in range(100):
                    result = await stmt.fetchval(i)
                    assert result == i * 2
            finally:
                await conn.close()

        asyncio.run(run())

    def test_multiple_prepared_statements(self):
        """Multiple different prepared statements on the same connection."""
        async def run():
            conn = await connect()
            try:
                stmt_add = await conn.prepare("SELECT $1::int + $2::int")
                stmt_mul = await conn.prepare("SELECT $1::int * $2::int")
                stmt_text = await conn.prepare("SELECT $1::text || $2::text")

                assert await stmt_add.fetchval(10, 20) == 30
                assert await stmt_mul.fetchval(5, 6) == 30
                assert await stmt_text.fetchval("hello", "world") == "helloworld"

                for i in range(20):
                    assert await stmt_add.fetchval(i, 1) == i + 1
                    assert await stmt_mul.fetchval(i, 2) == i * 2
            finally:
                await conn.close()

        asyncio.run(run())

    def test_prepared_statement_with_no_params(self):
        async def run():
            conn = await connect()
            try:
                stmt = await conn.prepare("SELECT 42")
                result = await stmt.fetchval()
                assert result == 42
            finally:
                await conn.close()

        asyncio.run(run())

    def test_prepared_statement_fetch_row(self):
        async def run():
            conn = await connect()
            try:
                stmt = await conn.prepare("SELECT $1::int AS a, $2::text AS b")
                row = await stmt.fetchrow(1, "hello")
                assert row["a"] == 1
                assert row["b"] == "hello"
            finally:
                await conn.close()

        asyncio.run(run())

    def test_prepared_statement_fetch_multiple_rows(self):
        async def run():
            conn = await connect()
            try:
                stmt = await conn.prepare("SELECT generate_series(1, $1::int)")
                rows = await stmt.fetch(10)
                assert len(rows) == 10
                values = [row[0] for row in rows]
                assert values == list(range(1, 11))
            finally:
                await conn.close()

        asyncio.run(run())

    def test_prepared_statement_various_types(self):
        async def run():
            conn = await connect()
            try:
                assert await conn.fetchval("SELECT $1::bigint", 2**40) == 2**40
                assert await conn.fetchval("SELECT $1::smallint", 42) == 42

                result = await conn.fetchval("SELECT $1::float8", 3.14)
                assert abs(result - 3.14) < 0.001

                assert await conn.fetchval("SELECT $1::bool", True) is True
                assert await conn.fetchval("SELECT $1::bool", False) is False
                assert await conn.fetchval("SELECT $1::text", "test string") == "test string"

                result = await conn.fetchval("SELECT $1::text", None)
                assert result is None

                data = b"\x00\x01\x02\xff"
                result = await conn.fetchval("SELECT $1::bytea", data)
                assert result == data
            finally:
                await conn.close()

        asyncio.run(run())


# ==================== Transactions ====================


class TestTransactions:
    def test_transaction_commit(self):
        async def run():
            conn = await connect()
            try:
                async with conn.transaction():
                    await conn.execute("DROP TABLE IF EXISTS test_txn")
                    await conn.execute("CREATE TEMPORARY TABLE IF NOT EXISTS test_txn (id int)")
                    await conn.execute("INSERT INTO test_txn VALUES (1)")
                    result = await conn.fetchval("SELECT id FROM test_txn WHERE id = 1")
                    assert result == 1
            finally:
                await conn.close()

        asyncio.run(run())

    def test_transaction_rollback(self):
        """Savepoint rollback within a transaction undoes the inner work."""
        async def run():
            conn = await connect()
            try:
                async with conn.transaction():
                    await conn.execute("DROP TABLE IF EXISTS test_rb")
                    await conn.execute("CREATE TEMPORARY TABLE IF NOT EXISTS test_rb (id int)")
                    try:
                        async with conn.transaction():
                            await conn.execute("INSERT INTO test_rb VALUES (1)")
                            raise Exception("force rollback")
                    except Exception:
                        pass
                    result = await conn.fetchval("SELECT count(*) FROM test_rb")
                    assert result == 0
            finally:
                await conn.close()

        asyncio.run(run())

    def test_prepared_in_transaction(self):
        async def run():
            conn = await connect()
            try:
                async with conn.transaction():
                    await conn.execute("DROP TABLE IF EXISTS test_prep_txn")
                    await conn.execute("CREATE TEMPORARY TABLE IF NOT EXISTS test_prep_txn (id int, val text) ON COMMIT DROP")
                    stmt = await conn.prepare("INSERT INTO test_prep_txn VALUES ($1, $2)")
                    for i in range(10):
                        await stmt.fetch(i, f"val_{i}")
                    rows = await conn.fetch("SELECT * FROM test_prep_txn ORDER BY id")
                    assert len(rows) == 10
                    assert rows[0]["id"] == 0
                    assert rows[9]["val"] == "val_9"
            finally:
                await conn.close()

        asyncio.run(run())


# ==================== Connection Pool ====================


class TestConnectionPool:
    def test_pool_basic(self):
        async def run():
            pool = await asyncpg.create_pool(
                host=PGCAT_HOST, port=PGCAT_PORT,
                database=DATABASE, user=USER, password=PASSWORD,
                min_size=2, max_size=5,
            )
            try:
                async with pool.acquire() as conn:
                    result = await conn.fetchval("SELECT 1")
                    assert result == 1
            finally:
                await pool.close()

        asyncio.run(run())

    def test_pool_concurrent_prepared(self):
        """Multiple concurrent connections using prepared statements."""
        async def worker(pool, worker_id, iterations):
            async with pool.acquire() as conn:
                stmt = await conn.prepare("SELECT $1::int + $2::int")
                for i in range(iterations):
                    result = await stmt.fetchval(worker_id, i)
                    assert result == worker_id + i

        async def run():
            pool = await asyncpg.create_pool(
                host=PGCAT_HOST, port=PGCAT_PORT,
                database=DATABASE, user=USER, password=PASSWORD,
                min_size=3, max_size=8,
            )
            try:
                tasks = [worker(pool, w, 50) for w in range(8)]
                await asyncio.gather(*tasks)
            finally:
                await pool.close()

        asyncio.run(run())

    def test_pool_sequential_acquire_release(self):
        """Acquire and release connections repeatedly -- tests server reuse with cache."""
        async def run():
            pool = await asyncpg.create_pool(
                host=PGCAT_HOST, port=PGCAT_PORT,
                database=DATABASE, user=USER, password=PASSWORD,
                min_size=1, max_size=2,
            )
            try:
                for i in range(20):
                    async with pool.acquire() as conn:
                        stmt = await conn.prepare("SELECT $1::int")
                        result = await stmt.fetchval(i)
                        assert result == i
            finally:
                await pool.close()

        asyncio.run(run())


# ==================== Error Handling ====================


class TestErrorHandling:
    def test_syntax_error_in_prepared(self):
        async def run():
            conn = await connect()
            try:
                with pytest.raises(asyncpg.PostgresSyntaxError):
                    await conn.prepare("SELEC INVALID")
            finally:
                await conn.close()

        asyncio.run(run())

    def test_runtime_error_in_prepared(self):
        async def run():
            conn = await connect()
            try:
                stmt = await conn.prepare("SELECT 1 / $1::int")
                with pytest.raises(asyncpg.DataError):
                    await stmt.fetchval(0)
            finally:
                await conn.close()

        asyncio.run(run())

    def test_connection_usable_after_error(self):
        async def run():
            conn = await connect()
            try:
                try:
                    await conn.execute("SELECT 1/0")
                except asyncpg.DataError:
                    pass
                result = await conn.fetchval("SELECT 42")
                assert result == 42
            finally:
                await conn.close()

        asyncio.run(run())

    def test_prepared_after_error(self):
        async def run():
            conn = await connect()
            try:
                try:
                    await conn.prepare("NOT VALID SQL")
                except asyncpg.PostgresSyntaxError:
                    pass
                stmt = await conn.prepare("SELECT $1::int")
                result = await stmt.fetchval(99)
                assert result == 99
            finally:
                await conn.close()

        asyncio.run(run())


# ==================== Extended Protocol Edge Cases ====================


class TestExtendedProtocol:
    def test_many_parameters(self):
        async def run():
            conn = await connect()
            try:
                n = 20
                params_sql = ", ".join(f"${i+1}::int" for i in range(n))
                stmt = await conn.prepare(f"SELECT {params_sql}")
                values = list(range(n))
                row = await stmt.fetchrow(*values)
                for i in range(n):
                    assert row[i] == i
            finally:
                await conn.close()

        asyncio.run(run())

    def test_large_result(self):
        async def run():
            conn = await connect()
            try:
                stmt = await conn.prepare("SELECT generate_series(1, $1::int)")
                rows = await stmt.fetch(10000)
                assert len(rows) == 10000
                assert rows[0][0] == 1
                assert rows[9999][0] == 10000
            finally:
                await conn.close()

        asyncio.run(run())

    def test_large_parameter(self):
        async def run():
            conn = await connect()
            try:
                large_text = "x" * 100_000
                result = await conn.fetchval("SELECT length($1::text)", large_text)
                assert result == 100_000
            finally:
                await conn.close()

        asyncio.run(run())

    def test_rapid_prepare_execute_cycle(self):
        """Rapidly prepare and execute different statements -- stress tests Flush handling."""
        async def run():
            conn = await connect()
            try:
                for i in range(100):
                    result = await conn.fetchval(f"SELECT {i}::int + $1::int", 1)
                    assert result == i + 1
            finally:
                await conn.close()

        asyncio.run(run())

    def test_cursor_iteration(self):
        async def run():
            conn = await connect()
            try:
                async with conn.transaction():
                    cursor = await conn.cursor("SELECT generate_series(1, 100)")
                    rows = await cursor.fetch(10)
                    assert len(rows) == 10
                    assert rows[0][0] == 1
                    assert rows[9][0] == 10

                    rows = await cursor.fetch(10)
                    assert rows[0][0] == 11
            finally:
                await conn.close()

        asyncio.run(run())

    def test_copy_to(self):
        import io
        async def run():
            conn = await connect()
            try:
                async with conn.transaction():
                    await conn.execute("CREATE TEMPORARY TABLE IF NOT EXISTS test_copy_out (id int, val text) ON COMMIT DROP")
                    await conn.execute("INSERT INTO test_copy_out SELECT g, 'row_' || g FROM generate_series(1, 100) g")
                    buf = io.BytesIO()
                    await conn.copy_from_table("test_copy_out", output=buf, format="csv")
                    lines = buf.getvalue().decode().strip().split("\n")
                    assert len(lines) == 100
            finally:
                await conn.close()

        asyncio.run(run())

    def test_copy_from(self):
        import io
        async def run():
            conn = await connect()
            try:
                async with conn.transaction():
                    await conn.execute("DROP TABLE IF EXISTS test_copy_in")
                    await conn.execute("CREATE TEMPORARY TABLE test_copy_in (id int, val text)")
                    data = "\n".join(f"{i}\tval_{i}" for i in range(50))
                    await conn.copy_to_table("test_copy_in", source=io.BytesIO(data.encode()), format="text")
                    count = await conn.fetchval("SELECT count(*) FROM test_copy_in")
                    assert count == 50
            finally:
                await conn.close()

        asyncio.run(run())
