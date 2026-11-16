-- pool: psql_tx
-- pool: psql_tx_no_cache
-- pool: psql_session
-- Extended-protocol pipelines: several Parse/Bind/Execute before one Sync.
-- pgcat buffers extended messages until Sync or Flush and rewrites named
-- statements; an error must skip the rest of its Sync segment only, and
-- \flushrequest (Flush) must deliver results without ending the segment.
DROP TABLE IF EXISTS psql_pipeline;
CREATE TABLE psql_pipeline (id int PRIMARY KEY);

\startpipeline
SELECT $1::int AS one \bind 1 \sendpipeline
INSERT INTO psql_pipeline VALUES ($1) \bind 10 \sendpipeline
SELECT 1/0 \bind \sendpipeline
INSERT INTO psql_pipeline VALUES ($1) \bind 11 \sendpipeline
\syncpipeline
INSERT INTO psql_pipeline VALUES ($1) \bind 12 \sendpipeline
SELECT count(*) AS rows_in_second_segment FROM psql_pipeline \bind \sendpipeline
\endpipeline
-- The first segment failed as a whole (implicit transaction), the second committed.
SELECT id FROM psql_pipeline ORDER BY id;

-- Flush in the middle of a segment.
\startpipeline
SELECT 'before flush' AS step \bind \sendpipeline
\flushrequest
\getresults
SELECT 'after flush' AS step \bind \sendpipeline
\endpipeline

-- Named statements inside a pipeline, executed twice with different params.
\startpipeline
SELECT $1::text || '!' AS shout \parse pipe_named
\bind_named pipe_named 'hello' \sendpipeline
\bind_named pipe_named 'again' \sendpipeline
\endpipeline
\close_prepared pipe_named

-- Explicit transaction spanning pipeline segments.
\startpipeline
BEGIN \bind \sendpipeline
INSERT INTO psql_pipeline VALUES ($1) \bind 20 \sendpipeline
\syncpipeline
INSERT INTO psql_pipeline VALUES ($1) \bind 10 \sendpipeline
\syncpipeline
ROLLBACK \bind \sendpipeline
\endpipeline
SELECT id FROM psql_pipeline ORDER BY id;

-- A failed Parse in a pipeline.
\startpipeline
SELEC 1 \parse broken
SELECT 'segment after failed parse' AS step \bind \sendpipeline
\syncpipeline
SELECT 'next segment' AS step \bind \sendpipeline
\endpipeline

DROP TABLE psql_pipeline;
