-- Standalone repro for missing-stats bushy-pruning cases.
-- Runs plain EXPLAIN output (no JSON).
--
-- Recommended:
--   build/src/bin/psql/psql -p 55432 -d postgres -v ON_ERROR_STOP=1 \
--     -f scripts/missing-stats-pruned-cases-explain.sql

SET client_min_messages = warning;

CREATE OR REPLACE FUNCTION ms_rebuild_target_schema(target_table text, row_count int)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  i int;
  t text;
BEGIN
  EXECUTE 'DROP SCHEMA IF EXISTS ms_bushy_count CASCADE';
  EXECUTE 'CREATE SCHEMA ms_bushy_count';

  FOR i IN 1..10 LOOP
    EXECUTE format($sql$
      CREATE TABLE ms_bushy_count.t%s AS
      SELECT g AS id,
             ((g * (11 + %s)) %% 2000) AS k1,
             ((g * (17 + %s)) %% 1000) AS k2,
             ((g * (23 + %s)) %% 100) AS k3
      FROM generate_series(1, %s) g
    $sql$, i, i, i, i, row_count);
  END LOOP;

  EXECUTE format(
    'ALTER TABLE ms_bushy_count.%I SET (autovacuum_enabled=off, toast.autovacuum_enabled=off)',
    target_table
  );

  FOR i IN 1..10 LOOP
    t := format('t%s', i);
    IF t <> target_table THEN
      EXECUTE format('ANALYZE ms_bushy_count.%I', t);
    END IF;
  END LOOP;
END;
$$;

SET geqo = off;
SET join_collapse_limit = 20;
SET from_collapse_limit = 20;
SET jit = off;
SET enable_left_deep_join = off;

-- Shared queries from /tmp/ms_bushy_examples
-- query_1
-- query_3
-- query_4

SELECT 'CASE 1: target=t2 query_1' AS case_name;
SELECT ms_rebuild_target_schema('t2', 6000);
SELECT relname, reltuples
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'ms_bushy_count' AND c.relname = 't2';
SET enable_left_deep_join_on_missing_stats = off;
EXPLAIN
SELECT count(*) FROM ms_bushy_count.t1 a, ms_bushy_count.t2 b, ms_bushy_count.t3 c, ms_bushy_count.t4 d, ms_bushy_count.t5 e, ms_bushy_count.t6 f, ms_bushy_count.t7 g, ms_bushy_count.t8 h, ms_bushy_count.t9 i, ms_bushy_count.t10 j WHERE b.k2 = a.k2 AND c.k2 = a.k2 AND d.k3 = c.k3 AND e.k1 = c.k1 AND f.k2 = e.k2 AND g.k2 = e.k2 AND h.k3 = a.k3 AND i.k3 = g.k3 AND j.k3 = a.k3 AND d.k1 = e.k1 AND b.k3 = i.k3 AND f.k1 = g.k1 AND f.k2 = e.k2 AND j.k2 = a.k2 AND d.k1 = i.k1 AND a.k2 = b.k2 AND g.k1 = c.k1 AND a.k1 = d.k1 AND e.k2 = a.k2 AND c.k1 = h.k1 AND g.k3 = j.k3 AND a.k3 < 5 AND j.k2 < 7;
SET enable_left_deep_join_on_missing_stats = on;
EXPLAIN
SELECT count(*) FROM ms_bushy_count.t1 a, ms_bushy_count.t2 b, ms_bushy_count.t3 c, ms_bushy_count.t4 d, ms_bushy_count.t5 e, ms_bushy_count.t6 f, ms_bushy_count.t7 g, ms_bushy_count.t8 h, ms_bushy_count.t9 i, ms_bushy_count.t10 j WHERE b.k2 = a.k2 AND c.k2 = a.k2 AND d.k3 = c.k3 AND e.k1 = c.k1 AND f.k2 = e.k2 AND g.k2 = e.k2 AND h.k3 = a.k3 AND i.k3 = g.k3 AND j.k3 = a.k3 AND d.k1 = e.k1 AND b.k3 = i.k3 AND f.k1 = g.k1 AND f.k2 = e.k2 AND j.k2 = a.k2 AND d.k1 = i.k1 AND a.k2 = b.k2 AND g.k1 = c.k1 AND a.k1 = d.k1 AND e.k2 = a.k2 AND c.k1 = h.k1 AND g.k3 = j.k3 AND a.k3 < 5 AND j.k2 < 7;

SELECT 'CASE 2: target=t4 query_4' AS case_name;
SELECT ms_rebuild_target_schema('t4', 6000);
SELECT relname, reltuples
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'ms_bushy_count' AND c.relname = 't4';
SET enable_left_deep_join_on_missing_stats = off;
EXPLAIN
SELECT count(*) FROM ms_bushy_count.t1 a, ms_bushy_count.t2 b, ms_bushy_count.t3 c, ms_bushy_count.t4 d, ms_bushy_count.t5 e, ms_bushy_count.t6 f, ms_bushy_count.t7 g, ms_bushy_count.t8 h, ms_bushy_count.t9 i, ms_bushy_count.t10 j WHERE b.k1 = a.k1 AND c.k3 = a.k3 AND d.k3 = b.k3 AND e.k2 = a.k2 AND f.k2 = c.k2 AND g.k2 = e.k2 AND h.k2 = b.k2 AND i.k1 = f.k1 AND j.k3 = g.k3 AND e.k3 = c.k3 AND a.k3 = c.k3 AND f.k1 = e.k1 AND c.k2 = f.k2 AND h.k2 = j.k2 AND j.k1 = a.k1 AND g.k3 = i.k3 AND j.k1 = f.k1 AND e.k1 = f.k1 AND g.k1 = j.k1 AND i.k3 = b.k3 AND b.k3 = j.k3 AND a.k3 < 5 AND j.k2 < 7;
SET enable_left_deep_join_on_missing_stats = on;
EXPLAIN
SELECT count(*) FROM ms_bushy_count.t1 a, ms_bushy_count.t2 b, ms_bushy_count.t3 c, ms_bushy_count.t4 d, ms_bushy_count.t5 e, ms_bushy_count.t6 f, ms_bushy_count.t7 g, ms_bushy_count.t8 h, ms_bushy_count.t9 i, ms_bushy_count.t10 j WHERE b.k1 = a.k1 AND c.k3 = a.k3 AND d.k3 = b.k3 AND e.k2 = a.k2 AND f.k2 = c.k2 AND g.k2 = e.k2 AND h.k2 = b.k2 AND i.k1 = f.k1 AND j.k3 = g.k3 AND e.k3 = c.k3 AND a.k3 = c.k3 AND f.k1 = e.k1 AND c.k2 = f.k2 AND h.k2 = j.k2 AND j.k1 = a.k1 AND g.k3 = i.k3 AND j.k1 = f.k1 AND e.k1 = f.k1 AND g.k1 = j.k1 AND i.k3 = b.k3 AND b.k3 = j.k3 AND a.k3 < 5 AND j.k2 < 7;

SELECT 'CASE 3: target=t5 query_4' AS case_name;
SELECT ms_rebuild_target_schema('t5', 6000);
SELECT relname, reltuples
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'ms_bushy_count' AND c.relname = 't5';
SET enable_left_deep_join_on_missing_stats = off;
EXPLAIN
SELECT count(*) FROM ms_bushy_count.t1 a, ms_bushy_count.t2 b, ms_bushy_count.t3 c, ms_bushy_count.t4 d, ms_bushy_count.t5 e, ms_bushy_count.t6 f, ms_bushy_count.t7 g, ms_bushy_count.t8 h, ms_bushy_count.t9 i, ms_bushy_count.t10 j WHERE b.k1 = a.k1 AND c.k3 = a.k3 AND d.k3 = b.k3 AND e.k2 = a.k2 AND f.k2 = c.k2 AND g.k2 = e.k2 AND h.k2 = b.k2 AND i.k1 = f.k1 AND j.k3 = g.k3 AND e.k3 = c.k3 AND a.k3 = c.k3 AND f.k1 = e.k1 AND c.k2 = f.k2 AND h.k2 = j.k2 AND j.k1 = a.k1 AND g.k3 = i.k3 AND j.k1 = f.k1 AND e.k1 = f.k1 AND g.k1 = j.k1 AND i.k3 = b.k3 AND b.k3 = j.k3 AND a.k3 < 5 AND j.k2 < 7;
SET enable_left_deep_join_on_missing_stats = on;
EXPLAIN
SELECT count(*) FROM ms_bushy_count.t1 a, ms_bushy_count.t2 b, ms_bushy_count.t3 c, ms_bushy_count.t4 d, ms_bushy_count.t5 e, ms_bushy_count.t6 f, ms_bushy_count.t7 g, ms_bushy_count.t8 h, ms_bushy_count.t9 i, ms_bushy_count.t10 j WHERE b.k1 = a.k1 AND c.k3 = a.k3 AND d.k3 = b.k3 AND e.k2 = a.k2 AND f.k2 = c.k2 AND g.k2 = e.k2 AND h.k2 = b.k2 AND i.k1 = f.k1 AND j.k3 = g.k3 AND e.k3 = c.k3 AND a.k3 = c.k3 AND f.k1 = e.k1 AND c.k2 = f.k2 AND h.k2 = j.k2 AND j.k1 = a.k1 AND g.k3 = i.k3 AND j.k1 = f.k1 AND e.k1 = f.k1 AND g.k1 = j.k1 AND i.k3 = b.k3 AND b.k3 = j.k3 AND a.k3 < 5 AND j.k2 < 7;

SELECT 'CASE 4: target=t7 query_1' AS case_name;
SELECT ms_rebuild_target_schema('t7', 6000);
SELECT relname, reltuples
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'ms_bushy_count' AND c.relname = 't7';
SET enable_left_deep_join_on_missing_stats = off;
EXPLAIN
SELECT count(*) FROM ms_bushy_count.t1 a, ms_bushy_count.t2 b, ms_bushy_count.t3 c, ms_bushy_count.t4 d, ms_bushy_count.t5 e, ms_bushy_count.t6 f, ms_bushy_count.t7 g, ms_bushy_count.t8 h, ms_bushy_count.t9 i, ms_bushy_count.t10 j WHERE b.k2 = a.k2 AND c.k2 = a.k2 AND d.k3 = c.k3 AND e.k1 = c.k1 AND f.k2 = e.k2 AND g.k2 = e.k2 AND h.k3 = a.k3 AND i.k3 = g.k3 AND j.k3 = a.k3 AND d.k1 = e.k1 AND b.k3 = i.k3 AND f.k1 = g.k1 AND f.k2 = e.k2 AND j.k2 = a.k2 AND d.k1 = i.k1 AND a.k2 = b.k2 AND g.k1 = c.k1 AND a.k1 = d.k1 AND e.k2 = a.k2 AND c.k1 = h.k1 AND g.k3 = j.k3 AND a.k3 < 5 AND j.k2 < 7;
SET enable_left_deep_join_on_missing_stats = on;
EXPLAIN
SELECT count(*) FROM ms_bushy_count.t1 a, ms_bushy_count.t2 b, ms_bushy_count.t3 c, ms_bushy_count.t4 d, ms_bushy_count.t5 e, ms_bushy_count.t6 f, ms_bushy_count.t7 g, ms_bushy_count.t8 h, ms_bushy_count.t9 i, ms_bushy_count.t10 j WHERE b.k2 = a.k2 AND c.k2 = a.k2 AND d.k3 = c.k3 AND e.k1 = c.k1 AND f.k2 = e.k2 AND g.k2 = e.k2 AND h.k3 = a.k3 AND i.k3 = g.k3 AND j.k3 = a.k3 AND d.k1 = e.k1 AND b.k3 = i.k3 AND f.k1 = g.k1 AND f.k2 = e.k2 AND j.k2 = a.k2 AND d.k1 = i.k1 AND a.k2 = b.k2 AND g.k1 = c.k1 AND a.k1 = d.k1 AND e.k2 = a.k2 AND c.k1 = h.k1 AND g.k3 = j.k3 AND a.k3 < 5 AND j.k2 < 7;

SELECT 'CASE 5: target=t8 query_1' AS case_name;
SELECT ms_rebuild_target_schema('t8', 6000);
SELECT relname, reltuples
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'ms_bushy_count' AND c.relname = 't8';
SET enable_left_deep_join_on_missing_stats = off;
EXPLAIN
SELECT count(*) FROM ms_bushy_count.t1 a, ms_bushy_count.t2 b, ms_bushy_count.t3 c, ms_bushy_count.t4 d, ms_bushy_count.t5 e, ms_bushy_count.t6 f, ms_bushy_count.t7 g, ms_bushy_count.t8 h, ms_bushy_count.t9 i, ms_bushy_count.t10 j WHERE b.k2 = a.k2 AND c.k2 = a.k2 AND d.k3 = c.k3 AND e.k1 = c.k1 AND f.k2 = e.k2 AND g.k2 = e.k2 AND h.k3 = a.k3 AND i.k3 = g.k3 AND j.k3 = a.k3 AND d.k1 = e.k1 AND b.k3 = i.k3 AND f.k1 = g.k1 AND f.k2 = e.k2 AND j.k2 = a.k2 AND d.k1 = i.k1 AND a.k2 = b.k2 AND g.k1 = c.k1 AND a.k1 = d.k1 AND e.k2 = a.k2 AND c.k1 = h.k1 AND g.k3 = j.k3 AND a.k3 < 5 AND j.k2 < 7;
SET enable_left_deep_join_on_missing_stats = on;
EXPLAIN
SELECT count(*) FROM ms_bushy_count.t1 a, ms_bushy_count.t2 b, ms_bushy_count.t3 c, ms_bushy_count.t4 d, ms_bushy_count.t5 e, ms_bushy_count.t6 f, ms_bushy_count.t7 g, ms_bushy_count.t8 h, ms_bushy_count.t9 i, ms_bushy_count.t10 j WHERE b.k2 = a.k2 AND c.k2 = a.k2 AND d.k3 = c.k3 AND e.k1 = c.k1 AND f.k2 = e.k2 AND g.k2 = e.k2 AND h.k3 = a.k3 AND i.k3 = g.k3 AND j.k3 = a.k3 AND d.k1 = e.k1 AND b.k3 = i.k3 AND f.k1 = g.k1 AND f.k2 = e.k2 AND j.k2 = a.k2 AND d.k1 = i.k1 AND a.k2 = b.k2 AND g.k1 = c.k1 AND a.k1 = d.k1 AND e.k2 = a.k2 AND c.k1 = h.k1 AND g.k3 = j.k3 AND a.k3 < 5 AND j.k2 < 7;

SELECT 'CASE 6: target=t8 query_4' AS case_name;
SELECT ms_rebuild_target_schema('t8', 6000);
SELECT relname, reltuples
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'ms_bushy_count' AND c.relname = 't8';
SET enable_left_deep_join_on_missing_stats = off;
EXPLAIN
SELECT count(*) FROM ms_bushy_count.t1 a, ms_bushy_count.t2 b, ms_bushy_count.t3 c, ms_bushy_count.t4 d, ms_bushy_count.t5 e, ms_bushy_count.t6 f, ms_bushy_count.t7 g, ms_bushy_count.t8 h, ms_bushy_count.t9 i, ms_bushy_count.t10 j WHERE b.k1 = a.k1 AND c.k3 = a.k3 AND d.k3 = b.k3 AND e.k2 = a.k2 AND f.k2 = c.k2 AND g.k2 = e.k2 AND h.k2 = b.k2 AND i.k1 = f.k1 AND j.k3 = g.k3 AND e.k3 = c.k3 AND a.k3 = c.k3 AND f.k1 = e.k1 AND c.k2 = f.k2 AND h.k2 = j.k2 AND j.k1 = a.k1 AND g.k3 = i.k3 AND j.k1 = f.k1 AND e.k1 = f.k1 AND g.k1 = j.k1 AND i.k3 = b.k3 AND b.k3 = j.k3 AND a.k3 < 5 AND j.k2 < 7;
SET enable_left_deep_join_on_missing_stats = on;
EXPLAIN
SELECT count(*) FROM ms_bushy_count.t1 a, ms_bushy_count.t2 b, ms_bushy_count.t3 c, ms_bushy_count.t4 d, ms_bushy_count.t5 e, ms_bushy_count.t6 f, ms_bushy_count.t7 g, ms_bushy_count.t8 h, ms_bushy_count.t9 i, ms_bushy_count.t10 j WHERE b.k1 = a.k1 AND c.k3 = a.k3 AND d.k3 = b.k3 AND e.k2 = a.k2 AND f.k2 = c.k2 AND g.k2 = e.k2 AND h.k2 = b.k2 AND i.k1 = f.k1 AND j.k3 = g.k3 AND e.k3 = c.k3 AND a.k3 = c.k3 AND f.k1 = e.k1 AND c.k2 = f.k2 AND h.k2 = j.k2 AND j.k1 = a.k1 AND g.k3 = i.k3 AND j.k1 = f.k1 AND e.k1 = f.k1 AND g.k1 = j.k1 AND i.k3 = b.k3 AND b.k3 = j.k3 AND a.k3 < 5 AND j.k2 < 7;

SELECT 'CASE 7: target=t9 query_1' AS case_name;
SELECT ms_rebuild_target_schema('t9', 6000);
SELECT relname, reltuples
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'ms_bushy_count' AND c.relname = 't9';
SET enable_left_deep_join_on_missing_stats = off;
EXPLAIN
SELECT count(*) FROM ms_bushy_count.t1 a, ms_bushy_count.t2 b, ms_bushy_count.t3 c, ms_bushy_count.t4 d, ms_bushy_count.t5 e, ms_bushy_count.t6 f, ms_bushy_count.t7 g, ms_bushy_count.t8 h, ms_bushy_count.t9 i, ms_bushy_count.t10 j WHERE b.k2 = a.k2 AND c.k2 = a.k2 AND d.k3 = c.k3 AND e.k1 = c.k1 AND f.k2 = e.k2 AND g.k2 = e.k2 AND h.k3 = a.k3 AND i.k3 = g.k3 AND j.k3 = a.k3 AND d.k1 = e.k1 AND b.k3 = i.k3 AND f.k1 = g.k1 AND f.k2 = e.k2 AND j.k2 = a.k2 AND d.k1 = i.k1 AND a.k2 = b.k2 AND g.k1 = c.k1 AND a.k1 = d.k1 AND e.k2 = a.k2 AND c.k1 = h.k1 AND g.k3 = j.k3 AND a.k3 < 5 AND j.k2 < 7;
SET enable_left_deep_join_on_missing_stats = on;
EXPLAIN
SELECT count(*) FROM ms_bushy_count.t1 a, ms_bushy_count.t2 b, ms_bushy_count.t3 c, ms_bushy_count.t4 d, ms_bushy_count.t5 e, ms_bushy_count.t6 f, ms_bushy_count.t7 g, ms_bushy_count.t8 h, ms_bushy_count.t9 i, ms_bushy_count.t10 j WHERE b.k2 = a.k2 AND c.k2 = a.k2 AND d.k3 = c.k3 AND e.k1 = c.k1 AND f.k2 = e.k2 AND g.k2 = e.k2 AND h.k3 = a.k3 AND i.k3 = g.k3 AND j.k3 = a.k3 AND d.k1 = e.k1 AND b.k3 = i.k3 AND f.k1 = g.k1 AND f.k2 = e.k2 AND j.k2 = a.k2 AND d.k1 = i.k1 AND a.k2 = b.k2 AND g.k1 = c.k1 AND a.k1 = d.k1 AND e.k2 = a.k2 AND c.k1 = h.k1 AND g.k3 = j.k3 AND a.k3 < 5 AND j.k2 < 7;

SELECT 'CASE 8: target=t9 query_4' AS case_name;
SELECT ms_rebuild_target_schema('t9', 6000);
SELECT relname, reltuples
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'ms_bushy_count' AND c.relname = 't9';
SET enable_left_deep_join_on_missing_stats = off;
EXPLAIN
SELECT count(*) FROM ms_bushy_count.t1 a, ms_bushy_count.t2 b, ms_bushy_count.t3 c, ms_bushy_count.t4 d, ms_bushy_count.t5 e, ms_bushy_count.t6 f, ms_bushy_count.t7 g, ms_bushy_count.t8 h, ms_bushy_count.t9 i, ms_bushy_count.t10 j WHERE b.k1 = a.k1 AND c.k3 = a.k3 AND d.k3 = b.k3 AND e.k2 = a.k2 AND f.k2 = c.k2 AND g.k2 = e.k2 AND h.k2 = b.k2 AND i.k1 = f.k1 AND j.k3 = g.k3 AND e.k3 = c.k3 AND a.k3 = c.k3 AND f.k1 = e.k1 AND c.k2 = f.k2 AND h.k2 = j.k2 AND j.k1 = a.k1 AND g.k3 = i.k3 AND j.k1 = f.k1 AND e.k1 = f.k1 AND g.k1 = j.k1 AND i.k3 = b.k3 AND b.k3 = j.k3 AND a.k3 < 5 AND j.k2 < 7;
SET enable_left_deep_join_on_missing_stats = on;
EXPLAIN
SELECT count(*) FROM ms_bushy_count.t1 a, ms_bushy_count.t2 b, ms_bushy_count.t3 c, ms_bushy_count.t4 d, ms_bushy_count.t5 e, ms_bushy_count.t6 f, ms_bushy_count.t7 g, ms_bushy_count.t8 h, ms_bushy_count.t9 i, ms_bushy_count.t10 j WHERE b.k1 = a.k1 AND c.k3 = a.k3 AND d.k3 = b.k3 AND e.k2 = a.k2 AND f.k2 = c.k2 AND g.k2 = e.k2 AND h.k2 = b.k2 AND i.k1 = f.k1 AND j.k3 = g.k3 AND e.k3 = c.k3 AND a.k3 = c.k3 AND f.k1 = e.k1 AND c.k2 = f.k2 AND h.k2 = j.k2 AND j.k1 = a.k1 AND g.k3 = i.k3 AND j.k1 = f.k1 AND e.k1 = f.k1 AND g.k1 = j.k1 AND i.k3 = b.k3 AND b.k3 = j.k3 AND a.k3 < 5 AND j.k2 < 7;

SELECT 'CASE 9: target=t10 query_3' AS case_name;
SELECT ms_rebuild_target_schema('t10', 6000);
SELECT relname, reltuples
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'ms_bushy_count' AND c.relname = 't10';
SET enable_left_deep_join_on_missing_stats = off;
EXPLAIN
SELECT count(*) FROM ms_bushy_count.t1 a, ms_bushy_count.t2 b, ms_bushy_count.t3 c, ms_bushy_count.t4 d, ms_bushy_count.t5 e, ms_bushy_count.t6 f, ms_bushy_count.t7 g, ms_bushy_count.t8 h, ms_bushy_count.t9 i, ms_bushy_count.t10 j WHERE b.k3 = a.k3 AND c.k2 = a.k2 AND d.k3 = b.k3 AND e.k2 = d.k2 AND f.k3 = c.k3 AND g.k2 = e.k2 AND h.k2 = b.k2 AND i.k3 = h.k3 AND j.k2 = d.k2 AND g.k2 = c.k2 AND a.k3 = g.k3 AND j.k1 = a.k1 AND j.k1 = f.k1 AND i.k2 = b.k2 AND e.k1 = i.k1 AND i.k1 = c.k1 AND a.k3 = f.k3 AND c.k2 = h.k2 AND d.k3 = j.k3 AND b.k2 = a.k2 AND f.k1 = a.k1 AND a.k3 < 5 AND j.k2 < 7;
SET enable_left_deep_join_on_missing_stats = on;
EXPLAIN
SELECT count(*) FROM ms_bushy_count.t1 a, ms_bushy_count.t2 b, ms_bushy_count.t3 c, ms_bushy_count.t4 d, ms_bushy_count.t5 e, ms_bushy_count.t6 f, ms_bushy_count.t7 g, ms_bushy_count.t8 h, ms_bushy_count.t9 i, ms_bushy_count.t10 j WHERE b.k3 = a.k3 AND c.k2 = a.k2 AND d.k3 = b.k3 AND e.k2 = d.k2 AND f.k3 = c.k3 AND g.k2 = e.k2 AND h.k2 = b.k2 AND i.k3 = h.k3 AND j.k2 = d.k2 AND g.k2 = c.k2 AND a.k3 = g.k3 AND j.k1 = a.k1 AND j.k1 = f.k1 AND i.k2 = b.k2 AND e.k1 = i.k1 AND i.k1 = c.k1 AND a.k3 = f.k3 AND c.k2 = h.k2 AND d.k3 = j.k3 AND b.k2 = a.k2 AND f.k1 = a.k1 AND a.k3 < 5 AND j.k2 < 7;

