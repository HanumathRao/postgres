-- ============================================================================
-- missing-stats-pruned-cases-portable.sql
--
-- Goal
--   Reproduce 9 validated cases where a target table has missing stats and:
--     1) POLICY OFF plan can include target table in a bushy subtree
--     2) POLICY ON  plan should remove target-involving bushy subtree
--
-- Policy under test
--   "Do not allow missing-stats table to participate in bushy joins.
--    Bushy joins among analyzed tables are still allowed."
--
-- IMPORTANT
--   This script uses plain EXPLAIN (text), no JSON.
--   For patched PostgreSQL, set/flip your custom GUC before OFF/ON EXPLAINs.
--   For Redshift/postgres-like engines, replace the policy toggle commands with
--   the equivalent engine-specific setting, or run OFF/ON in separate sessions.
--
-- Suggested run:
--   build/src/bin/psql/psql -p 55432 -d postgres -v ON_ERROR_STOP=1 \
--     -f scripts/missing-stats-pruned-cases-portable.sql | tee /tmp/output.out
-- ============================================================================

-- -------------------------------
-- 0) Session setup
-- -------------------------------
SET client_min_messages = warning;
SET geqo = off;
SET join_collapse_limit = 20;
SET from_collapse_limit = 20;
SET jit = off;

DROP SCHEMA IF EXISTS ms_bushy_count CASCADE;
CREATE SCHEMA ms_bushy_count;

-- -------------------------------
-- 1) Deterministic row source
-- -------------------------------
-- 6000 rows, deterministic and portable (recursive CTE).
-- If your engine does not support recursive CTE, replace this temp table
-- creation with any 1..6000 number table.
CREATE TEMP TABLE ms_seq_6000 AS
WITH RECURSIVE seq(n) AS (
  SELECT 1
  UNION ALL
  SELECT n + 1 FROM seq WHERE n < 6000
)
SELECT n FROM seq;

-- -------------------------------
-- 2) Base table build helpers
-- -------------------------------
-- Build all t1..t10 once. Per case we re-create only the target table
-- without ANALYZE to keep target stats missing.
CREATE TABLE ms_bushy_count.t1 AS
SELECT n AS id, ((n * 12) % 2000) AS k1, ((n * 18) % 1000) AS k2, ((n * 24) % 100) AS k3 FROM ms_seq_6000;
CREATE TABLE ms_bushy_count.t2 AS
SELECT n AS id, ((n * 13) % 2000) AS k1, ((n * 19) % 1000) AS k2, ((n * 25) % 100) AS k3 FROM ms_seq_6000;
CREATE TABLE ms_bushy_count.t3 AS
SELECT n AS id, ((n * 14) % 2000) AS k1, ((n * 20) % 1000) AS k2, ((n * 26) % 100) AS k3 FROM ms_seq_6000;
CREATE TABLE ms_bushy_count.t4 AS
SELECT n AS id, ((n * 15) % 2000) AS k1, ((n * 21) % 1000) AS k2, ((n * 27) % 100) AS k3 FROM ms_seq_6000;
CREATE TABLE ms_bushy_count.t5 AS
SELECT n AS id, ((n * 16) % 2000) AS k1, ((n * 22) % 1000) AS k2, ((n * 28) % 100) AS k3 FROM ms_seq_6000;
CREATE TABLE ms_bushy_count.t6 AS
SELECT n AS id, ((n * 17) % 2000) AS k1, ((n * 23) % 1000) AS k2, ((n * 29) % 100) AS k3 FROM ms_seq_6000;
CREATE TABLE ms_bushy_count.t7 AS
SELECT n AS id, ((n * 18) % 2000) AS k1, ((n * 24) % 1000) AS k2, ((n * 30) % 100) AS k3 FROM ms_seq_6000;
CREATE TABLE ms_bushy_count.t8 AS
SELECT n AS id, ((n * 19) % 2000) AS k1, ((n * 25) % 1000) AS k2, ((n * 31) % 100) AS k3 FROM ms_seq_6000;
CREATE TABLE ms_bushy_count.t9 AS
SELECT n AS id, ((n * 20) % 2000) AS k1, ((n * 26) % 1000) AS k2, ((n * 32) % 100) AS k3 FROM ms_seq_6000;
CREATE TABLE ms_bushy_count.t10 AS
SELECT n AS id, ((n * 21) % 2000) AS k1, ((n * 27) % 1000) AS k2, ((n * 33) % 100) AS k3 FROM ms_seq_6000;

-- Helper: analyze all tables.
ANALYZE ms_bushy_count.t1;
ANALYZE ms_bushy_count.t2;
ANALYZE ms_bushy_count.t3;
ANALYZE ms_bushy_count.t4;
ANALYZE ms_bushy_count.t5;
ANALYZE ms_bushy_count.t6;
ANALYZE ms_bushy_count.t7;
ANALYZE ms_bushy_count.t8;
ANALYZE ms_bushy_count.t9;
ANALYZE ms_bushy_count.t10;

-- ============================================================================
-- CASE BLOCKS
-- For each case:
--   - Recreate target table only (no ANALYZE) => target missing stats
--   - OFF plan: expect target may be in bushy subtree
--   - ON  plan: expect target NOT in bushy subtree
-- Correctness rationale:
--   The policy prunes bushy pair generation whenever either side contains a
--   missing-stats relation. Bushy joins among analyzed-only rels can remain.
--
-- In patched PostgreSQL, uncomment/replace policy toggle lines.
-- ============================================================================

-- ---------------- CASE 1: target=t2, query=q1 ----------------
SELECT 'CASE 1: target=t2, query=q1' AS case_name;
ANALYZE ms_bushy_count.t1; ANALYZE ms_bushy_count.t2; ANALYZE ms_bushy_count.t3; ANALYZE ms_bushy_count.t4; ANALYZE ms_bushy_count.t5;
ANALYZE ms_bushy_count.t6; ANALYZE ms_bushy_count.t7; ANALYZE ms_bushy_count.t8; ANALYZE ms_bushy_count.t9; ANALYZE ms_bushy_count.t10;
DROP TABLE ms_bushy_count.t2;
CREATE TABLE ms_bushy_count.t2 AS
SELECT n AS id, ((n * 13) % 2000) AS k1, ((n * 19) % 1000) AS k2, ((n * 25) % 100) AS k3 FROM ms_seq_6000;
-- PostgreSQL-only optional check:
-- SELECT relname, reltuples FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='ms_bushy_count' AND relname='t2';
-- POLICY OFF
-- SET enable_left_deep_join = off;
-- SET enable_left_deep_join_on_missing_stats = off;
EXPLAIN
SELECT count(*) FROM ms_bushy_count.t1 a, ms_bushy_count.t2 b, ms_bushy_count.t3 c, ms_bushy_count.t4 d,
ms_bushy_count.t5 e, ms_bushy_count.t6 f, ms_bushy_count.t7 g, ms_bushy_count.t8 h,
ms_bushy_count.t9 i, ms_bushy_count.t10 j
WHERE b.k2 = a.k2 AND c.k2 = a.k2 AND d.k3 = c.k3 AND e.k1 = c.k1
AND f.k2 = e.k2 AND g.k2 = e.k2 AND h.k3 = a.k3 AND i.k3 = g.k3
AND j.k3 = a.k3 AND d.k1 = e.k1 AND b.k3 = i.k3 AND f.k1 = g.k1
AND f.k2 = e.k2 AND j.k2 = a.k2 AND d.k1 = i.k1 AND a.k2 = b.k2
AND g.k1 = c.k1 AND a.k1 = d.k1 AND e.k2 = a.k2 AND c.k1 = h.k1
AND g.k3 = j.k3 AND a.k3 < 5 AND j.k2 < 7;
-- POLICY ON
-- SET enable_left_deep_join = off;
-- SET enable_left_deep_join_on_missing_stats = on;
EXPLAIN
SELECT count(*) FROM ms_bushy_count.t1 a, ms_bushy_count.t2 b, ms_bushy_count.t3 c, ms_bushy_count.t4 d,
ms_bushy_count.t5 e, ms_bushy_count.t6 f, ms_bushy_count.t7 g, ms_bushy_count.t8 h,
ms_bushy_count.t9 i, ms_bushy_count.t10 j
WHERE b.k2 = a.k2 AND c.k2 = a.k2 AND d.k3 = c.k3 AND e.k1 = c.k1
AND f.k2 = e.k2 AND g.k2 = e.k2 AND h.k3 = a.k3 AND i.k3 = g.k3
AND j.k3 = a.k3 AND d.k1 = e.k1 AND b.k3 = i.k3 AND f.k1 = g.k1
AND f.k2 = e.k2 AND j.k2 = a.k2 AND d.k1 = i.k1 AND a.k2 = b.k2
AND g.k1 = c.k1 AND a.k1 = d.k1 AND e.k2 = a.k2 AND c.k1 = h.k1
AND g.k3 = j.k3 AND a.k3 < 5 AND j.k2 < 7;

-- ---------------- CASE 2: target=t4, query=q4 ----------------
SELECT 'CASE 2: target=t4, query=q4' AS case_name;
ANALYZE ms_bushy_count.t1; ANALYZE ms_bushy_count.t2; ANALYZE ms_bushy_count.t3; ANALYZE ms_bushy_count.t4; ANALYZE ms_bushy_count.t5;
ANALYZE ms_bushy_count.t6; ANALYZE ms_bushy_count.t7; ANALYZE ms_bushy_count.t8; ANALYZE ms_bushy_count.t9; ANALYZE ms_bushy_count.t10;
DROP TABLE ms_bushy_count.t4;
CREATE TABLE ms_bushy_count.t4 AS
SELECT n AS id, ((n * 15) % 2000) AS k1, ((n * 21) % 1000) AS k2, ((n * 27) % 100) AS k3 FROM ms_seq_6000;
-- POLICY OFF
EXPLAIN
SELECT count(*) FROM ms_bushy_count.t1 a, ms_bushy_count.t2 b, ms_bushy_count.t3 c, ms_bushy_count.t4 d,
ms_bushy_count.t5 e, ms_bushy_count.t6 f, ms_bushy_count.t7 g, ms_bushy_count.t8 h,
ms_bushy_count.t9 i, ms_bushy_count.t10 j
WHERE b.k1 = a.k1 AND c.k3 = a.k3 AND d.k3 = b.k3 AND e.k2 = a.k2
AND f.k2 = c.k2 AND g.k2 = e.k2 AND h.k2 = b.k2 AND i.k1 = f.k1
AND j.k3 = g.k3 AND e.k3 = c.k3 AND a.k3 = c.k3 AND f.k1 = e.k1
AND c.k2 = f.k2 AND h.k2 = j.k2 AND j.k1 = a.k1 AND g.k3 = i.k3
AND j.k1 = f.k1 AND e.k1 = f.k1 AND g.k1 = j.k1 AND i.k3 = b.k3
AND b.k3 = j.k3 AND a.k3 < 5 AND j.k2 < 7;
-- POLICY ON
EXPLAIN
SELECT count(*) FROM ms_bushy_count.t1 a, ms_bushy_count.t2 b, ms_bushy_count.t3 c, ms_bushy_count.t4 d,
ms_bushy_count.t5 e, ms_bushy_count.t6 f, ms_bushy_count.t7 g, ms_bushy_count.t8 h,
ms_bushy_count.t9 i, ms_bushy_count.t10 j
WHERE b.k1 = a.k1 AND c.k3 = a.k3 AND d.k3 = b.k3 AND e.k2 = a.k2
AND f.k2 = c.k2 AND g.k2 = e.k2 AND h.k2 = b.k2 AND i.k1 = f.k1
AND j.k3 = g.k3 AND e.k3 = c.k3 AND a.k3 = c.k3 AND f.k1 = e.k1
AND c.k2 = f.k2 AND h.k2 = j.k2 AND j.k1 = a.k1 AND g.k3 = i.k3
AND j.k1 = f.k1 AND e.k1 = f.k1 AND g.k1 = j.k1 AND i.k3 = b.k3
AND b.k3 = j.k3 AND a.k3 < 5 AND j.k2 < 7;

-- ---------------- CASE 3: target=t5, query=q4 ----------------
SELECT 'CASE 3: target=t5, query=q4' AS case_name;
ANALYZE ms_bushy_count.t1; ANALYZE ms_bushy_count.t2; ANALYZE ms_bushy_count.t3; ANALYZE ms_bushy_count.t4; ANALYZE ms_bushy_count.t5;
ANALYZE ms_bushy_count.t6; ANALYZE ms_bushy_count.t7; ANALYZE ms_bushy_count.t8; ANALYZE ms_bushy_count.t9; ANALYZE ms_bushy_count.t10;
DROP TABLE ms_bushy_count.t5;
CREATE TABLE ms_bushy_count.t5 AS
SELECT n AS id, ((n * 16) % 2000) AS k1, ((n * 22) % 1000) AS k2, ((n * 28) % 100) AS k3 FROM ms_seq_6000;
-- POLICY OFF
EXPLAIN
SELECT count(*) FROM ms_bushy_count.t1 a, ms_bushy_count.t2 b, ms_bushy_count.t3 c, ms_bushy_count.t4 d,
ms_bushy_count.t5 e, ms_bushy_count.t6 f, ms_bushy_count.t7 g, ms_bushy_count.t8 h,
ms_bushy_count.t9 i, ms_bushy_count.t10 j
WHERE b.k1 = a.k1 AND c.k3 = a.k3 AND d.k3 = b.k3 AND e.k2 = a.k2
AND f.k2 = c.k2 AND g.k2 = e.k2 AND h.k2 = b.k2 AND i.k1 = f.k1
AND j.k3 = g.k3 AND e.k3 = c.k3 AND a.k3 = c.k3 AND f.k1 = e.k1
AND c.k2 = f.k2 AND h.k2 = j.k2 AND j.k1 = a.k1 AND g.k3 = i.k3
AND j.k1 = f.k1 AND e.k1 = f.k1 AND g.k1 = j.k1 AND i.k3 = b.k3
AND b.k3 = j.k3 AND a.k3 < 5 AND j.k2 < 7;
-- POLICY ON
EXPLAIN
SELECT count(*) FROM ms_bushy_count.t1 a, ms_bushy_count.t2 b, ms_bushy_count.t3 c, ms_bushy_count.t4 d,
ms_bushy_count.t5 e, ms_bushy_count.t6 f, ms_bushy_count.t7 g, ms_bushy_count.t8 h,
ms_bushy_count.t9 i, ms_bushy_count.t10 j
WHERE b.k1 = a.k1 AND c.k3 = a.k3 AND d.k3 = b.k3 AND e.k2 = a.k2
AND f.k2 = c.k2 AND g.k2 = e.k2 AND h.k2 = b.k2 AND i.k1 = f.k1
AND j.k3 = g.k3 AND e.k3 = c.k3 AND a.k3 = c.k3 AND f.k1 = e.k1
AND c.k2 = f.k2 AND h.k2 = j.k2 AND j.k1 = a.k1 AND g.k3 = i.k3
AND j.k1 = f.k1 AND e.k1 = f.k1 AND g.k1 = j.k1 AND i.k3 = b.k3
AND b.k3 = j.k3 AND a.k3 < 5 AND j.k2 < 7;

-- ---------------- CASE 4: target=t7, query=q1 ----------------
SELECT 'CASE 4: target=t7, query=q1' AS case_name;
ANALYZE ms_bushy_count.t1; ANALYZE ms_bushy_count.t2; ANALYZE ms_bushy_count.t3; ANALYZE ms_bushy_count.t4; ANALYZE ms_bushy_count.t5;
ANALYZE ms_bushy_count.t6; ANALYZE ms_bushy_count.t7; ANALYZE ms_bushy_count.t8; ANALYZE ms_bushy_count.t9; ANALYZE ms_bushy_count.t10;
DROP TABLE ms_bushy_count.t7;
CREATE TABLE ms_bushy_count.t7 AS
SELECT n AS id, ((n * 18) % 2000) AS k1, ((n * 24) % 1000) AS k2, ((n * 30) % 100) AS k3 FROM ms_seq_6000;
-- POLICY OFF
EXPLAIN
SELECT count(*) FROM ms_bushy_count.t1 a, ms_bushy_count.t2 b, ms_bushy_count.t3 c, ms_bushy_count.t4 d,
ms_bushy_count.t5 e, ms_bushy_count.t6 f, ms_bushy_count.t7 g, ms_bushy_count.t8 h,
ms_bushy_count.t9 i, ms_bushy_count.t10 j
WHERE b.k2 = a.k2 AND c.k2 = a.k2 AND d.k3 = c.k3 AND e.k1 = c.k1
AND f.k2 = e.k2 AND g.k2 = e.k2 AND h.k3 = a.k3 AND i.k3 = g.k3
AND j.k3 = a.k3 AND d.k1 = e.k1 AND b.k3 = i.k3 AND f.k1 = g.k1
AND f.k2 = e.k2 AND j.k2 = a.k2 AND d.k1 = i.k1 AND a.k2 = b.k2
AND g.k1 = c.k1 AND a.k1 = d.k1 AND e.k2 = a.k2 AND c.k1 = h.k1
AND g.k3 = j.k3 AND a.k3 < 5 AND j.k2 < 7;
-- POLICY ON
EXPLAIN
SELECT count(*) FROM ms_bushy_count.t1 a, ms_bushy_count.t2 b, ms_bushy_count.t3 c, ms_bushy_count.t4 d,
ms_bushy_count.t5 e, ms_bushy_count.t6 f, ms_bushy_count.t7 g, ms_bushy_count.t8 h,
ms_bushy_count.t9 i, ms_bushy_count.t10 j
WHERE b.k2 = a.k2 AND c.k2 = a.k2 AND d.k3 = c.k3 AND e.k1 = c.k1
AND f.k2 = e.k2 AND g.k2 = e.k2 AND h.k3 = a.k3 AND i.k3 = g.k3
AND j.k3 = a.k3 AND d.k1 = e.k1 AND b.k3 = i.k3 AND f.k1 = g.k1
AND f.k2 = e.k2 AND j.k2 = a.k2 AND d.k1 = i.k1 AND a.k2 = b.k2
AND g.k1 = c.k1 AND a.k1 = d.k1 AND e.k2 = a.k2 AND c.k1 = h.k1
AND g.k3 = j.k3 AND a.k3 < 5 AND j.k2 < 7;

-- ---------------- CASE 5: target=t8, query=q1 ----------------
SELECT 'CASE 5: target=t8, query=q1' AS case_name;
ANALYZE ms_bushy_count.t1; ANALYZE ms_bushy_count.t2; ANALYZE ms_bushy_count.t3; ANALYZE ms_bushy_count.t4; ANALYZE ms_bushy_count.t5;
ANALYZE ms_bushy_count.t6; ANALYZE ms_bushy_count.t7; ANALYZE ms_bushy_count.t8; ANALYZE ms_bushy_count.t9; ANALYZE ms_bushy_count.t10;
DROP TABLE ms_bushy_count.t8;
CREATE TABLE ms_bushy_count.t8 AS
SELECT n AS id, ((n * 19) % 2000) AS k1, ((n * 25) % 1000) AS k2, ((n * 31) % 100) AS k3 FROM ms_seq_6000;
-- POLICY OFF
EXPLAIN
SELECT count(*) FROM ms_bushy_count.t1 a, ms_bushy_count.t2 b, ms_bushy_count.t3 c, ms_bushy_count.t4 d,
ms_bushy_count.t5 e, ms_bushy_count.t6 f, ms_bushy_count.t7 g, ms_bushy_count.t8 h,
ms_bushy_count.t9 i, ms_bushy_count.t10 j
WHERE b.k2 = a.k2 AND c.k2 = a.k2 AND d.k3 = c.k3 AND e.k1 = c.k1
AND f.k2 = e.k2 AND g.k2 = e.k2 AND h.k3 = a.k3 AND i.k3 = g.k3
AND j.k3 = a.k3 AND d.k1 = e.k1 AND b.k3 = i.k3 AND f.k1 = g.k1
AND f.k2 = e.k2 AND j.k2 = a.k2 AND d.k1 = i.k1 AND a.k2 = b.k2
AND g.k1 = c.k1 AND a.k1 = d.k1 AND e.k2 = a.k2 AND c.k1 = h.k1
AND g.k3 = j.k3 AND a.k3 < 5 AND j.k2 < 7;
-- POLICY ON
EXPLAIN
SELECT count(*) FROM ms_bushy_count.t1 a, ms_bushy_count.t2 b, ms_bushy_count.t3 c, ms_bushy_count.t4 d,
ms_bushy_count.t5 e, ms_bushy_count.t6 f, ms_bushy_count.t7 g, ms_bushy_count.t8 h,
ms_bushy_count.t9 i, ms_bushy_count.t10 j
WHERE b.k2 = a.k2 AND c.k2 = a.k2 AND d.k3 = c.k3 AND e.k1 = c.k1
AND f.k2 = e.k2 AND g.k2 = e.k2 AND h.k3 = a.k3 AND i.k3 = g.k3
AND j.k3 = a.k3 AND d.k1 = e.k1 AND b.k3 = i.k3 AND f.k1 = g.k1
AND f.k2 = e.k2 AND j.k2 = a.k2 AND d.k1 = i.k1 AND a.k2 = b.k2
AND g.k1 = c.k1 AND a.k1 = d.k1 AND e.k2 = a.k2 AND c.k1 = h.k1
AND g.k3 = j.k3 AND a.k3 < 5 AND j.k2 < 7;

-- ---------------- CASE 6: target=t8, query=q4 ----------------
SELECT 'CASE 6: target=t8, query=q4' AS case_name;
ANALYZE ms_bushy_count.t1; ANALYZE ms_bushy_count.t2; ANALYZE ms_bushy_count.t3; ANALYZE ms_bushy_count.t4; ANALYZE ms_bushy_count.t5;
ANALYZE ms_bushy_count.t6; ANALYZE ms_bushy_count.t7; ANALYZE ms_bushy_count.t8; ANALYZE ms_bushy_count.t9; ANALYZE ms_bushy_count.t10;
DROP TABLE ms_bushy_count.t8;
CREATE TABLE ms_bushy_count.t8 AS
SELECT n AS id, ((n * 19) % 2000) AS k1, ((n * 25) % 1000) AS k2, ((n * 31) % 100) AS k3 FROM ms_seq_6000;
-- POLICY OFF
EXPLAIN
SELECT count(*) FROM ms_bushy_count.t1 a, ms_bushy_count.t2 b, ms_bushy_count.t3 c, ms_bushy_count.t4 d,
ms_bushy_count.t5 e, ms_bushy_count.t6 f, ms_bushy_count.t7 g, ms_bushy_count.t8 h,
ms_bushy_count.t9 i, ms_bushy_count.t10 j
WHERE b.k1 = a.k1 AND c.k3 = a.k3 AND d.k3 = b.k3 AND e.k2 = a.k2
AND f.k2 = c.k2 AND g.k2 = e.k2 AND h.k2 = b.k2 AND i.k1 = f.k1
AND j.k3 = g.k3 AND e.k3 = c.k3 AND a.k3 = c.k3 AND f.k1 = e.k1
AND c.k2 = f.k2 AND h.k2 = j.k2 AND j.k1 = a.k1 AND g.k3 = i.k3
AND j.k1 = f.k1 AND e.k1 = f.k1 AND g.k1 = j.k1 AND i.k3 = b.k3
AND b.k3 = j.k3 AND a.k3 < 5 AND j.k2 < 7;
-- POLICY ON
EXPLAIN
SELECT count(*) FROM ms_bushy_count.t1 a, ms_bushy_count.t2 b, ms_bushy_count.t3 c, ms_bushy_count.t4 d,
ms_bushy_count.t5 e, ms_bushy_count.t6 f, ms_bushy_count.t7 g, ms_bushy_count.t8 h,
ms_bushy_count.t9 i, ms_bushy_count.t10 j
WHERE b.k1 = a.k1 AND c.k3 = a.k3 AND d.k3 = b.k3 AND e.k2 = a.k2
AND f.k2 = c.k2 AND g.k2 = e.k2 AND h.k2 = b.k2 AND i.k1 = f.k1
AND j.k3 = g.k3 AND e.k3 = c.k3 AND a.k3 = c.k3 AND f.k1 = e.k1
AND c.k2 = f.k2 AND h.k2 = j.k2 AND j.k1 = a.k1 AND g.k3 = i.k3
AND j.k1 = f.k1 AND e.k1 = f.k1 AND g.k1 = j.k1 AND i.k3 = b.k3
AND b.k3 = j.k3 AND a.k3 < 5 AND j.k2 < 7;

-- ---------------- CASE 7: target=t9, query=q1 ----------------
SELECT 'CASE 7: target=t9, query=q1' AS case_name;
ANALYZE ms_bushy_count.t1; ANALYZE ms_bushy_count.t2; ANALYZE ms_bushy_count.t3; ANALYZE ms_bushy_count.t4; ANALYZE ms_bushy_count.t5;
ANALYZE ms_bushy_count.t6; ANALYZE ms_bushy_count.t7; ANALYZE ms_bushy_count.t8; ANALYZE ms_bushy_count.t9; ANALYZE ms_bushy_count.t10;
DROP TABLE ms_bushy_count.t9;
CREATE TABLE ms_bushy_count.t9 AS
SELECT n AS id, ((n * 20) % 2000) AS k1, ((n * 26) % 1000) AS k2, ((n * 32) % 100) AS k3 FROM ms_seq_6000;
-- POLICY OFF
EXPLAIN
SELECT count(*) FROM ms_bushy_count.t1 a, ms_bushy_count.t2 b, ms_bushy_count.t3 c, ms_bushy_count.t4 d,
ms_bushy_count.t5 e, ms_bushy_count.t6 f, ms_bushy_count.t7 g, ms_bushy_count.t8 h,
ms_bushy_count.t9 i, ms_bushy_count.t10 j
WHERE b.k2 = a.k2 AND c.k2 = a.k2 AND d.k3 = c.k3 AND e.k1 = c.k1
AND f.k2 = e.k2 AND g.k2 = e.k2 AND h.k3 = a.k3 AND i.k3 = g.k3
AND j.k3 = a.k3 AND d.k1 = e.k1 AND b.k3 = i.k3 AND f.k1 = g.k1
AND f.k2 = e.k2 AND j.k2 = a.k2 AND d.k1 = i.k1 AND a.k2 = b.k2
AND g.k1 = c.k1 AND a.k1 = d.k1 AND e.k2 = a.k2 AND c.k1 = h.k1
AND g.k3 = j.k3 AND a.k3 < 5 AND j.k2 < 7;
-- POLICY ON
EXPLAIN
SELECT count(*) FROM ms_bushy_count.t1 a, ms_bushy_count.t2 b, ms_bushy_count.t3 c, ms_bushy_count.t4 d,
ms_bushy_count.t5 e, ms_bushy_count.t6 f, ms_bushy_count.t7 g, ms_bushy_count.t8 h,
ms_bushy_count.t9 i, ms_bushy_count.t10 j
WHERE b.k2 = a.k2 AND c.k2 = a.k2 AND d.k3 = c.k3 AND e.k1 = c.k1
AND f.k2 = e.k2 AND g.k2 = e.k2 AND h.k3 = a.k3 AND i.k3 = g.k3
AND j.k3 = a.k3 AND d.k1 = e.k1 AND b.k3 = i.k3 AND f.k1 = g.k1
AND f.k2 = e.k2 AND j.k2 = a.k2 AND d.k1 = i.k1 AND a.k2 = b.k2
AND g.k1 = c.k1 AND a.k1 = d.k1 AND e.k2 = a.k2 AND c.k1 = h.k1
AND g.k3 = j.k3 AND a.k3 < 5 AND j.k2 < 7;

-- ---------------- CASE 8: target=t9, query=q4 ----------------
SELECT 'CASE 8: target=t9, query=q4' AS case_name;
ANALYZE ms_bushy_count.t1; ANALYZE ms_bushy_count.t2; ANALYZE ms_bushy_count.t3; ANALYZE ms_bushy_count.t4; ANALYZE ms_bushy_count.t5;
ANALYZE ms_bushy_count.t6; ANALYZE ms_bushy_count.t7; ANALYZE ms_bushy_count.t8; ANALYZE ms_bushy_count.t9; ANALYZE ms_bushy_count.t10;
DROP TABLE ms_bushy_count.t9;
CREATE TABLE ms_bushy_count.t9 AS
SELECT n AS id, ((n * 20) % 2000) AS k1, ((n * 26) % 1000) AS k2, ((n * 32) % 100) AS k3 FROM ms_seq_6000;
-- POLICY OFF
EXPLAIN
SELECT count(*) FROM ms_bushy_count.t1 a, ms_bushy_count.t2 b, ms_bushy_count.t3 c, ms_bushy_count.t4 d,
ms_bushy_count.t5 e, ms_bushy_count.t6 f, ms_bushy_count.t7 g, ms_bushy_count.t8 h,
ms_bushy_count.t9 i, ms_bushy_count.t10 j
WHERE b.k1 = a.k1 AND c.k3 = a.k3 AND d.k3 = b.k3 AND e.k2 = a.k2
AND f.k2 = c.k2 AND g.k2 = e.k2 AND h.k2 = b.k2 AND i.k1 = f.k1
AND j.k3 = g.k3 AND e.k3 = c.k3 AND a.k3 = c.k3 AND f.k1 = e.k1
AND c.k2 = f.k2 AND h.k2 = j.k2 AND j.k1 = a.k1 AND g.k3 = i.k3
AND j.k1 = f.k1 AND e.k1 = f.k1 AND g.k1 = j.k1 AND i.k3 = b.k3
AND b.k3 = j.k3 AND a.k3 < 5 AND j.k2 < 7;
-- POLICY ON
EXPLAIN
SELECT count(*) FROM ms_bushy_count.t1 a, ms_bushy_count.t2 b, ms_bushy_count.t3 c, ms_bushy_count.t4 d,
ms_bushy_count.t5 e, ms_bushy_count.t6 f, ms_bushy_count.t7 g, ms_bushy_count.t8 h,
ms_bushy_count.t9 i, ms_bushy_count.t10 j
WHERE b.k1 = a.k1 AND c.k3 = a.k3 AND d.k3 = b.k3 AND e.k2 = a.k2
AND f.k2 = c.k2 AND g.k2 = e.k2 AND h.k2 = b.k2 AND i.k1 = f.k1
AND j.k3 = g.k3 AND e.k3 = c.k3 AND a.k3 = c.k3 AND f.k1 = e.k1
AND c.k2 = f.k2 AND h.k2 = j.k2 AND j.k1 = a.k1 AND g.k3 = i.k3
AND j.k1 = f.k1 AND e.k1 = f.k1 AND g.k1 = j.k1 AND i.k3 = b.k3
AND b.k3 = j.k3 AND a.k3 < 5 AND j.k2 < 7;

-- ---------------- CASE 9: target=t10, query=q3 ----------------
SELECT 'CASE 9: target=t10, query=q3' AS case_name;
ANALYZE ms_bushy_count.t1; ANALYZE ms_bushy_count.t2; ANALYZE ms_bushy_count.t3; ANALYZE ms_bushy_count.t4; ANALYZE ms_bushy_count.t5;
ANALYZE ms_bushy_count.t6; ANALYZE ms_bushy_count.t7; ANALYZE ms_bushy_count.t8; ANALYZE ms_bushy_count.t9; ANALYZE ms_bushy_count.t10;
DROP TABLE ms_bushy_count.t10;
CREATE TABLE ms_bushy_count.t10 AS
SELECT n AS id, ((n * 21) % 2000) AS k1, ((n * 27) % 1000) AS k2, ((n * 33) % 100) AS k3 FROM ms_seq_6000;
-- POLICY OFF
EXPLAIN
SELECT count(*) FROM ms_bushy_count.t1 a, ms_bushy_count.t2 b, ms_bushy_count.t3 c, ms_bushy_count.t4 d,
ms_bushy_count.t5 e, ms_bushy_count.t6 f, ms_bushy_count.t7 g, ms_bushy_count.t8 h,
ms_bushy_count.t9 i, ms_bushy_count.t10 j
WHERE b.k3 = a.k3 AND c.k2 = a.k2 AND d.k3 = b.k3 AND e.k2 = d.k2
AND f.k3 = c.k3 AND g.k2 = e.k2 AND h.k2 = b.k2 AND i.k3 = h.k3
AND j.k2 = d.k2 AND g.k2 = c.k2 AND a.k3 = g.k3 AND j.k1 = a.k1
AND j.k1 = f.k1 AND i.k2 = b.k2 AND e.k1 = i.k1 AND i.k1 = c.k1
AND a.k3 = f.k3 AND c.k2 = h.k2 AND d.k3 = j.k3 AND b.k2 = a.k2
AND f.k1 = a.k1 AND a.k3 < 5 AND j.k2 < 7;
-- POLICY ON
EXPLAIN
SELECT count(*) FROM ms_bushy_count.t1 a, ms_bushy_count.t2 b, ms_bushy_count.t3 c, ms_bushy_count.t4 d,
ms_bushy_count.t5 e, ms_bushy_count.t6 f, ms_bushy_count.t7 g, ms_bushy_count.t8 h,
ms_bushy_count.t9 i, ms_bushy_count.t10 j
WHERE b.k3 = a.k3 AND c.k2 = a.k2 AND d.k3 = b.k3 AND e.k2 = d.k2
AND f.k3 = c.k3 AND g.k2 = e.k2 AND h.k2 = b.k2 AND i.k3 = h.k3
AND j.k2 = d.k2 AND g.k2 = c.k2 AND a.k3 = g.k3 AND j.k1 = a.k1
AND j.k1 = f.k1 AND i.k2 = b.k2 AND e.k1 = i.k1 AND i.k1 = c.k1
AND a.k3 = f.k3 AND c.k2 = h.k2 AND d.k3 = j.k3 AND b.k2 = a.k2
AND f.k1 = a.k1 AND a.k3 < 5 AND j.k2 < 7;

-- ============================================================================
-- Interpretation guide (all engines)
-- ----------------------------------------------------------------------------
-- Correct policy behavior per case:
--   OFF plan: target table may appear inside a bushy subtree.
--   ON  plan: no bushy subtree should include the target table.
--   ON  plan may still have bushy joins among fully analyzed tables.
--
-- For strict automated verification on patched PostgreSQL:
--   scripts/verify-pruned-cases-no-target-bushy.sh
-- ============================================================================
