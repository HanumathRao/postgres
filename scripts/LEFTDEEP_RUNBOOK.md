# Left-Deep / Missing-Stats Runbook

This file captures the scripts used in this session, what each one proves, and how to restore context quickly.

## Scope

Planner policy explored:

- Global: `enable_left_deep_join`
- Conditional: `enable_left_deep_join_on_missing_stats`
- Debug counters: `debug_left_deep_stats`

Target behavior for conditional policy:

- Missing-stats table must **not** be part of a bushy subtree.
- Bushy joins among analyzed tables are still allowed.

---

## Start / Stop

- Start server (patched build):
```bash
build/src/bin/pg_ctl/pg_ctl \
  -D /tmp/pg_leftdeep \
  -l /tmp/pg_leftdeep.log \
  -o "-p 55432 -c jit=off" \
  -p /home/hmaduri/contribs/postgres/build/src/backend/postgres \
  -w start
```

- Stop server:
```bash
build/src/bin/pg_ctl/pg_ctl \
  -D /tmp/pg_leftdeep \
  -p /home/hmaduri/contribs/postgres/build/src/backend/postgres \
  -m fast stop
```

---

## Scripts Tried

### 1) `scripts/test-left-deep.sh`

- Purpose: basic OFF vs ON left-deep check with synthetic data.
- Example:
```bash
scripts/test-left-deep.sh --rows 10000 --skip-build
```

### 2) `scripts/find-left-deep-proof.sh`

- Purpose: search for query where OFF is bushy and ON is left-deep.
- Example:
```bash
scripts/find-left-deep-proof.sh --workload auto --max-tries 1000 --rows 60000 --extra-edges 10 --skip-build
```

### 3) `scripts/find-missing-stats-proof.sh`

- Purpose: search for query where missing-stats policy changes plan shape.
- Supports:
  - `--workload auto|tpch-like|random`
  - `--tpch-complexity 1..3`
  - `--tpch-missing-tables a,b,c`
- Example:
```bash
scripts/find-missing-stats-proof.sh \
  --skip-build \
  --plan-only \
  --workload tpch-like \
  --tpch-complexity 3 \
  --tpch-missing-tables lineitem,orders,partsupp \
  --rows 20000 \
  --max-tries 6000 \
  --statement-timeout-ms 2500
```

### 4) `scripts/generate-leftdeep-repro-sql.sh`

- Purpose: generate standalone SQL from a found left-deep proof query.

### 5) `scripts/generate-missing-stats-repro-sql.sh`

- Purpose: generate standalone SQL from a missing-stats proof query.

### 6) `scripts/check-leftdeep-completeness.sh`

- Purpose: completeness/debug harness for left-deep search checks.

### 7) `scripts/check-leftdeep-varied-queries.sh`

- Purpose: varied SQL suite (inner/outer/subquery/derived) with ON/OFF compare.

### 8) `scripts/check-leftdeep-hashbuild-policy.sh`

- Purpose: policy check for “no intermediate hash-build” style constraints.

### 9) `scripts/visualize-plan-failures-html.sh`

- Purpose: HTML report for failing plans.
- Example:
```bash
scripts/visualize-plan-failures-html.sh \
  --plans-dir /tmp/leftdeep_policy_plans \
  --check no-intermediate-hash-build \
  --mode off \
  --output /tmp/leftdeep_policy_failures.html
```

### 10) `scripts/plan-json-ascii.sh`

- Purpose: quick ASCII tree diff from plan JSON files.
- Example:
```bash
scripts/plan-json-ascii.sh /tmp/plan_off.json /tmp/plan_on.json
```

### 11) `scripts/target-missing-stats-from-bushy.sh`

- Purpose: find bushy with all stats, pick a right-subtree table, then retest OFF/ON with that table missing.
- Example:
```bash
scripts/target-missing-stats-from-bushy.sh --skip-build --rows 15000 --max-tries 2000
```

### 12) `scripts/verify-pruned-cases-no-target-bushy.sh`

- Purpose: strict validator for known pruned cases.
- Assertion:
  - OFF: target is in bushy subtree
  - ON: target is not in bushy subtree
- Example:
```bash
scripts/verify-pruned-cases-no-target-bushy.sh
```

### 13) `scripts/run-postgres.sh`

- Purpose: convenience server launcher.

---

## SQL Files Added

### `scripts/missing-stats-pruned-cases-explain.sql`

- Self-contained PostgreSQL script for 9 validated cases.
- Uses plain `EXPLAIN` (text).

### `scripts/missing-stats-pruned-cases-portable.sql`

- Portable-style SQL with case descriptions and expected behavior notes.
- Uses plain `EXPLAIN` (text).
- Contains comments showing where to flip OFF/ON policy per engine.

---

## Known Artifact Paths

- Example proof query:
  - `/tmp/missing_stats_proof.sql`
- OFF/ON proof plans:
  - `/tmp/ms_plan_off_proof.json`
  - `/tmp/ms_plan_on_proof.json`
- Matrix summary:
  - `/tmp/ms_bushy_matrix_v2.tsv`
- Strict verifier summary:
  - `/tmp/ms_pruned_verify_summary.tsv`
- Strict verifier plans:
  - `/tmp/ms_pruned_verify_plans`

---

## Important Fix Applied

Perl checker exit precedence bug fixed in these scripts:

- `scripts/find-missing-stats-proof.sh`
- `scripts/target-missing-stats-from-bushy.sh`
- `scripts/verify-pruned-cases-no-target-bushy.sh`

Pattern fixed:

- from: `exit has_x(...) ? 0 : 1;`
- to: `exit(has_x(...) ? 0 : 1);`

---

## One-Command Validation

```bash
scripts/verify-pruned-cases-no-target-bushy.sh
```

Expected:

- `pass=9 fail=0 inconclusive=0`

---

## Context Restore Prompt (copy/paste)

```text
You are continuing work in /home/hmaduri/contribs/postgres on planner policy:
- enable_left_deep_join_on_missing_stats should prevent missing-stats tables from participating in bushy subtrees.
- Bushy joins among analyzed tables are allowed.

Please load and follow scripts/LEFTDEEP_RUNBOOK.md.

Current key files:
- src/backend/optimizer/path/joinrels.c
- src/backend/optimizer/path/allpaths.c
- src/backend/utils/misc/guc_parameters.dat
- src/include/optimizer/paths.h
- scripts/find-missing-stats-proof.sh
- scripts/target-missing-stats-from-bushy.sh
- scripts/verify-pruned-cases-no-target-bushy.sh

First actions:
1) Ensure server is running on port 55432 from build tree.
2) Run scripts/verify-pruned-cases-no-target-bushy.sh.
3) If failures appear, report failing case IDs and propose minimal fix.
```

