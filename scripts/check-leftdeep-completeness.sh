#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/check-leftdeep-completeness.sh [options]

Runs a completeness check for left-deep join enumeration on fully connected
inner-join queries and validates planner debug stats emitted by:
  SET debug_left_deep_stats = on

Checks:
  1) bushy_make_calls == 0
  2) for completeness=on lines: actual == expected (C(n,k))

Options:
  --port N            Server port (default: 55432)
  --data-dir PATH     Data directory (default: /tmp/pg_leftdeep)
  --log-file PATH     Log file (default: /tmp/pg_leftdeep_completeness.log)
  --rows N            Rows per table (default: 4000)
  --min-n N           Smallest join size n (default: 3)
  --max-n N           Largest join size n (default: 8)
  --skip-build        Skip `meson compile -C build`
  --keep-running      Do not stop server at end
  --help              Show help
EOF
}

PORT=55432
DATA_DIR=/tmp/pg_leftdeep
LOG_FILE=/tmp/pg_leftdeep_completeness.log
ROWS=4000
MIN_N=3
MAX_N=8
SKIP_BUILD=0
KEEP_RUNNING=0

while (($# > 0)); do
  case "$1" in
    --port) PORT="$2"; shift 2 ;;
    --data-dir) DATA_DIR="$2"; shift 2 ;;
    --log-file) LOG_FILE="$2"; shift 2 ;;
    --rows) ROWS="$2"; shift 2 ;;
    --min-n) MIN_N="$2"; shift 2 ;;
    --max-n) MAX_N="$2"; shift 2 ;;
    --skip-build) SKIP_BUILD=1; shift ;;
    --keep-running) KEEP_RUNNING=1; shift ;;
    --help|-h) usage; exit 0 ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if (( MIN_N < 2 || MAX_N < MIN_N )); then
  echo "Invalid n range: min=${MIN_N} max=${MAX_N}" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BUILD_DIR="${REPO_ROOT}/build"

POSTGRES_BIN="${BUILD_DIR}/src/backend/postgres"
PSQL_BIN="${BUILD_DIR}/src/bin/psql/psql"
PGCTL_BIN="${BUILD_DIR}/src/bin/pg_ctl/pg_ctl"
INITDB_BIN="${BUILD_DIR}/src/bin/initdb/initdb"

for bin in "$POSTGRES_BIN" "$PSQL_BIN" "$PGCTL_BIN" "$INITDB_BIN"; do
  if [[ ! -x "$bin" ]]; then
    echo "Missing binary: $bin" >&2
    echo "Build first with: CCACHE_DISABLE=1 meson compile -C ${BUILD_DIR}" >&2
    exit 1
  fi
done

if [[ "$SKIP_BUILD" -eq 0 ]]; then
  echo "[1/7] Compiling build tree..."
  (
    cd "$REPO_ROOT"
    CCACHE_DISABLE=1 meson compile -C "$BUILD_DIR"
  )
fi

if [[ ! -s "${DATA_DIR}/PG_VERSION" ]]; then
  echo "[2/7] Initializing data directory ${DATA_DIR}..."
  rm -rf "${DATA_DIR}"
  "$INITDB_BIN" -D "${DATA_DIR}" >/dev/null
else
  echo "[2/7] Reusing existing data directory ${DATA_DIR}"
fi

echo "[3/7] Starting patched server..."
rm -f "${LOG_FILE}"
"$PGCTL_BIN" -D "${DATA_DIR}" -p "${POSTGRES_BIN}" -m fast stop >/dev/null 2>&1 || true
"$PGCTL_BIN" -D "${DATA_DIR}" -l "${LOG_FILE}" \
  -o "-p ${PORT} -c jit=off -c log_min_messages=log" \
  -p "${POSTGRES_BIN}" -w start >/dev/null

cleanup() {
  if [[ "$KEEP_RUNNING" -eq 0 ]]; then
    echo "[cleanup] Stopping server..."
    "$PGCTL_BIN" -D "${DATA_DIR}" -p "${POSTGRES_BIN}" -m fast stop >/dev/null || true
  else
    echo "[cleanup] Leaving server running on port ${PORT}"
  fi
}
trap cleanup EXIT

echo "[4/7] Verifying required debug GUCs..."
if ! "$PSQL_BIN" -XqAt -p "${PORT}" -d postgres \
  -c "SHOW enable_left_deep_join;" >/dev/null; then
  echo "FAIL: enable_left_deep_join is missing in running server." >&2
  echo "Rebuild and relink postgres, then rerun without --skip-build." >&2
  exit 1
fi

if ! "$PSQL_BIN" -XqAt -p "${PORT}" -d postgres \
  -c "SHOW debug_left_deep_stats;" >/dev/null; then
  echo "FAIL: debug_left_deep_stats is missing in running server." >&2
  echo "Rebuild and relink postgres, then rerun without --skip-build." >&2
  exit 1
fi

echo "[5/7] Preparing connected-inner-join test schema..."
"$PSQL_BIN" -Xq -v ON_ERROR_STOP=1 -p "${PORT}" -d postgres <<SQL
DROP SCHEMA IF EXISTS ld_complete CASCADE;
CREATE SCHEMA ld_complete;
SQL

for ((i = 1; i <= MAX_N; i++)); do
  "$PSQL_BIN" -Xq -v ON_ERROR_STOP=1 -p "${PORT}" -d postgres <<SQL
CREATE TABLE ld_complete.t${i} AS
SELECT g AS id, (g % 100) AS k
FROM generate_series(1, ${ROWS}) g;
ANALYZE ld_complete.t${i};
SQL
done

echo "[6/7] Running EXPLAINs with debug_left_deep_stats=on..."
for ((n = MIN_N; n <= MAX_N; n++)); do
  from_clause=""
  where_clause=""

  for ((i = 1; i <= n; i++)); do
    from_clause+="ld_complete.t${i} a${i}"
    if (( i < n )); then
      from_clause+=", "
    fi
  done

  first=1
  for ((i = 1; i <= n; i++)); do
    for ((j = i + 1; j <= n; j++)); do
      cond="a${i}.k = a${j}.k"
      if (( first == 1 )); then
        where_clause="${cond}"
        first=0
      else
        where_clause+=" AND ${cond}"
      fi
    done
  done

  query="SELECT count(*) FROM ${from_clause} WHERE ${where_clause}"

  PGOPTIONS="-c geqo=off -c join_collapse_limit=20 -c from_collapse_limit=20 -c enable_nestloop=off -c enable_left_deep_join=on -c debug_left_deep_stats=on" \
    "$PSQL_BIN" -Xq -v ON_ERROR_STOP=1 -p "${PORT}" -d postgres \
    -c "EXPLAIN (COSTS OFF) ${query}" >/dev/null
done

echo "[7/7] Validating leftdeep_stats from ${LOG_FILE}..."
awk '
/leftdeep_stats level=/ {
  seen = 1
  level = actual = expected = comp = bushy = ""
  for (i = 1; i <= NF; i++) {
    split($i, kv, "=")
    if (kv[1] == "level") level = kv[2]
    else if (kv[1] == "actual") actual = kv[2]
    else if (kv[1] == "expected") expected = kv[2]
    else if (kv[1] == "completeness") comp = kv[2]
    else if (kv[1] == "bushy_make_calls") bushy = kv[2]
  }

  if (bushy != "" && bushy + 0 != 0) {
    printf("FAIL: bushy_make_calls=%s at level=%s\n", bushy, level)
    fail = 1
  }

  if (comp == "on" && actual != "" && expected != "" && (actual + 0) != (expected + 0)) {
    printf("FAIL: completeness mismatch at level=%s actual=%s expected=%s\n", level, actual, expected)
    fail = 1
  }
}
END {
  if (!seen) {
    print "FAIL: no leftdeep_stats lines found in log"
    exit 2
  }
  if (fail)
    exit 1
  print "PASS: no bushy joins generated and completeness counts matched."
}
' "${LOG_FILE}"
