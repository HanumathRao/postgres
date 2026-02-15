#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/test-left-deep.sh [options]

Options:
  --port N            Server port (default: 55432)
  --data-dir PATH     Data directory (default: /tmp/pg_leftdeep)
  --log-file PATH     Log file (default: /tmp/pg_leftdeep.log)
  --off-plan PATH     Output file for plan with GUC off (default: /tmp/plan_off.json)
  --on-plan PATH      Output file for plan with GUC on  (default: /tmp/plan_on.json)
  --rows N            Rows per table for test data (default: 50000)
  --skip-build        Skip `meson compile -C build`
  --keep-running      Do not stop the server at script end
  --help              Show this help
EOF
}

PORT=55432
DATA_DIR=/tmp/pg_leftdeep
LOG_FILE=/tmp/pg_leftdeep.log
OFF_PLAN=/tmp/plan_off.json
ON_PLAN=/tmp/plan_on.json
ROWS=50000
SKIP_BUILD=0
KEEP_RUNNING=0

while (($# > 0)); do
  case "$1" in
    --port)
      PORT="$2"
      shift 2
      ;;
    --data-dir)
      DATA_DIR="$2"
      shift 2
      ;;
    --log-file)
      LOG_FILE="$2"
      shift 2
      ;;
    --off-plan)
      OFF_PLAN="$2"
      shift 2
      ;;
    --on-plan)
      ON_PLAN="$2"
      shift 2
      ;;
    --rows)
      ROWS="$2"
      shift 2
      ;;
    --skip-build)
      SKIP_BUILD=1
      shift
      ;;
    --keep-running)
      KEEP_RUNNING=1
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

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

echo "[3/7] Starting patched server on port ${PORT}..."
"$PGCTL_BIN" -D "${DATA_DIR}" -p "${POSTGRES_BIN}" -m fast stop >/dev/null 2>&1 || true
"$PGCTL_BIN" -D "${DATA_DIR}" -l "${LOG_FILE}" -o "-p ${PORT} -c jit=off" -p "${POSTGRES_BIN}" -w start >/dev/null

cleanup() {
  if [[ "$KEEP_RUNNING" -eq 0 ]]; then
    echo "[7/7] Stopping server..."
    "$PGCTL_BIN" -D "${DATA_DIR}" -p "${POSTGRES_BIN}" -m fast stop >/dev/null || true
  else
    echo "[7/7] Leaving server running on port ${PORT}"
  fi
}
trap cleanup EXIT

echo "[4/7] Verifying custom GUC exists..."
"$PSQL_BIN" -XqAt -p "${PORT}" -d postgres -c "SHOW enable_left_deep_join;" >/dev/null

echo "[5/7] Preparing test data (rows per table: ${ROWS})..."
"$PSQL_BIN" -Xq -p "${PORT}" -d postgres <<SQL
DROP TABLE IF EXISTS t1,t2,t3,t4,t5,t6;
CREATE TABLE t1 AS SELECT g AS id, (g % 2000) AS k FROM generate_series(1, ${ROWS}) g;
CREATE TABLE t2 AS SELECT g AS id, (g % 2000) AS k FROM generate_series(1, ${ROWS}) g;
CREATE TABLE t3 AS SELECT g AS id, (g % 2000) AS k FROM generate_series(1, ${ROWS}) g;
CREATE TABLE t4 AS SELECT g AS id, (g % 2000) AS k FROM generate_series(1, ${ROWS}) g;
CREATE TABLE t5 AS SELECT g AS id, (g % 2000) AS k FROM generate_series(1, ${ROWS}) g;
CREATE TABLE t6 AS SELECT g AS id, (g % 2000) AS k FROM generate_series(1, ${ROWS}) g;
ANALYZE;
SQL

QUERY=$(cat <<'SQL'
SELECT count(*)
FROM t1
JOIN t2 ON t1.k = t2.k
JOIN t3 ON t1.k = t3.k
JOIN t5 ON t2.k = t5.k
JOIN t4 ON t4.k = t5.k
JOIN t6 ON t5.k = t6.k AND t4.k = t6.k
SQL
)

echo "[6/7] Capturing plans..."
PGOPTIONS="-c jit=off -c geqo=off -c join_collapse_limit=20 -c from_collapse_limit=20 -c enable_left_deep_join=off" \
  "$PSQL_BIN" -XqAt -p "${PORT}" -d postgres -c "EXPLAIN (FORMAT JSON) ${QUERY}" >"${OFF_PLAN}"

PGOPTIONS="-c jit=off -c geqo=off -c join_collapse_limit=20 -c from_collapse_limit=20 -c enable_left_deep_join=on" \
  "$PSQL_BIN" -XqAt -p "${PORT}" -d postgres -c "EXPLAIN (FORMAT JSON) ${QUERY}" >"${ON_PLAN}"

check_left_deep() {
  local file="$1"
  perl -MJSON::PP -e '
my $txt = do { local $/; <> };
my $j = decode_json($txt);

sub is_join {
  my ($t) = @_;
  return $t =~ /^(Hash Join|Merge Join|Nested Loop)$/;
}

sub bad_shape {
  my ($n) = @_;
  return 0 unless ref($n) eq "HASH";
  my $plans = $n->{Plans} || [];

  if (is_join($n->{"Node Type"} // "") && @$plans >= 2) {
    my $right = $plans->[1];
    return 1 if ref($right) eq "HASH" && is_join($right->{"Node Type"} // "");
  }

  for my $c (@$plans) {
    return 1 if bad_shape($c);
  }
  return 0;
}

exit bad_shape($j->[0]{Plan}) ? 1 : 0;
' "$file"
}

if check_left_deep "${OFF_PLAN}"; then
  echo "OFF plan: left-deep"
else
  echo "OFF plan: NOT left-deep"
fi

if check_left_deep "${ON_PLAN}"; then
  echo "ON plan: left-deep"
else
  echo "ON plan: NOT left-deep"
fi

echo "Saved:"
echo "  off: ${OFF_PLAN}"
echo "   on: ${ON_PLAN}"
