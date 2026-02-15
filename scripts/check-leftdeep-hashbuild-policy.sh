#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/check-leftdeep-hashbuild-policy.sh [options]

Validates the policy:
  1) enable_left_deep_join=on: NO hash join may build hash table from an
     intermediate joinrel (join/subquery result)
  2) enable_left_deep_join=off: should violate (1) for at least N queries
     to prove the change is observable
  3) query scalar results must match between off vs on

Options:
  --port N                      Server port (default: 55432)
  --data-dir PATH               Data directory (default: /tmp/pg_leftdeep)
  --log-file PATH               Log file (default: /tmp/pg_leftdeep_policy.log)
  --rows N                      Rows per table (default: 6000)
  --plans-dir PATH              Plan output dir (default: /tmp/leftdeep_policy_plans)
  --expect-off-bad-at-least N   Minimum off-mode violations required (default: 1)
  --skip-build                  Skip `meson compile -C build`
  --keep-running                Do not stop server at end
  --help                        Show help
EOF
}

PORT=55432
DATA_DIR=/tmp/pg_leftdeep
LOG_FILE=/tmp/pg_leftdeep_policy.log
ROWS=6000
PLANS_DIR=/tmp/leftdeep_policy_plans
EXPECT_OFF_BAD_AT_LEAST=1
SKIP_BUILD=0
KEEP_RUNNING=0

while (($# > 0)); do
  case "$1" in
    --port) PORT="$2"; shift 2 ;;
    --data-dir) DATA_DIR="$2"; shift 2 ;;
    --log-file) LOG_FILE="$2"; shift 2 ;;
    --rows) ROWS="$2"; shift 2 ;;
    --plans-dir) PLANS_DIR="$2"; shift 2 ;;
    --expect-off-bad-at-least) EXPECT_OFF_BAD_AT_LEAST="$2"; shift 2 ;;
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
  echo "[1/8] Compiling build tree..."
  (
    cd "$REPO_ROOT"
    CCACHE_DISABLE=1 meson compile -C "$BUILD_DIR"
  )
fi

if [[ ! -s "${DATA_DIR}/PG_VERSION" ]]; then
  echo "[2/8] Initializing data directory ${DATA_DIR}..."
  rm -rf "${DATA_DIR}"
  "$INITDB_BIN" -D "${DATA_DIR}" >/dev/null
else
  echo "[2/8] Reusing existing data directory ${DATA_DIR}"
fi

echo "[3/8] Starting patched server..."
rm -f "${LOG_FILE}"
"$PGCTL_BIN" -D "${DATA_DIR}" -p "${POSTGRES_BIN}" -m fast stop >/dev/null 2>&1 || true
"$PGCTL_BIN" -D "${DATA_DIR}" -l "${LOG_FILE}" \
  -o "-p ${PORT} -c jit=off -c log_min_messages=warning" \
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

echo "[4/8] Verifying enable_left_deep_join exists..."
"$PSQL_BIN" -XqAt -p "${PORT}" -d postgres -c "SHOW enable_left_deep_join;" >/dev/null

echo "[5/8] Preparing test schema..."
"$PSQL_BIN" -Xq -v ON_ERROR_STOP=1 -p "${PORT}" -d postgres <<SQL
DROP SCHEMA IF EXISTS ld_varied CASCADE;
CREATE SCHEMA ld_varied;
SQL

for i in {1..8}; do
  "$PSQL_BIN" -Xq -v ON_ERROR_STOP=1 -p "${PORT}" -d postgres <<SQL
CREATE TABLE ld_varied.t${i} AS
SELECT g AS id,
       CASE WHEN g % 13 = 0 THEN NULL ELSE (g % 300) END AS k1,
       CASE WHEN g % 17 = 0 THEN NULL ELSE (g % 200) END AS k2,
       (g * (7 + ${i})) % 1000 AS v
FROM generate_series(1, ${ROWS}) g;
ANALYZE ld_varied.t${i};
SQL
done

mkdir -p "${PLANS_DIR}"
rm -f "${PLANS_DIR}"/*.json

QDIR="$(mktemp -d)"
trap 'rm -rf "${QDIR}"; cleanup' EXIT

cat > "${QDIR}/q01_inner_6way.sql" <<'SQL'
SELECT count(*)
FROM ld_varied.t1 a
JOIN ld_varied.t2 b ON a.id = b.id
JOIN ld_varied.t3 c ON b.id = c.id
JOIN ld_varied.t4 d ON c.id = d.id
JOIN ld_varied.t5 e ON d.id = e.id
JOIN ld_varied.t6 f ON e.id = f.id
WHERE a.id <= 3000
SQL

cat > "${QDIR}/q02_left_outer_chain.sql" <<'SQL'
SELECT count(*)
FROM ld_varied.t1 a
LEFT JOIN ld_varied.t2 b ON a.id = b.id AND b.v % 3 = 0
LEFT JOIN ld_varied.t3 c ON b.id = c.id
LEFT JOIN ld_varied.t4 d ON c.id = d.id
WHERE a.id <= 3200
SQL

cat > "${QDIR}/q03_full_outer.sql" <<'SQL'
SELECT count(*)
FROM ld_varied.t1 a
FULL OUTER JOIN ld_varied.t2 b ON a.id = b.id
FULL OUTER JOIN ld_varied.t3 c ON COALESCE(a.id, b.id) = c.id
WHERE COALESCE(a.id, b.id, c.id) <= 2500
SQL

cat > "${QDIR}/q04_mixed_outer_inner.sql" <<'SQL'
SELECT sum(COALESCE(a.v, 0) + COALESCE(b.v, 0) + COALESCE(c.v, 0) + COALESCE(d.v, 0))
FROM ld_varied.t1 a
LEFT JOIN ld_varied.t2 b ON a.id = b.id
JOIN ld_varied.t3 c ON c.id = a.id
LEFT JOIN ld_varied.t4 d ON d.id = c.id
WHERE a.id <= 2800
SQL

cat > "${QDIR}/q05_in_subquery.sql" <<'SQL'
SELECT count(*)
FROM ld_varied.t1 a
WHERE a.id IN (
  SELECT b.id
  FROM ld_varied.t2 b
  JOIN ld_varied.t3 c ON b.id = c.id
  WHERE b.id <= 2400
)
SQL

cat > "${QDIR}/q06_exists_correlated.sql" <<'SQL'
SELECT count(*)
FROM ld_varied.t1 a
WHERE EXISTS (
  SELECT 1
  FROM ld_varied.t4 d
  WHERE d.id = a.id
    AND d.v > a.v
)
SQL

cat > "${QDIR}/q07_derived_join.sql" <<'SQL'
SELECT count(*)
FROM (
  SELECT a.id, a.v, b.v AS bv
  FROM ld_varied.t1 a
  LEFT JOIN ld_varied.t2 b ON a.id = b.id
) x
JOIN (
  SELECT c.id, c.v AS cv
  FROM ld_varied.t3 c
  JOIN ld_varied.t5 e ON c.id = e.id
) y ON x.id = y.id
WHERE x.id <= 2300
  AND COALESCE(x.bv, 0) + y.cv > 0
SQL

cat > "${QDIR}/q08_full_outer_with_subquery.sql" <<'SQL'
SELECT count(*)
FROM ld_varied.t1 a
FULL OUTER JOIN ld_varied.t2 b ON a.id = b.id
WHERE COALESCE(a.id, b.id) IN (
  SELECT f.id
  FROM ld_varied.t6 f
  WHERE f.id <= 2000
)
SQL

check_no_intermediate_hash_build() {
  local file="$1"
  perl -MJSON::PP -e '
my $txt = do { local $/; <> };
$txt =~ s/^[^\[]*//s;
my $j = decode_json($txt);

sub is_join {
  my ($t) = @_;
  return defined($t) && $t =~ /^(Hash Join|Merge Join|Nested Loop)$/;
}

sub has_bad_hash_build {
  my ($n) = @_;
  return 0 unless ref($n) eq "HASH";
  my $nt = $n->{"Node Type"} // "";
  my $plans = $n->{Plans} || [];

  if ($nt eq "Hash Join" && @$plans >= 2) {
    my $inner = $plans->[1];
    if (ref($inner) eq "HASH" && (($inner->{"Node Type"} // "") eq "Hash")) {
      my $build = ($inner->{Plans} || [])->[0];
      if (ref($build) eq "HASH" && is_join($build->{"Node Type"} // "")) {
        return 1;
      }
    }
  }

  for my $c (@$plans) {
    return 1 if has_bad_hash_build($c);
  }
  return 0;
}

exit has_bad_hash_build($j->[0]{Plan}) ? 1 : 0;
' "$file"
}

capture_plan() {
  local mode="$1"
  local query_file="$2"
  local out_file="$3"
  local opts

  if [[ "$mode" == "off" ]]; then
    opts="-c jit=off -c geqo=off -c join_collapse_limit=20 -c from_collapse_limit=20 -c enable_left_deep_join=off"
  else
    opts="-c jit=off -c geqo=off -c join_collapse_limit=20 -c from_collapse_limit=20 -c enable_left_deep_join=on"
  fi

  PGOPTIONS="${opts}" "$PSQL_BIN" -XqAt -v ON_ERROR_STOP=1 -p "${PORT}" -d postgres >"${out_file}" <<SQL
EXPLAIN (FORMAT JSON)
$(cat "${query_file}");
SQL
}

run_query_scalar() {
  local mode="$1"
  local query_file="$2"
  local opts

  if [[ "$mode" == "off" ]]; then
    opts="-c jit=off -c geqo=off -c join_collapse_limit=20 -c from_collapse_limit=20 -c enable_left_deep_join=off"
  else
    opts="-c jit=off -c geqo=off -c join_collapse_limit=20 -c from_collapse_limit=20 -c enable_left_deep_join=on"
  fi

  PGOPTIONS="${opts}" "$PSQL_BIN" -XqAt -v ON_ERROR_STOP=1 -p "${PORT}" -d postgres <<SQL
$(cat "${query_file}");
SQL
}

echo "[6/8] Running policy query suite..."
total=0
fails=0
off_bad_count=0

for qf in "${QDIR}"/q*.sql; do
  qname="$(basename "${qf}" .sql)"
  off_plan="${PLANS_DIR}/${qname}.off.json"
  on_plan="${PLANS_DIR}/${qname}.on.json"

  ((total += 1))
  echo "  - ${qname}"

  off_result="$(run_query_scalar off "${qf}")"
  on_result="$(run_query_scalar on "${qf}")"
  if [[ "${off_result}" != "${on_result}" ]]; then
    echo "    FAIL: result mismatch off=${off_result} on=${on_result}"
    ((fails += 1))
    continue
  fi

  capture_plan off "${qf}" "${off_plan}"
  capture_plan on "${qf}" "${on_plan}"

  if check_no_intermediate_hash_build "${off_plan}"; then
    off_status="clean"
  else
    off_status="violates"
    ((off_bad_count += 1))
  fi

  if check_no_intermediate_hash_build "${on_plan}"; then
    on_status="clean"
  else
    on_status="violates"
    echo "    FAIL: on-plan violates no-intermediate-hash-build (${on_plan})"
    ((fails += 1))
  fi

  echo "    status: off=${off_status} on=${on_status}"
done

if (( off_bad_count < EXPECT_OFF_BAD_AT_LEAST )); then
  echo "[7/8] FAIL: off-mode violations=${off_bad_count}, expected at least ${EXPECT_OFF_BAD_AT_LEAST}"
  ((fails += 1))
else
  echo "[7/8] PASS: off-mode violations=${off_bad_count}, expected at least ${EXPECT_OFF_BAD_AT_LEAST}"
fi

echo "[8/8] Summary: total=${total} failed=${fails} off_violations=${off_bad_count} plans_dir=${PLANS_DIR}"
if (( fails > 0 )); then
  exit 1
fi

echo "PASS: policy validated. on-mode clean and off-mode shows expected violations."
