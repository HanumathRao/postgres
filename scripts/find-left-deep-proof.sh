#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/find-left-deep-proof.sh [options]

Searches for a query where:
  - enable_left_deep_join=off produces a non-left-deep plan
  - enable_left_deep_join=on  produces a left-deep plan

Options:
  --workload MODE     auto|tpch-like|random (default: auto)
  --port N            Server port (default: 55432)
  --data-dir PATH     Data directory (default: /tmp/pg_leftdeep)
  --log-file PATH     Log file (default: /tmp/pg_leftdeep.log)
  --rows N            Rows per synthetic table (default: 30000)
  --max-tries N       Number of generated queries to try (default: 120)
  --seed N            Base seed for query generation (default: 42)
  --extra-edges N     Extra random join edges beyond spanning tree (default: 7)
  --off-plan PATH     Output JSON plan when GUC is off (default: /tmp/plan_off_proof.json)
  --on-plan PATH      Output JSON plan when GUC is on  (default: /tmp/plan_on_proof.json)
  --query-out PATH    Output SQL file for the proving query (default: /tmp/leftdeep_proof.sql)
  --skip-build        Skip `meson compile -C build`
  --keep-running      Do not stop server at end
  --help              Show this help
EOF
}

PORT=55432
WORKLOAD=auto
DATA_DIR=/tmp/pg_leftdeep
LOG_FILE=/tmp/pg_leftdeep.log
ROWS=30000
MAX_TRIES=120
SEED=42
EXTRA_EDGES=7
OFF_PLAN=/tmp/plan_off_proof.json
ON_PLAN=/tmp/plan_on_proof.json
QUERY_OUT=/tmp/leftdeep_proof.sql
SKIP_BUILD=0
KEEP_RUNNING=0

while (($# > 0)); do
  case "$1" in
    --workload) WORKLOAD="$2"; shift 2 ;;
    --port) PORT="$2"; shift 2 ;;
    --data-dir) DATA_DIR="$2"; shift 2 ;;
    --log-file) LOG_FILE="$2"; shift 2 ;;
    --rows) ROWS="$2"; shift 2 ;;
    --max-tries) MAX_TRIES="$2"; shift 2 ;;
    --seed) SEED="$2"; shift 2 ;;
    --extra-edges) EXTRA_EDGES="$2"; shift 2 ;;
    --off-plan) OFF_PLAN="$2"; shift 2 ;;
    --on-plan) ON_PLAN="$2"; shift 2 ;;
    --query-out) QUERY_OUT="$2"; shift 2 ;;
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

if [[ "${WORKLOAD}" != "auto" && "${WORKLOAD}" != "tpch-like" && "${WORKLOAD}" != "random" ]]; then
  echo "Invalid --workload: ${WORKLOAD}" >&2
  echo "Allowed: auto, tpch-like, random" >&2
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
  echo "[1/9] Compiling build tree..."
  (
    cd "$REPO_ROOT"
    CCACHE_DISABLE=1 meson compile -C "$BUILD_DIR"
  )
fi

if [[ ! -s "${DATA_DIR}/PG_VERSION" ]]; then
  echo "[2/9] Initializing data directory ${DATA_DIR}..."
  rm -rf "${DATA_DIR}"
  "$INITDB_BIN" -D "${DATA_DIR}" >/dev/null
else
  echo "[2/9] Reusing existing data directory ${DATA_DIR}"
fi

echo "[3/9] Starting patched server on port ${PORT}..."
"$PGCTL_BIN" -D "${DATA_DIR}" -p "${POSTGRES_BIN}" -m fast stop >/dev/null 2>&1 || true
"$PGCTL_BIN" -D "${DATA_DIR}" -l "${LOG_FILE}" -o "-p ${PORT} -c jit=off" -p "${POSTGRES_BIN}" -w start >/dev/null

cleanup() {
  if [[ "$KEEP_RUNNING" -eq 0 ]]; then
    echo "[9/9] Stopping server..."
    "$PGCTL_BIN" -D "${DATA_DIR}" -p "${POSTGRES_BIN}" -m fast stop >/dev/null || true
  else
    echo "[9/9] Leaving server running on port ${PORT}"
  fi
}
trap cleanup EXIT

echo "[4/9] Verifying custom GUC exists..."
"$PSQL_BIN" -XqAt -p "${PORT}" -d postgres -c "SHOW enable_left_deep_join;" >/dev/null

echo "[5/9] Preparing synthetic schema..."
if [[ "${WORKLOAD}" == "auto" || "${WORKLOAD}" == "random" ]]; then
  "$PSQL_BIN" -Xq -v ON_ERROR_STOP=1 -p "${PORT}" -d postgres <<SQL
DROP SCHEMA IF EXISTS leftdeep CASCADE;
CREATE SCHEMA leftdeep;
SQL

  for i in {1..10}; do
    "$PSQL_BIN" -Xq -v ON_ERROR_STOP=1 -p "${PORT}" -d postgres <<SQL
CREATE TABLE leftdeep.t${i} AS
SELECT g AS id,
       ((g * (11 + ${i})) % 2000) AS k1,
       ((g * (17 + ${i})) % 1000) AS k2,
       ((g * (23 + ${i})) % 100) AS k3
FROM generate_series(1, ${ROWS}) g;
SQL
  done

  for i in {1..10}; do
    "$PSQL_BIN" -Xq -v ON_ERROR_STOP=1 -p "${PORT}" -d postgres -c "ANALYZE leftdeep.t${i};"
  done
fi

if [[ "${WORKLOAD}" == "auto" || "${WORKLOAD}" == "tpch-like" ]]; then
  # Build a compact TPCH-like schema and data with enough joins to allow bushy alternatives.
  "$PSQL_BIN" -Xq -v ON_ERROR_STOP=1 -p "${PORT}" -d postgres <<SQL
DROP SCHEMA IF EXISTS tpch_like CASCADE;
CREATE SCHEMA tpch_like;

CREATE TABLE tpch_like.region (
  r_regionkey int PRIMARY KEY,
  r_name text
);
CREATE TABLE tpch_like.nation (
  n_nationkey int PRIMARY KEY,
  n_regionkey int NOT NULL,
  n_name text
);
CREATE TABLE tpch_like.supplier (
  s_suppkey int PRIMARY KEY,
  s_nationkey int NOT NULL
);
CREATE TABLE tpch_like.customer (
  c_custkey int PRIMARY KEY,
  c_nationkey int NOT NULL
);
CREATE TABLE tpch_like.part (
  p_partkey int PRIMARY KEY,
  p_brand text
);
CREATE TABLE tpch_like.partsupp (
  ps_partkey int NOT NULL,
  ps_suppkey int NOT NULL,
  ps_supplycost numeric
);
CREATE TABLE tpch_like.orders (
  o_orderkey int PRIMARY KEY,
  o_custkey int NOT NULL,
  o_orderdate date
);
CREATE TABLE tpch_like.lineitem (
  l_orderkey int NOT NULL,
  l_partkey int NOT NULL,
  l_suppkey int NOT NULL,
  l_quantity int,
  l_extendedprice numeric,
  l_discount numeric,
  l_shipdate date
);

INSERT INTO tpch_like.region
SELECT i, 'R' || i::text
FROM generate_series(0, 4) i;

INSERT INTO tpch_like.nation
SELECT i, (i % 5), 'N' || i::text
FROM generate_series(0, 24) i;

INSERT INTO tpch_like.supplier
SELECT i, (i % 25)
FROM generate_series(1, GREATEST(1000, ${ROWS} / 8)) i;

INSERT INTO tpch_like.customer
SELECT i, (i % 25)
FROM generate_series(1, GREATEST(5000, ${ROWS} / 2)) i;

INSERT INTO tpch_like.part
SELECT i, 'B' || (i % 40)::text
FROM generate_series(1, GREATEST(3000, ${ROWS} / 3)) i;

INSERT INTO tpch_like.partsupp
SELECT p.p_partkey,
       (((p.p_partkey * 37 + s.s_suppkey * 11) % GREATEST(1000, ${ROWS} / 8)) + 1),
       ((p.p_partkey % 100) + 1)::numeric
FROM tpch_like.part p
CROSS JOIN LATERAL (SELECT 1 AS s_suppkey UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4) s;

INSERT INTO tpch_like.orders
SELECT i,
       ((i * 13) % GREATEST(5000, ${ROWS} / 2)) + 1,
       date '1993-01-01' + ((i * 17) % 2555)
FROM generate_series(1, GREATEST(30000, ${ROWS})) i;

INSERT INTO tpch_like.lineitem
SELECT o.o_orderkey,
       ((o.o_orderkey * x.n * 7) % GREATEST(3000, ${ROWS} / 3)) + 1,
       ((o.o_orderkey * x.n * 3) % GREATEST(1000, ${ROWS} / 8)) + 1,
       ((o.o_orderkey + x.n) % 50) + 1,
       ((o.o_orderkey % 1000) + x.n * 5)::numeric,
       ((x.n % 10) / 100.0)::numeric,
       o.o_orderdate + ((x.n * 5) % 120)
FROM tpch_like.orders o
JOIN (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4) x ON true;

ANALYZE tpch_like.region;
ANALYZE tpch_like.nation;
ANALYZE tpch_like.supplier;
ANALYZE tpch_like.customer;
ANALYZE tpch_like.part;
ANALYZE tpch_like.partsupp;
ANALYZE tpch_like.orders;
ANALYZE tpch_like.lineitem;
SQL
fi

check_left_deep() {
  local file="$1"
  perl -MJSON::PP -e '
my $txt = do { local $/; <> };
my $j = decode_json($txt);

sub is_join {
  my ($t) = @_;
  return $t =~ /^(Hash Join|Merge Join|Nested Loop)$/;
}

sub unwrap_one_child {
  my ($n) = @_;
  return $n unless ref($n) eq "HASH";

  while (ref($n) eq "HASH" && ref($n->{Plans}) eq "ARRAY" && @{$n->{Plans}} == 1) {
    my $t = $n->{"Node Type"} // "";
    last if is_join($t);
    if ($t =~ /^(Hash|Sort|Materialize|Memoize|Gather|Gather Merge|Result|ProjectSet|Unique|Incremental Sort|Aggregate|Group|Limit)$/) {
      $n = $n->{Plans}[0];
      next;
    }
    last;
  }
  return $n;
}

sub bad_shape {
  my ($n) = @_;
  return 0 unless ref($n) eq "HASH";
  my $plans = $n->{Plans} || [];

  if (is_join($n->{"Node Type"} // "") && @$plans >= 2) {
    my $right = unwrap_one_child($plans->[1]);
    return 1 if ref($right) eq "HASH" && is_join($right->{"Node Type"} // "");
  }

  for my $c (@$plans) {
    $c = unwrap_one_child($c);
    return 1 if bad_shape($c);
  }
  return 0;
}

exit bad_shape($j->[0]{Plan}) ? 1 : 0;
' "$file"
}

generate_query() {
  local seed="$1"
  local -a aliases=(a b c d e f g h i j)
  local -a keys=(k1 k2 k3)
  RANDOM="$seed"

  local from_clause=""
  local idx
  for idx in "${!aliases[@]}"; do
    local tnum=$((idx + 1))
    from_clause+="leftdeep.t${tnum} ${aliases[$idx]}"
    if (( idx < ${#aliases[@]} - 1 )); then
      from_clause+=", "
    fi
  done

  local -a conds=()
  local parent key alias p_alias

  # Random spanning tree to keep graph connected.
  for ((idx = 1; idx < ${#aliases[@]}; idx++)); do
    parent=$((RANDOM % idx))
    key="${keys[$((RANDOM % ${#keys[@]}))]}"
    alias="${aliases[$idx]}"
    p_alias="${aliases[$parent]}"
    conds+=("${alias}.${key} = ${p_alias}.${key}")
  done

  # Additional random edges increase chances of bushy plans.
  for ((idx = 0; idx < EXTRA_EDGES; idx++)); do
    local a_idx=$((RANDOM % ${#aliases[@]}))
    local b_idx=$((RANDOM % ${#aliases[@]}))
    if (( a_idx == b_idx )); then
      b_idx=$(((b_idx + 1) % ${#aliases[@]}))
    fi
    key="${keys[$((RANDOM % ${#keys[@]}))]}"
    conds+=("${aliases[$a_idx]}.${key} = ${aliases[$b_idx]}.${key}")
  done

  # Add selective filters to create uneven cardinalities.
  local filters=0
  local limit
  for alias in "${aliases[@]}"; do
    if (( RANDOM % 100 < 45 )); then
      limit=$((RANDOM % 8 + 1))
      conds+=("${alias}.k3 < ${limit}")
      ((filters += 1))
    fi
  done

  if (( filters < 2 )); then
    conds+=("a.k3 < 4")
    conds+=("f.k3 < 4")
  fi

  local where_clause
  where_clause="${conds[0]}"
  for ((idx = 1; idx < ${#conds[@]}; idx++)); do
    where_clause+=" AND ${conds[$idx]}"
  done
  printf 'SELECT count(*) FROM %s WHERE %s;' "${from_clause}" "${where_clause}"
}

capture_plan() {
  local mode="$1"
  local query="$2"
  local out_file="$3"
  local opt

  if [[ "$mode" == "off" ]]; then
    opt="-c jit=off -c geqo=off -c join_collapse_limit=20 -c from_collapse_limit=20 -c enable_nestloop=off -c enable_left_deep_join=off"
  else
    opt="-c jit=off -c geqo=off -c join_collapse_limit=20 -c from_collapse_limit=20 -c enable_nestloop=off -c enable_left_deep_join=on"
  fi

  PGOPTIONS="${opt}" "$PSQL_BIN" -XqAt -v ON_ERROR_STOP=1 -p "${PORT}" -d postgres >"${out_file}" <<SQL
EXPLAIN (FORMAT JSON)
${query}
SQL
}

echo "[6/9] Searching for proving query (tries: ${MAX_TRIES})..."

tmp_off="$(mktemp)"
tmp_on="$(mktemp)"
found=0

# First try deterministic TPCH-like queries.
if [[ "${WORKLOAD}" == "auto" || "${WORKLOAD}" == "tpch-like" ]]; then
  declare -a TPCH_QUERIES=(
"SELECT sum(l.l_extendedprice * (1 - l.l_discount))
 FROM tpch_like.customer c
 JOIN tpch_like.orders o ON o.o_custkey = c.c_custkey
 JOIN tpch_like.lineitem l ON l.l_orderkey = o.o_orderkey
 JOIN tpch_like.supplier s ON s.s_suppkey = l.l_suppkey
 JOIN tpch_like.nation n1 ON n1.n_nationkey = s.s_nationkey
 JOIN tpch_like.region r ON r.r_regionkey = n1.n_regionkey
 JOIN tpch_like.nation n2 ON n2.n_nationkey = c.c_nationkey
 WHERE r.r_name IN ('R1', 'R2')
   AND o.o_orderdate BETWEEN date '1994-01-01' AND date '1996-12-31'
   AND n2.n_regionkey = r.r_regionkey"
,
"SELECT sum(l.l_extendedprice - ps.ps_supplycost * l.l_quantity)
 FROM tpch_like.part p
 JOIN tpch_like.partsupp ps ON ps.ps_partkey = p.p_partkey
 JOIN tpch_like.supplier s ON s.s_suppkey = ps.ps_suppkey
 JOIN tpch_like.nation n ON n.n_nationkey = s.s_nationkey
 JOIN tpch_like.lineitem l
   ON l.l_partkey = ps.ps_partkey
  AND l.l_suppkey = ps.ps_suppkey
 JOIN tpch_like.orders o ON o.o_orderkey = l.l_orderkey
 JOIN tpch_like.customer c ON c.c_custkey = o.o_custkey
 WHERE p.p_brand IN ('B1', 'B7', 'B13')
   AND n.n_regionkey IN (1, 2, 3)
   AND o.o_orderdate BETWEEN date '1995-01-01' AND date '1997-12-31'"
,
"SELECT count(*)
 FROM tpch_like.lineitem l
 JOIN tpch_like.orders o ON o.o_orderkey = l.l_orderkey
 JOIN tpch_like.customer c ON c.c_custkey = o.o_custkey
 JOIN tpch_like.nation nc ON nc.n_nationkey = c.c_nationkey
 JOIN tpch_like.supplier s ON s.s_suppkey = l.l_suppkey
 JOIN tpch_like.nation ns ON ns.n_nationkey = s.s_nationkey
 JOIN tpch_like.region rc ON rc.r_regionkey = nc.n_regionkey
 JOIN tpch_like.region rs ON rs.r_regionkey = ns.n_regionkey
 WHERE rc.r_name = 'R1'
   AND rs.r_name = 'R2'
   AND o.o_orderdate BETWEEN date '1994-01-01' AND date '1998-12-31'
   AND l.l_shipdate BETWEEN date '1994-01-01' AND date '1999-12-31'"
  )

  for q in "${TPCH_QUERIES[@]}"; do
    capture_plan off "${q}" "${tmp_off}"
    capture_plan on "${q}" "${tmp_on}"
    if ! check_left_deep "${tmp_off}" && check_left_deep "${tmp_on}"; then
      found=1
      printf '%s;\n' "${q}" >"${QUERY_OUT}"
      cp "${tmp_off}" "${OFF_PLAN}"
      cp "${tmp_on}" "${ON_PLAN}"
      echo "[7/9] Found proving TPCH-like query"
      break
    fi
  done
fi

# Then random search if needed.
if [[ "${found}" -eq 0 && ( "${WORKLOAD}" == "auto" || "${WORKLOAD}" == "random" ) ]]; then
  for ((try = 1; try <= MAX_TRIES; try++)); do
    q="$(generate_query "$((SEED + try))")"

    capture_plan off "${q}" "${tmp_off}"
    capture_plan on "${q}" "${tmp_on}"

    if ! check_left_deep "${tmp_off}" && check_left_deep "${tmp_on}"; then
      found=1
      printf '%s\n' "${q}" >"${QUERY_OUT}"
      cp "${tmp_off}" "${OFF_PLAN}"
      cp "${tmp_on}" "${ON_PLAN}"
      echo "[7/9] Found proving random query at try ${try}"
      break
    fi
  done
fi

rm -f "${tmp_off}" "${tmp_on}"

if [[ "${found}" -eq 0 ]]; then
  if [[ "${WORKLOAD}" == "tpch-like" ]]; then
    echo "[7/9] No proving query found in tpch-like candidate set"
    echo "Try random search next, e.g.:"
    echo "  scripts/find-left-deep-proof.sh --workload random --max-tries 600 --rows 50000 --skip-build"
  elif [[ "${WORKLOAD}" == "random" ]]; then
    echo "[7/9] No proving query found in ${MAX_TRIES} random tries"
    echo "Try again with higher search budget, e.g.:"
    echo "  scripts/find-left-deep-proof.sh --workload random --max-tries 1000 --rows 60000 --skip-build"
  else
    echo "[7/9] No proving query found in tpch-like candidates + ${MAX_TRIES} random tries"
    echo "Try again with higher search budget, e.g.:"
    echo "  scripts/find-left-deep-proof.sh --workload auto --max-tries 1000 --rows 60000 --extra-edges 10 --skip-build"
  fi
  exit 1
fi

echo "[8/9] Proof artifacts:"
echo "  query: ${QUERY_OUT}"
echo "  off:   ${OFF_PLAN}"
echo "  on:    ${ON_PLAN}"
