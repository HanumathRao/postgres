#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/find-missing-stats-proof.sh [options]

Searches for a query where:
  - enable_left_deep_join=off
  - enable_left_deep_join_on_missing_stats=off
  produces a bushy join involving an unanalyzed table, while
  - enable_left_deep_join_on_missing_stats=on
  removes that specific bushy pattern.

Options:
  --workload MODE     auto|tpch-like|random (default: auto)
  --tpch-complexity N TPCH-like query complexity 1..3 (default: 2)
  --port N            Server port (default: 55432)
  --data-dir PATH     Data directory (default: /tmp/pg_leftdeep)
  --log-file PATH     Log file (default: /tmp/pg_leftdeep_missing_stats.log)
  --rows N            Rows per synthetic table (default: 30000)
  --max-tries N       Number of generated queries to try (default: 400)
  --seed N            Base seed for query generation (default: 42)
  --extra-edges N     Extra random join edges beyond spanning tree (default: 8)
  --statement-timeout-ms N
                      Per-candidate timeout in ms (default: 2000)
  --missing-table N   Unanalyzed table index (1..10, default: 10)
  --tpch-missing-table NAME
                      Unanalyzed TPCH-like table name (default: lineitem)
  --tpch-missing-tables LIST
                      Comma-separated unanalyzed TPCH-like tables
                      (overrides --tpch-missing-table)
  --off-plan PATH     Output JSON plan when feature GUC is off (default: /tmp/ms_plan_off_proof.json)
  --on-plan PATH      Output JSON plan when feature GUC is on  (default: /tmp/ms_plan_on_proof.json)
  --query-out PATH    Output SQL file for proving query (default: /tmp/missing_stats_proof.sql)
  --plan-only         Skip result execution check; compare plans only
  --skip-build        Skip `meson compile -C build`
  --keep-running      Do not stop server at end
  --help              Show this help
EOF
}

PORT=55432
WORKLOAD=auto
DATA_DIR=/tmp/pg_leftdeep
LOG_FILE=/tmp/pg_leftdeep_missing_stats.log
ROWS=30000
MAX_TRIES=400
SEED=42
EXTRA_EDGES=8
STATEMENT_TIMEOUT_MS=2000
MISSING_TABLE=10
TPCH_MISSING_TABLE=lineitem
TPCH_MISSING_TABLES=
TPCH_COMPLEXITY=2
OFF_PLAN=/tmp/ms_plan_off_proof.json
ON_PLAN=/tmp/ms_plan_on_proof.json
QUERY_OUT=/tmp/missing_stats_proof.sql
SKIP_BUILD=0
KEEP_RUNNING=0
PLAN_ONLY=0

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
    --statement-timeout-ms) STATEMENT_TIMEOUT_MS="$2"; shift 2 ;;
    --missing-table) MISSING_TABLE="$2"; shift 2 ;;
    --tpch-missing-table) TPCH_MISSING_TABLE="$2"; shift 2 ;;
    --tpch-missing-tables) TPCH_MISSING_TABLES="$2"; shift 2 ;;
    --tpch-complexity) TPCH_COMPLEXITY="$2"; shift 2 ;;
    --off-plan) OFF_PLAN="$2"; shift 2 ;;
    --on-plan) ON_PLAN="$2"; shift 2 ;;
    --query-out) QUERY_OUT="$2"; shift 2 ;;
    --plan-only) PLAN_ONLY=1; shift ;;
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

if ! [[ "${MISSING_TABLE}" =~ ^[0-9]+$ ]] || (( MISSING_TABLE < 1 || MISSING_TABLE > 10 )); then
  echo "--missing-table must be in [1,10], got: ${MISSING_TABLE}" >&2
  exit 2
fi

if ! [[ "${TPCH_COMPLEXITY}" =~ ^[0-9]+$ ]] || (( TPCH_COMPLEXITY < 1 || TPCH_COMPLEXITY > 3 )); then
  echo "--tpch-complexity must be in [1,3], got: ${TPCH_COMPLEXITY}" >&2
  exit 2
fi

is_valid_tpch_table() {
  case "$1" in
    region|nation|supplier|customer|part|partsupp|orders|lineitem) return 0 ;;
    *) return 1 ;;
  esac
}

declare -a TPCH_MISSING_TABLE_ARR=()
declare -A TPCH_MISSING_SET=()
if [[ -n "${TPCH_MISSING_TABLES}" ]]; then
  IFS=',' read -r -a raw_tpch_missing <<< "${TPCH_MISSING_TABLES}"
  for t in "${raw_tpch_missing[@]}"; do
    t="${t//[[:space:]]/}"
    [[ -z "${t}" ]] && continue
    if ! is_valid_tpch_table "${t}"; then
      echo "--tpch-missing-tables contains invalid table: ${t}" >&2
      echo "Allowed: region,nation,supplier,customer,part,partsupp,orders,lineitem" >&2
      exit 2
    fi
    if [[ -z "${TPCH_MISSING_SET[$t]+x}" ]]; then
      TPCH_MISSING_TABLE_ARR+=("${t}")
      TPCH_MISSING_SET["$t"]=1
    fi
  done
  if (( ${#TPCH_MISSING_TABLE_ARR[@]} == 0 )); then
    echo "--tpch-missing-tables did not contain any valid table names" >&2
    exit 2
  fi
else
  if ! is_valid_tpch_table "${TPCH_MISSING_TABLE}"; then
    echo "--tpch-missing-table must be one of: region,nation,supplier,customer,part,partsupp,orders,lineitem" >&2
    echo "Got: ${TPCH_MISSING_TABLE}" >&2
    exit 2
  fi
  TPCH_MISSING_TABLE_ARR=("${TPCH_MISSING_TABLE}")
  TPCH_MISSING_SET["${TPCH_MISSING_TABLE}"]=1
fi

TPCH_MISSING_DESC="$(IFS=,; echo "${TPCH_MISSING_TABLE_ARR[*]}")"

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

echo "[3/8] Starting patched server on port ${PORT}..."
rm -f "${LOG_FILE}"
"$PGCTL_BIN" -D "${DATA_DIR}" -p "${POSTGRES_BIN}" -m fast stop >/dev/null 2>&1 || true
"$PGCTL_BIN" -D "${DATA_DIR}" -l "${LOG_FILE}" -o "-p ${PORT} -c jit=off" -p "${POSTGRES_BIN}" -w start >/dev/null

cleanup() {
  if [[ "$KEEP_RUNNING" -eq 0 ]]; then
    echo "[8/8] Stopping server..."
    "$PGCTL_BIN" -D "${DATA_DIR}" -p "${POSTGRES_BIN}" -m fast stop >/dev/null || true
  else
    echo "[8/8] Leaving server running on port ${PORT}"
  fi
}
trap cleanup EXIT

echo "[4/8] Verifying custom GUC exists..."
"$PSQL_BIN" -XqAt -p "${PORT}" -d postgres -c "SHOW enable_left_deep_join_on_missing_stats;" >/dev/null

echo "[5/8] Preparing test schema(s)..."
if [[ "${WORKLOAD}" == "auto" || "${WORKLOAD}" == "random" ]]; then
  echo "  - random schema msproof (leave t${MISSING_TABLE} unanalyzed)"
  "$PSQL_BIN" -Xq -v ON_ERROR_STOP=1 -p "${PORT}" -d postgres <<SQL
DROP SCHEMA IF EXISTS msproof CASCADE;
CREATE SCHEMA msproof;
SQL

  for i in {1..10}; do
    "$PSQL_BIN" -Xq -v ON_ERROR_STOP=1 -p "${PORT}" -d postgres <<SQL
CREATE TABLE msproof.t${i} AS
SELECT g AS id,
       ((g * (11 + ${i})) % 2000) AS k1,
       ((g * (17 + ${i})) % 1000) AS k2,
       ((g * (23 + ${i})) % 100) AS k3
FROM generate_series(1, ${ROWS}) g;
SQL
  done

  "$PSQL_BIN" -Xq -v ON_ERROR_STOP=1 -p "${PORT}" -d postgres <<SQL
ALTER TABLE msproof.t${MISSING_TABLE}
  SET (autovacuum_enabled = off, toast.autovacuum_enabled = off);
SQL

  for i in {1..10}; do
    if (( i == MISSING_TABLE )); then
      continue
    fi
    "$PSQL_BIN" -Xq -v ON_ERROR_STOP=1 -p "${PORT}" -d postgres -c "ANALYZE msproof.t${i};"
  done
fi

if [[ "${WORKLOAD}" == "auto" || "${WORKLOAD}" == "tpch-like" ]]; then
  echo "  - tpch-like schema msproof_tpch (leave ${TPCH_MISSING_DESC} unanalyzed)"
  "$PSQL_BIN" -Xq -v ON_ERROR_STOP=1 -p "${PORT}" -d postgres <<SQL
DROP SCHEMA IF EXISTS msproof_tpch CASCADE;
CREATE SCHEMA msproof_tpch;

CREATE TABLE msproof_tpch.region (
  r_regionkey int PRIMARY KEY,
  r_name text
);
CREATE TABLE msproof_tpch.nation (
  n_nationkey int PRIMARY KEY,
  n_regionkey int NOT NULL,
  n_name text
);
CREATE TABLE msproof_tpch.supplier (
  s_suppkey int PRIMARY KEY,
  s_nationkey int NOT NULL
);
CREATE TABLE msproof_tpch.customer (
  c_custkey int PRIMARY KEY,
  c_nationkey int NOT NULL
);
CREATE TABLE msproof_tpch.part (
  p_partkey int PRIMARY KEY,
  p_brand text
);
CREATE TABLE msproof_tpch.partsupp (
  ps_partkey int NOT NULL,
  ps_suppkey int NOT NULL,
  ps_supplycost numeric
);
CREATE TABLE msproof_tpch.orders (
  o_orderkey int PRIMARY KEY,
  o_custkey int NOT NULL,
  o_orderdate date
);
CREATE TABLE msproof_tpch.lineitem (
  l_orderkey int NOT NULL,
  l_partkey int NOT NULL,
  l_suppkey int NOT NULL,
  l_quantity int,
  l_extendedprice numeric,
  l_discount numeric,
  l_shipdate date
);

INSERT INTO msproof_tpch.region
SELECT i, 'R' || i::text
FROM generate_series(0, 4) i;

INSERT INTO msproof_tpch.nation
SELECT i, (i % 5), 'N' || i::text
FROM generate_series(0, 24) i;

INSERT INTO msproof_tpch.supplier
SELECT i, (i % 25)
FROM generate_series(1, GREATEST(1000, ${ROWS} / 8)) i;

INSERT INTO msproof_tpch.customer
SELECT i, (i % 25)
FROM generate_series(1, GREATEST(5000, ${ROWS} / 2)) i;

INSERT INTO msproof_tpch.part
SELECT i, 'B' || (i % 40)::text
FROM generate_series(1, GREATEST(3000, ${ROWS} / 3)) i;

INSERT INTO msproof_tpch.partsupp
SELECT p.p_partkey,
       (((p.p_partkey * 37 + s.s_suppkey * 11) % GREATEST(1000, ${ROWS} / 8)) + 1),
       ((p.p_partkey % 100) + 1)::numeric
FROM msproof_tpch.part p
CROSS JOIN LATERAL (SELECT 1 AS s_suppkey UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4) s;

INSERT INTO msproof_tpch.orders
SELECT i,
       ((i * 13) % GREATEST(5000, ${ROWS} / 2)) + 1,
       date '1993-01-01' + ((i * 17) % 2555)
FROM generate_series(1, GREATEST(30000, ${ROWS})) i;

INSERT INTO msproof_tpch.lineitem
SELECT o.o_orderkey,
       ((o.o_orderkey * x.n * 7) % GREATEST(3000, ${ROWS} / 3)) + 1,
       ((o.o_orderkey * x.n * 3) % GREATEST(1000, ${ROWS} / 8)) + 1,
       ((o.o_orderkey + x.n) % 50) + 1,
       ((o.o_orderkey % 1000) + x.n * 5)::numeric,
       ((x.n % 10) / 100.0)::numeric,
       o.o_orderdate + ((x.n * 5) % 120)
FROM msproof_tpch.orders o
JOIN (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4) x ON true;
SQL

  for t in "${TPCH_MISSING_TABLE_ARR[@]}"; do
    "$PSQL_BIN" -Xq -v ON_ERROR_STOP=1 -p "${PORT}" -d postgres <<SQL
ALTER TABLE msproof_tpch.${t}
  SET (autovacuum_enabled = off, toast.autovacuum_enabled = off);
SQL
  done

  for t in region nation supplier customer part partsupp orders lineitem; do
    if [[ -n "${TPCH_MISSING_SET[$t]+x}" ]]; then
      continue
    fi
    "$PSQL_BIN" -Xq -v ON_ERROR_STOP=1 -p "${PORT}" -d postgres -c "ANALYZE msproof_tpch.${t};"
  done
fi

check_bushy_involving_missing() {
  local file="$1"
  local missing_table="$2"
  perl -MJSON::PP -e '
my ($file, $missing) = @ARGV;
open my $fh, "<", $file or die "$file: $!";
local $/;
my $txt = <$fh>;
close $fh;
$txt =~ s/^[^\[]*//s;
my $j = decode_json($txt);

sub is_join {
  my ($t) = @_;
  return defined($t) && $t =~ /^(Hash Join|Merge Join|Nested Loop)$/;
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

sub union_keys {
  my ($a, $b) = @_;
  my %u = (%{$a // {}}, %{$b // {}});
  return \%u;
}

sub collect_relset {
  my ($n) = @_;
  return {} unless ref($n) eq "HASH";
  my $set = {};
  if (defined $n->{"Relation Name"}) {
    $set->{$n->{"Relation Name"}} = 1;
  }
  for my $c (@{$n->{Plans} || []}) {
    my $cs = collect_relset($c);
    $set = union_keys($set, $cs);
  }
  return $set;
}

sub has_bad_bushy_missing {
  my ($n, $missing) = @_;
  return 0 unless ref($n) eq "HASH";
  my $plans = $n->{Plans} || [];

  if (is_join($n->{"Node Type"} // "") && @$plans >= 2) {
    my $left = unwrap_one_child($plans->[0]);
    my $right = unwrap_one_child($plans->[1]);

    my $lset = collect_relset($left);
    my $rset = collect_relset($right);
    my $lcount = scalar keys %$lset;
    my $rcount = scalar keys %$rset;
    my $has_missing = exists($lset->{$missing}) || exists($rset->{$missing});

    if ($lcount > 1 && $rcount > 1 && $has_missing) {
      return 1;
    }
  }

  for my $c (@$plans) {
    return 1 if has_bad_bushy_missing($c, $missing);
  }
  return 0;
}

exit(has_bad_bushy_missing($j->[0]{Plan}, $missing) ? 0 : 1);
' "$file" "$missing_table"
}

check_bushy_involving_any_missing() {
  local file="$1"
  local missing_csv="$2"
  local t
  IFS=',' read -r -a missing_arr <<< "${missing_csv}"
  for t in "${missing_arr[@]}"; do
    [[ -z "${t}" ]] && continue
    if check_bushy_involving_missing "${file}" "${t}"; then
      return 0
    fi
  done
  return 1
}

generate_random_query() {
  local seed="$1"
  local -a aliases=(a b c d e f g h i j)
  local -a keys=(k1 k2 k3)
  local missing_alias="${aliases[$((MISSING_TABLE - 1))]}"
  RANDOM="$seed"

  local from_clause=""
  local idx
  for idx in "${!aliases[@]}"; do
    local tnum=$((idx + 1))
    from_clause+="msproof.t${tnum} ${aliases[$idx]}"
    if (( idx < ${#aliases[@]} - 1 )); then
      from_clause+=", "
    fi
  done

  local -a conds=()
  local parent key alias p_alias

  for ((idx = 1; idx < ${#aliases[@]}; idx++)); do
    parent=$((RANDOM % idx))
    key="${keys[$((RANDOM % ${#keys[@]}))]}"
    alias="${aliases[$idx]}"
    p_alias="${aliases[$parent]}"
    conds+=("${alias}.${key} = ${p_alias}.${key}")
  done

  for ((idx = 0; idx < EXTRA_EDGES; idx++)); do
    local a_idx=$((RANDOM % ${#aliases[@]}))
    local b_idx=$((RANDOM % ${#aliases[@]}))
    if (( a_idx == b_idx )); then
      b_idx=$(((b_idx + 1) % ${#aliases[@]}))
    fi
    key="${keys[$((RANDOM % ${#keys[@]}))]}"
    conds+=("${aliases[$a_idx]}.${key} = ${aliases[$b_idx]}.${key}")
  done

  # Encourage optimizer to keep the missing-stats table relevant.
  conds+=("${missing_alias}.k3 < 5")
  conds+=("a.k3 < 5")

  local where_clause="${conds[0]}"
  for ((idx = 1; idx < ${#conds[@]}; idx++)); do
    where_clause+=" AND ${conds[$idx]}"
  done
  printf 'SELECT count(*) FROM %s WHERE %s;' "${from_clause}" "${where_clause}"
}

generate_tpch_query() {
  local qid="$1"
  case "$qid" in
    1)
      cat <<'SQL'
SELECT sum(l.l_extendedprice * (1 - l.l_discount))
FROM msproof_tpch.customer c
JOIN msproof_tpch.orders o ON o.o_custkey = c.c_custkey
JOIN msproof_tpch.lineitem l ON l.l_orderkey = o.o_orderkey
JOIN msproof_tpch.supplier s ON s.s_suppkey = l.l_suppkey
JOIN msproof_tpch.nation n1 ON n1.n_nationkey = s.s_nationkey
JOIN msproof_tpch.region r ON r.r_regionkey = n1.n_regionkey
JOIN msproof_tpch.nation n2 ON n2.n_nationkey = c.c_nationkey
WHERE r.r_regionkey IN (1,2,3)
  AND n2.n_regionkey = r.r_regionkey
  AND o.o_orderdate BETWEEN date '1994-01-01' AND date '1996-12-31'
  AND l.l_shipdate BETWEEN date '1994-01-01' AND date '1997-12-31'
SQL
      ;;
    2)
      cat <<'SQL'
SELECT sum(l.l_extendedprice - ps.ps_supplycost * l.l_quantity)
FROM msproof_tpch.part p
JOIN msproof_tpch.partsupp ps ON ps.ps_partkey = p.p_partkey
JOIN msproof_tpch.supplier s ON s.s_suppkey = ps.ps_suppkey
JOIN msproof_tpch.nation n ON n.n_nationkey = s.s_nationkey
JOIN msproof_tpch.lineitem l
  ON l.l_partkey = ps.ps_partkey
 AND l.l_suppkey = ps.ps_suppkey
JOIN msproof_tpch.orders o ON o.o_orderkey = l.l_orderkey
JOIN msproof_tpch.customer c ON c.c_custkey = o.o_custkey
WHERE p.p_brand IN ('B1', 'B7', 'B13')
  AND n.n_regionkey IN (1,2,3)
  AND o.o_orderdate BETWEEN date '1995-01-01' AND date '1997-12-31'
SQL
      ;;
    3)
      cat <<'SQL'
SELECT count(*)
FROM msproof_tpch.lineitem l
JOIN msproof_tpch.orders o ON o.o_orderkey = l.l_orderkey
JOIN msproof_tpch.customer c ON c.c_custkey = o.o_custkey
JOIN msproof_tpch.nation nc ON nc.n_nationkey = c.c_nationkey
JOIN msproof_tpch.supplier s ON s.s_suppkey = l.l_suppkey
JOIN msproof_tpch.nation ns ON ns.n_nationkey = s.s_nationkey
JOIN msproof_tpch.region rc ON rc.r_regionkey = nc.n_regionkey
JOIN msproof_tpch.region rs ON rs.r_regionkey = ns.n_regionkey
WHERE rc.r_regionkey = 1
  AND rs.r_regionkey = 2
  AND o.o_orderdate BETWEEN date '1994-01-01' AND date '1998-12-31'
  AND l.l_shipdate BETWEEN date '1994-01-01' AND date '1999-12-31'
SQL
      ;;
    4)
      cat <<'SQL'
SELECT sum(l.l_quantity)
FROM msproof_tpch.orders o
JOIN msproof_tpch.lineitem l ON l.l_orderkey = o.o_orderkey
JOIN msproof_tpch.partsupp ps
  ON ps.ps_partkey = l.l_partkey
 AND ps.ps_suppkey = l.l_suppkey
JOIN msproof_tpch.part p ON p.p_partkey = ps.ps_partkey
JOIN msproof_tpch.supplier s ON s.s_suppkey = ps.ps_suppkey
JOIN msproof_tpch.nation n ON n.n_nationkey = s.s_nationkey
JOIN msproof_tpch.customer c ON c.c_custkey = o.o_custkey
WHERE o.o_orderdate BETWEEN date '1993-01-01' AND date '1996-12-31'
  AND n.n_regionkey IN (0,1,2)
  AND p.p_brand IN ('B3', 'B9', 'B21')
SQL
      ;;
    5)
      cat <<'SQL'
SELECT count(*)
FROM msproof_tpch.customer c
JOIN msproof_tpch.orders o ON o.o_custkey = c.c_custkey
JOIN msproof_tpch.lineitem l ON l.l_orderkey = o.o_orderkey
JOIN msproof_tpch.part p ON p.p_partkey = l.l_partkey
JOIN msproof_tpch.supplier s ON s.s_suppkey = l.l_suppkey
JOIN msproof_tpch.nation ns ON ns.n_nationkey = s.s_nationkey
JOIN msproof_tpch.nation nc ON nc.n_nationkey = c.c_nationkey
JOIN msproof_tpch.region r ON r.r_regionkey = ns.n_regionkey
WHERE r.r_regionkey IN (1,2,3,4)
  AND nc.n_regionkey = r.r_regionkey
  AND p.p_brand IN ('B5', 'B11')
  AND o.o_orderdate BETWEEN date '1994-01-01' AND date '1997-12-31'
SQL
      ;;
    6)
      cat <<'SQL'
SELECT sum(li.rev)
FROM (
  SELECT l.l_orderkey, l.l_partkey, l.l_suppkey,
         sum(l.l_extendedprice * (1 - l.l_discount)) AS rev
  FROM msproof_tpch.lineitem l
  WHERE l.l_shipdate BETWEEN date '1994-01-01' AND date '1999-12-31'
  GROUP BY l.l_orderkey, l.l_partkey, l.l_suppkey
) li
JOIN msproof_tpch.orders o ON o.o_orderkey = li.l_orderkey
JOIN msproof_tpch.customer c ON c.c_custkey = o.o_custkey
JOIN msproof_tpch.nation nc ON nc.n_nationkey = c.c_nationkey
JOIN msproof_tpch.supplier s ON s.s_suppkey = li.l_suppkey
JOIN msproof_tpch.nation ns ON ns.n_nationkey = s.s_nationkey
JOIN msproof_tpch.region r ON r.r_regionkey = ns.n_regionkey
JOIN msproof_tpch.part p ON p.p_partkey = li.l_partkey
JOIN msproof_tpch.partsupp ps
  ON ps.ps_partkey = li.l_partkey
 AND ps.ps_suppkey = li.l_suppkey
WHERE nc.n_regionkey = r.r_regionkey
  AND p.p_brand IN ('B2', 'B8', 'B17')
  AND o.o_orderdate BETWEEN date '1994-01-01' AND date '1998-12-31'
  AND EXISTS (
    SELECT 1
    FROM msproof_tpch.partsupp ps2
    WHERE ps2.ps_partkey = li.l_partkey
      AND ps2.ps_suppkey = li.l_suppkey
      AND ps2.ps_supplycost <= ps.ps_supplycost + 10
  )
SQL
      ;;
    7)
      cat <<'SQL'
WITH fo AS (
  SELECT o.o_orderkey, o.o_custkey, o.o_orderdate, c.c_nationkey
  FROM msproof_tpch.orders o
  JOIN msproof_tpch.customer c ON c.c_custkey = o.o_custkey
  WHERE o.o_orderdate BETWEEN date '1994-01-01' AND date '1997-12-31'
),
lp AS (
  SELECT l.l_orderkey, l.l_partkey, l.l_suppkey, l.l_quantity
  FROM msproof_tpch.lineitem l
  WHERE l.l_shipdate BETWEEN date '1994-01-01' AND date '1999-12-31'
)
SELECT count(*)
FROM fo
JOIN lp ON lp.l_orderkey = fo.o_orderkey
JOIN msproof_tpch.part p ON p.p_partkey = lp.l_partkey
JOIN msproof_tpch.partsupp ps
  ON ps.ps_partkey = lp.l_partkey
 AND ps.ps_suppkey = lp.l_suppkey
JOIN msproof_tpch.supplier s ON s.s_suppkey = lp.l_suppkey
JOIN msproof_tpch.nation ns ON ns.n_nationkey = s.s_nationkey
JOIN msproof_tpch.region r ON r.r_regionkey = ns.n_regionkey
JOIN msproof_tpch.nation nc ON nc.n_nationkey = fo.c_nationkey
WHERE nc.n_regionkey = r.r_regionkey
  AND p.p_brand LIKE 'B1%'
  AND ps.ps_supplycost > 10
SQL
      ;;
    8)
      cat <<'SQL'
SELECT sum(l.l_quantity)
FROM msproof_tpch.lineitem l
JOIN msproof_tpch.orders o ON o.o_orderkey = l.l_orderkey
JOIN msproof_tpch.customer c ON c.c_custkey = o.o_custkey
JOIN msproof_tpch.nation nc ON nc.n_nationkey = c.c_nationkey
JOIN msproof_tpch.region rc ON rc.r_regionkey = nc.n_regionkey
JOIN msproof_tpch.partsupp ps
  ON ps.ps_partkey = l.l_partkey
 AND ps.ps_suppkey = l.l_suppkey
JOIN msproof_tpch.part p ON p.p_partkey = l.l_partkey
JOIN msproof_tpch.supplier s ON s.s_suppkey = l.l_suppkey
JOIN msproof_tpch.nation ns ON ns.n_nationkey = s.s_nationkey
JOIN msproof_tpch.region rs ON rs.r_regionkey = ns.n_regionkey
WHERE rc.r_regionkey IN (0,1,2)
  AND rs.r_regionkey IN (2,3,4)
  AND p.p_brand IN ('B3', 'B9', 'B19')
  AND o.o_orderdate BETWEEN date '1993-01-01' AND date '1997-12-31'
  AND EXISTS (
    SELECT 1
    FROM msproof_tpch.orders o2
    JOIN msproof_tpch.customer c2 ON c2.c_custkey = o2.o_custkey
    WHERE o2.o_orderkey = l.l_orderkey
      AND c2.c_nationkey = nc.n_nationkey
  )
SQL
      ;;
    9)
      cat <<'SQL'
SELECT count(*)
FROM (
  SELECT s.s_suppkey, ns.n_regionkey
  FROM msproof_tpch.supplier s
  JOIN msproof_tpch.nation ns ON ns.n_nationkey = s.s_nationkey
) sr
JOIN msproof_tpch.lineitem l ON l.l_suppkey = sr.s_suppkey
JOIN msproof_tpch.orders o ON o.o_orderkey = l.l_orderkey
JOIN msproof_tpch.customer c ON c.c_custkey = o.o_custkey
JOIN msproof_tpch.nation nc ON nc.n_nationkey = c.c_nationkey
JOIN msproof_tpch.region r ON r.r_regionkey = sr.n_regionkey
JOIN msproof_tpch.part p ON p.p_partkey = l.l_partkey
JOIN msproof_tpch.partsupp ps
  ON ps.ps_partkey = l.l_partkey
 AND ps.ps_suppkey = l.l_suppkey
WHERE nc.n_regionkey = r.r_regionkey
  AND p.p_brand IN ('B6', 'B14', 'B27')
  AND o.o_orderdate BETWEEN date '1994-01-01' AND date '1998-12-31'
  AND EXISTS (
    SELECT 1 FROM msproof_tpch.region rx
    WHERE rx.r_regionkey = nc.n_regionkey
  )
SQL
      ;;
    10)
      cat <<'SQL'
SELECT sum(x.rev)
FROM (
  SELECT l.l_orderkey, l.l_partkey, l.l_suppkey,
         sum(l.l_extendedprice) AS rev
  FROM msproof_tpch.lineitem l
  GROUP BY l.l_orderkey, l.l_partkey, l.l_suppkey
) x
JOIN msproof_tpch.orders o ON o.o_orderkey = x.l_orderkey
JOIN msproof_tpch.customer c ON c.c_custkey = o.o_custkey
JOIN msproof_tpch.nation nc ON nc.n_nationkey = c.c_nationkey
JOIN msproof_tpch.supplier s ON s.s_suppkey = x.l_suppkey
JOIN msproof_tpch.nation ns ON ns.n_nationkey = s.s_nationkey
JOIN msproof_tpch.region r ON r.r_regionkey = ns.n_regionkey
JOIN msproof_tpch.part p ON p.p_partkey = x.l_partkey
JOIN msproof_tpch.partsupp ps
  ON ps.ps_partkey = x.l_partkey
 AND ps.ps_suppkey = x.l_suppkey
WHERE nc.n_regionkey = r.r_regionkey
  AND p.p_brand IN ('B0', 'B4', 'B12', 'B31')
  AND o.o_orderdate BETWEEN date '1994-01-01' AND date '1998-12-31'
  AND x.rev > (
    SELECT avg(l2.l_extendedprice)
    FROM msproof_tpch.lineitem l2
    WHERE l2.l_partkey = x.l_partkey
  )
SQL
      ;;
    11)
      cat <<'SQL'
SELECT count(*)
FROM msproof_tpch.customer c
JOIN msproof_tpch.orders o ON o.o_custkey = c.c_custkey
JOIN msproof_tpch.lineitem l ON l.l_orderkey = o.o_orderkey
JOIN msproof_tpch.part p ON p.p_partkey = l.l_partkey
JOIN msproof_tpch.partsupp ps
  ON ps.ps_partkey = l.l_partkey
 AND ps.ps_suppkey = l.l_suppkey
JOIN msproof_tpch.supplier s ON s.s_suppkey = l.l_suppkey
JOIN msproof_tpch.nation ns ON ns.n_nationkey = s.s_nationkey
JOIN msproof_tpch.nation nc ON nc.n_nationkey = c.c_nationkey
JOIN msproof_tpch.region r ON r.r_regionkey = ns.n_regionkey
WHERE nc.n_regionkey = r.r_regionkey
  AND p.p_brand IN ('B1', 'B5', 'B9', 'B13')
  AND o.o_orderdate BETWEEN date '1993-01-01' AND date '1998-12-31'
  AND o.o_orderkey IN (
    SELECT o3.o_orderkey
    FROM msproof_tpch.orders o3
    JOIN msproof_tpch.customer c3 ON c3.c_custkey = o3.o_custkey
    WHERE c3.c_nationkey = nc.n_nationkey
      AND o3.o_orderdate <= o.o_orderdate
  )
SQL
      ;;
    12)
      cat <<'SQL'
WITH psel AS (
  SELECT p.p_partkey, ps.ps_suppkey
  FROM msproof_tpch.part p
  JOIN msproof_tpch.partsupp ps ON ps.ps_partkey = p.p_partkey
  WHERE p.p_brand IN ('B2', 'B7', 'B22', 'B37')
    AND ps.ps_supplycost > 20
),
csel AS (
  SELECT c.c_custkey, c.c_nationkey, r.r_regionkey
  FROM msproof_tpch.customer c
  JOIN msproof_tpch.nation nc ON nc.n_nationkey = c.c_nationkey
  JOIN msproof_tpch.region r ON r.r_regionkey = nc.n_regionkey
  WHERE r.r_regionkey IN (1,2,3)
)
SELECT sum(l.l_extendedprice)
FROM csel
JOIN msproof_tpch.orders o ON o.o_custkey = csel.c_custkey
JOIN msproof_tpch.lineitem l ON l.l_orderkey = o.o_orderkey
JOIN psel ON psel.p_partkey = l.l_partkey AND psel.ps_suppkey = l.l_suppkey
JOIN msproof_tpch.supplier s ON s.s_suppkey = l.l_suppkey
JOIN msproof_tpch.nation ns ON ns.n_nationkey = s.s_nationkey
JOIN msproof_tpch.region r2 ON r2.r_regionkey = ns.n_regionkey
WHERE o.o_orderdate BETWEEN date '1994-01-01' AND date '1999-12-31'
  AND csel.r_regionkey = r2.r_regionkey
  AND EXISTS (
    SELECT 1
    FROM msproof_tpch.nation n2
    WHERE n2.n_nationkey = csel.c_nationkey
      AND n2.n_regionkey = r2.r_regionkey
  )
SQL
      ;;
    *)
      return 1
      ;;
  esac
}

tpch_template_qids() {
  case "${TPCH_COMPLEXITY}" in
    1) echo "1 2 3 4 5" ;;
    2) echo "1 2 3 4 5 6 7 8" ;;
    3) echo "1 2 3 4 5 6 7 8 9 10 11 12" ;;
    *) return 1 ;;
  esac
}

generate_tpch_random_query() {
  local seed="$1"
  RANDOM="$seed"

  local region_a=$((RANDOM % 5))
  local region_b=$((RANDOM % 5))
  local region_c=$((RANDOM % 5))
  local brand_a=$((RANDOM % 40))
  local brand_b=$((RANDOM % 40))
  local brand_c=$((RANDOM % 40))
  local date_shift=$((RANDOM % 365))
  local date_shift2=$((RANDOM % 365))
  local date_shift3=$((RANDOM % 365))
  local shape_max="${TPCH_COMPLEXITY}"
  local shape=$((RANDOM % shape_max + 1))

  case "${shape}" in
    1)
      cat <<SQL
SELECT count(*)
FROM msproof_tpch.customer c
JOIN msproof_tpch.orders o ON o.o_custkey = c.c_custkey
JOIN msproof_tpch.lineitem l ON l.l_orderkey = o.o_orderkey
JOIN msproof_tpch.supplier s ON s.s_suppkey = l.l_suppkey
JOIN msproof_tpch.nation ns ON ns.n_nationkey = s.s_nationkey
JOIN msproof_tpch.region r ON r.r_regionkey = ns.n_regionkey
JOIN msproof_tpch.part p ON p.p_partkey = l.l_partkey
JOIN msproof_tpch.partsupp ps
  ON ps.ps_partkey = p.p_partkey
 AND ps.ps_suppkey = s.s_suppkey
WHERE r.r_regionkey IN (${region_a}, ${region_b})
  AND p.p_brand IN ('B${brand_a}', 'B${brand_b}')
  AND o.o_orderdate BETWEEN (date '1994-01-01' + ${date_shift}) AND (date '1997-12-31' - ${date_shift2})
  AND l.l_shipdate BETWEEN date '1994-01-01' AND date '1999-12-31'
  AND ps.ps_supplycost > 0;
SQL
      ;;
    2)
      cat <<SQL
SELECT sum(li.rev)
FROM (
  SELECT l.l_orderkey, l.l_partkey, l.l_suppkey,
         sum(l.l_extendedprice * (1 - l.l_discount)) AS rev
  FROM msproof_tpch.lineitem l
  WHERE l.l_shipdate BETWEEN date '1994-01-01' AND date '1999-12-31'
  GROUP BY l.l_orderkey, l.l_partkey, l.l_suppkey
) li
JOIN msproof_tpch.orders o ON o.o_orderkey = li.l_orderkey
JOIN msproof_tpch.customer c ON c.c_custkey = o.o_custkey
JOIN msproof_tpch.nation nc ON nc.n_nationkey = c.c_nationkey
JOIN msproof_tpch.supplier s ON s.s_suppkey = li.l_suppkey
JOIN msproof_tpch.nation ns ON ns.n_nationkey = s.s_nationkey
JOIN msproof_tpch.region r ON r.r_regionkey = ns.n_regionkey
JOIN msproof_tpch.part p ON p.p_partkey = li.l_partkey
JOIN msproof_tpch.partsupp ps
  ON ps.ps_partkey = li.l_partkey
 AND ps.ps_suppkey = li.l_suppkey
WHERE r.r_regionkey IN (${region_a}, ${region_b}, ${region_c})
  AND nc.n_regionkey = r.r_regionkey
  AND p.p_brand IN ('B${brand_a}', 'B${brand_b}', 'B${brand_c}')
  AND o.o_orderdate BETWEEN (date '1994-01-01' + ${date_shift}) AND (date '1998-12-31' - ${date_shift2})
  AND ps.ps_supplycost > 0
  AND EXISTS (
    SELECT 1
    FROM msproof_tpch.orders o2
    WHERE o2.o_custkey = c.c_custkey
      AND o2.o_orderdate >= (date '1994-01-01' + ${date_shift3})
  );
SQL
      ;;
    *)
      cat <<SQL
WITH fo AS (
  SELECT o.o_orderkey, o.o_custkey, o.o_orderdate
  FROM msproof_tpch.orders o
  WHERE o.o_orderdate BETWEEN (date '1994-01-01' + ${date_shift}) AND (date '1999-12-31' - ${date_shift2})
),
pf AS (
  SELECT p.p_partkey, ps.ps_suppkey
  FROM msproof_tpch.part p
  JOIN msproof_tpch.partsupp ps ON ps.ps_partkey = p.p_partkey
  WHERE p.p_brand IN ('B${brand_a}', 'B${brand_b}', 'B${brand_c}')
)
SELECT count(*)
FROM fo
JOIN msproof_tpch.customer c ON c.c_custkey = fo.o_custkey
JOIN msproof_tpch.nation nc ON nc.n_nationkey = c.c_nationkey
JOIN msproof_tpch.lineitem l ON l.l_orderkey = fo.o_orderkey
JOIN pf ON pf.p_partkey = l.l_partkey AND pf.ps_suppkey = l.l_suppkey
JOIN msproof_tpch.supplier s ON s.s_suppkey = l.l_suppkey
JOIN msproof_tpch.nation ns ON ns.n_nationkey = s.s_nationkey
JOIN msproof_tpch.region r ON r.r_regionkey = ns.n_regionkey
WHERE r.r_regionkey IN (${region_a}, ${region_b}, ${region_c})
  AND nc.n_regionkey = r.r_regionkey
  AND l.l_shipdate BETWEEN date '1994-01-01' AND date '1999-12-31'
  AND EXISTS (
    SELECT 1 FROM msproof_tpch.region r2
    WHERE r2.r_regionkey = nc.n_regionkey
  );
SQL
      ;;
  esac
}

capture_plan() {
  local mode="$1"
  local query="$2"
  local out_file="$3"
  local opt

  if [[ "$mode" == "off" ]]; then
    opt="-c jit=off -c geqo=off -c join_collapse_limit=20 -c from_collapse_limit=20 -c statement_timeout=${STATEMENT_TIMEOUT_MS}ms -c enable_left_deep_join=off -c enable_left_deep_join_on_missing_stats=off"
  else
    opt="-c jit=off -c geqo=off -c join_collapse_limit=20 -c from_collapse_limit=20 -c statement_timeout=${STATEMENT_TIMEOUT_MS}ms -c enable_left_deep_join=off -c enable_left_deep_join_on_missing_stats=on"
  fi

  PGOPTIONS="${opt}" "$PSQL_BIN" -XqAt -v ON_ERROR_STOP=1 -p "${PORT}" -d postgres >"${out_file}" <<SQL
EXPLAIN (FORMAT JSON)
${query}
SQL
}

run_query_scalar() {
  local mode="$1"
  local query="$2"
  local opt

  if [[ "$mode" == "off" ]]; then
    opt="-c jit=off -c geqo=off -c join_collapse_limit=20 -c from_collapse_limit=20 -c statement_timeout=${STATEMENT_TIMEOUT_MS}ms -c enable_left_deep_join=off -c enable_left_deep_join_on_missing_stats=off"
  else
    opt="-c jit=off -c geqo=off -c join_collapse_limit=20 -c from_collapse_limit=20 -c statement_timeout=${STATEMENT_TIMEOUT_MS}ms -c enable_left_deep_join=off -c enable_left_deep_join_on_missing_stats=on"
  fi

  PGOPTIONS="${opt}" "$PSQL_BIN" -XqAt -v ON_ERROR_STOP=1 -p "${PORT}" -d postgres -c "${query}"
}

echo "[6/8] Searching for proving query (tries: ${MAX_TRIES})..."
tmp_off="$(mktemp)"
tmp_on="$(mktemp)"
found=0

# First try TPCH-like workload candidates if requested.
if [[ "${WORKLOAD}" == "auto" || "${WORKLOAD}" == "tpch-like" ]]; then
  read -r -a tpch_qids <<< "$(tpch_template_qids)"
  for qid in "${tpch_qids[@]}"; do
    q="$(generate_tpch_query "${qid}")"

    if (( PLAN_ONLY == 0 )); then
      if ! off_result="$(run_query_scalar off "${q}" 2>/dev/null)"; then
        continue
      fi
      if ! on_result="$(run_query_scalar on "${q}" 2>/dev/null)"; then
        continue
      fi
      if [[ "${off_result}" != "${on_result}" ]]; then
        continue
      fi
    fi

    if ! capture_plan off "${q}" "${tmp_off}" 2>/dev/null; then
      continue
    fi
    if ! capture_plan on "${q}" "${tmp_on}" 2>/dev/null; then
      continue
    fi

    if check_bushy_involving_any_missing "${tmp_off}" "${TPCH_MISSING_DESC}" &&
       ! check_bushy_involving_any_missing "${tmp_on}" "${TPCH_MISSING_DESC}"; then
      found=1
      printf '%s\n' "${q}" > "${QUERY_OUT}"
      cp "${tmp_off}" "${OFF_PLAN}"
      cp "${tmp_on}" "${ON_PLAN}"
      echo "[7/8] Found proving TPCH-like query (qid=${qid})"
      break
    fi
  done

  if [[ "${found}" -eq 0 ]]; then
    for ((try = 1; try <= MAX_TRIES; try++)); do
      if (( try % 100 == 0 )); then
        echo "  ... tpch-like tried ${try}/${MAX_TRIES}"
      fi
      q="$(generate_tpch_random_query "$((SEED + 10000 + try))")"

      if (( PLAN_ONLY == 0 )); then
        if ! off_result="$(run_query_scalar off "${q}" 2>/dev/null)"; then
          continue
        fi
        if ! on_result="$(run_query_scalar on "${q}" 2>/dev/null)"; then
          continue
        fi
        if [[ "${off_result}" != "${on_result}" ]]; then
          continue
        fi
      fi

      if ! capture_plan off "${q}" "${tmp_off}" 2>/dev/null; then
        continue
      fi
      if ! capture_plan on "${q}" "${tmp_on}" 2>/dev/null; then
        continue
      fi

      if check_bushy_involving_any_missing "${tmp_off}" "${TPCH_MISSING_DESC}" &&
         ! check_bushy_involving_any_missing "${tmp_on}" "${TPCH_MISSING_DESC}"; then
        found=1
        printf '%s\n' "${q}" > "${QUERY_OUT}"
        cp "${tmp_off}" "${OFF_PLAN}"
        cp "${tmp_on}" "${ON_PLAN}"
        echo "[7/8] Found proving randomized TPCH-like query at try ${try}"
        break
      fi
    done
  fi
fi

# Then random-search if still needed.
if [[ "${found}" -eq 0 && ( "${WORKLOAD}" == "auto" || "${WORKLOAD}" == "random" ) ]]; then
  for ((try = 1; try <= MAX_TRIES; try++)); do
    if (( try % 100 == 0 )); then
      echo "  ... tried ${try}/${MAX_TRIES}"
    fi
    q="$(generate_random_query "$((SEED + try))")"

    if (( PLAN_ONLY == 0 )); then
      if ! off_result="$(run_query_scalar off "${q}" 2>/dev/null)"; then
        continue
      fi
      if ! on_result="$(run_query_scalar on "${q}" 2>/dev/null)"; then
        continue
      fi
      if [[ "${off_result}" != "${on_result}" ]]; then
        continue
      fi
    fi

    if ! capture_plan off "${q}" "${tmp_off}" 2>/dev/null; then
      continue
    fi
    if ! capture_plan on "${q}" "${tmp_on}" 2>/dev/null; then
      continue
    fi

    if check_bushy_involving_missing "${tmp_off}" "t${MISSING_TABLE}" &&
       ! check_bushy_involving_missing "${tmp_on}" "t${MISSING_TABLE}"; then
      found=1
      printf '%s\n' "${q}" > "${QUERY_OUT}"
      cp "${tmp_off}" "${OFF_PLAN}"
      cp "${tmp_on}" "${ON_PLAN}"
      echo "[7/8] Found proving random query at try ${try}"
      break
    fi
  done
fi

rm -f "${tmp_off}" "${tmp_on}"

if [[ "${found}" -eq 0 ]]; then
  if [[ "${WORKLOAD}" == "tpch-like" ]]; then
    echo "[7/8] No proving query found in TPCH-like fixed + randomized candidate set"
    echo "Try random search, e.g.:"
    echo "  scripts/find-missing-stats-proof.sh --workload random --max-tries 1200 --rows 60000 --extra-edges 12 --skip-build --plan-only"
  elif [[ "${WORKLOAD}" == "random" ]]; then
    echo "[7/8] No proving query found in ${MAX_TRIES} random tries"
    echo "Try higher search budget, e.g.:"
    echo "  scripts/find-missing-stats-proof.sh --workload random --max-tries 2000 --rows 60000 --extra-edges 12 --skip-build --plan-only"
  else
    echo "[7/8] No proving query found in TPCH-like candidates + ${MAX_TRIES} random tries"
    echo "Try higher search budget, e.g.:"
    echo "  scripts/find-missing-stats-proof.sh --workload auto --max-tries 2000 --rows 60000 --extra-edges 12 --skip-build --plan-only"
  fi
  exit 1
fi

echo "[7/8] Proof artifacts:"
echo "  query: ${QUERY_OUT}"
echo "  off:   ${OFF_PLAN}"
echo "  on:    ${ON_PLAN}"
if [[ "${WORKLOAD}" == "auto" || "${WORKLOAD}" == "tpch-like" ]]; then
  echo "  missing-tables(tpch): ${TPCH_MISSING_DESC}"
fi
