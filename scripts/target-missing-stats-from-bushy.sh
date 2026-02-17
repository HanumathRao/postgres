#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/target-missing-stats-from-bushy.sh [options]

Find a query that is bushy with all stats present, then pick a table from the
right subtree of that bushy join, leave only that table unanalyzed, and
compare plans with:
  - enable_left_deep_join_on_missing_stats=off
  - enable_left_deep_join_on_missing_stats=on

Options:
  --port N                 Server port (default: 55432)
  --data-dir PATH          Data directory (default: /tmp/pg_leftdeep)
  --log-file PATH          Log file (default: /tmp/pg_leftdeep_target.log)
  --rows N                 Rows per table (default: 40000)
  --seed N                 Base seed (default: 42)
  --max-tries N            Query search tries (default: 1200)
  --extra-edges N          Extra random join edges (default: 12)
  --statement-timeout-ms N Per-candidate timeout ms (default: 2000)
  --schema NAME            Working schema name (default: ms_target)
  --query-out PATH         Proving query SQL (default: /tmp/ms_target_query.sql)
  --all-plan PATH          All-stats bushy plan JSON (default: /tmp/ms_target_allstats_plan.json)
  --off-plan PATH          Missing-stats OFF plan JSON (default: /tmp/ms_target_off_plan.json)
  --on-plan PATH           Missing-stats ON plan JSON (default: /tmp/ms_target_on_plan.json)
  --target-out PATH        Target table file (default: /tmp/ms_target_table.txt)
  --skip-build             Skip build step
  --keep-running           Keep server running at end
  --help                   Show this help
EOF
}

PORT=55432
DATA_DIR=/tmp/pg_leftdeep
LOG_FILE=/tmp/pg_leftdeep_target.log
ROWS=40000
SEED=42
MAX_TRIES=1200
EXTRA_EDGES=12
STATEMENT_TIMEOUT_MS=2000
SCHEMA=ms_target
QUERY_OUT=/tmp/ms_target_query.sql
ALL_PLAN=/tmp/ms_target_allstats_plan.json
OFF_PLAN=/tmp/ms_target_off_plan.json
ON_PLAN=/tmp/ms_target_on_plan.json
TARGET_OUT=/tmp/ms_target_table.txt
SKIP_BUILD=0
KEEP_RUNNING=0

while (($# > 0)); do
  case "$1" in
    --port) PORT="$2"; shift 2 ;;
    --data-dir) DATA_DIR="$2"; shift 2 ;;
    --log-file) LOG_FILE="$2"; shift 2 ;;
    --rows) ROWS="$2"; shift 2 ;;
    --seed) SEED="$2"; shift 2 ;;
    --max-tries) MAX_TRIES="$2"; shift 2 ;;
    --extra-edges) EXTRA_EDGES="$2"; shift 2 ;;
    --statement-timeout-ms) STATEMENT_TIMEOUT_MS="$2"; shift 2 ;;
    --schema) SCHEMA="$2"; shift 2 ;;
    --query-out) QUERY_OUT="$2"; shift 2 ;;
    --all-plan) ALL_PLAN="$2"; shift 2 ;;
    --off-plan) OFF_PLAN="$2"; shift 2 ;;
    --on-plan) ON_PLAN="$2"; shift 2 ;;
    --target-out) TARGET_OUT="$2"; shift 2 ;;
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
rm -f "${LOG_FILE}"
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

prepare_schema() {
  local missing_table="$1"
  local i

  "$PSQL_BIN" -Xq -v ON_ERROR_STOP=1 -p "${PORT}" -d postgres <<SQL
DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE;
CREATE SCHEMA ${SCHEMA};
SQL

  for i in {1..10}; do
    "$PSQL_BIN" -Xq -v ON_ERROR_STOP=1 -p "${PORT}" -d postgres <<SQL
CREATE TABLE ${SCHEMA}.t${i} AS
SELECT g AS id,
       ((g * (11 + ${i})) % 2000) AS k1,
       ((g * (17 + ${i})) % 1000) AS k2,
       ((g * (23 + ${i})) % 100) AS k3
FROM generate_series(1, ${ROWS}) g;
SQL
  done

  if [[ -n "${missing_table}" ]]; then
    "$PSQL_BIN" -Xq -v ON_ERROR_STOP=1 -p "${PORT}" -d postgres <<SQL
ALTER TABLE ${SCHEMA}.${missing_table}
  SET (autovacuum_enabled = off, toast.autovacuum_enabled = off);
SQL
  fi

  for i in {1..10}; do
    local t="t${i}"
    if [[ -n "${missing_table}" && "${t}" == "${missing_table}" ]]; then
      continue
    fi
    "$PSQL_BIN" -Xq -v ON_ERROR_STOP=1 -p "${PORT}" -d postgres -c "ANALYZE ${SCHEMA}.${t};"
  done
}

generate_random_query() {
  local seed="$1"
  local -a aliases=(a b c d e f g h i j)
  local -a keys=(k1 k2 k3)
  RANDOM="$seed"

  local from_clause=""
  local idx
  for idx in "${!aliases[@]}"; do
    local tnum=$((idx + 1))
    from_clause+="${SCHEMA}.t${tnum} ${aliases[$idx]}"
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

  conds+=("a.k3 < 5")
  conds+=("j.k2 < 7")

  local where_clause="${conds[0]}"
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

  if [[ "$mode" == "all" ]]; then
    opt="-c geqo=off -c join_collapse_limit=20 -c from_collapse_limit=20 -c statement_timeout=${STATEMENT_TIMEOUT_MS}ms -c enable_left_deep_join=off -c enable_left_deep_join_on_missing_stats=off"
  elif [[ "$mode" == "off" ]]; then
    opt="-c geqo=off -c join_collapse_limit=20 -c from_collapse_limit=20 -c statement_timeout=${STATEMENT_TIMEOUT_MS}ms -c enable_left_deep_join=off -c enable_left_deep_join_on_missing_stats=off"
  else
    opt="-c geqo=off -c join_collapse_limit=20 -c from_collapse_limit=20 -c statement_timeout=${STATEMENT_TIMEOUT_MS}ms -c enable_left_deep_join=off -c enable_left_deep_join_on_missing_stats=on"
  fi

  PGOPTIONS="${opt}" "$PSQL_BIN" -XqAt -v ON_ERROR_STOP=1 -p "${PORT}" -d postgres >"${out_file}" <<SQL
EXPLAIN (FORMAT JSON)
${query}
SQL
}

plan_has_bushy() {
  local file="$1"
  perl -MJSON::PP -e '
my ($file) = @ARGV;
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

sub relset {
  my ($n) = @_;
  return {} unless ref($n) eq "HASH";
  my %set;
  if (defined $n->{"Relation Name"}) {
    $set{$n->{"Relation Name"}} = 1;
  }
  for my $c (@{$n->{Plans} || []}) {
    my $cs = relset($c);
    @set{keys %$cs} = values %$cs;
  }
  return \%set;
}

sub has_bushy {
  my ($n) = @_;
  return 0 unless ref($n) eq "HASH";
  my $plans = $n->{Plans} || [];
  if (is_join($n->{"Node Type"} // "") && @$plans >= 2) {
    my $ls = relset($plans->[0]);
    my $rs = relset($plans->[1]);
    return 1 if (scalar(keys %$ls) > 1 && scalar(keys %$rs) > 1);
  }
  for my $c (@$plans) {
    return 1 if has_bushy($c);
  }
  return 0;
}

exit(has_bushy($j->[0]{Plan}) ? 0 : 1);
' "$file"
}

first_bushy_right_table() {
  local file="$1"
  perl -MJSON::PP -e '
my ($file) = @ARGV;
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

sub relset {
  my ($n) = @_;
  return {} unless ref($n) eq "HASH";
  my %set;
  if (defined $n->{"Relation Name"}) {
    $set{$n->{"Relation Name"}} = 1;
  }
  for my $c (@{$n->{Plans} || []}) {
    my $cs = relset($c);
    @set{keys %$cs} = values %$cs;
  }
  return \%set;
}

sub pick_from_right_bushy {
  my ($n) = @_;
  return undef unless ref($n) eq "HASH";
  my $plans = $n->{Plans} || [];
  if (is_join($n->{"Node Type"} // "") && @$plans >= 2) {
    my $ls = relset($plans->[0]);
    my $rs = relset($plans->[1]);
    if (scalar(keys %$ls) > 1 && scalar(keys %$rs) > 1) {
      my @r = sort keys %$rs;
      return $r[0] if @r;
    }
  }
  for my $c (@$plans) {
    my $p = pick_from_right_bushy($c);
    return $p if defined $p;
  }
  return undef;
}

my $t = pick_from_right_bushy($j->[0]{Plan});
print $t if defined $t;
' "$file"
}

bushy_involving_table() {
  local file="$1"
  local table="$2"
  perl -MJSON::PP -e '
my ($file, $table) = @ARGV;
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

sub relset {
  my ($n) = @_;
  return {} unless ref($n) eq "HASH";
  my %set;
  if (defined $n->{"Relation Name"}) {
    $set{$n->{"Relation Name"}} = 1;
  }
  for my $c (@{$n->{Plans} || []}) {
    my $cs = relset($c);
    @set{keys %$cs} = values %$cs;
  }
  return \%set;
}

sub has_match {
  my ($n, $table) = @_;
  return 0 unless ref($n) eq "HASH";
  my $plans = $n->{Plans} || [];
  if (is_join($n->{"Node Type"} // "") && @$plans >= 2) {
    my $ls = relset($plans->[0]);
    my $rs = relset($plans->[1]);
    my $bushy = (scalar(keys %$ls) > 1 && scalar(keys %$rs) > 1);
    my $hit = exists($ls->{$table}) || exists($rs->{$table});
    return 1 if ($bushy && $hit);
  }
  for my $c (@$plans) {
    return 1 if has_match($c, $table);
  }
  return 0;
}

exit(has_match($j->[0]{Plan}, $table) ? 0 : 1);
' "$file" "$table"
}

echo "[4/9] Preparing all-stats schema (${SCHEMA})..."
prepare_schema ""

echo "[5/9] Searching for bushy query with all stats (tries: ${MAX_TRIES})..."
tmp_plan="$(mktemp)"
found=0
target_table=""
query=""

for ((try = 1; try <= MAX_TRIES; try++)); do
  if (( try % 100 == 0 )); then
    echo "  ... tried ${try}/${MAX_TRIES}"
  fi

  query="$(generate_random_query "$((SEED + try))")"
  if ! capture_plan all "${query}" "${tmp_plan}" 2>/dev/null; then
    continue
  fi

  if plan_has_bushy "${tmp_plan}"; then
    target_table="$(first_bushy_right_table "${tmp_plan}")"
    if [[ -n "${target_table}" ]]; then
      found=1
      printf '%s\n' "${query}" > "${QUERY_OUT}"
      cp "${tmp_plan}" "${ALL_PLAN}"
      printf '%s\n' "${target_table}" > "${TARGET_OUT}"
      echo "[6/9] Found bushy all-stats query at try ${try}; target=${target_table}"
      break
    fi
  fi
done

rm -f "${tmp_plan}"

if [[ "${found}" -eq 0 ]]; then
  echo "[6/9] No bushy all-stats query found in ${MAX_TRIES} tries"
  echo "Try increasing --max-tries or --rows"
  exit 1
fi

echo "[7/9] Rebuilding schema with missing stats only on ${target_table}..."
prepare_schema "${target_table}"

echo "[8/9] Capturing OFF/ON plans with target missing stats..."
capture_plan off "${query}" "${OFF_PLAN}"
capture_plan on "${query}" "${ON_PLAN}"

if bushy_involving_table "${OFF_PLAN}" "${target_table}"; then
  off_status="bushy-involving-target"
else
  off_status="not-bushy-involving-target"
fi

if bushy_involving_table "${ON_PLAN}" "${target_table}"; then
  on_status="bushy-involving-target"
else
  on_status="not-bushy-involving-target"
fi

echo "Summary:"
echo "  target table: ${target_table}"
echo "  all-stats plan: ${ALL_PLAN}"
echo "  missing-stats OFF: ${off_status} (${OFF_PLAN})"
echo "  missing-stats ON : ${on_status} (${ON_PLAN})"
echo "  query: ${QUERY_OUT}"
echo "  target-file: ${TARGET_OUT}"
