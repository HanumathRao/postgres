#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PSQL="${ROOT}/build/src/bin/psql/psql"
PGCTL="${ROOT}/build/src/bin/pg_ctl/pg_ctl"
POSTGRES_BIN="${ROOT}/build/src/backend/postgres"

PORT=55432
DATA_DIR=/tmp/pg_leftdeep
ROWS=6000
OUT_DIR=/tmp/ms_pruned_verify_plans
SUMMARY=/tmp/ms_pruned_verify_summary.tsv

usage() {
  cat <<EOF
Usage: scripts/verify-pruned-cases-no-target-bushy.sh [options]

Verifies pruned cases found earlier:
  - OFF plan has target table in a bushy subtree
  - ON  plan does NOT have target table in a bushy subtree

Options:
  --port N       PostgreSQL port (default: ${PORT})
  --data-dir D   Data directory (default: ${DATA_DIR})
  --rows N       Rows per table (default: ${ROWS})
  --out-dir D    Plan output dir (default: ${OUT_DIR})
  --help         Show this help
EOF
}

while (($# > 0)); do
  case "$1" in
    --port) PORT="$2"; shift 2 ;;
    --data-dir) DATA_DIR="$2"; shift 2 ;;
    --rows) ROWS="$2"; shift 2 ;;
    --out-dir) OUT_DIR="$2"; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

mkdir -p "${OUT_DIR}"

if ! "${PSQL}" -XqAt -p "${PORT}" -d postgres -c 'select 1' >/dev/null 2>&1; then
  "${PGCTL}" -D "${DATA_DIR}" -l /tmp/pg_leftdeep.log \
    -o "-p ${PORT} -c jit=off" -p "${POSTGRES_BIN}" -w start >/dev/null
fi

rebuild_target_schema() {
  local target="$1"
  local i

  "${PSQL}" -Xq -v ON_ERROR_STOP=1 -p "${PORT}" -d postgres <<SQL
DROP SCHEMA IF EXISTS ms_bushy_count CASCADE;
CREATE SCHEMA ms_bushy_count;
SQL

  for i in {1..10}; do
    "${PSQL}" -Xq -v ON_ERROR_STOP=1 -p "${PORT}" -d postgres <<SQL
CREATE TABLE ms_bushy_count.t${i} AS
SELECT g AS id,
       ((g * (11 + ${i})) % 2000) AS k1,
       ((g * (17 + ${i})) % 1000) AS k2,
       ((g * (23 + ${i})) % 100) AS k3
FROM generate_series(1, ${ROWS}) g;
SQL
  done

  "${PSQL}" -Xq -v ON_ERROR_STOP=1 -p "${PORT}" -d postgres <<SQL
ALTER TABLE ms_bushy_count.${target}
  SET (autovacuum_enabled=off, toast.autovacuum_enabled=off);
SQL

  for i in {1..10}; do
    local t="t${i}"
    if [[ "${t}" == "${target}" ]]; then
      continue
    fi
    "${PSQL}" -Xq -v ON_ERROR_STOP=1 -p "${PORT}" -d postgres \
      -c "ANALYZE ms_bushy_count.${t};"
  done
}

capture_plan_json() {
  local mode="$1"
  local query="$2"
  local out="$3"
  local opt

  if [[ "${mode}" == "off" ]]; then
    opt="-c jit=off -c geqo=off -c join_collapse_limit=20 -c from_collapse_limit=20 -c enable_left_deep_join=off -c enable_left_deep_join_on_missing_stats=off"
  else
    opt="-c jit=off -c geqo=off -c join_collapse_limit=20 -c from_collapse_limit=20 -c enable_left_deep_join=off -c enable_left_deep_join_on_missing_stats=on"
  fi

  PGOPTIONS="${opt}" "${PSQL}" -XqAt -v ON_ERROR_STOP=1 -p "${PORT}" -d postgres \
    -c "EXPLAIN (FORMAT JSON) ${query}" >"${out}"
}

capture_plan_text() {
  local mode="$1"
  local query="$2"
  local out="$3"
  local opt

  if [[ "${mode}" == "off" ]]; then
    opt="-c jit=off -c geqo=off -c join_collapse_limit=20 -c from_collapse_limit=20 -c enable_left_deep_join=off -c enable_left_deep_join_on_missing_stats=off"
  else
    opt="-c jit=off -c geqo=off -c join_collapse_limit=20 -c from_collapse_limit=20 -c enable_left_deep_join=off -c enable_left_deep_join_on_missing_stats=on"
  fi

  PGOPTIONS="${opt}" "${PSQL}" -XqAt -v ON_ERROR_STOP=1 -p "${PORT}" -d postgres \
    -c "EXPLAIN ${query}" >"${out}"
}

has_target_bushy() {
  local file="$1"
  local target="$2"
  perl -MJSON::PP -e '
my ($file,$target)=@ARGV;
open my $fh,"<",$file or die $!;
local $/; my $txt=<$fh>; close $fh;
$txt =~ s/^[^\[]*//s;
my $j = decode_json($txt);

sub is_join { my ($t)=@_; defined($t) && $t =~ /^(Hash Join|Merge Join|Nested Loop)$/; }
sub relset {
  my ($n)=@_;
  return {} unless ref($n) eq "HASH";
  my %s;
  $s{$n->{"Relation Name"}}=1 if defined $n->{"Relation Name"};
  for my $c (@{$n->{Plans} || []}) {
    my $cs = relset($c);
    @s{keys %$cs} = values %$cs;
  }
  return \%s;
}
sub walk {
  my ($n,$target)=@_;
  return 0 unless ref($n) eq "HASH";
  my $p = $n->{Plans} || [];
  if (is_join($n->{"Node Type"} // "") && @$p >= 2) {
    my $ls = relset($p->[0]);
    my $rs = relset($p->[1]);
    my $bushy = (scalar(keys %$ls) > 1 && scalar(keys %$rs) > 1);
    my $hit = exists($ls->{$target}) || exists($rs->{$target});
    return 1 if ($bushy && $hit);
  }
  for my $c (@$p) {
    return 1 if walk($c,$target);
  }
  return 0;
}
exit(walk($j->[0]{Plan},$target) ? 0 : 1);
' "${file}" "${target}"
}

query_text() {
  local qid="$1"
  case "${qid}" in
    query_1)
      cat <<'SQL'
SELECT count(*) FROM ms_bushy_count.t1 a, ms_bushy_count.t2 b, ms_bushy_count.t3 c, ms_bushy_count.t4 d, ms_bushy_count.t5 e, ms_bushy_count.t6 f, ms_bushy_count.t7 g, ms_bushy_count.t8 h, ms_bushy_count.t9 i, ms_bushy_count.t10 j WHERE b.k2 = a.k2 AND c.k2 = a.k2 AND d.k3 = c.k3 AND e.k1 = c.k1 AND f.k2 = e.k2 AND g.k2 = e.k2 AND h.k3 = a.k3 AND i.k3 = g.k3 AND j.k3 = a.k3 AND d.k1 = e.k1 AND b.k3 = i.k3 AND f.k1 = g.k1 AND f.k2 = e.k2 AND j.k2 = a.k2 AND d.k1 = i.k1 AND a.k2 = b.k2 AND g.k1 = c.k1 AND a.k1 = d.k1 AND e.k2 = a.k2 AND c.k1 = h.k1 AND g.k3 = j.k3 AND a.k3 < 5 AND j.k2 < 7
SQL
      ;;
    query_3)
      cat <<'SQL'
SELECT count(*) FROM ms_bushy_count.t1 a, ms_bushy_count.t2 b, ms_bushy_count.t3 c, ms_bushy_count.t4 d, ms_bushy_count.t5 e, ms_bushy_count.t6 f, ms_bushy_count.t7 g, ms_bushy_count.t8 h, ms_bushy_count.t9 i, ms_bushy_count.t10 j WHERE b.k3 = a.k3 AND c.k2 = a.k2 AND d.k3 = b.k3 AND e.k2 = d.k2 AND f.k3 = c.k3 AND g.k2 = e.k2 AND h.k2 = b.k2 AND i.k3 = h.k3 AND j.k2 = d.k2 AND g.k2 = c.k2 AND a.k3 = g.k3 AND j.k1 = a.k1 AND j.k1 = f.k1 AND i.k2 = b.k2 AND e.k1 = i.k1 AND i.k1 = c.k1 AND a.k3 = f.k3 AND c.k2 = h.k2 AND d.k3 = j.k3 AND b.k2 = a.k2 AND f.k1 = a.k1 AND a.k3 < 5 AND j.k2 < 7
SQL
      ;;
    query_4)
      cat <<'SQL'
SELECT count(*) FROM ms_bushy_count.t1 a, ms_bushy_count.t2 b, ms_bushy_count.t3 c, ms_bushy_count.t4 d, ms_bushy_count.t5 e, ms_bushy_count.t6 f, ms_bushy_count.t7 g, ms_bushy_count.t8 h, ms_bushy_count.t9 i, ms_bushy_count.t10 j WHERE b.k1 = a.k1 AND c.k3 = a.k3 AND d.k3 = b.k3 AND e.k2 = a.k2 AND f.k2 = c.k2 AND g.k2 = e.k2 AND h.k2 = b.k2 AND i.k1 = f.k1 AND j.k3 = g.k3 AND e.k3 = c.k3 AND a.k3 = c.k3 AND f.k1 = e.k1 AND c.k2 = f.k2 AND h.k2 = j.k2 AND j.k1 = a.k1 AND g.k3 = i.k3 AND j.k1 = f.k1 AND e.k1 = f.k1 AND g.k1 = j.k1 AND i.k3 = b.k3 AND b.k3 = j.k3 AND a.k3 < 5 AND j.k2 < 7
SQL
      ;;
    *)
      echo "Unknown query id: ${qid}" >&2
      return 1
      ;;
  esac
}

cases=(
  "t2 query_1"
  "t4 query_4"
  "t5 query_4"
  "t7 query_1"
  "t8 query_1"
  "t8 query_4"
  "t9 query_1"
  "t9 query_4"
  "t10 query_3"
)

printf "target\tquery\toff_target_bushy\ton_target_bushy\tresult\n" >"${SUMMARY}"

pass=0
fail=0
inconclusive=0

for c in "${cases[@]}"; do
  read -r target qid <<<"${c}"
  echo "[case ${target}/${qid}] rebuilding schema..."
  rebuild_target_schema "${target}"

  reltuples="$("${PSQL}" -XqAt -p "${PORT}" -d postgres -c \
    "SELECT reltuples FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='ms_bushy_count' AND c.relname='${target}';")"
  if [[ "${reltuples}" != "-1" ]]; then
    echo "Expected ${target}.reltuples = -1, got ${reltuples}" >&2
    exit 1
  fi

  q="$(query_text "${qid}")"
  off_json="${OUT_DIR}/${target}_${qid}.off.json"
  on_json="${OUT_DIR}/${target}_${qid}.on.json"
  off_txt="${OUT_DIR}/${target}_${qid}.off.txt"
  on_txt="${OUT_DIR}/${target}_${qid}.on.txt"

  capture_plan_json off "${q}" "${off_json}"
  capture_plan_json on  "${q}" "${on_json}"
  capture_plan_text off "${q}" "${off_txt}"
  capture_plan_text on  "${q}" "${on_txt}"

  off_bushy=0
  on_bushy=0
  has_target_bushy "${off_json}" "${target}" && off_bushy=1 || true
  has_target_bushy "${on_json}" "${target}" && on_bushy=1 || true

  if [[ "${off_bushy}" == "1" && "${on_bushy}" == "0" ]]; then
    result="PASS"
    pass=$((pass + 1))
  elif [[ "${off_bushy}" == "0" && "${on_bushy}" == "0" ]]; then
    result="INCONCLUSIVE_OFF_NOT_BUSHY"
    inconclusive=$((inconclusive + 1))
  else
    result="FAIL"
    fail=$((fail + 1))
  fi

  printf "%s\t%s\t%s\t%s\t%s\n" \
    "${target}" "${qid}" "${off_bushy}" "${on_bushy}" "${result}" >>"${SUMMARY}"
done

echo "Summary: pass=${pass} fail=${fail} inconclusive=${inconclusive}"
echo "Details: ${SUMMARY}"
echo "Plans: ${OUT_DIR}"

if [[ "${fail}" -gt 0 ]]; then
  exit 1
fi
