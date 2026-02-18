#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/plan-stats-hypotheses.sh PLAN_JSON [options]

Given EXPLAIN (FORMAT JSON) output, infer plausible stats hypotheses that
could produce the same planning behavior.

Options:
  --selectivities CSV   Filter selectivity grid (default: 0.5,0.1,0.01)
  --output PATH         Write report to file (default: stdout)
  --help                Show help

Notes:
  - Input can be raw JSON or a psql output file that contains leading text.
  - This is heuristic guidance, not an exact inversion of planner stats.
EOF
}

if (($# < 1)); then
  usage >&2
  exit 2
fi

PLAN_FILE="$1"
shift

SELECTIVITIES="0.5,0.1,0.01"
OUTFILE=""

while (($# > 0)); do
  case "$1" in
    --selectivities) SELECTIVITIES="$2"; shift 2 ;;
    --output) OUTFILE="$2"; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ ! -f "${PLAN_FILE}" ]]; then
  echo "Plan file not found: ${PLAN_FILE}" >&2
  exit 1
fi

tmp_out="$(mktemp)"

perl -MJSON::PP -e '
use strict;
use warnings;

my ($plan_file, $sel_csv) = @ARGV;
open my $fh, "<", $plan_file or die "$plan_file: $!";
local $/;
my $txt = <$fh>;
close $fh;

$txt =~ s/^[^\[]*//s;
my $j = decode_json($txt);
my $root = $j->[0]{Plan};

sub is_join {
  my ($t) = @_;
  return defined($t) && $t =~ /^(Hash Join|Merge Join|Nested Loop)$/;
}

our @scans;
our @joins;
our $node_id = 0;

sub walk {
  my ($n, $path) = @_;
  return unless ref($n) eq "HASH";

  my $id = ++$node_id;
  my $nt = $n->{"Node Type"} // "";
  my $rows = $n->{"Plan Rows"} // 0;
  my $rel = $n->{"Relation Name"};
  my $alias = $n->{Alias};
  my $filter = $n->{Filter};

  if (defined $rel) {
    push @scans, {
      id => $id,
      path => $path,
      node_type => $nt,
      rel => $rel,
      alias => (defined $alias ? $alias : ""),
      rows => $rows + 0,
      has_filter => (defined $filter ? 1 : 0),
      filter => (defined $filter ? $filter : ""),
    };
  }

  my $plans = $n->{Plans} || [];
  if (is_join($nt) && @$plans >= 2) {
    my $left_rows = ($plans->[0]{"Plan Rows"} // 0) + 0;
    my $right_rows = ($plans->[1]{"Plan Rows"} // 0) + 0;
    my $out_rows = $rows + 0;

    my $cond = "";
    $cond = $n->{"Hash Cond"} if defined $n->{"Hash Cond"};
    $cond = $n->{"Merge Cond"} if $cond eq "" && defined $n->{"Merge Cond"};
    $cond = $n->{"Join Filter"} if $cond eq "" && defined $n->{"Join Filter"};

    my $sel = undef;
    my $ndv_req = undef;
    if ($left_rows > 0 && $right_rows > 0) {
      $sel = $out_rows / ($left_rows * $right_rows);
      if ($out_rows > 0) {
        $ndv_req = ($left_rows * $right_rows) / $out_rows;
      }
    }

    push @joins, {
      id => $id,
      path => $path,
      node_type => $nt,
      join_type => ($n->{"Join Type"} // ""),
      left_rows => $left_rows,
      right_rows => $right_rows,
      out_rows => $out_rows,
      cond => $cond,
      sel => $sel,
      ndv_req => $ndv_req,
    };
  }

  my $i = 0;
  for my $c (@$plans) {
    walk($c, $path . "." . $i);
    $i++;
  }
}

walk($root, "0");

my %rels;
for my $s (@scans) {
  my $r = $s->{rel};
  $rels{$r}{count}++;
  $rels{$r}{max_rows} = $s->{rows}
    if !defined($rels{$r}{max_rows}) || $s->{rows} > $rels{$r}{max_rows};
  $rels{$r}{has_filter} ||= $s->{has_filter};
  if ($s->{has_filter}) {
    push @{$rels{$r}{filters}}, $s->{filter};
  }
  push @{$rels{$r}{aliases}}, $s->{alias} if $s->{alias} ne "";
}

my @sels = grep { $_ > 0 && $_ <= 1 } map { $_ + 0 } split /,/, $sel_csv;
@sels = (0.5, 0.1, 0.01) unless @sels;

print "Plan stats hypotheses report\n";
print "Input file: $plan_file\n";
print "Root estimated rows: " . (($root->{"Plan Rows"} // 0) + 0) . "\n";
print "Scan nodes: " . scalar(@scans) . ", join nodes: " . scalar(@joins) . "\n";
print "\n";

print "=== Base relation observations ===\n";
print "relation\tmax_scan_rows\thas_filter\tscan_count\taliases\n";
for my $r (sort keys %rels) {
  my %alias_seen;
  $alias_seen{$_} = 1 for @{$rels{$r}{aliases} || []};
  my $aliases = join(",", sort keys %alias_seen);
  print join("\t",
             $r,
             ($rels{$r}{max_rows} // 0),
             ($rels{$r}{has_filter} ? "yes" : "no"),
             ($rels{$r}{count} // 0),
             $aliases) . "\n";
}
print "\n";

print "=== Join observations (estimation hints) ===\n";
print "id\tnode_type\tjoin_type\tleft_rows\tright_rows\tout_rows\tjoin_sel\tmax_ndv_hint\tcond\n";
for my $j (@joins) {
  my $sel = defined($j->{sel}) ? sprintf("%.3e", $j->{sel}) : "n/a";
  my $ndv = defined($j->{ndv_req}) ? sprintf("%.2f", $j->{ndv_req}) : "n/a";
  my $cond = $j->{cond};
  $cond =~ s/\s+/ /g;
  print join("\t",
             $j->{id},
             $j->{node_type},
             $j->{join_type},
             $j->{left_rows},
             $j->{right_rows},
             $j->{out_rows},
             $sel,
             $ndv,
             $cond) . "\n";
}
print "\n";

print "=== Candidate rowcount scenarios ===\n";
print "Interpretation: candidate reltuples that can plausibly lead to observed scan-row estimates.\n";
print "For filtered scans: reltuples ~= max_scan_rows / selectivity_assumption.\n";
print "For unfiltered scans: reltuples ~= max_scan_rows.\n\n";

for my $sel (@sels) {
  my $tag = sprintf("sel=%.3f", $sel);
  print "-- Scenario $tag\n";
  print "relation\tsuggested_reltuples\treason\n";
  for my $r (sort keys %rels) {
    my $scan_rows = $rels{$r}{max_rows} || 0;
    my $suggested = $scan_rows;
    my $reason = "no filter observed";
    if ($rels{$r}{has_filter}) {
      $suggested = int(($scan_rows / $sel) + 0.999999);
      $reason = "filtered scan rows / assumed selectivity";
    }
    print join("\t", $r, $suggested, $reason) . "\n";
  }
  print "\n";
}

print "=== Repro guidance ===\n";
print "1) Pick one scenario (sel=...) and generate table rowcounts close to suggested_reltuples.\n";
print "2) Keep join-key distributions similar across joined columns.\n";
print "3) For equality joins with tiny join_sel, raise NDV on one or both join sides.\n";
print "4) Re-run EXPLAIN and compare join tree shape first, then costs.\n";
print "\n";
print "Note: many stats configurations can map to the same plan; this script outputs plausible families, not a unique solution.\n";
' "${PLAN_FILE}" "${SELECTIVITIES}" > "${tmp_out}"

if [[ -n "${OUTFILE}" ]]; then
  cp "${tmp_out}" "${OUTFILE}"
  echo "Wrote: ${OUTFILE}"
else
  cat "${tmp_out}"
fi

rm -f "${tmp_out}"
