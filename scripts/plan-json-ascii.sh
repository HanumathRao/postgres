#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  cat <<'EOF'
Usage: scripts/plan-json-ascii.sh PLAN_JSON [PLAN_JSON ...]

Print an ASCII tree from EXPLAIN (FORMAT JSON) output.
It also marks any right-side subtree that starts with a join as:
  [RIGHT-JOIN-SUBTREE]
which indicates a non-left-deep shape at that point.
EOF
  exit 1
fi

perl -MJSON::PP -e '
use strict;
use warnings;

sub is_join {
  my ($t) = @_;
  return defined($t) && $t =~ /^(Hash Join|Merge Join|Nested Loop)$/;
}

sub unwrap_one_child {
  my ($n) = @_;
  return $n unless ref($n) eq "HASH";

  while (ref($n) eq "HASH" && ref($n->{Plans}) eq "ARRAY" && @{$n->{Plans}} == 1)
  {
    my $t = $n->{"Node Type"} // "";
    last if is_join($t);
    if ($t =~ /^(Hash|Sort|Materialize|Memoize|Gather|Gather Merge|Result|ProjectSet|Unique|Incremental Sort|Aggregate|Group|Limit)$/)
    {
      $n = $n->{Plans}[0];
      next;
    }
    last;
  }

  return $n;
}

sub starts_with_join {
  my ($n) = @_;
  $n = unwrap_one_child($n);
  return (ref($n) eq "HASH" && is_join($n->{"Node Type"} // ""));
}

sub node_label {
  my ($n) = @_;
  my @parts;

  push @parts, ($n->{"Node Type"} // "?");
  push @parts, "[" . $n->{"Join Type"} . "]" if defined $n->{"Join Type"};

  if (defined $n->{"Relation Name"})
  {
    my $rel = $n->{"Relation Name"};
    $rel .= " " . $n->{"Alias"} if defined $n->{"Alias"} && $n->{"Alias"} ne $rel;
    push @parts, "{" . $rel . "}";
  }
  elsif (defined $n->{"Alias"})
  {
    push @parts, "{".$n->{"Alias"}."}";
  }

  return join(" ", @parts);
}

sub print_tree {
  my ($n, $prefix, $is_last, $side, $is_root) = @_;

  my $branch = $is_root ? "" : ($is_last ? "\\- " : "|- ");
  my $line = $prefix . $branch . node_label($n);

  if ($side eq "R" && starts_with_join($n))
  {
    $line .= "  [RIGHT-JOIN-SUBTREE]";
  }

  print $line, "\n";

  my $plans = $n->{Plans};
  return unless ref($plans) eq "ARRAY" && @$plans > 0;

  my $next_prefix = $prefix . ($is_root ? "" : ($is_last ? "   " : "|  "));

  for (my $i = 0; $i < @$plans; $i++)
  {
    my $child = $plans->[$i];
    my $last = ($i == $#$plans);
    my $child_side = ($i == 0) ? "L" : "R";
    print_tree($child, $next_prefix, $last, $child_side, 0);
  }
}

for my $file (@ARGV)
{
  open my $fh, "<", $file or die "cannot open $file: $!";
  local $/;
  my $txt = <$fh>;
  close $fh;

  # Be forgiving if file contains leading status lines.
  $txt =~ s/^[^\[]*//s;

  my $json = decode_json($txt);
  my $root = $json->[0]{Plan};
  die "no root Plan in $file\n" unless ref($root) eq "HASH";

  print "=== $file ===\n";
  print_tree($root, "", 1, "ROOT", 1);
  print "\n";
}
' "$@"
