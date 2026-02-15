#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/visualize-plan-failures-html.sh [options]

Generate a self-contained HTML report for failing plans from a plans directory.

Checks:
  no-intermediate-hash-build  FAIL when a Hash Join builds hash table from an
                              intermediate join subtree on the inner side.
  leftdeep-shape              FAIL when any join has a right-side join subtree.

Options:
  --plans-dir PATH            Directory containing *.off.json / *.on.json
                              (default: /tmp/leftdeep_policy_plans)
  --output PATH               Output HTML file
                              (default: /tmp/leftdeep_plan_failures.html)
  --check NAME                no-intermediate-hash-build | leftdeep-shape
                              (default: no-intermediate-hash-build)
  --mode NAME                 off | on | both  (default: both)
  --help                      Show help
EOF
}

PLANS_DIR=/tmp/leftdeep_policy_plans
OUT_HTML=/tmp/leftdeep_plan_failures.html
CHECK=no-intermediate-hash-build
MODE=both

while (($# > 0)); do
  case "$1" in
    --plans-dir) PLANS_DIR="$2"; shift 2 ;;
    --output) OUT_HTML="$2"; shift 2 ;;
    --check) CHECK="$2"; shift 2 ;;
    --mode) MODE="$2"; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ ! -d "$PLANS_DIR" ]]; then
  echo "Plans dir not found: $PLANS_DIR" >&2
  exit 1
fi

if [[ "$CHECK" != "no-intermediate-hash-build" && "$CHECK" != "leftdeep-shape" ]]; then
  echo "Invalid --check: $CHECK" >&2
  exit 2
fi

if [[ "$MODE" != "off" && "$MODE" != "on" && "$MODE" != "both" ]]; then
  echo "Invalid --mode: $MODE" >&2
  exit 2
fi

mkdir -p "$(dirname "$OUT_HTML")"

matches_mode() {
  local f="$1"
  case "$MODE" in
    both) return 0 ;;
    off) [[ "$f" == *.off.json ]] ;;
    on) [[ "$f" == *.on.json ]] ;;
  esac
}

fails_check() {
  local file="$1"
  local check="$2"

  if [[ "$check" == "no-intermediate-hash-build" ]]; then
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

exit(has_bad_hash_build($j->[0]{Plan}) ? 0 : 1);
' "$file"
  else
    perl -MJSON::PP -e '
my $txt = do { local $/; <> };
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

exit(bad_shape($j->[0]{Plan}) ? 0 : 1);
' "$file"
  fi
}

HTML_TMP="$(mktemp)"
trap 'rm -f "$HTML_TMP"' EXIT

cat > "$HTML_TMP" <<EOF
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Plan Failures</title>
  <style>
    :root { --bg:#f6f8fb; --fg:#1f2937; --muted:#475569; --card:#ffffff; --line:#cbd5e1; --bad:#b91c1c; --good:#065f46; }
    body { margin:0; font:14px/1.4 ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; color:var(--fg); background:linear-gradient(180deg,#f8fafc 0%,#eef2ff 100%); }
    .wrap { max-width:1200px; margin:24px auto; padding:0 16px 24px; }
    h1 { margin:0 0 8px; font-size:24px; }
    .meta { color:var(--muted); margin-bottom:16px; }
    .card { background:var(--card); border:1px solid var(--line); border-radius:10px; margin:14px 0; box-shadow:0 1px 2px rgba(0,0,0,.04); }
    .head { padding:10px 12px; border-bottom:1px solid var(--line); display:flex; gap:10px; align-items:center; flex-wrap:wrap; }
    .name { font-weight:700; }
    .badge { border-radius:999px; padding:1px 8px; font-size:12px; border:1px solid transparent; }
    .fail { color:#fff; background:var(--bad); }
    .mode { color:#111827; background:#e2e8f0; border-color:#cbd5e1; }
    .reason { color:#111827; background:#dbeafe; border-color:#93c5fd; }
    .body { padding:10px 12px; display:grid; grid-template-columns: 1fr 1fr; gap:12px; }
    .treebox, .rawbox { border:1px solid var(--line); border-radius:8px; padding:10px; background:#f8fafc; overflow:auto; }
    .treebox ul { list-style:none; margin:0; padding-left:18px; border-left:1px dashed #cbd5e1; }
    .treebox li { margin:4px 0; }
    .node { white-space:nowrap; }
    .node.bad { color:var(--bad); font-weight:700; }
    pre { margin:0; white-space:pre-wrap; word-break:break-word; }
    .empty { padding:16px; background:#ecfeff; border:1px solid #a5f3fc; border-radius:8px; color:#0f172a; }
    @media (max-width: 900px) { .body { grid-template-columns:1fr; } }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Failing Plan Visualization</h1>
    <div class="meta">check=<span id="checkName"></span> mode=<span id="modeName"></span> source=<span id="srcDir"></span></div>
    <div id="cards"></div>
  </div>
  <script>
    const CHECK = ${CHECK@Q};
    const MODE = ${MODE@Q};
    const SRC_DIR = ${PLANS_DIR@Q};
    const ITEMS = [];
  </script>
EOF

count=0
for f in "$PLANS_DIR"/*.json; do
  [[ -e "$f" ]] || continue
  if ! matches_mode "$f"; then
    continue
  fi
  if ! fails_check "$f" "$CHECK"; then
    continue
  fi

  base="$(basename "$f")"
  query="${base%.off.json}"
  query="${query%.on.json}"
  mode="off"
  [[ "$base" == *.on.json ]] && mode="on"
  reason="$CHECK"

  plan_json="$(perl -0pe 's/^[^\[]*//s' "$f")"
  plan_json="${plan_json//<\/script>/<\\/script>}"

  cat >> "$HTML_TMP" <<EOF
  <script type="application/json" class="plan-json"
          data-query="${query}"
          data-mode="${mode}"
          data-file="${f}"
          data-reason="${reason}">
${plan_json}
  </script>
EOF
  count=$((count + 1))
done

cat >> "$HTML_TMP" <<'EOF'
  <script>
    const WRAPPER_NODE = /^(Hash|Sort|Materialize|Memoize|Gather|Gather Merge|Result|ProjectSet|Unique|Incremental Sort|Aggregate|Group|Limit)$/;
    const JOIN_NODE = /^(Hash Join|Merge Join|Nested Loop)$/;

    function isJoin(n) {
      return !!(n && JOIN_NODE.test(n["Node Type"] || ""));
    }

    function unwrapOneChild(n) {
      let cur = n;
      while (cur && cur.Plans && cur.Plans.length === 1) {
        const t = cur["Node Type"] || "";
        if (isJoin(cur)) break;
        if (!WRAPPER_NODE.test(t)) break;
        cur = cur.Plans[0];
      }
      return cur;
    }

    function hasBadShape(n) {
      if (!n || typeof n !== "object") return false;
      const plans = n.Plans || [];
      if (isJoin(n) && plans.length >= 2) {
        const right = unwrapOneChild(plans[1]);
        if (isJoin(right)) return true;
      }
      for (const c of plans) {
        if (hasBadShape(unwrapOneChild(c))) return true;
      }
      return false;
    }

    function hasBadHashBuild(n) {
      if (!n || typeof n !== "object") return false;
      const plans = n.Plans || [];
      if ((n["Node Type"] || "") === "Hash Join" && plans.length >= 2) {
        const inner = plans[1];
        if (inner && (inner["Node Type"] || "") === "Hash" && inner.Plans && inner.Plans[0]) {
          if (isJoin(inner.Plans[0])) return true;
        }
      }
      for (const c of plans) {
        if (hasBadHashBuild(c)) return true;
      }
      return false;
    }

    function markBad(n, check) {
      if (!n || typeof n !== "object") return false;
      const plans = n.Plans || [];
      let bad = false;

      if (check === "leftdeep-shape" && isJoin(n) && plans.length >= 2) {
        const right = unwrapOneChild(plans[1]);
        if (isJoin(right)) bad = true;
      }

      if (check === "no-intermediate-hash-build" && (n["Node Type"] || "") === "Hash Join" && plans.length >= 2) {
        const inner = plans[1];
        if (inner && (inner["Node Type"] || "") === "Hash" && inner.Plans && inner.Plans[0] && isJoin(inner.Plans[0])) {
          bad = true;
        }
      }

      for (const c of plans) {
        const childBad = markBad(c, check);
        bad = bad || childBad;
      }
      n.__bad = bad;
      return bad;
    }

    function nodeLabel(n) {
      const t = n["Node Type"] || "Unknown";
      const jt = n["Join Type"] ? ` [${n["Join Type"]}]` : "";
      const rel = n["Relation Name"] ? ` {${n["Relation Name"]}${n["Alias"] ? " " + n["Alias"] : ""}}` : "";
      return `${t}${jt}${rel}`;
    }

    function renderNode(n, parent) {
      const li = document.createElement("li");
      const sp = document.createElement("span");
      sp.className = "node" + (n.__bad ? " bad" : "");
      sp.textContent = nodeLabel(n);
      li.appendChild(sp);
      const plans = n.Plans || [];
      if (plans.length) {
        const ul = document.createElement("ul");
        for (const c of plans) renderNode(c, ul);
        li.appendChild(ul);
      }
      parent.appendChild(li);
    }

    function renderCard(it) {
      const root = it.plan[0]?.Plan || it.plan.Plan || it.plan;
      markBad(root, CHECK);

      const card = document.createElement("div");
      card.className = "card";
      card.innerHTML = `
        <div class="head">
          <span class="badge fail">FAIL</span>
          <span class="name">${it.query}</span>
          <span class="badge mode">${it.mode}</span>
          <span class="badge reason">${it.reason}</span>
          <span>${it.file}</span>
        </div>
        <div class="body">
          <div class="treebox"><strong>Plan Tree</strong><div class="tree"></div></div>
          <div class="rawbox"><strong>Raw JSON</strong><pre></pre></div>
        </div>`;

      const ul = document.createElement("ul");
      renderNode(root, ul);
      card.querySelector(".tree").appendChild(ul);
      card.querySelector("pre").textContent = JSON.stringify(it.plan, null, 2);
      return card;
    }

    document.getElementById("checkName").textContent = CHECK;
    document.getElementById("modeName").textContent = MODE;
    document.getElementById("srcDir").textContent = SRC_DIR;

    const tags = Array.from(document.querySelectorAll("script.plan-json"));
    for (const t of tags) {
      try {
        ITEMS.push({
          query: t.dataset.query,
          mode: t.dataset.mode,
          file: t.dataset.file,
          reason: t.dataset.reason,
          plan: JSON.parse(t.textContent)
        });
      } catch (e) {
        ITEMS.push({
          query: t.dataset.query,
          mode: t.dataset.mode,
          file: t.dataset.file,
          reason: `${t.dataset.reason} (parse error: ${e.message})`,
          plan: [{ Plan: { "Node Type": "Invalid JSON" } }]
        });
      }
      t.remove();
    }

    const cards = document.getElementById("cards");
    if (!ITEMS.length) {
      const d = document.createElement("div");
      d.className = "empty";
      d.textContent = "No failing plans found for the selected check/mode.";
      cards.appendChild(d);
    } else {
      for (const it of ITEMS) cards.appendChild(renderCard(it));
    }
  </script>
</body>
</html>
EOF

mv "$HTML_TMP" "$OUT_HTML"
trap - EXIT

echo "Generated HTML report: $OUT_HTML"
echo "Failing plans included: $count"
