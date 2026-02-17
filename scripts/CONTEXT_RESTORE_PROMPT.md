# Context Restore Prompt

```text
You are continuing work in /home/hmaduri/contribs/postgres.

Objective:
- Validate and iterate planner behavior for missing-stats-aware bushy pruning:
  - missing-stats table must not participate in bushy subtree when policy is ON
  - bushy joins among analyzed tables may still exist

Read first:
- scripts/LEFTDEEP_RUNBOOK.md

Primary code files:
- src/backend/optimizer/path/joinrels.c
- src/backend/optimizer/path/allpaths.c
- src/backend/utils/misc/guc_parameters.dat
- src/include/optimizer/paths.h

Primary harness scripts:
- scripts/find-missing-stats-proof.sh
- scripts/target-missing-stats-from-bushy.sh
- scripts/verify-pruned-cases-no-target-bushy.sh

Immediate checklist:
1) Start patched server on port 55432.
2) Run scripts/verify-pruned-cases-no-target-bushy.sh.
3) Report pass/fail summary.
4) If any failure: list case IDs and show minimal code or harness fix.
5) Keep artifacts in /tmp and include paths in final report.
```

