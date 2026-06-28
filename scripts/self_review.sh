#!/usr/bin/env bash
# Strategy self-review guardrails — run before merging any strategy / SL-TP /
# config change. Part of the loop in docs/self_review/SELF_REVIEW_WORKFLOW.md.
#
# Exit 0 only if the SL/TP & strategy-config invariants hold (known bugs are
# xfail, so they pass until fixed; a fixed bug XPASSes and -> fails CI, your
# cue to delete the xfail marker).
#
# Usage:
#   bash scripts/self_review.sh            # invariants + type-check
#   bash scripts/self_review.sh --full     # also run the engine test suite
set -uo pipefail
cd "$(dirname "$0")/.." || exit 2

PY="${PYTHON:-python3}"
rc=0

echo "==> [1/3] SL/TP & strategy-config invariants"
$PY -m pytest tests/engine/test_sltp_invariants.py -q -p no:cacheprovider || rc=1

echo
echo "==> [2/3] Type-check engine (mypy, advisory)"
if $PY -m mypy --version >/dev/null 2>&1; then
  $PY -m mypy src/nadobro/engine || echo "   (mypy issues above — advisory, not blocking)"
else
  echo "   mypy not installed; skipping"
fi

echo
if [[ "${1:-}" == "--full" ]]; then
  echo "==> [3/3] Engine test suite"
  $PY -m pytest tests/engine -q -p no:cacheprovider || rc=1
else
  echo "==> [3/3] Strategy checklist reminder"
  echo "   Review open items in docs/self_review/SELF_REVIEW_WORKFLOW.md"
  echo "   Run with --full to execute the whole engine suite."
fi

echo
if [[ $rc -eq 0 ]]; then
  echo "SELF-REVIEW PASS — invariants hold. (Open checklist items remain; see workflow doc.)"
else
  echo "SELF-REVIEW FAIL — an invariant regressed or a known bug was unexpectedly fixed."
  echo "If a guardrail XPASSed: delete its @pytest.mark.xfail in tests/engine/test_sltp_invariants.py."
fi
exit $rc
