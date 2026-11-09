#!/bin/bash
# Unattended overnight run of the registrar overhead study (no agent needed).
# Start from p2p/discover with:
#   nohup caffeinate -dimsu topicindex/cmd/registrarbench/study/run_overnight.sh > results/overnight.log 2>&1 &
# Stages run in order; each has a hard stop time (local, HH:MM). A failed or
# killed stage is logged and the next one starts. results/overnight-status.md
# is rewritten after every stage. The script ends by regenerating the write-up
# and committing the regenerated documents locally. It never pushes.
#
# Environment overrides (for the smoke test): ROOT, SEEDS, JOBS, MICRO_COUNT,
# STOP_SIMS, STOP_AUX, STOP_MICRO, STOP_REALTIME, STOP_ABANDON, SMOKE=1, NOCOMMIT=1.
set -uo pipefail
cd "$(dirname "$0")/../../../.."   # p2p/discover
study=topicindex/cmd/registrarbench/study
ROOT="${ROOT:-results/registrar-overhead}"
SEEDS="${SEEDS:-5}"
JOBS="${JOBS:-8}"
MICRO_COUNT="${MICRO_COUNT:-10}"
STOP_SIMS="${STOP_SIMS:-01:55}"
STOP_AUX="${STOP_AUX:-02:05}"
STOP_MICRO="${STOP_MICRO:-02:30}"
STOP_REALTIME="${STOP_REALTIME:-06:30}"
STOP_ABANDON="${STOP_ABANDON:-07:05}"
PYTHON="${PYTHON:-results/venv/bin/python}"
[ -x "$PYTHON" ] || PYTHON=python3
export PYTHON
status="$(dirname "$ROOT")/overnight-status.md"
[ "${SMOKE:-}" = 1 ] && status="$ROOT/overnight-status.md"
mkdir -p "$ROOT"
dl() { python3 "$study/with_deadline.py" "$@"; }

rows=()
write_status() {
  {
    echo "# Overnight run status"
    echo
    echo "Updated $(date '+%Y-%m-%d %H:%M:%S'). Revision $(git rev-parse --short HEAD). Results in \`$ROOT\`."
    echo
    echo "| stage | started | ended | result |"
    echo "|---|---|---|---|"
    for r in "${rows[@]}"; do echo "$r"; done
    echo
    echo "Log: \`results/overnight.log\`. Realtime point status: \`$ROOT/realtime/status.json\`."
  } > "$status"
}
stage() {
  local name="$1" stop="$2"; shift 2
  local t0; t0=$(date +%H:%M)
  echo "=== $(date '+%H:%M:%S') stage $name (stop $stop): $*"
  dl "$stop" "$@"
  local rc=$?
  local res="ok"
  [ $rc -eq 124 ] && res="stopped at $stop (partial)"
  [ $rc -ne 0 ] && [ $rc -ne 124 ] && res="failed (exit $rc)"
  echo "=== $(date '+%H:%M:%S') stage $name: $res"
  rows+=("| $name | $t0 | $(date +%H:%M) | $res |")
  write_status
}

write_status
if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
  echo "tracked files are modified; study builds would refuse to run" >&2
  rows+=("| preflight | $(date +%H:%M) | $(date +%H:%M) | failed: dirty tree |"); write_status; exit 1
fi

smoke_args=()
[ "${SMOKE:-}" = 1 ] && smoke_args=(--smoke)

# 2/3. Simulated sweeps: required experiments first, then topic skew.
stage sims "$STOP_SIMS" python3 "$study/run_sweeps.py" "$ROOT" --only demand query capacity ipdist bounded skew \
  --seeds "$SEEDS" --jobs "$JOBS" "${smoke_args[@]}"
stage findings "$STOP_AUX" "$study/run_findings.sh" "$ROOT"
stage memory-audit "$STOP_AUX" "$study/run_memory_audit.sh" "$ROOT" "${smoke_args[@]}"
# 2. Microbenchmarks on an otherwise idle machine.
stage microbench "$STOP_MICRO" "$study/run_microbench.sh" "$ROOT/microbench" "$MICRO_COUNT" "${MICRO_BENCHTIME:-1s}"
# 5. Realtime block; nothing else runs during it.
stage realtime "$STOP_REALTIME" python3 "$study/run_realtime.py" "$ROOT" --deadline "$STOP_REALTIME" "${smoke_args[@]}"
# 3 (optional). Abandonment-policy sweep, shortened to 36 h runs (24 h warm-up).
stage abandon "$STOP_ABANDON" python3 "$study/run_sweeps.py" "$ROOT" --only abandon --seeds "$SEEDS" --jobs 10 \
  --abandon-duration 36h --abandon-warmup 24h "${smoke_args[@]}"
# 6. Regenerate the write-up and commit the regenerated documents.
stage regenerate 23:59 "$study/regenerate.sh" "$ROOT" docs/registrar-overhead
if [ "${NOCOMMIT:-}" != 1 ]; then
  git add docs/registrar-overhead
  if git commit -q -m "p2p/discover/docs: regenerate the registrar overhead write-up from the overnight runs

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"; then
    rows+=("| commit | $(date +%H:%M) | $(date +%H:%M) | $(git rev-parse --short HEAD) |")
  else
    rows+=("| commit | $(date +%H:%M) | $(date +%H:%M) | nothing to commit or failed |")
  fi
fi
write_status
echo "=== $(date '+%H:%M:%S') overnight run finished"
