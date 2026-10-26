#!/bin/bash
# Controlled-fill memory runs and allocation audits.
# Usage (from p2p/discover): run_memory_audit.sh <results-root>
set -euo pipefail
root="$1"
bin="$root/bin/registrarbench"
mkdir -p "$root/memfill" "$root/audit"
"$(dirname "$0")/build.sh" "$bin"
for C in 1000 5000; do
  for dist in empirical diverse; do
    out="$root/memfill/C$C-$dist.jsonl"
    [ -s "$out" ] && grep -q memfill_bounds "$out" && continue
    "$bin" --mode memfill --cache-size "$C" --ad-lifetime 15m --ip-dist "$dist" \
      --memfill-reps 5 --output "$out"
  done
done
# Allocation audit at the realistic configuration (independent ENR copies, as
# production; decoding happens outside the instrumented bracket).
for seed in 1 2 3; do
  out="$root/audit/C1000-fresh5-seed$seed.jsonl"
  [ -s "$out" ] && tail -1 "$out" | grep -q summary && continue
  "$bin" --mode simulated --allocation-audit --cache-size 1000 --fresh-rate 5 --query-rate 20 \
    --advertisers 200000 --ad-lifetime 15m --duration 18h --warmup 6h --sample-interval 60s \
    --raw-ops=false --independent-enr=true --seed "$seed" --output "$out.partial"
  mv "$out.partial" "$out"
done
