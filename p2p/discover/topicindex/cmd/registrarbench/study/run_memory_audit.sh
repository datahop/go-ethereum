#!/bin/bash
# Controlled-fill memory runs and allocation audits.
# Usage (from p2p/discover): run_memory_audit.sh <results-root> [--smoke]
set -euo pipefail
root="$1"
reps=5; levels="0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1"; audit_dur=18h; audit_warm=6h
if [ "${2:-}" = --smoke ]; then reps=1; levels="0,0.5,1"; audit_dur=40m; audit_warm=20m; fi
bin="$root/bin/registrarbench"
mkdir -p "$root/memfill" "$root/audit"
"$(dirname "$0")/build.sh" "$bin"
for C in 1000 5000; do
  for dist in empirical diverse; do
    out="$root/memfill/C$C-$dist.jsonl"
    [ -s "$out" ] && grep -q memfill_bounds "$out" && continue
    "$bin" --mode memfill --cache-size "$C" --ad-lifetime 15m --ip-dist "$dist" \
      --memfill-reps "$reps" --memfill-levels "$levels" --output "$out"
  done
done
# Records padded to near the 300-byte ENR size limit, for the upper bound.
for C in 1000 5000; do
  out="$root/memfill/C$C-empirical-maxenr.jsonl"
  [ -s "$out" ] && grep -q memfill_bounds "$out" && continue
  "$bin" --mode memfill --cache-size "$C" --ad-lifetime 15m --ip-dist empirical --enr-pad 160 \
    --memfill-reps "$reps" --memfill-levels "$levels" --output "$out"
done
# Allocation audit at the realistic configuration (independent ENR copies, as
# production; decoding happens outside the instrumented bracket).
for seed in 1 2 3; do
  out="$root/audit/C1000-fresh5-seed$seed.jsonl"
  [ -s "$out" ] && tail -1 "$out" | grep -q summary && continue
  "$bin" --mode simulated --allocation-audit --cache-size 1000 --fresh-rate 5 --query-rate 20 \
    --advertisers 200000 --ad-lifetime 15m --duration "$audit_dur" --warmup "$audit_warm" --sample-interval 60s \
    --raw-ops=false --independent-enr=true --seed "$seed" --output "$out.partial"
  mv "$out.partial" "$out"
done
