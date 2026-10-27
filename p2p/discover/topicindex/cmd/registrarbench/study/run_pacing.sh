#!/bin/bash
# Sleep vs spin pacing check before the realtime sweep: two load points, each
# with 15 min of real-time measurement after a 6 h simulated warm-up. Runs are
# serial; the machine should otherwise be idle.
# Usage (from p2p/discover): run_pacing.sh <results-root>
set -euo pipefail
root="$1"
bin="$root/bin/registrarbench"
mkdir -p "$root/pacing"
"$(dirname "$0")/build.sh" "$bin"
for point in "5 20" "100 1000"; do
  set -- $point
  for pacing in sleep spin; do
    out="$root/pacing/C1000-fresh$1-query$2-$pacing.jsonl"
    [ -s "$out" ] && tail -1 "$out" | grep -q summary && continue
    "$bin" --mode hybrid --pacing "$pacing" --arrivals poisson --cache-size 1000 \
      --fresh-rate "$1" --query-rate "$2" --advertisers 200000 --ad-lifetime 15m \
      --duration 6h15m --warmup 6h --sample-interval 10s --raw-ops=true \
      --independent-enr=false --seed 1 --output "$out.partial"
    mv "$out.partial" "$out"
  done
done
