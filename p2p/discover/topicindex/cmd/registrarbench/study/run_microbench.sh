#!/bin/bash
# Runs the topicindex microbenchmarks natively (arm64 on Apple Silicon).
# Usage (from p2p/discover): run_microbench.sh <outdir> [count]
set -euo pipefail
out="$1"; count="${2:-10}"
here="$(cd "$(dirname "$0")" && pwd)"
mkdir -p "$out"
arch=$(uname -m); [ "$arch" = x86_64 ] && arch=amd64
GOARCH=$arch go test -c -o "$out/topicindex.test" ./topicindex
"$here/env.sh" "$out/topicindex.test" > "$out/env.json"
case "$(file -b "$out/topicindex.test")" in
  *"$(uname -m)"*) ;;
  *) echo "test binary is not native: $(file -b "$out/topicindex.test")" >&2; exit 1 ;;
esac
# Benchmarks that pause the timer every iteration (StopTimer reads MemStats)
# use a fixed iteration count; the others use the default 1s benchtime.
(cd topicindex &&
  "$out/topicindex.test" -test.run '^$' -test.bench '^Benchmark(RegisterFresh|RegisterDuplicate|RandomNodes|IPScore|RemoveNode)$' -test.benchmem -test.count "$count" &&
  "$out/topicindex.test" -test.run '^$' -test.bench '^Benchmark(RegisterRetryAdmit|Expire)$' -test.benchtime 20000x -test.benchmem -test.count "$count") | tee "$out/bench.txt"
