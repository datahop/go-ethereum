#!/bin/bash
# Runs the topicindex microbenchmarks natively (arm64 on Apple Silicon), and the
# REGTOPIC validation benchmarks of p2p/discover twice: with cgo (libsecp256k1,
# as release builds of geth use) and without (pure-Go signature verification).
# Usage (from p2p/discover): run_microbench.sh <outdir> [count] [benchtime]
set -euo pipefail
out="$1"; count="${2:-10}"; bt="${3:-1s}"; fixed=20000x
[ "$bt" != 1s ] && fixed=500x
here="$(cd "$(dirname "$0")" && pwd)"
mkdir -p "$out"
arch=$(uname -m); [ "$arch" = x86_64 ] && arch=amd64
native() {
  case "$(file -b "$1")" in
    *"$(uname -m)"*) ;;
    *) echo "test binary is not native: $(file -b "$1")" >&2; exit 1 ;;
  esac
}
GOARCH=$arch go test -c -o "$out/topicindex.test" ./topicindex
GOARCH=$arch CGO_ENABLED=0 go test -c -o "$out/discover-purego.test" .
GOARCH=$arch CGO_ENABLED=1 CC="clang -arch $(uname -m)" go test -c -o "$out/discover-cgo.test" .
for b in topicindex discover-purego discover-cgo; do native "$out/$b.test"; done
"$here/env.sh" "$out/topicindex.test" > "$out/env.json"
# Benchmarks that pause the timer every iteration (StopTimer reads MemStats)
# use a fixed iteration count; the others use the default 1s benchtime.
(cd topicindex &&
  "$out/topicindex.test" -test.run '^$' -test.bench '^Benchmark(RegisterFresh|RegisterDuplicate|RandomNodes|IPScore|RemoveNode)$' -test.benchtime "$bt" -test.benchmem -test.count "$count" &&
  "$out/topicindex.test" -test.run '^$' -test.bench '^Benchmark(RegisterRetryAdmit|Expire|Occupancy)$' -test.benchtime "$fixed" -test.benchmem -test.count "$count") | tee "$out/bench.txt"
for v in cgo purego; do
  "$out/discover-$v.test" -test.run '^$' -test.bench '^BenchmarkValidation' -test.benchtime "$bt" -test.benchmem -test.count "$count" | tee "$out/validation-$v.txt"
done
rm -f "$out"/*.test
if command -v benchstat >/dev/null || [ -x "$HOME/go/bin/benchstat" ]; then
  bs=$(command -v benchstat || echo "$HOME/go/bin/benchstat")
  "$bs" "$out/bench.txt" > "$out/benchstat.txt" || true
  "$bs" cgo="$out/validation-cgo.txt" purego="$out/validation-purego.txt" > "$out/benchstat-validation.txt" || true
fi
