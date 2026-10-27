#!/bin/bash
# Runs the specification-conformance and high-occupancy tests that back the
# findings section, natively and (on Apple Silicon) as amd64 under Rosetta 2,
# because float-to-integer overflow differs between the two architectures.
# Usage (from p2p/discover): run_findings.sh <results-root>
set -euo pipefail
out="$1/findings"
mkdir -p "$out"
archs="$(go env GOHOSTARCH)"
[ "$(uname -m)" = arm64 ] && archs="arm64 amd64"
for arch in $archs; do
  header="# revision $(git rev-parse HEAD) dirty=$([ -z "$(git status --porcelain --untracked-files=no)" ] && echo false || echo true) goarch=$arch $(go version)"
  {
    echo "$header"
    GOARCH=$arch go test ./topicindex -count=1 -v \
      -run 'TestSpecIPScoreDeviation|TestSpecWaitTimeDeviations|TestSpecNoWaitCap|TestFullCacheWait|TestBoundedLowerBoundState' 2>&1
    GOARCH=$arch go test . -count=1 -v \
      -run 'TestRegisterHighOccupancy$|TestRegisterOverflowThreshold|TestRegisterFullCacheNaN|TestTicketOutlivesKey' 2>&1
  } > "$out/$arch.txt" || true
  # Strict variants are expected to fail with the current code.
  {
    echo "$header"
    TOPDISC_SPEC_STRICT=1 GOARCH=$arch go test ./topicindex -count=1 -v -run 'TestIPTreeCountersMatchContents' 2>&1
    TOPDISC_SPEC_STRICT=1 GOARCH=$arch go test . -count=1 -v -run 'TestRegisterHighOccupancyInvariants' 2>&1
  } > "$out/$arch-strict.txt" || true
done
