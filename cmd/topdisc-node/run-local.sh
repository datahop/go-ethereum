#!/usr/bin/env bash
# Start N topdisc-nodes on loopback: node 0 is the bootnode. Ports 30300+i, status 40300+i.
set -e
N=${1:-100}; BIN=${BIN:-./topdisc-node}; OUT=${OUT:-./local-run}
mkdir -p $OUT; pkill -f "topdisc-node -port" 2>/dev/null || true; sleep 1
$BIN -port 30300 -v 3 > $OUT/node0.log 2>&1 &
for i in $(seq 1 40); do E=$(curl -s 127.0.0.1:40300/status | python3 -c 'import json,sys;print(json.load(sys.stdin)["enode"])' 2>/dev/null) && break; sleep 0.5; done
[ -n "$E" ] || { echo "bootnode did not come up"; exit 1; }
echo "bootnode: $E"
for i in $(seq 1 $((N-1))); do $BIN -port $((30300+i)) -bootnodes "$E" -v 2 > $OUT/node$i.log 2>&1 & done
echo "started $N nodes; logs in $OUT/"
