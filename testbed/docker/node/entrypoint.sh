#!/bin/bash
set -e

# Apply per-node bandwidth and latency shaping if configured.
# Both are optional. When set, NODE_BW_MBIT caps egress bandwidth and
# NODE_LATENCY_MS adds a fixed delay on egress.
if [ -n "$NODE_BW_MBIT" ] || [ -n "$NODE_LATENCY_MS" ]; then
    BW=${NODE_BW_MBIT:-1000}
    tc qdisc add dev eth0 root handle 1: htb default 11
    tc class add dev eth0 parent 1: classid 1:11 htb rate "${BW}mbit"
    if [ -n "$NODE_LATENCY_MS" ]; then
        tc qdisc add dev eth0 parent 1:11 handle 10: netem delay "${NODE_LATENCY_MS}ms"
    fi
fi

exec /usr/local/bin/devp2p "$@"
