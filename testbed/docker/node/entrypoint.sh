#!/bin/bash
set -e

# Set default gateway to router if GATEWAY is set.
if [ -n "$GATEWAY" ]; then
    echo "Setting default gateway to $GATEWAY"
    ip route del default 2>/dev/null || true
    ip route add default via "$GATEWAY"
fi

exec "$@"
