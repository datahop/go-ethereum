#!/bin/bash
set -e

# NODE_MAPPINGS is a file with one line per node:
#   <private_ip> <public_ip> <type>
# where type is: public, natted, port-forwarded
#
# Example:
#   10.100.0.11 45.1.1.1 public
#   10.100.0.12 51.1.2.1 natted
#   10.100.0.13 64.1.3.1 port-forwarded

MAPPINGS_FILE="${MAPPINGS_FILE:-/config/node_mappings.txt}"
DISCV5_PORT="${DISCV5_PORT:-30303}"

if [ ! -f "$MAPPINGS_FILE" ]; then
    echo "ERROR: Mappings file not found: $MAPPINGS_FILE"
    exit 1
fi

# Enable IP forwarding.
echo 1 > /proc/sys/net/ipv4/ip_forward

# Create dummy interface for virtual public IPs.
ip link add dummy0 type dummy
ip link set dummy0 up

# Flush existing rules.
iptables -F FORWARD
iptables -t nat -F

# Default: allow outbound from all nodes, drop unsolicited inbound.
# Per-node rules below override as needed.

echo "Loading node mappings from $MAPPINGS_FILE"

while read -r PRIV_IP PUB_IP TYPE; do
    # Skip empty lines and comments.
    [ -z "$PRIV_IP" ] && continue
    [[ "$PRIV_IP" == \#* ]] && continue

    echo "  $PRIV_IP -> $PUB_IP ($TYPE)"

    # Add virtual public IP to dummy interface.
    ip addr add "$PUB_IP/32" dev dummy0 2>/dev/null || true

    # SNAT: outbound from private IP appears as public IP.
    iptables -t nat -A POSTROUTING -s "$PRIV_IP" -j SNAT --to-source "$PUB_IP"

    case "$TYPE" in
        public)
            # Full DNAT + unrestricted forwarding.
            iptables -t nat -A PREROUTING -d "$PUB_IP" -j DNAT --to-destination "$PRIV_IP"
            iptables -A FORWARD -d "$PRIV_IP" -j ACCEPT
            ;;
        port-forwarded)
            # DNAT only on the discv5 UDP port.
            iptables -t nat -A PREROUTING -d "$PUB_IP" -p udp --dport "$DISCV5_PORT" \
                -j DNAT --to-destination "$PRIV_IP:$DISCV5_PORT"
            iptables -A FORWARD -d "$PRIV_IP" -p udp --dport "$DISCV5_PORT" -j ACCEPT
            # Everything else: only established/related (responses to outbound).
            iptables -A FORWARD -d "$PRIV_IP" -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
            ;;
        natted)
            # No DNAT. Only responses to the node's own outbound get through.
            iptables -A FORWARD -d "$PRIV_IP" -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
            ;;
        *)
            echo "  WARNING: unknown type '$TYPE', treating as natted"
            iptables -A FORWARD -d "$PRIV_IP" -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
            ;;
    esac

    # Always allow outbound from this node.
    iptables -A FORWARD -s "$PRIV_IP" -j ACCEPT

done < "$MAPPINGS_FILE"

# Drop everything else.
iptables -A FORWARD -j DROP

echo ""
echo "Router ready. $(wc -l < "$MAPPINGS_FILE") node mappings loaded."
echo "iptables rules:"
iptables -L FORWARD -n --line-numbers
echo ""
iptables -t nat -L -n --line-numbers

# Keep running.
exec sleep infinity
