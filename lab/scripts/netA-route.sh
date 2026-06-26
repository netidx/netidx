set -e
# persistent, idempotent: route to net B via the router's net A leg
cat > /etc/network/if-up.d/netb-route <<'EOF'
#!/bin/sh
[ "$IFACE" = enp1s0 ] || exit 0
ip route replace 192.168.60.0/24 via 192.168.50.2 || true
EOF
chmod +x /etc/network/if-up.d/netb-route
ip route replace 192.168.60.0/24 via 192.168.50.2
echo "=== $(hostname) routes ==="; ip route | grep -E 'default|60\.0'
