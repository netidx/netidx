set -e
# hostname
hostnamectl set-hostname router
sed -i 's/127\.0\.1\.1.*/127.0.1.1\trouter/' /etc/hosts || true

# net B side: static .60.2, NO gateway (router keeps its single default via net A)
if ! grep -q 'BEGIN netB router leg' /etc/network/interfaces; then
cat >> /etc/network/interfaces <<'EOF'

# BEGIN netB router leg
auto enp7s0
iface enp7s0 inet static
    address 192.168.60.2/24
# END netB router leg
EOF
fi
ifdown enp7s0 2>/dev/null || true
ifup enp7s0

# forwarding + loose rp_filter, persistent
cat > /etc/sysctl.d/99-router.conf <<'EOF'
net.ipv4.ip_forward=1
net.ipv4.conf.all.rp_filter=0
net.ipv4.conf.default.rp_filter=0
EOF
sysctl -q -p /etc/sysctl.d/99-router.conf

echo "=== RESULT ==="
echo "hostname: $(hostname)"
ip -br addr show enp1s0; ip -br addr show enp7s0
echo "ip_forward=$(cat /proc/sys/net/ipv4/ip_forward) rp_filter.all=$(cat /proc/sys/net/ipv4/conf/all/rp_filter)"
echo "--- FORWARD policy / rules ---"
{ nft list chain ip filter FORWARD 2>/dev/null || iptables -S FORWARD 2>/dev/null || echo "no nft/iptables FORWARD (default ACCEPT)"; }
echo "--- routes ---"; ip route
