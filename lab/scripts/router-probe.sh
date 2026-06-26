set +e
echo "hostname: $(hostname)"
echo "--- links (MAC -> name) ---"; ip -br link
echo "--- addrs ---"; ip -br addr
echo "--- routes ---"; ip route
echo "--- ifupdown files ---"; ls -la /etc/network/interfaces /etc/network/interfaces.d/ 2>&1
echo "--- networkd/NM enabled? ---"; systemctl is-enabled systemd-networkd NetworkManager 2>&1
echo "--- /etc/network/interfaces ---"; cat /etc/network/interfaces 2>&1
echo "--- interfaces.d contents ---"; for f in /etc/network/interfaces.d/*; do echo "## $f"; cat "$f"; done 2>&1
echo "--- kernel cmdline ---"; cat /proc/cmdline
echo "--- ip_forward ---"; cat /proc/sys/net/ipv4/ip_forward
echo "--- netidx services ---"; systemctl list-units --type=service --all 2>/dev/null | grep -i netidx || echo "none"
