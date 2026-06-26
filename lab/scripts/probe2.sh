echo "== $(hostname) =="
echo "--- links ---"; ip -br link | grep -v lo
echo "--- addr ---"; ip -br addr | grep -v '127.0.0.1'
echo "--- routes ---"; ip route
echo "--- interfaces ---"; cat /etc/network/interfaces
