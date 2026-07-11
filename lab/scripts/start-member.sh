set +e
ID="${1:-0}"
D=/root/.config/netidx
pkill -f 'resolver-server -c'; pkill -f 'component server run'
sleep 1
pkill -9 -f 'resolver-server -c'; pkill -9 -f 'component server run'
sleep 1
nohup /usr/local/bin/netidx resolver-server -c "$D/resolver.json" --id "$ID" -f >/root/resolver.log 2>&1 &
nohup /usr/local/bin/netidx admin component server run -c "$D/admin-server.json" -f >/root/admin-server.log 2>&1 &
echo "started member id=$ID on $(hostname): resolver+admin-server (nohup)"
