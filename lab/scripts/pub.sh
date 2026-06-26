pkill -f 'netidx publisher' 2>/dev/null
sleep 1
nohup bash -c '(echo "/clustertest/foo|string|hello-from-.12"; sleep 180) | /usr/local/bin/netidx publisher -c /root/.config/netidx/client.json' >/root/pub.log 2>&1 &
echo "publisher launched pid $!"
sleep 3
echo "--- pub.log ---"; cat /root/pub.log 2>&1 | head -10
echo "--- is publisher alive? ---"; pgrep -af 'netidx publisher' | sed 's/^/  /' || echo "  NOT RUNNING"
