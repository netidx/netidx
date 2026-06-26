pkill -f 'netidx publisher' 2>/dev/null
sleep 1
nohup bash -c '(echo "/eu/foo|string|hello-from-eu-.15"; sleep 1800) | /usr/local/bin/netidx publisher -c /root/.config/netidx/client.json' >/root/pub.log 2>&1 &
echo "eu publisher launched pid $!"
sleep 3
echo "--- pub.log ---"; cat /root/pub.log 2>&1 | head
echo "--- alive? ---"; pgrep -af 'netidx publisher' | sed 's/^/  /' || echo "  NOT RUNNING"
