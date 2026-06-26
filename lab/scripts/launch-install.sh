pkill -9 -f 'resolver install' 2>/dev/null
pkill -9 -f 'resolver-server -c' 2>/dev/null
rm -rf /root/.config/netidx
rm -f /root/install-asia.log
setsid bash -c 'expect /tmp/install-asia.exp >/root/install-asia.log 2>&1' </dev/null >/dev/null 2>&1 &
echo "launched install-asia pid $!"
sleep 6
echo "--- install-asia.log so far ---"
cat /root/install-asia.log 2>&1
