pkill -9 -f 'component server run' 2>/dev/null
sleep 1
nohup /usr/local/bin/netidx conf component server run -c /root/.config/netidx/conf-server.json -f >/root/confserver.log 2>&1 &
echo "conf server started pid $!"
sleep 2
echo "4565 listeners: $(ss -ltn 2>/dev/null | grep -c 4565)"
