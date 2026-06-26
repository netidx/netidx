pkill -f 'add-parent' 2>/dev/null
sleep 1
rm -f /root/addparent.log
setsid bash -c 'expect /tmp/add-parent.exp >/root/addparent.log 2>&1' </dev/null >/dev/null 2>&1 &
echo "launched add-parent, pid $!"
sleep 5
echo "--- addparent.log (should show glyph confirm + request code + polling) ---"
cat /root/addparent.log 2>&1
