set +e
echo "== $(hostname) teardown =="
# stop + disable any netidx systemd units (system scope; templates registered via sudo)
units=$(systemctl list-units --no-legend 'netidx*' 2>/dev/null | awk '{print $1}')
[ -n "$units" ] && systemctl stop $units 2>/dev/null
ufiles=$(systemctl list-unit-files --no-legend 'netidx*' 2>/dev/null | awk '{print $1}')
[ -n "$ufiles" ] && systemctl disable $ufiles 2>/dev/null
# kill any netidx processes (activation daemon, resolver-server, admin server, admin agent)
pkill -f '/usr/local/bin/netidx'; pkill -f 'resolver-server'
pkill -9 -f '/usr/local/bin/netidx'; pkill -9 -f 'resolver-server'
# wipe config dirs for both users (mixed root/eric scheme across the lab)
rm -rf /root/.config/netidx /home/eric/.config/netidx
echo "  remaining netidx procs:"; pgrep -af netidx 2>/dev/null | grep -v pgrep | sed 's/^/    /' || echo "    none"
echo "  remaining config dirs:"; { ls -d /root/.config/netidx /home/eric/.config/netidx 2>/dev/null | sed 's/^/    /'; } || true
echo "  netidx unit-files left:"; systemctl list-unit-files 'netidx*' --no-legend 2>/dev/null | sed 's/^/    /' || echo "    none"
