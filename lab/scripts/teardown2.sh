set +e
echo "== $(hostname) reset =="
# Match the executable name exactly. The deployed process may appear in argv as
# either `/usr/local/bin/netidx` or just `netidx`; path-only `pkill -f` misses
# the latter and can leave an orphan controller holding the CA lock.
pkill -9 -x netidx 2>/dev/null
# Lab publisher probes keep stdin open with a `sh -c (...; sleep N) | netidx
# publisher ...` wrapper. Killing the netidx child leaves that harmless but
# noisy wrapper behind, so remove the narrowly matched harness process too.
pkill -f 'sh -c .*netidx publisher -c /.*netidx/client.json' 2>/dev/null
units=$(systemctl list-units --no-legend 'netidx*' 2>/dev/null | awk '{print $1}')
[ -n "$units" ] && systemctl stop $units 2>/dev/null
ufiles=$(systemctl list-unit-files --no-legend 'netidx*' 2>/dev/null | awk '{print $1}')
[ -n "$ufiles" ] && systemctl disable $ufiles 2>/dev/null
pkill -f '/usr/local/bin/netidx'; pkill -f 'resolver-server'
pkill -9 -f '/usr/local/bin/netidx'; pkill -9 -f 'resolver-server'
rm -rf /root/.config/netidx /home/eric/.config/netidx /root/.local/share/netidx /home/eric/.local/share/netidx
rm -f /etc/systemd/system/netidx*.service
rm -f /etc/systemd/system/multi-user.target.wants/netidx* 2>/dev/null
systemctl daemon-reload 2>/dev/null
echo -n "  procs: "; pgrep -af netidx 2>/dev/null | grep -v pgrep || echo "none"
echo -n "  units: "; systemctl list-unit-files 'netidx*' --no-legend 2>/dev/null || echo "none"
echo -n "  cfg:   "; ls -d /root/.config/netidx /home/eric/.config/netidx 2>/dev/null || echo "gone"
