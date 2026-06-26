echo "== $(hostname) $(hostname -I 2>/dev/null) =="
for u in /root /home/eric; do
  d="$u/.config/netidx"
  if [ -d "$d" ]; then
    echo "-- $d --"; ls -1 "$d" 2>/dev/null
    [ -f "$d/install.json" ] && echo "  install.json: $(cat "$d/install.json" 2>/dev/null)"
  fi
done
echo "-- running netidx procs --"; pgrep -af 'netidx|resolver-server' 2>/dev/null | grep -v pgrep || echo "  none"
echo "-- netidx system units --"; systemctl list-unit-files 'netidx*' --no-legend 2>/dev/null | head || echo "  none"
echo "-- krb5 realm (cosmetic, only matters for krb5) --"; [ -f /etc/krb5.conf ] && grep -E 'default_realm' /etc/krb5.conf 2>/dev/null || echo "  no krb5.conf"
