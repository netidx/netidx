cd /home/eric/.config/netidx 2>/dev/null || cd /root/.config/netidx
echo "===== $(hostname) : $(pwd) ====="
echo "----- conf-server.json -----"; cat conf-server.json 2>/dev/null || echo "(none)"
echo "----- resolver.json -----"; cat resolver.json 2>/dev/null || echo "(none)"
echo "----- client.json -----"; cat client.json 2>/dev/null || echo "(none)"
echo "----- ca/ tree -----"; find ca -maxdepth 3 2>/dev/null | sort || echo "(no ca)"
echo "----- tls/ tree -----"; find tls -maxdepth 3 2>/dev/null | sort || echo "(no tls)"
