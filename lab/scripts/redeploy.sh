#!/bin/bash
# redeploy.sh [target-ip ...] — sync repo to devbox, incremental-build there,
# strip, and deploy the stripped binary to each target VM. No args => devbox only.
set -euo pipefail
DEV=192.168.50.14
# The lab does not depend on host-wide SSH client configuration. In particular,
# a libvirt/systemd-generated ssh_config drop-in may have ownership OpenSSH
# refuses after a host restore, which should not prevent deploying to guests.
SSH="ssh -F /dev/null -o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=15"

echo "[1/4] sync repo -> devbox ($DEV)"
rsync -az --delete --exclude '/target/' --exclude '/.git/' \
  -e "$SSH" /home/eric/proj/netidx/ root@$DEV:/root/netidx/

echo "[2/4] incremental build on devbox"
t0=$(date +%s)
$SSH root@$DEV 'cd /root/netidx && . $HOME/.cargo/env && CARGO_INCREMENTAL=0 cargo build -p netidx-tools --bin netidx'
echo "    build took $(( $(date +%s) - t0 ))s"

echo "[3/4] strip"
$SSH root@$DEV 'strip -o /root/netidx.deploy /root/netidx/target/debug/netidx; ls -lh /root/netidx.deploy | awk "{print \"    stripped: \"\$5}"'
rsync -z -e "$SSH" root@$DEV:/root/netidx.deploy /tmp/netidx.deploy

echo "[4/4] deploy -> $*"
for ip in "$@"; do
  rsync -z -e "$SSH" /tmp/netidx.deploy root@"$ip":/usr/local/bin/netidx
  $SSH root@"$ip" 'chmod 755 /usr/local/bin/netidx; printf "    %s: " "$(hostname)"; netidx --version'
done
echo "done."
