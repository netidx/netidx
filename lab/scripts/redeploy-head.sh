#!/bin/bash
# redeploy-head.sh [target-ip ...] — like redeploy.sh, but builds a pristine
# export of git HEAD instead of the working tree.
#
# The working tree is shared: other agents edit these repos while a lab session
# is running. redeploy.sh rsyncs the tree, so a concurrent edit lands in the
# binary under test — and since everything still compiles, nothing says so. A
# lab result is only meaningful if you know which commit produced the binary.
set -euo pipefail
DEV=192.168.50.14
SSH="ssh -F /dev/null -o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=15"
REPO=/home/eric/proj/netidx
EXPORT=$(mktemp -d /tmp/netidx-head.XXXXXX)
trap 'rm -rf "$EXPORT"' EXIT

REV=$(git -C "$REPO" rev-parse --short HEAD)
echo "[0/5] export HEAD ($REV) -> $EXPORT"
git -C "$REPO" archive HEAD | tar -x -C "$EXPORT"

echo "[1/5] sync -> devbox ($DEV)"
rsync -az --delete --exclude '/target/' -e "$SSH" "$EXPORT"/ root@$DEV:/root/netidx/

echo "[2/5] build on devbox"
t0=$(date +%s)
$SSH root@$DEV 'cd /root/netidx && . $HOME/.cargo/env && CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo build -p netidx-tools --bin netidx'
echo "    build took $(( $(date +%s) - t0 ))s"

echo "[3/5] strip"
$SSH root@$DEV 'strip -o /root/netidx.deploy /root/netidx/target/debug/netidx; ls -lh /root/netidx.deploy | awk "{print \"    stripped: \"\$5}"'
rsync -z -e "$SSH" root@$DEV:/root/netidx.deploy /tmp/netidx.deploy

echo "[4/5] deploy -> $*"
for ip in "$@"; do
  rsync -z -e "$SSH" /tmp/netidx.deploy root@"$ip":/usr/local/bin/netidx
  $SSH root@"$ip" 'chmod 755 /usr/local/bin/netidx'
done

echo "[5/5] built from $REV"
