#!/usr/bin/env bash
# Re-stage the lab harnesses + scripts into /tmp, where they were authored to
# live. The expect drivers and shell scripts reference each other (and the
# deployed binary) by absolute /tmp paths, so they must sit in /tmp to run as
# written. Run this once after a host reboot to restore the working lab env.
set -eu
here="$(cd "$(dirname "$0")" && pwd)"
cp -pv "$here"/harness/*.exp "$here"/scripts/* /tmp/
echo "restored $(ls "$here"/harness | wc -l) harnesses + $(($(ls "$here"/scripts | wc -l))) scripts to /tmp"
