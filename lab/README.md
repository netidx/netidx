# netidx conf / CA test lab

A libvirt VM lab for exercising the `netidx conf` setup wizards, the CA + conf
server, resolver/publisher/workstation installs, delegation, remote service
control, and client housekeeping — across the anonymous, TLS, and Kerberos auth
schemes, over a WAN-shaped multi-site topology.

These files were authored in `/tmp` on the host and are kept here so a host
reboot doesn't lose them. **After a reboot, run `./restore-to-tmp.sh`** to
re-stage everything into `/tmp` — the expect drivers and shell scripts call
each other (and the deployed `/tmp/netidx.deploy` binary) by absolute `/tmp`
paths, so they must live in `/tmp` to run as written.

The stripped lab binary itself (`/tmp/netidx.deploy`, ~43 MB) is **not** kept
in the tree — rebuild it with `scripts/redeploy.sh`.

## libvirt

Everything is on the **system** libvirt instance, not the session one:

```sh
export LIBVIRT_DEFAULT_URI=qemu:///system   # or: virsh -c qemu:///system ...
```

The three lab networks (`netidx-test`, `netidx-eu`, `netidx-ap`) are persistent
+ autostart and are **not** torn down between runs. Domain names contain spaces
(`debian13 resolver0`), so quote them: `virsh start "debian13 resolver0"`.

## Topology

Three sites joined by one tri-homed netem WAN router (`scripts/wan` injects
loss/latency/partitions). HQ serves `/`, EU serves `/eu`, AP serves `/ap`.

| Domain                     | IP             | Role |
|----------------------------|----------------|------|
| `debian13 resolver0`       | 192.168.50.11  | **HQ keystone**: CA host + conf-server (:4565) + resolver member 0 + KDC. superuser `eric` / `testpw12345`. CA dir `/root/.config/netidx/ca` |
| `debian13 publisher`       | 192.168.50.12  | HQ resolver member 1 (disk named "publisher", repurposed) |
| `debian13 workstation`     | 192.168.50.13  | HQ workstation / subscriber |
| `debian13 dev`             | 192.168.50.14  | devbox / build box (16 G); rsync+build target for `redeploy.sh` |
| `debian13 hq-publisher`    | 192.168.50.17  | HQ publisher |
| `debian13 resolver1`       | 192.168.60.15  | EU satellite resolver member 0 (serves `/eu`) |
| `debian13 resolver2`       | 192.168.60.16  | EU satellite resolver member 1 |
| `debian13 eu-publisher`    | 192.168.60.17  | EU publisher |
| `debian13 eu-workstation`  | 192.168.60.18  | EU workstation |
| `debian13 ap-resolver-a`   | 192.168.70.11  | AP satellite resolver member 0 (serves `/ap`) |
| `debian13 ap-resolver-b`   | 192.168.70.12  | AP satellite resolver member 1 |
| `debian13 ap-publisher`    | 192.168.70.13  | AP publisher |
| `debian13 ap-workstation`  | 192.168.70.14  | AP workstation |
| `debian13 router`          | .50.2/.60.2/.70.2 | tri-homed netem WAN router; hosts `wan` at `/usr/local/bin/wan` |
| `win11`                    | management: DHCP on `default` (currently 192.168.122.10); netidx: DHCP on `netidx-test` (currently 192.168.50.163) | Windows 11 workstation test VM; passwordless SSH as `eric` |

Networks: `netidx-test` 192.168.50.0/24 (HQ, NAT), `netidx-eu` 192.168.60.0/24
(isolated — router is the only path off-subnet), `netidx-ap` 192.168.70.0/24
(isolated). HQ guests get a route to 60/70 via .50.2; EU/AP guests get their
default gw from DHCP (router .60.2/.70.2).

The Windows VM has two NICs. The libvirt `default` NAT network is its stable
management/SSH path and may need `virsh net-start default` after a host reboot.
The `netidx-test` NIC puts the workstation directly on the HQ network for
admin-plane and resolver testing; do not rely on the management NAT to route
the 50/60/70 lab networks. Discover the management address with
`virsh net-dhcp-leases default`, then connect without a password:

```sh
ssh eric@<windows-ip>
```

The Windows VM needs **routes to the satellite networks** that the Linux HQ
guests get from `/etc/network/if-up.d/netroutes`. Without them a Windows client
reaches HQ but not EU/AP, and a referral walk off-site fails with a bare
`oneshot canceled`. Add them once, from an elevated shell (they persist):

```
route -p add 192.168.60.0 mask 255.255.255.0 192.168.50.2
route -p add 192.168.70.0 mask 255.255.255.0 192.168.50.2
```

Do **not** put the netidx binaries in `C:\netidx` — that is the Windows
*system* config root (`paths::system_config_root`), and `netidx admin
uninstall` sweeps it. `C:\bin` is a good home.

To drive a publisher over SSH, keep the literal `path|type|value` line out of
`cmd`'s hands — inside a `( )` block `^|` is eaten and the line is split:

```
(type C:\bin\publine.txt & ping -n 600 127.0.0.1 >nul) | netidx.exe publisher -c %APPDATA%\netidx\client.json
```

If an older `win11` definition has only the management NIC, attach the HQ NIC
once (both live and persistent):

```sh
virsh -c qemu:///system attach-interface win11 network netidx-test \
  --model virtio --live --config
```

The Windows distribution consists of **two sibling executables**:
`netidx.exe` and the GUI-subsystem `netidx-activation.exe`. A workstation
install with its per-user logon task requires both. Cross-build them with:

```sh
cargo build -p netidx-tools \
  --bin netidx --bin netidx-activation --target x86_64-pc-windows-gnu
```

Deploying over a running install fails with `scp: dest open ... Failure` — the
running processes hold the image open. Stop them first, then start the logon
task again:

```
schtasks /end /tn netidx & taskkill /f /im netidx.exe & taskkill /f /im netidx-activation.exe
... scp ...
schtasks /run /tn netidx
```

There is no tmux on the Windows guest, so the TUI is driven from a host-side
pty instead. `scripts/win-tui-drive.py <rawlog> <steps>` opens `ssh -tt` on a
50×200 pty, replays a step file (`sleep <secs>` / `send <literal>` /
`mark <name>`) and records the raw stream; `scripts/win-tui-render.py <rawlog>
[mark]` replays that stream into a screen buffer and prints the frame as it
stood at `mark`. The renderer models only what ratatui emits and does not track
partial redraws, so read newly-drawn regions and ignore leftovers from an
earlier frame.

## Clean

VMs are throwaway (snapshot/discard). Per-host reset over SSH (root key-auth):

```sh
ssh root@<ip> 'bash -s' < scripts/teardown2.sh      # thorough: units, procs,
                                                     # ~/.config + ~/.local/share + /etc units
ssh root@<ip> 'bash -s' < scripts/teardown.sh        # lighter: only ~/.config/netidx
```

Use `teardown2.sh` for a truly pristine box. The KDC on .11 (realm
`NETIDX.TEST`) survives a reset. Networks are left alone — don't touch them.

Kerberos fixture credentials: the ordinary test principal is
`eric@NETIDX.TEST` with password `testpw12345`. Resolver and publisher service
principals live in each role VM's `/etc/krb5.keytab`; all six resolver VMs have
host-specific `netidx/resolver-<site>-<member>.netidx.test@NETIDX.TEST` entries,
and the three publisher VMs have matching `netidx/publisher-<site>.netidx.test`
entries. These keytabs and the KDC database intentionally survive config
teardown.

## Bring up

1. `virsh start` the domains you need (router first for a WAN run). Start
   `debian13 dev` for builds.
2. Deploy the binary: `scripts/redeploy.sh <ip> [<ip> ...]` — rsyncs the host
   repo to devbox .14, `cargo build -p netidx-tools --bin netidx`, strips, scps
   to each `/usr/local/bin/netidx`. (`scripts/build2.sh` / `build-fixed.sh` are
   convenience wrappers for specific host sets.)
3. Bootstrap the CA and first resolver on .11 with `netidx admin
   resolver install --with-admin-server --insecure-no-tpm ...`. The recovery
   password is **printed once** — capture it.
4. Enroll the rest with the strict `netidx admin ... install` commands or the
   bare `netidx admin` TUI. Approve queued identities on .11 with `netidx admin
   ca approve`; approve resolver hierarchy changes with `netidx admin resolver
   approve-delegation`.
5. Run daemons by hand with `scripts/start-member.sh <id>` (resolver + admin
   server), or launch the generated activation directory directly. The helper
   writes `nohup` logs under `/root`.

## Permanently removing a dead admin server

Use this only when a machine cannot run the normal `netidx admin uninstall`
path and will not return with its old identity. Inventory is keyed by immutable
server UUID, not by its mutable socket address:

```sh
netidx admin ca servers
netidx admin ca remove-server <exact-server-uuid> \
  --admin <name> --password-file <path>
```

The TUI exposes the same operation under **Admin Domain → Admin Servers**: select a
non-CA row and press `x`. It shows the UUID, last address, resolver cluster, and
an irreversible confirmation. The active CA is visible but protected.

Removal revokes every live serving certificate for the UUID, deletes its
enrollment grant, updates the CA-owned map, and pushes the resulting referral
topology to surviving resolvers. It never restarts a service. If a resolver
restart is needed, do the normal manual roll: restart one member, wait the
resolver `delay-reads` period for publishers to republish, then restart the next
member. Fanout failures identify both server UUID and address; repeating the
same removal UUID is the idempotent manual reconciliation path.

## Daemon start commands

```sh
# admin server (CA on .11, :4565), foreground:
netidx admin component server run -c /root/.config/netidx/admin-server.json -f
# resolver member (:4564), --id selects the resolver cluster member index:
netidx resolver-server -c /root/.config/netidx/resolver.json --id <N> -f
# the client housekeeping daemon (admin domain sync + cert renewal).
# --sync-interval is a minimum; each wait is drawn from it to twice it, so
# 20 gives a 20-40s cycle instead of the 12-24h default:
netidx admin component admin-agent run --sync-interval 20 -f
```

## CLI renamed: `netidx conf …` → `netidx admin …`

The CLI subcommand was renamed `conf` → `admin`, and the interactive prompt
layer was removed — `netidx admin` is now **strict** (every option is a flag or
it errors; there is no `$EDITOR`/y-n ceremony left in the CLI). The old
`harness/*.exp` expect scripts drive `netidx conf` with interactive prompts and
are therefore **stale** — keep them for reference, but drive the strict CLI
directly (flags) and drive the interactive experience through the **TUI**
(`netidx admin`, no subcommand) over tmux (below).

## Driving the TUI (tmux)

The `netidx admin` TUI (ratatui) needs a real PTY, so drive it from a detached
tmux session and snapshot the pane. Size the pane generously — the layout is
width/height sensitive.

```sh
# start a detached, fixed-size session running the TUI on a lab host (ssh -t
# for a PTY). Do this from the host; target any VM by ip.
tmux new-session -d -s tui -x 220 -y 50 \
  'ssh -tt -o StrictHostKeyChecking=no root@192.168.50.11 netidx admin'

sleep 2
tmux capture-pane -t tui -p          # snapshot the screen (pipe to a file/Read)

tmux send-keys  -t tui Down Down Enter   # navigate: arrows / Enter
tmux send-keys  -t tui Tab               # switch Local/Admin Domain tab
tmux send-keys  -t tui -l 'sometext'     # literal text (-l) into a field
tmux send-keys  -t tui Enter
tmux send-keys  -t tui Escape            # back / dismiss a dialog

tmux capture-pane -t tui -p          # re-snapshot after each step
tmux kill-session -t tui             # done
```

Notes: `capture-pane -p` prints the current buffer (use `-e` to keep ANSI
colors). Send one keystroke group, `sleep` briefly (the TUI polls its op future
on the UI task), then capture — don't blind-fire a whole sequence. For a
fresh-machine flow, teardown the host first (below) so the welcome/install path
runs. `Ctrl-c` quits: `tmux send-keys -t tui C-c`.

## Harnesses (`harness/*.exp`)

Grouped by function; names are prefixed so the directory listing sorts into
these groups. Some are **iteration variants** (`-fixed`, `-full`, `-retry`,
`-i`) kept for safety — prune to the canonical one once confirmed.

- **CA bootstrap / ops**: `ca-init.exp`, `ca-init-i.exp`, `ca-approve.exp`,
  `ca-peek.exp`, `ca-issue.exp`, `ca-issue-stolen.exp`, `ca-revoke.exp`,
  `recovery-rotate.exp`
- **Resolver installs**: `res.exp`, `res-install-11.exp`, `res-b-install.exp`,
  `res-hq-a-i.exp`, `res-anon-11.exp`, `res-anon-install.exp`, `res-krb5-11.exp`,
  `res-eu-a*.exp`, `install-resolver-krb5*.exp`, `install-anon-11.exp`,
  `install-asia.exp`, `drive-resolver-tls-real.exp`, `safe-drive-resolver.exp`,
  `dryrun-resolver-krb5.exp`
- **Publisher installs**: `pub.exp`, `pub-install.exp`, `pub-install-hq.exp`,
  `pub-anon-install.exp`, `pub-enroll-eric.exp`, `pub-krb5-install.exp`,
  `pub-krb5-dryrun.exp`, `approve-pub.exp`, `revoke-pub.exp`,
  `drive-publisher-enroll.exp`, `drive-ca-sign.exp`
- **Workstation installs**: `ws.exp`, `ws-install-hq.exp`, `ws-join-anon.exp`,
  `ws-join-cmd.exp`, `ws-krb5-install.exp`, `ws-krb5-dryrun.exp`,
  `drive-workstation.exp`, `drive-uninstall.exp`
- **Delegation**: `add-parent.exp`, `join-dryrun-12.exp`, `review-deleg.exp`,
  `review-deleg-fixed.exp`, `review.exp`, `review-retry.exp`, `review-deny.exp`,
  `sat1-install.exp`, `sat2-install.exp`, `verify-b.exp`
- **Admin / RBAC**: `add-role.exp`, `add-role-euops.exp`, `ca-approve-euops.exp`,
  `escalate-test.exp`
- **Perms edit**: `perms-edit.exp`, `perms-show.exp` (paired with the fake
  `$EDITOR` injectors `scripts/edit-add.sh` / `edit-inject.sh`)
- **Service control**: `sc.exp`
- **Anonymous conf**: `confserver-anon.exp`

## Scripts (`scripts/`)

- **Lifecycle**: `teardown.sh`, `teardown2.sh`, `start-member.sh`,
  `start-conf.sh`, `inv.sh` (inventory probe), `redeploy.sh`, `build2.sh`,
  `build-fixed.sh`, `capture.sh` (dump a host's configs), `probe2.sh` (net
  probe), `launch-install.sh`, `launch-addparent.sh`, `runpub.sh`, `pub.sh`,
  `pub-eu.sh`
- **Topology / router**: `wan` (2-leg netem helper), `wan3.sh` (3-leg; deployed
  to the router as `/usr/local/bin/wan`), `gendom.py` (domain XML generator),
  `hosts3seg` (unified `/etc/hosts`), `netA-route.sh`, `router-setup.sh`,
  `router-probe.sh`
- **Delegation tamper tests**: `drop_peer.py`, `untamper.py`,
  `verify_and_tamper.py`
