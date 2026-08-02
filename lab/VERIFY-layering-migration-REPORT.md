# Lab verification report — the netidx-admin layering migration

Covers `93c95e4e`..`80790626` on the 3-site WAN lab, from a clean slate,
through the installed OS service. Plan: `VERIFY-layering-migration.md`.

Four complete build-outs, each a two-level hierarchy with cross-site data:

| Pass | Frontend | Data plane | Levels | Windows |
|---|---|---|---|---|
| A | strict CLI | TLS | `/` + `/eu` | yes |
| B | TUI | TLS | `/` + `/eu` + `/ap` | yes |
| C | strict CLI | krb5 | `/` + `/eu` | n/a |
| D | TUI | krb5 | `/` + `/eu` | n/a |

Windows is deliberately not repeated for krb5: a Windows workstation uses
Local auth for its data plane and TLS for the admin plane whatever the
domain's data-plane scheme, so a krb5 repeat exercises nothing new.

## Findings

Eleven, all found by running the plan, all fixed on the branch with a test
where a test could have caught it. Each was reproduced before it was called.
Number 11 was raised as an observation rather than a defect and became a
finding once Eric ruled on it.

| # | Commit | What |
|---|---|---|
| 1 | `bbf1b0d6` | A system-scope remnant this process cannot remove aborted the whole teardown, including the unprivileged half it could have done |
| 2 | `b2e2fa4f` | A bare `logout` with nothing cached said nothing at all |
| 3 | `b2e2fa4f` | The read-gate warning never fired when **opening** a gate — the CLI passed `current: None`, and opening early is the quieter mistake |
| 4 | `b2e2fa4f` | `tls join`'s clobber guard ran *after* enrollment, burning a certificate, so the `--force` retry it recommends then failed at the CA |
| 5 | `67a98d25` | The post-`--external-sign` message named `--signed-cert`, a flag that does not exist |
| 6 | `301a6fa4` | The TUI Services panel could not be *opened* without a supervisor, making the previous commit's create/edit/delete fix unreachable |
| 7 | `4172e78c` | `netidx admin uninstall` with no `--yes` printed the privileged half and then failed, never showing the half it could do itself |
| 8 | `310c6558` | The Admin Domain tab refused the whole of Windows for a reason that stopped being true three commits before the migration started |
| 9 | `0aa8ae2e` | `$EDITOR` unset fell back to `vi`, which is not on a Windows host |
| 10 | `ef82e15e` | The perms auto-seed wrote a group grant that `--id-map-mode none` can never match |
| 11 | `80790626` | A delegated subtree's auth prompt defaulted to `tls` whatever its parent ran |

Two of them — 6 and 8 — are the same shape: a commit fixed something behind
a door nobody could open. Worth watching for; the fix in both cases was to
open the door, not to redo the fix.

## Behaviour bullets

Every `Behaviour:` bullet in the 22 commits, with how it was verified.
`git log --grep='^Behaviour:' 93c95e4e..HEAD` enumerates the same set.

### `6000d75e` one login ceremony
- **exercised** (2.1) `login` with no `--server` dials loopback, not `0.0.0.0`.
- **exercised** (2.2) `login` finds the admin name with `$USER` unset.
- **failed → fixed → re-verified** (2.3) `logout` with nothing cached was
  silent. Finding 2.
- **not lab-observable** the one-shot login token is no longer cloned.

### `593342c7` one route to an externally-signed CA
- **exercised** (3c) the TUI no longer takes the daemon path on a host
  running an admin server for a *different* CA — the misrouting bug.
- **exercised** (3) a CA dir reached through a symlink and through a
  relative path both route the same.
- **exercised** (3b) either frontend emits a CSR / installs a certificate
  with the daemon stopped.
- **exercised** the TUI no longer refuses a served CA's offline renewal.
- **not lab-observable** `ExternalInstallOutcome::HotRenewed` as a value.

### `504c756a` one sync pass
- **exercised** (2.11) `<role> update` locks the install record's directory.
- **exercised** (2.11) `update` / `status` refuse on a CA.
- **exercised** (7) `update` errors on lock contention while the
  admin-agent skips and retries — the two policies stay different.
- **not lab-observable** `fetch_map_for` derives `NodeKind` from the role.

### `9e728db7` id-map edits are engine operations
- **exercised** (2.8) existence is decided under the guard.
- **exercised** (2.8) `component id-map init` refuses on what it found.

### `c5e4fdd9` perms edits are engine operations
- **exercised** (2.10) `component client edit` takes the guard.
- **exercised** (2.9) invalid permission bits get one message from both
  frontends and the daemon.
- **exercised** (2.9) `perms set` loses the redundant validation context.
- **exercised** (Pass B) the TUI's local perms panel no longer pre-fills
  `/` when the resolver config is unreadable — it refuses for want of a path.

### `ab91b77c` a unit takes effect when you add it
- **exercised** (2.6) `activation add` reloads a live supervisor — no SIGHUP.
- **exercised** (2.7) a colliding `OnAccess` path is refused before writing.
- **failed → fixed → re-verified** the TUI Services panel with no
  supervisor. Finding 6.
- **exercised** (2.6) `--restart` accepts `rate-limited (2.5s)`, the form
  the TUI's Restart column shows.

### `006defe1` the engine drives restore end to end
- **exercised** (Pass B) the bundle is written once. Per-file inode
  tracking across a whole restore: `perms.json` and `id-map.json` one
  inode each; `resolver.json` two — the bundle write plus the address
  override the prompt says it applies. A double restore doubles all three.
- **exercised** (Pass B) clearing the resolver address keeps the *whole*
  bundled endpoint — `addr` and `bind_addr` both, not half-applied.
- **exercised** (4) a completed restore re-run starts no units.
- **exercised** (4) `restore` without `--old-ca-fenced` gives the
  single-writer rationale *and* the flag name.
- **exercised** (4) `backup` resolves a relative target.
- **not lab-observable as run** the re-enrollment holding the config lock,
  and the lock being acquired after the last question rather than before
  the first. Both are visible only by instrumenting the lock during a
  restore, which this run did not do. Unit-covered.

### `0c27e0e4` the engine drives uninstall
- **exercised** (Pass B) the elevated child is not handed the user's
  `--config-dir`. Captured argv from a real escalation:
  `sudo --preserve-env=NETIDX_ELEVATED /usr/local/bin/netidx admin
  uninstall --scope system --for-user eric --service-name netidx --yes`.
  Exactly one sudo call. **This was the plan's named gap** — everything
  before Pass B ran as root, so `is_elevated()` short-circuited it.
- **exercised** (Pass B) a teardown needing no root spawns no process at
  all. The only children of the TUI were two `systemctl --user` calls.
- **exercised** (Pass B) `needs_root` comes from the probe, not the role.
- **exercised** (5) deregistration is reported through the `Answerer`.
- **exercised** (0) `uninstall --service-name` loses its clap default.
- **not lab-observable** `ca_destroyed` provenance.

### `a9ac929d` rules move to the engine, as data
- **failed → fixed → re-verified** `read-gate` warned in one direction
  only. Finding 3.
- **failed → fixed → re-verified** `--external-sign` named a flag that
  does not exist. Finding 5.
- **exercised** (Pass B) the parent picker lists candidates from every
  cluster and excludes this host's own identity; a mixed-cluster
  selection is refused by the engine.

### `f7f83f0d` tls join enrolls like everything else
- **exercised** (2.12) `--key-protection`.
- **exercised** (2.12) without `--admin` the request queues and waits.
- **failed → fixed → re-verified** the overwrite guard ran after
  enrollment. Finding 4.
- **exercised** (2.12) `--validity` reaches the CA — asserted against the
  issued certificate's actual lifetime.

### `b63f02f0` the TUI keeps its bookmarks in its own config directory
- **failed → fixed → re-verified** the Admin Domain tab seeds this host's
  own domain on Windows. The *seeding* worked from the first try —
  `%APPDATA%\netidx-admin-tui\admin-domains.json` held `netidx.test` with
  the right fingerprint, from the upstream address recorded at join. The
  *tab* rendered a stub. Finding 8.
- **exercised** (Pass B, Windows) a bookmark planted in the old location
  is ignored.
- **exercised** (Pass B, Windows) the bookmark file survives `uninstall` —
  it now sits outside the managed config root, which is the whole point.

### `6530b6e7` make the layering not rot again
- **not lab-observable** the eight dead `Field` variants (compile-time).
- **exercised** (2.13) `ca revoke` / `ca deny` without `--reason` error
  naming the flag; the engine asks when it is absent.

### `bbf1b0d6` an unremovable system service must not block the user teardown
- **exercised** (Windows) the remnant is named, the elevated command is
  given, and the user scope is torn down.

### `b2e2fa4f` three lab findings
- **exercised** all three, on the CLI/TLS topology.

### `cafa59a5` registering the OS service is the default
- **exercised** (Pass C) every strict-CLI install in the krb5 pass passed
  neither `--with-service` nor `--no-service` and registered the service.

### `301a6fa4` list units when there is no supervisor to ask
- **exercised** (Pass B) panel opens with the supervisor stopped, all
  units listed "not loaded", create through `$EDITOR` and delete both work.

### `4172e78c` describing a privileged teardown is not attempting one
- **exercised** (Pass B) preview exits 0 and touches nothing; `--yes`
  escalates once; a re-run is a no-op that never escalates.

### `310c6558` the Admin Domain tab works on Windows
- **exercised** (Pass B, Windows) tab opens on its own seeded domain,
  glyph confirmed over a cross-network TLS fetch, `root` authenticates —
  "Connected to netidx.test at 192.168.50.12:4565 as root" — and a wrong
  password is refused.

### `0aa8ae2e` the default editor is the platform's own
- **not exercised.** The change is one line and unit-reasoned, but no
  editor-backed panel was driven on Windows in this run. The nearest
  thing verified is that the panels are now *reachable* there.

### `80790626` a delegated child is offered its parent's auth scheme
- **exercised** (AP host, against the TUI-built krb5 hierarchy) the
  delegated-subtree prompt comes up with `krb5` selected; `tls` and
  `anonymous` are still one keystroke away.

### `ef82e15e` don't seed a group grant where there are no groups
- **exercised** (Pass C, CLI) same install twice on one host:
  `--id-map-mode none` ⇒ `id_map_type: DoNotMap`, no `users` row, warning
  printed; `--id-map-mode platform` ⇒ `id_map_type: Command`, `users:
  swl` still seeded, no warning.
- **exercised** (Pass D, TUI) the founding krb5 install through the TUI
  produced the same perms file and the same warning in its install log —
  both frontends driving one engine rule, which is what the migration was for.

## Gaps, stated

- **`0aa8ae2e` is not lab-verified** (above).
- **A genuine interrupted-restore resume** was not tested. Restores were
  driven to completion; the resume path is exercised only in the sense
  that `Staged` crosses the privileged step.
- **The Windows data plane needs an interactive logon.** The workstation's
  logon task runs as `InteractiveToken`, so a headless SSH session cannot
  start it; Windows coverage here is the admin plane plus install/teardown.
- **The restore lock timing** (above).
- **`--id-map-mode platform` was never run against a real IdM.** The lab
  VMs have no SSSD, so `platform` was only checked for what it *seeds*,
  not for whether the mapping resolves. Working as intended, and already
  recorded in the lab notes.

## Not findings — checked and left alone

- **A torn-down publisher / workstation leaves its leaf certificate live
  at the CA.** Deregistration is an admin-server concern; a leaf identity
  is revoked by an admin (`ca revoke`), which is why reinstalling under a
  live name is refused. Pre-existing and by design.
- **`uninstall` leaves a zero-byte `.netidx.netidx.lock`** in the config
  root's *parent*. Removing a lock file another process may hold is worse
  than leaving it.
- **A Kerberos *service* principal cannot use the `$[user]` perms entry**
  — `netidx/host@REALM` contains `/`, the netidx path separator, so it is
  never one path segment. Inherent to the two namespaces, not a defect.
  A *user* principal (`eric@NETIDX.TEST`) works, which is the
  discriminating test that isolated finding 10.

## Lab notes earned this run

Folded into `README.md`:

- A Windows guest needs persistent routes to the satellite networks, or a
  referral walk off-site fails with a bare `oneshot canceled`.
- `C:\netidx` is the Windows *system* config root — not a place for binaries.
- Deploying over a running Windows install fails with `scp: dest open ...
  Failure`; stop the logon task first.
- There is no tmux on the Windows guest; `scripts/win-tui-drive.py` +
  `win-tui-render.py` drive and render the TUI from a host-side pty.
- The EU and AP networks are isolated and have no route to the internet,
  so packages must be relayed from an HQ guest — `apt-get download` there,
  `tar` over two ssh hops, `dpkg -i`. Shared libraries a package needs may
  have to be copied the same way.
- `ssh -n` nulls a stdin redirect; drop it when piping a script.
- `netidx subscriber` reads ADD/DROP commands from stdin, so it must be
  run with stdin closed inside a piped shell script.
