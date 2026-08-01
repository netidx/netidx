# Lab verification plan — the netidx-admin layering migration

Verifies commits `93c95e4e`..`6530b6e7` (15) on the real 3-site WAN lab, from a
clean slate, through the installed OS service — never hand-started daemons.

## What has to be true at the end

1. Four complete build-outs work: **CLI/TLS**, **TUI/TLS**, **CLI/krb5**,
   **TUI/krb5** — each a two-level hierarchy (root `/` + a delegated child),
   each with a publisher and a workstation, each proving cross-site data.
2. Every `Behaviour:` bullet in the twelve commits is either **exercised** or
   **explicitly recorded as not lab-observable** (unit-covered only). No bullet
   is silently skipped.
3. Windows participates: install, admin-plane enrollment, the Admin Domain tab
   seeding its own domain (the `#[cfg(unix)]` that `b63f02f0` removed), and a
   teardown.

## Standing rules

- **Run it like prod.** Every daemon under `netidx@<user>.service`. If a step
  needs a hand-started process, that is a finding, not a workaround.
- **Drive the same change twice.** The config-watch bug (2026-07-29) only
  appeared on the *second* rename-over. Any state-changing step gets repeated.
- **Verify before declaring a bug.** Two prior passes over-called findings that
  evaporated. Reproduce, read the code, then call it.
- **Never inline-approve enrollments over tmux** — password mangling causes
  auth failures. Queue, then approve from the admin console.
- **Never drive a cross-site subscribe from a resolver host** — server
  identities get no id-map group.
- Reinstalling a node under a name whose cert is still live is **refused by
  design**. Revoke first. Wiping `~/.config/netidx` is not a teardown.

## Topology per pass

| Host | IP | Role |
|---|---|---|
| `debian13 resolver0` | 192.168.50.11 | HQ: CA + admin server + resolver `/` (KDC for krb5) |
| `debian13 publisher` | 192.168.50.12 | HQ resolver member 1 |
| `debian13 hq-publisher` | 192.168.50.17 | HQ publisher |
| `debian13 workstation` | 192.168.50.13 | HQ workstation |
| `debian13 resolver1` | 192.168.60.15 | EU child resolver `/eu` |
| `debian13 ap-resolver-a` | 192.168.70.11 | AP child resolver `/ap` (Pass B only) |
| `debian13 router` | .50.2/.60.2/.70.2 | netem WAN |
| `win11` | mgmt DHCP on `default`; HQ NIC on `netidx-test` | Windows workstation |
| `debian13 dev` | 192.168.50.14 | build box (up only to build) |

`virsh net-start default` is needed for Windows management SSH after a host
reboot — it is currently **inactive**.

---

## Phase 0 — bring-up and baseline

1. Start `router` first, then the role VMs, then `dev`.
2. `lab/scripts/redeploy.sh` to every role VM. Confirm freshness by a
   **post-refactor marker**, not `--version` (still 0.32.0): `netidx admin tls
   join --help` must show `--force` and `--key-protection` (both new in
   `f7f83f0d`).
3. Cross-build Windows: `cargo build -p netidx-tools --bin netidx --bin
   netidx-activation --target x86_64-pc-windows-gnu`, strip, scp both.
4. `teardown2.sh` on every VM. Assert: no procs, no units, no config.
5. Record the baseline: `wan clear`, all pings, ttl=63 one hop.

---

## Phase 1 — Pass A: CLI, TLS, two levels

Strict CLI only. Every value a flag; **no TTY** — run each command with stdin
closed so an accidental prompt fails loudly rather than hanging.

1. **Found HQ** `.11`: `admin resolver install --with-admin-server
   --insecure-no-tpm` + TLS flags. Capture the recovery password (printed once)
   and the CA glyph.
2. **Second HQ member** `.12`: enroll, queue, approve from `.11`, verify
   `netidx@root` active and the resolver answering on :4564.
3. **Delegate EU** `.60.15` (`/eu`): child-side `admin resolver install` with
   `--parent-admin-server`, approve the delegation from `.11`, then **restart
   HQ's resolver unit** (children are captured at startup — intended workflow).
4. **Publisher** `.17` and **workstation** `.13`: enroll, approve accepting the
   `users` id-map default (never `-` for pub/ws).
5. **Windows workstation**: `netidx admin workstation install` over SSH. Assert
   the logon task registered and `netidx-activation.exe` present.
6. **Data plane**: publish at HQ, subscribe from EU and back — both directions,
   2-hop, through each site's own resolver.
7. Repeat step 3's delegation approve a second time (idempotency).

---

## Phase 2 — targeted probes on the Pass-A topology

These do not fall out of a normal build. Host state from Phase 1.

| # | Probe | Covers |
|---|---|---|
| 2.1 | `admin login` with **no `--server`** on `.11` (admin server binds wildcard) — must dial loopback, not `0.0.0.0` | `6000d75e` |
| 2.2 | `env -u USER admin login …` — must still find the admin name | `6000d75e` |
| 2.3 | `admin logout --accept-glyph <ca>` with nothing cached — must say so, not fail silently | `6000d75e` |
| 2.4 | On a no-TPM host: CLI must **error naming sealing**; TUI must connect in-memory **and say so** | `6000d75e` |
| 2.5 | `admin resolver read-gate --target <id> --shut` — must now **warn** about the change | `a9ac929d` |
| 2.6 | `component activation add --restart 'rate-limited:2.5'` on a host with a **live supervisor** — unit must take effect with **no SIGHUP**; read the Restart column back off the TUI and feed that exact string (`rate-limited (2.5s)`) to `--restart` | `ab91b77c` |
| 2.7 | Add a unit whose `OnAccess` path collides with an existing one — must be refused | `ab91b77c` |
| 2.8 | `component id-map init` on a host that already has one — must refuse on what it found | `9e728db7` |
| 2.9 | `component perms set` and `component client edit` with **invalid permission bits** — one identical message from both | `c5e4fdd9` |
| 2.10 | `component client edit` while another process holds the config lock — must block/refuse (it took no guard before) | `c5e4fdd9` |
| 2.11 | `admin ca update` / `admin ca status` — must refuse; `<role> update` on `.17` while the lock is held must **error**, while the admin-agent **skips and retries** | `504c756a` |
| 2.12 | `admin tls join` re-run with an existing name — must fail naming `--force`; then with `--force` succeeds; with `--key-protection`; with `--validity 10m` (assert the issued cert's actual lifetime); without `--admin` it **queues** and waits for remote approval | `f7f83f0d` |
| 2.13 | `admin ca revoke` / `ca deny` **without** `--reason` — must error naming the flag (engine now asks) | `6530b6e7` |

**Not lab-observable** (unit-covered, recorded as such): the one-shot login
token no longer being cloned; `ExternalInstallOutcome::HotRenewed`;
`fetch_map_for` deriving `NodeKind` from the role; `ca_destroyed` provenance;
the eight deleted `Field` variants.

---

## Phase 3 — external CA, four quadrants (the riskiest routing change)

On `.11`, both frontends. `593342c7` changed which route each takes.

| Quadrant | Setup | Expected |
|---|---|---|
| (a) | served CA, daemon **up**, config owns this CA | control socket |
| (b) | served CA, daemon **stopped** | **offline** — not a failure |
| (c) | config present but `--ca-dir` points at a **second** CA dir | **offline** (this is the misrouting bug) |
| (d) | offline CA, no admin-server config | offline |

Plus: a CA dir reached through a **symlink** and through a **relative path** —
both must resolve to the same routing decision. And `admin ca install
--external-sign` must **print the CSR path** and what to do with it.

---

## Phase 4 — backup and restore

1. `admin backup` on the CA host with a **relative** target — the library must
   resolve it.
2. Restore onto a **clean VM** with explicit addresses; confirm co-located
   identities re-enroll and the hierarchy reconciles.
3. Restore **in place** on another host with **no address flags** — must keep
   the bundled addresses (silence must not relocate a CA).
4. `admin restore` **without** `--old-ca-fenced` — must fail with the
   single-writer rationale **and** the flag name.
5. Repeat (2) **from the TUI**: clearing the resolver address field must keep
   the whole bundled endpoint; the bundle must be unpacked **once**.
6. Re-run a completed restore — units must **not** be restarted when every
   identity was already restored.

---

## Phase 5 — uninstall matrix (this tears Pass A down)

| Target | Expectation |
|---|---|
| Windows workstation | teardown; logon task removed |
| HQ workstation `.13` (user config + user service) | **no privileged child spawned at all** |
| HQ publisher `.17` | deregisters from the CA **before** its certs are deleted |
| EU resolver `.60.15` (user config + **system** service) | escalates only for the service; the elevated child must **not** be handed the user `--config-dir` |
| HQ member `.12` | same |
| CA host `.11` with `--with-ca` | must **not** deregister from itself |

Confirm on the CA that each host left the map *before* its certs were deleted.
Re-run each for idempotency. Then `teardown2.sh` everywhere.

---

## Phase 6 — Pass B: TUI, TLS, two levels (+ AP)

Same build-out as Pass A, driven entirely through the TUI over tmux. Adds:

- **Two children of one root** (EU `/eu` and AP `/ap`) so the parent picker has
  two clusters — deliberately select resolvers from **both**; must refuse.
- The TUI's local **perms panel** must no longer pre-fill `/` when this host's
  resolver config can't be read.
- TUI **Services** create/edit/delete on a host with **no supervisor running** —
  must succeed (it failed before).
- **Windows**: the Admin Domain tab must seed this host's own domain (the
  ungating in `b63f02f0`); bookmarks must land in
  `%APPDATA%\netidx-admin-tui\admin-domains.json`, **not** the managed config
  root; an old bookmark file in the old location is ignored.
- TUI **uninstall** of a workstation — must spawn **no subprocess**.

---

## Phase 7 — Pass C: CLI, krb5, two levels

`teardown2.sh`, then rebuild with the krb5 data plane (admin plane stays TLS).

- Reuse the KDC and keytabs on `.11` (they survive teardown).
- id-map **`none`** — `platform` needs SSSD, which the lab VMs lack.
- A manual krb5 publisher needs its **own** `--spn`.
- Add `192.168.50.11 resolver.netidx.test` to `/etc/hosts` on AP nodes if used.
- Cross-site data both directions.

## Phase 8 — Pass D: TUI, krb5, two levels

Same, through the TUI. Windows is **not** repeated here — a Windows workstation
uses Local auth for its data plane and TLS for the admin plane regardless of the
domain's data-plane scheme, so a krb5 repeat exercises nothing new. Stated as an
assumption; say so in the report.

---

## Phase 9 — report

- Every `Behaviour:` bullet: exercised / not-lab-observable / **failed**.
- Every finding: reproduced twice, root-caused, with the file:line.
- Fixes committed on the branch with their own tests where a test would have
  caught it.

## Effort and check-in points

Four full build-outs plus five probe phases. I propose checking in after
**Phase 5** — Pass A plus every targeted probe, the external-CA quadrants,
backup/restore, and the uninstall matrix is the highest-information point, and
it is where a serious problem would most likely surface. Passes B–D are then
repetition through a different frontend and a different auth scheme.
