# Lab verification plan — the Graphix admin TUI against the ratatui one

During this verification both TUIs were built into one binary: `netidx
admin tui` (Graphix, the subject) and `netidx admin tui-old` (ratatui, the
reference). The reference was deleted afterwards and the Graphix TUI is bare
`netidx admin`. The question is whether the new one can replace the old one: the
same build-outs, the same operations, the same failures handled — and where
the two differ, which is right.

## What has to be true at the end

1. The two TUI build-outs of the earlier passes work again, driven by the
   **new** TUI: **TLS** (Pass 1) and **krb5** (Pass 2), each a two-level
   hierarchy with a publisher and a workstation, each proving cross-site data.
2. Every panel and action the old TUI has is reached in the new one, or is
   recorded as missing with a verdict (port it / drop it).
3. Where a step behaves differently between the two, the old TUI is driven
   through the same step on the same box and the difference is written down
   as a finding: a bug in the new one, a bug in the old one, or a deliberate
   change.
4. The failure modes the driver work added are exercised on a real guest: a
   dropped ssh session, no TTY, `--server` on a fresh box, a wrong password,
   a cancelled ceremony at every question.

## Standing rules

- **Run it like prod.** Every daemon under `netidx@<user>.service`.
- **Drive the same change twice.**
- **Verify before declaring a bug.** Three prior findings evaporated.
- **Never inline-approve enrollments over tmux.** Queue, approve from the
  admin console.
- **Never drive a cross-site subscribe from a resolver host.**
- Reinstalling a name whose cert is live is refused by design: revoke first.
- At the approve id-map prompt, pub/ws take the `users` default; resolvers
  and admin servers take none.
- The install-complete overlay eats the first key; the first key after any
  screen transition may be eaten: re-capture, re-send.

## Method

The TUI runs on the guest under tmux (`reference: driving the TUI in the
lab`): `tmux new-session -d -s t -x 150 -y 45 "netidx admin tui"`, keys by
`send-keys` (`-l` for literal text; no spaces in a literal), frames by
`capture-pane -p`. Every step is a captured frame before and after the key,
kept under `lab/frames/<pass>/<step>-<tui>.txt` when the two TUIs differ.

The comparison is per step, not per pass: the new TUI drives the build-out;
when a step surprises (a different question, a missing action, an error, a
different frame the operator would act on), the old TUI is run through that
step on the same box before the state moves on, and both frames are kept.

## Topology per pass

As `VERIFY-layering-migration.md`: HQ `.11` (CA + admin server + resolver
`/`), HQ member `.12`, publisher `.17`, workstation `.13`; EU `.60.15`
(`/eu`), EU publisher `.60.17`, workstation `.60.18`; AP `.70.11` (`/ap`),
AP publisher `.70.13`, workstation `.70.14`; router; devbox `.14`.

---

## Phase 0 — bring-up and baseline

1. `redeploy.sh` the **quick-profile** binary to every role VM (the debug
   build's Graphix cold start is seconds of blank screen). Freshness marker:
   `netidx admin --help` lists both `tui` and `tui-old`.
2. `teardown2.sh` every VM; on the CA host also destroy the CA. Assert no
   procs, no units, no config, and that the CA no longer holds live certs
   (else the re-enrollments are refused).
3. `wan clear`; pings; ttl=63.
4. Startup time on a guest, both TUIs, cold and warm (`time` to first frame
   via tmux polling). Record.

---

## Phase 1 — Pass 1 mirror: TLS, three sites, through the new TUI

Each step: new TUI drives; frame kept; old TUI only if the step surprises.

1. **Fresh box, no install** `.11`: welcome dialog, role menu, blurbs,
   `p` preview of each role (the dry run reaches the first question and is
   cancelled).
2. **Found HQ** `.11`: CA role → domain name, admin name/password, TPM-absent
   → proceed, admin server address/port, listen/port/cert name, key
   protection, id-map, OS service. Capture the CA glyph and the recovery
   password (shown once: the `OneTimeSecret` modal). Assert `netidx@root`
   active, status card shows the domain + glyph, Admins panel opens over the
   control socket.
3. **Second HQ member** `.12`: Resolver role → discovery (mDNS finds HQ) →
   glyph identity confirm → subtree gate blank → auth auto-import → queue
   enrollment (admin-present? No) with out-of-band code → **approve from the
   `.11` console** (Enrollment queue, `a`, code shown in the confirm) → id-map
   none → admin-server enrollment (second round) → OS service → installed
   view. Then `U` on `.13` later must offer `.12` as a new member.
4. **Publisher** `.17` and **workstation** `.13`: enroll, approve with the
   `users` default.
5. **Delegate EU** `.60.15` (`/eu`): discovery empty over the WAN → manual
   admin-server address → glyph confirm → subtree `/eu` → delegation code →
   **approve delegation from `.11`** (Delegation requests, `a`) → child's own
   admin server (second queue+approve) → OS service. Then **restart HQ's
   resolver unit from the Services panel** (path `/` → resolver → `R`).
   Assert HQ `resolver.json` carries `children=[/eu]`.
6. **Delegate AP** `.70.11` (`/ap`) the same way; second HQ restart.
7. **EU/AP publishers and workstations** as step 4.
8. **Data plane**: `AP→HQ→EU` and `EU→HQ→AP`, each from a workstation.
9. **Admin Domain tab, every panel** on `.11` and from `.13` (a client that
   must `admin login`): enrollment queue (empty), delegation requests
   (empty), admins (add a role admin, reset its password, remove it),
   servers (the CA row protected, `x` on a non-CA row refused/confirmed),
   issued certs (revoke one by serial with a reason, list refreshes),
   services (status, stop/start/restart one unit), permissions (show, set,
   remove, an invalid bit refused), id-map (show, add/remove group/user/
   member), read gate (`--shut` warns), drift.
10. **Revoke + re-enroll** `.17` (the pass-1 id-map lesson): revoke by
    serial, `teardown`, install again, approve with `users`.
11. **Uninstall**: plain `u` → `y` on a leaf; the CA node: Enter → "Uninstall
    + destroy the CA" → `y`. Assert everything gone.

---

## Phase 2 — Pass 2 mirror: krb5, three resolvers, through the new TUI

`scratchpad/krb5-provision.sh` state survives teardown (keytabs, KDC). HQ
founded with krb5 data plane + TLS admin plane; SPN default must equal the
keytab's; id-map `none` chosen in the TUI on all three (not corrected
in-place afterwards); EU and AP delegated directly under HQ; HQ resolver
restarted from the Services panel after both approvals; cross-site data both
directions with `kinit eric`.

---

## Phase 3 — the TUI's own failure modes (both TUIs, same box)

| # | Probe | Expected (new) |
|---|---|---|
| 3.1 | `netidx admin tui </dev/null` | refuses, exit 1, names the subcommand route |
| 3.2 | tmux `kill-session` with the TUI up, `trap '' HUP` | process gone within ~2 s, exit 1, reason on stderr; old TUI same via its liveness probe |
| 3.3 | `--server <addr>` on a fresh box | Local install view has the keys; Tab does nothing |
| 3.4 | `--server <addr>` on an installed box | opens the connect screen at that address |
| 3.5 | wrong admin password, three times | the error is shown each time, no lockout of the TUI itself, `Esc` backs out |
| 3.6 | admin server stopped while the Admin Domain tab is open | every panel's error is readable; restarting it recovers without a TUI restart |
| 3.7 | `wan seg eu loss 100%` during an EU delegation approve from HQ | the approve fails with a readable error, the queue row survives, `wan clear` + retry succeeds |
| 3.8 | Esc at **every** question of the resolver install ceremony | the tab reports the install failing/cancelled, nothing half-written on disk, the box is still "not installed" |
| 3.9 | terminal resize mid-ceremony (tmux `resize-window`) | the modal re-lays out, no panic |
| 3.10 | run the TUI as `eric` (non-root) and install a system service | the privileged step escalates (sudo/su), the request it carries is the one shown, the terminal is handed back cleanly |
| 3.11 | `$EDITOR` steps (perms edit) | suspend/resume: keys after the editor arrive (no dropped Enter/Esc) |
| 3.12 | Ctrl-C at the top level and inside a modal | exits cleanly (terminal restored) — decide whether it should confirm first |

---

## Phase 4 — parity inventory

Walk the old TUI's key hints on every screen (`local.rs`, `remote.rs`,
`services.rs`, `admin_domains.rs`) and tick each key/action off against the
new one. The table goes in the report with one of: **same**, **moved**
(where), **missing** (verdict), **new**.

---

## Report

`VERIFY-graphix-tui-REPORT.md`: per phase, what ran, the findings table
(id, step, old frame, new frame, verdict, fix commit), the parity table, the
startup numbers, and the not-observable list.
