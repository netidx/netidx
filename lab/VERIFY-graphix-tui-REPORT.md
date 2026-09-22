# Lab verification report — the Graphix admin TUI against the ratatui one

Plan: `VERIFY-graphix-tui.md`. Lab on washu-chan (moved from mazikeen
2026-09-22). Binary: the `quick` profile built on the devbox, redeployed
after each fix. **In progress** — Phases 0–2 are done; Phases 3–4
remain.

## Phase 0 — bring-up

- 14 domains + 3 networks copied and defined here; all 13 role VMs on
  their reserved addresses; cross-segment pings clean, `wan clear`.
- Every role VM torn down with `teardown2.sh`; CA destroyed with it.
- Startup to the welcome frame on a 2-vCPU guest, from `tmux new-session`:
  **new TUI 694 ms cold / 113–135 ms warm; old TUI 14–17 ms.**
- `tmux` relayed to the six satellite guests (needs `libevent_core` too).

## Phase 1 — Pass 1 mirror, TLS, three sites, through the new TUI

Topology differs from the earlier pass in one way: `.11` is a **dedicated
CA** (the CA role), and `.12` is the domain's first resolver at `/`. Every
node below was installed through `netidx admin tui`; every approval came
from the HQ console's Admin Domain tab (new TUI unless noted).

| Node | Role | Reached | Result |
|---|---|---|---|
| `.11` | CA | — | founded; glyph `UDY4N 46O6I…`, recovery password captured |
| `.12` | Resolver `/` | mDNS | cert + admin-server rounds, `netidx@root` active |
| `.17` | Publisher | mDNS | approved from the **old** TUI (comparison), `users` |
| `.13` | Workstation | mDNS | user-scope service (by design), local resolver up |
| `.60.15` | Resolver `/eu` | manual `.12:4565` | delegated, approved, HQ resolver restarted from Services |
| `.70.11` | Resolver `/ap` | manual `.12:4565` | same; second HQ restart |
| `.60.17` `.60.18` `.70.13` `.70.14` | pub/ws | mDNS (own site) | `users`, all active |

Data plane, from workstations: `AP→HQ→EU` delivered `EU-DATA-tls`,
`EU→HQ→AP` delivered `AP-DATA-tls` (2 hops each, own-site entry).

Preview (`p`) reaches the first question and cancels clean; Esc at a
mid-ceremony question (the key-password prompt on `.13`) leaves nothing
on disk.

Panels (step 9), all from the new TUI on `.11` and, for the login path,
from workstation `.13`: roster (add a scoped role admin `euops` → one-time
password shown; reset its password → new one-time password; remove it);
servers (`x` on the CA row: nothing; `x` on `/eu`: confirm, cancelled;
read gate shut 5 m with the warning confirm → row shows `shut 4m` →
reopened early with its own warning); issued certificates (revoke serial 5
with a reason → gone); permissions (`/eu`: add `/eu/test users swlq` →
"invalid permission bits … valid bits are !swlpd"; add with `swl` → row;
remove → gone at the CA and, within a register, from the EU member's
`perms.json`); services (restart, above). From `.13`: the Admin Domain
tab lands on the landing list seeded with the domain, a wrong password
gives "login refused: authentication failed", the right one opens the
menu with the session at the CA.

Revoke + re-enroll (step 10): `.17` torn down and installed again through
the TUI after its cert was revoked; approved with `users`, no refusal.

Uninstall (step 11): `u` → confirm on the seven leaves and children and
`.12`, each back to the role menu with config and units gone; the CA node
`u` → confirm → "This install owns a CA" chooser → DESTROY → "Removed 7
path(s) and the OS service". Every box clean afterwards.

F2 comparison: a CA founded through the **old** TUI shows the identical
"CA credentials: another netidx administrative process owns …" line and
no Rotate items right after its install.

## Phase 2 — Pass 2 mirror, krb5, three resolvers, through the new TUI

Same dedicated-CA topology, rebuilt after the Phase 1 fixes landed. CA on
`.11`: this time the credential probe's retry (F2) found the daemon a few
seconds after the install — Rotate items present, "Auto-approve: active /
Recovery slot: set" — and the status line was the legend, never a stale
"working…" (F4). `.12` as the krb5 resolver at `/`: SPN default
`netidx/resolver-hq-b.netidx.test@NETIDX.TEST` equal to the keytab's,
id-map `none`, admin-server round queued and approved. (A first attempt
landed on TLS because the auth list defaults to `tls` and my `Down`
clamped — my sequence, not the TUI; the server was removed from the Admin
Servers panel — F11/F12's new confirm and CA-row message verified — its
stale resolver cert revoked from the sorted Issued panel — F13 verified —
and the box reinstalled.) EU and AP delegated as krb5 children **reaching
the dedicated CA** (F8's case): the subtree offer appears and `.12` is
the parent. That exposed two more library bugs, both fixed (`d58f0e66`):
the delegation looked the parent up by the confirmed identity's server id
(the CA's) and found no cluster, and `get_map_pinned` handed back a
member's stale copy of the map whenever the confirmed identity was the
CA. Both HQ resolver restarts from the Services panel; the unit rows came
back in the same order on two loads (F9 verified). Four krb5 leaves (a
krb5 publisher asks only its bind; no enrollment). Enrollment queue and
certificate panels show the new detail pane (F7 verified).

Data plane with `kinit eric`, publishers on the publisher hosts with their
keytab SPN: `AP ws → HQ → EU` delivered `EU-DATA-krb5`, `EU ws → HQ → AP`
delivered `AP-DATA-krb5`. (A publisher without `--spn` fails with
"kerberos error": the writer's SPN requirement, as recorded in Pass 2.)

## Findings

| # | Where | Old | New | Verdict / fix |
|---|---|---|---|---|
| F1 | preview cancel | — | "Install failed — the operator cancelled" after Esc in a *preview* | **fixed** `15f50522`: a cancel is its own error variant; "Preview cancelled / Nothing was changed" |
| F2 | CA install done | **same** | Status card "CA credentials: another netidx administrative process owns …", Rotate actions absent; a fresh process shows "Auto-approve: active / Recovery slot: set" + both actions | pre-existing in both: the probe runs at install completion before the daemon's control socket is up, falls to the lock path, and the result is kept. **fixed** `15f50522`: a failed probe is retried a few times, seconds apart; verified in Phase 2 |
| F3 | Local CA actions | lists "Permissions" on a CA-only host; opening it fails "a target path is required for the perms panel" | hides it (no own resolver base) | new is right |
| F4 | every ceremony | progress line replaced per stage | "working… searching for a netidx admin domain…" stays through every later question and after completion | **fixed** `15f50522`: a question, the code's clearing and the ceremony's end each drop the note; verified in Phase 2 |
| F5 | enrollment queued | progress box shows identicon + code | nothing: `Pump.code` was never rendered | **fixed** `b80ec201`; verified on `.17`, `.13`, `.60.15`, `.70.11`, four leaves |
| F6 | approve confirm | paragraphs, full code, glyph | sentences run together, no glyph | **fixed** `b80ec201` + graphix `96944ba2` (string paragraphs dropped newlines); verified on `.60.15` |
| F6b | approved toast | two lines | "…approvedid-map groups:" | fixed by the same graphix change |
| F7 | enrollment queue | one card per request with glyph + full code | a table with an 8-char short code | **fixed** `15f50522`: the detail pane (identicon beside summary + grouped code) under the queue, delegation and certificate tables, as the old one; verified in Phase 2 |
| F8 | EU delegation | same | same | **library**: the install-time delegation offer (`plan/install/resolver.rs:198`) is gated on the *reached* admin server running a resolver; reaching the dedicated CA skipped it although the domain had resolvers. **fixed** `8d6f93b7` + `d58f0e66` (Eric: only a resolver can delegate; a pure CA is filtered out); verified in Phase 2 |
| F9 | Services panel | same | same | **library, fixed** `1650b90b`: the supervisor answered an all-units request in `HashMap` order; three loads gave three orders and `R` on row 1 restarted `.12`'s admin server instead of its resolver |
| F10 | Services panel | same | same | restarting the admin server one is connected through loses the reply: "the CA refused … peer closed connection without sending TLS close_notify" although the restart happened. Expected; the message could say what happened. |
| — | workstation-eu | — | `netidx@root` reported `failed` | a ghost of the teardown's `pkill -9` (unit file gone, `reset-failed` not run); not a TUI matter |

| F11 | Admin Servers | ? | `x` on the CA row does nothing, silently | **fixed** `15f50522`: "The active CA stays — removed by uninstalling its host" |
| F12 | Admin Servers remove | UUID, last address, resolver cluster | only the UUID | **fixed** `15f50522`: address, cluster, "there is no undo" |
| F13 | Issued Certificates | ? | rows in hash order (6, 14, 7, 8, 2, 9, 13, 3, 11, 5, 10, 12, 4); `Expires` truncated to "UT" | **fixed** `2e0fe622` (library sorts by serial; both TUIs were unsorted) + the column widened |
| F14 | client login refused | ? | after "login refused" the connect screen's address field is empty; retry retypes it | **fixed** `15f50522` |
| F15 | CA uninstall | `u` → one confirm whose text folds in "also destroy it?" → `y` destroys | `u` → confirm → chooser Keep / DESTROY → Enter destroys | new is one step more careful; a typed confirmation is still worth considering |
| F16 | any modal / list | the first key after a screen transition is sometimes eaten | same (observed on `Up` after the CA status card, `q` after the Uninstalled overlay, and the admin-name prompt on `.13`) | pre-existing in both; worth a look at where the first event after a layer change goes |

Also observed, same for both TUIs (library defaults): the resolver's
default cert name is `resolver.<domain>` (role.domain) while a
publisher's is `<hostname>.<domain>`; two resolvers accepting the default
would collide under the one-live-cert-per-name rule. Set explicitly here.

## Remaining

Phase 3 (failure modes: 3.1–3.5 covered — no-TTY refusal, dropped
session, `--server` on a fresh and an installed box, wrong password; 3.6–
3.12 open), Phase 4 (parity table). Still open from the findings: F10
(message wording), F16 (first key after a transition, both TUIs).

Driving harness: `scratchpad/tui.sh` (tmux over ssh; **zsh does not
word-split unquoted variables — use arrays**), `scratchpad/drive.sh`
(scripted ceremonies with a title check at every question; on the HQ
menu `Esc` disconnects, so back out only while the title is a panel's).
