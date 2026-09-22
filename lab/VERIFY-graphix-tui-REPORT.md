# Lab verification report — the Graphix admin TUI against the ratatui one

Plan: `VERIFY-graphix-tui.md`. Lab on washu-chan (moved from mazikeen
2026-09-22). Binary: the `quick` profile built on the devbox, redeployed
after each fix. **In progress** — Phases 0 and 1 are done; Phases 2–4
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

## Findings

| # | Where | Old | New | Verdict / fix |
|---|---|---|---|---|
| F1 | preview cancel | — | "Install failed — the operator cancelled" after Esc in a *preview* | wording; not compared yet |
| F2 | CA install done | **same** | Status card "CA credentials: another netidx administrative process owns …", Rotate actions absent; a fresh process shows "Auto-approve: active / Recovery slot: set" + both actions | pre-existing in both: the probe runs at install completion before the daemon's control socket is up, falls to the lock path, and the result is kept. Fix belongs where the install completes: re-probe once the service is active (or make `R` the documented recovery). Open. |
| F3 | Local CA actions | lists "Permissions" on a CA-only host; opening it fails "a target path is required for the perms panel" | hides it (no own resolver base) | new is right |
| F4 | every ceremony | progress line replaced per stage | "working… searching for a netidx admin domain…" stays through every later question and after completion | **bug (new)**, open |
| F5 | enrollment queued | progress box shows identicon + code | nothing: `Pump.code` was never rendered | **fixed** `b80ec201`; verified on `.17`, `.13`, `.60.15`, `.70.11`, four leaves |
| F6 | approve confirm | paragraphs, full code, glyph | sentences run together, no glyph | **fixed** `b80ec201` + graphix `96944ba2` (string paragraphs dropped newlines); verified on `.60.15` |
| F6b | approved toast | two lines | "…approvedid-map groups:" | fixed by the same graphix change |
| F7 | enrollment queue | one card per request with glyph + full code | a table with an 8-char short code | design point for Eric; the confirm now carries the full glyph/code either way |
| F8 | EU delegation | same | same | **library**: the install-time delegation offer (`plan/install/resolver.rs:198`) is gated on the *reached* admin server running a resolver; reaching the dedicated CA skips it although the domain has resolvers. Eric's call. |
| F9 | Services panel | same | same | **library, fixed** `1650b90b`: the supervisor answered an all-units request in `HashMap` order; three loads gave three orders and `R` on row 1 restarted `.12`'s admin server instead of its resolver |
| F10 | Services panel | same | same | restarting the admin server one is connected through loses the reply: "the CA refused … peer closed connection without sending TLS close_notify" although the restart happened. Expected; the message could say what happened. |
| — | workstation-eu | — | `netidx@root` reported `failed` | a ghost of the teardown's `pkill -9` (unit file gone, `reset-failed` not run); not a TUI matter |

| F11 | Admin Servers | ? | `x` on the CA row does nothing, silently | minor; check whether the old says "protected" |
| F12 | Admin Servers remove | UUID, last address, resolver cluster | only the UUID | less context in an irreversible confirm; minor |
| F13 | Issued Certificates | ? | rows in hash order (6, 14, 7, 8, 2, 9, 13, 3, 11, 5, 10, 12, 4); `Expires` truncated to "UT" | sort (library rows or panel) and widen; check old |
| F14 | client login refused | ? | after "login refused" the connect screen's address field is empty; retry retypes it | minor |
| F15 | CA uninstall | `u` → one confirm whose text folds in "also destroy it?" → `y` destroys | `u` → confirm → chooser Keep / DESTROY → Enter destroys | new is one step more careful; a typed confirmation is still worth considering |
| F16 | any modal / list | the first key after a screen transition is sometimes eaten | same (observed on `Up` after the CA status card, `q` after the Uninstalled overlay, and the admin-name prompt on `.13`) | pre-existing in both; worth a look at where the first event after a layer change goes |

Also observed, same for both TUIs (library defaults): the resolver's
default cert name is `resolver.<domain>` (role.domain) while a
publisher's is `<hostname>.<domain>`; two resolvers accepting the default
would collide under the one-live-cert-per-name rule. Set explicitly here.

## Remaining

Phase 2 (krb5), Phase 3 (failure modes: 3.1–3.4 partly covered — no-TTY
refusal, `--server` on a fresh and an installed box; 3.5 wrong password
covered), Phase 4 (parity table).

Driving harness: `scratchpad/tui.sh` (tmux over ssh; **zsh does not
word-split unquoted variables — use arrays**), `scratchpad/drive.sh`
(scripted ceremonies with a title check at every question; on the HQ
menu `Esc` disconnects, so back out only while the title is a panel's).
