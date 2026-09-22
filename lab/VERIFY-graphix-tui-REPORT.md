# Lab verification report — the Graphix admin TUI against the ratatui one

Plan: `VERIFY-graphix-tui.md`. Lab on washu-chan (moved from mazikeen
2026-09-22). Binary: the `quick` profile built on the devbox, redeployed
after each fix. Phases 0–4 are done.

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

## Phase 3 — failure modes

3.1–3.5 were covered while the driver was built (no-TTY refusal, dropped
session, `--server` on a fresh and an installed box, wrong password).

**3.6 — admin server stopped while the Admin Domain tab is open.** On
`.11`, session at the CA's own admin server, `netidx admin host
activation stop admin-server`. Every panel's load fails with a readable
dialog — "Loading the queue failed: contacting admin server
192.168.50.11:4565: … Connection refused (os error 111)" — with the
rows of the last successful load still behind it. `start admin-server`
and `r`: the panel loads again without restarting the TUI. But the
admin server's session table is in memory, so the first load after the
restart is refused "login required: session is unknown", and from then
on every panel load asked the admin password (F17, both TUIs). A
second observation on the way: a session from the previous evening
had passed the login's 8-hour absolute lifetime, and the first panel
after that asked the password with no error first — the same F17 shape
from the client side (the process cache expires it locally).

**3.7 — WAN loss during a delegation approve.** The EU workstation
box `.60.18` re-installed as a krb5 child resolver at `/eu/lab`, reached
through its own site's admin server (mDNS found `.60.15`), its admin
server approved from HQ, its delegation request pending. Then `wan loss
100%` and `a` on the request from HQ: "Delegation approved — /eu/lab
approved · failed b41dc13e…: timed out connecting to admin server
192.168.60.15:4565: deadline has elapsed" — the approval is recorded at
the CA (the CA is authoritative; the parent converges on its next
register) and the peer that could not be told is named; the row stays
as "approved — a reconciles". `wan clear`, `a` again: "updated
b41dc13e…". The child, whose poll of the CA had crossed the cut the
whole time, went on to "register OS service" by itself, and its
`resolver.json` carries the parent referral. Lab put back afterwards:
the child uninstalled, its dead server row removed from Admin Servers
(F12's confirm: address, cluster, no undo → "revoked 1 certificates ·
updated b41dc13e…"), the workstation leaf re-installed.

**3.8 — Esc at every question of the resolver install.** On the fresh
`.13`, the krb5 resolver ceremony (subtree `/lab`), cancelled at each of
its twelve questions in turn, the box's config dir and units checked
after each: questions 1–9 (choose an admin domain, select, the CA
glyph, subtree, auth, SPN, listen address, port, id-map) all end in
"Install cancelled / Nothing was changed." with nothing on disk — the
glyph question is the exception in wording only: Esc there is a reject,
"Install failed — the admin domain identity was not confirmed; nothing
was sent". Question 10, "admin server listen IP", ended in the same
"Nothing was changed" **with `resolver.json`, `client.json`,
`install.json`, `perms.json` and the activation units on disk** and the
box thereafter detected as an installed resolver without an admin server
(F20). The verification-code box takes no keys, so a queued enrollment
cannot be cancelled from the keyboard (both TUIs; the old one's progress
dialog is the same).

**3.9 — resize mid-modal.** The revoke confirm at 150×45, the window
resized to 80×24: the modal re-lays out with its text wrapped, the table
and detail pane behind it clipped cleanly; back to 150×45, the same
frame as before. No panic.

**3.10 — as `eric`, a system service.** `netidx admin tui` under
`su - eric` on `.13`, publisher role (krb5, no enrollment), "register OS
service? Yes": the TUI releases the terminal, prints "Administrator
privileges are needed to install the system service." and sudo asks
eric's password on the plain terminal; the escalated command (in `ps`)
is `netidx admin host service install --scope system --for-user eric
--service-name netidx --netidx-binary /usr/local/bin/netidx
--activation-dir /home/eric/.config/netidx/activation` under a shell
that prefers a cached sudo credential, then sudo, then su. After the
password the TUI resumes on "Publisher (running)" with "OS service
registered — registered the system service (netidx)";
`netidx@eric.service` active. `u` → confirm → the uninstall escalates
the same way (sudo's cached credential, no second prompt), the terminal
is released and resumed again, the box is back on the role menu with no
units and no config. Keys after each resume (Enter, `u`, `y`) all
arrived.

**3.11 — `$EDITOR`.** No subject: the new TUI edits permissions,
policies and units in forms, never in an editor (see the parity table);
the suspend/resume path it does have is the escalation of 3.10, above.

**3.12 — Ctrl-C.** Inside a confirm modal and on the panel menu: the
process exits 0, the alternate screen is left, the shell prompt is back
and typed input echoes. The exit status is 0 rather than the 130 a shell
would expect of an interrupt; harmless, worth a thought.

## Phase 4 — parity inventory

Every key the old TUI binds, walked from its legends and its `KeyCode`
matches, against the new one. **same** = same key, same action;
**moved** = reached another way; **missing** = not there (with a
verdict); **new** = the new TUI only.

| Screen | Old | New | Verdict |
|---|---|---|---|
| global | `Tab` switch, `q` quit, `Ctrl-C` | same | same |
| global | `l` opens a Log pane of past results (`↑/↓`, `PgUp/PgDn` scroll) | none; each result is a toast, dismissed and gone | **missing** — verdict: drop, unless a scrollable history is wanted; the strict CLI is the record |
| global | every key but `Ctrl-C` swallowed while an op runs ("working… · Ctrl-C quit") | status line says "working… [stage]"; keys still reach the layers | differs; the new one lets the operator cancel with `Esc` at the question, which is the better behaviour |
| Welcome | any key | any key | same |
| Set Up This Machine | `↑↓/kj`, `Enter` install, `p` preview | same; plus `F` finish a staged restore, `R` re-detect | same + **new** |
| installed home | `↑↓/kj`, `←/→` switch install, `Enter` run, `u` uninstall, `U` update, `r` renew | same; plus `R` re-detect, `F` finish restore | same + **new** |
| installed home, menu items | Status · Update Resolvers · Join · Preview Join · Add a Parent · Renew · Services · Admins · Permissions · Auto-Renew enable/rotate · Rotate Recovery · External-CA CSR/Install · Back Up · Uninstall | same list | same |
| status card | any key closes | same | same |
| uninstall on a CA host | one three-way prompt `y` destroy / `n` keep / `Esc` | confirm, then a Keep / DESTROY chooser | **moved** (F15: new is fine) |
| Add a Parent | one modal ticks **several** parent members (`Space`), `Enter` confirms the set, manual row at the end | "Pick a parent" picks **one** member (or manual), then an "Add a parent" form (parent, subtree) | differs — see F18 |
| Local Services | `s t R c e d r Esc` | same keys | same |
| Local Services `c` create | name prompt, then the template opens in `$EDITOR` | a "New unit" form (all fields), `Enter` saves | **moved**: form instead of `$EDITOR` |
| Local Services `e` edit | the unit file in `$EDITOR` (terminal suspended) | an "Edit [name]" form | **moved**: form instead of `$EDITOR`; nothing in the new TUI suspends the terminal, so probe 3.11 has no subject |
| Admin Domains landing | `↑↓/kj`, `Enter` connect, `d` discover, `c` connect direct, `r` refresh | same; plus `Esc` clears a discovery report | same + **new** |
| connect direct | host and port fields, `Tab` between | one `host:port` field | **moved** |
| panel menu | `↑↓/kj`, `Enter`, `Esc` disconnect, **`L` logout** (revoke the cached login) | `↑↓/kj`, `Enter`, `Esc` disconnect | **missing** `L` — verdict: port it; the new TUI keeps a cached login for the process lifetime (and sealed on disk where the platform can), and an operator leaving a shared box has nothing but `netidx admin logout` |
| every panel | `↑↓/kj`, `r`, `Esc` | same | same |
| Enrollment Queue | `a d R r Esc`; `d` asks a reason | same keys; the reason is the library's `refusal_reason` question, through the pump | same |
| Delegation Requests | `a d r Esc`; `d` asks a reason | same | same |
| Admin Roster | `a e p d r Esc`; `a`/`e` edit the policy as JSON in `$EDITOR` | `a e p d r Esc` with a policy form; plus **`c` change my password** | **moved** (form) + **new** `c` |
| Admin Servers | `g c x r Esc` | same | same |
| read gate | Open / Shut / Shut until (a typed duration, `1h` default) | Open / Shut / 5 m / 30 m / 2 h / 8 h | **moved**: fixed choices instead of a typed duration; verdict: fine, add a typed row only if asked |
| Issued Certificates | `x r Esc`; revoke asks a reason | same; revoke asks a reason | same |
| Permissions | `e` edits the whole document in `$EDITOR`, `r`, `Esc` | `a` add, `e` edit one entry, `d` remove, `r`, `Esc` | **moved**: per-entry form; the whole-document edit is the CLI's `perms edit` |
| remote Services | `s t R r Esc` | same | same |
| question modals | text/secret (`Ctrl-U` clears), choice, confirm (`←/→`/`Tab`/`h`/`l` toggle), select domain, identity confirm (`a`/`Enter`, `r`/`n`/`Esc`), announce, one-time secret | same set; text fields have `Home`/`End`/`←/→`/`Delete`, no `Ctrl-U`; confirm toggles with `←/→`/`Tab` (no `h`/`l`) | same in substance |
| verification code | progress dialog with the code, no keys | code box, no keys | same |
| id-map, drift | no panel in either | no panel in either | same (CLI only) |

Findings from the table:

| # | Where | Old | New | Verdict / fix |
|---|---|---|---|---|
| F18 | Add a Parent | ticks several members of the parent cluster | picks one | the delegation's `parent_resolvers` is the referral the child writes; naming one member of a multi-member cluster is a weaker referral. Verdict wanted: port the multi-select, or have the library expand the picked member to its cluster (a decision, so the library's) |
| F19 | panel menu `L` | logs out (revokes the cached login) | none | **fixed**: ported — `L` on the menu revokes the login at the CA, forgets it, toasts the outcome and lands on the domain list; the next connect asks the password |
| F20 | 3.8, resolver install | same | same | **library, fixed**: the resolver config was written before the admin-server questions, so a cancel there left a half install that the TUI reported as "Nothing was changed". Two fixes: the admin-server questions (whether, listen IP, port) are asked before anything is written, and a post-write stop of any kind is a typed `InstallIncomplete` — "the resolver core install completed and is recorded at …, but its post-install setup did not finish: …" — which the TUI shows as "Install incomplete" and re-detects on; only a pre-write cancel says "Nothing was changed" |

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
| F17 | 3.6, session lost | after the CA's admin server restarts, `r` says "login required: session is unknown", then **every** later panel load asks the password, and none logs in again | same, except roster/servers stayed silent: the connect target kept the *password* and re-sent it per op while the other ops used the dead cached token | pre-existing in both, **fixed** (library + TUI): the refusal is a typed `LoginRequired`, the connect target holds the token it minted (the password is dropped), a change-password drops the cached login the CA just closed, and the panels close to the connect screen on `LoginRequired` — one login, at the connect, never a prompt per op |

Also observed, same for both TUIs (library defaults): the resolver's
default cert name is `resolver.<domain>` (role.domain) while a
publisher's is `<hostname>.<domain>`; two resolvers accepting the default
would collide under the one-live-cert-per-name rule. Set explicitly here.

## Remaining

Phases 0–4 are done. Open: a verdict on F18 (one parent member or
several), F10 (message wording), F16 (first key after a transition,
both TUIs). Still open from the findings: F10
(message wording), F16 (first key after a transition, both TUIs).

Driving harness: `scratchpad/tui.sh` (tmux over ssh; **zsh does not
word-split unquoted variables — use arrays**), `scratchpad/drive.sh`
(scripted ceremonies with a title check at every question; on the HQ
menu `Esc` disconnects, so back out only while the title is a panel's).
