# Graphix findings from the admin TUI campaign

The running log required by `design/graphix-admin.md`. One dated entry
per wall: what we were trying to do, what happened, disposition
(graphix issue filed / fixed in graphix / consciously accepted). The
rule: no entry, no workaround — if the port routed around something,
it's logged here first.

Compile-time milestones (log at every ~1k lines of `.gx`):

| date | .gx lines | `--check` time | notes |
|------|-----------|----------------|-------|

---

## 2026-08-18 — no modal/overlay widget in graphix-package-tui

Found by survey before any code: the admin TUI is modal-driven
(`tui/answer.rs`'s whole protocol renders as modals) and the tui
package has no stack/popup/Clear-overlay equivalent — the gui package
has `stack`, tui has nothing. **Disposition: prerequisite work item in
the graphix repo (phase C), blocks TUI phase 1.**

## 2026-08-18 — no terminal suspend/resume

`tui/privileged.rs` suspends ratatui (raw mode + alternate screen) to
run `sudo`/`su` children and `$EDITOR` on the real terminal, then
resumes. No tui-package or `sys::process` capability covers this.
**Disposition: graphix work item, composes with `sys::process`
Inherit stdio; blocks TUI phase 5 only.**

## 2026-08-18 — program scale is unmeasured territory

Largest `.gx` program ever written is 209 lines (soak-dash.gx); the
admin TUI will be thousands. Compiler/typechecker behavior at that
size is a known unknown with a bad history (the GUI wedge).
**Disposition: continuous measurement via the milestone table above;
any wall is its own entry.**

## 2026-08-18 — reserved type keywords cannot be field names

The very first real data model hit it: `Progress.duration` (the field
is literally named `duration` in the Rust struct) fails to parse in the
`.gxi` — primitive-type names (`duration`, `string`, `i64`, `bool`,
`datetime`, …) are reserved words in every identifier position (`let`,
lambda params, struct fields, field access). The reservation is
consistent, so this is a language-design question, not an
inconsistency: field-name position (`{ x: … }`, `.x`) is grammatically
unambiguous, and mirrors of external data will keep wanting `string`/
`duration`/`bool` as field names. **Disposition: FIXED in graphix
(f100e4fa, same day — Eric: "we should fix that one"): field names may
be any lowercase identifier in struct literals/types/patterns, field
access, and functional update; keyword shorthand stays refused (it
names a binding); printers never re-sugar keyword fields; the
round-trip generator and the tree-sitter grammar exercise the space.
The package's interim rename is reverted — the field is `duration`.**

## 2026-08-18 — reserved-word parse errors are unactionable

The failure above reported ``Unexpected `(` — Expected whitespaces,
`,` or `]``` pointing at the variant's paren, lines of grammar away
from the offending `duration:`. Nothing names the real problem
("`duration` is a reserved word") or its position. The combine
committed-error merge again (the known refusal-message problem —
`grow::parsing` solved this for depth refusals via the thread-local;
reserved-word refusals need the same treatment or a keyword check with
its own error). **Disposition: graphix work item — diagnostics. (Still open: the
keyword-field fix removes this instance, but any reserved word in a
binding position still reports the misleading merged expectation.)**

## 2026-08-18 — parameterized abstract types cross the builtin boundary (positive)

`type Ceremony<'r>;` + `val events: fn(c: Ceremony<'r>) -> Event<'r>`
typechecks and runs through `defpackage!` on the first try — the
`connect_refused` fixture drives `Ceremony<Target>` through `events`
into a `select` and extracts `Done`'s `Result<Target, AdminError>`
payload on both engines. The design doc's open question 2 is closed:
no per-ceremony concrete event types needed.

## 2026-08-18 — overlay widget built; synthetic key events mostly exist

Prerequisite 1 (modal/overlay) is DONE in graphix: `tui::overlay` —
`overlay(#layers: &Array<Layer>, base)` + `layer(#width?, #height?,
#size?, child)`, centered/cleared layers, topmost captures input,
empty stack = base (graphix repo, with book chapter and a runnable
modal example). Discovery along the way: prerequisite 3 (synthetic
key events) is MOSTLY BUILT already — graphix-package-tui's headless
`TuiTestHarness` (src/test/mod.rs) renders to a `TestBackend` buffer,
dispatches crossterm events into `handle_event` (the live path), and
watches named bindings; the overlay input-capture tests drive a real
modal with it. What remains for the TUI-app lab is packaging that
harness for programs outside the tui crate (it is `pub(crate)`).

## 2026-08-18 — binding relaxation completes the field fix

Eric's follow-up ruling after probing the shorthand refusal: type-name
keywords are legal BINDING names too (graphix f2690c29) — `let
duration`, params, labeled args, pattern binds, tvar names — so
`{duration}` shorthand and `let {duration, ..} = x` destructuring now
work, which matters because users will immediately destructure the
keyword fields the first fix enabled. Two facts surfaced during
adjudication: bare type patterns (`select v { i64 => .. }`) were NEVER
legal — `as` is the type-test marker — so a relaxed bare `duration`
pattern binds without shadowing anything; and `let true = 5` parses as
a refutable LITERAL-pattern let, not a binding, so the literal words'
reservation is untouched. The select-arm parser needed zero changes
(the `typ() .. as` attempt already backtracks). Control keywords and
literals stay reserved as bindings and as shorthand. 2048-case hunt
clean, 32k-case hunt running.

## 2026-08-18 — the hunt found the one poisoned keyword: `bytes`

Eric's "let the regression test find anything we've missed" paid off
in one 32k-case run: `let bytes: T = v` is genuinely ambiguous —
`bytes:` is the only literal prefix whose payload (base64) overlaps
the identifier alphabet and admits short/empty payloads, so the
annotated-bind reading collides with a refutable literal-pattern let.
Every other primitive's payload can't look like a type. Resolution
(graphix): `bytes` retreats to field-only — bindable nowhere, still a
legal field name (fields never meet the literal grammar). Pinned;
8k-case re-hunt clean. Method note for the campaign: the shrunk
witnesses all landed on the SAME word in one run — generator-driven
keyword mixing is doing exactly what it was asked to.

## 2026-08-18 — the ops surface is in; the missing piece is a live harness

Stage 3 (6a5e79d5): the whole Remote-panel vocabulary — queue, revoke,
delegation, servers, roster, id-map, discovery — 26 vals in the two
shapes the ratatui TUI proved (ceremonies-over-Target with the
pinned-glyph answerer; plain async builtins for AdminTarget ops).
Observation while looking for integration coverage: NO frontend has
any — netidx-admin's e2e tests cover templates/resolvers, the
admin_server tests drive handlers directly with no listener, and the
ratatui TUI tests fake at the PanelRow level. The interactive flows
(connect's identity gesture, the queue Q/A, session cache) are
manually tested everywhere. An in-process admin-domain harness (CA +
`admin_server::runtime::serve` on a loopback port, bootstrapped by
driving `plan::ca_setup` with a scripted answerer) would serve
netidx-admin itself, the ratatui TUI, AND the graphix package — the
package's ceremony machinery would then get a REAL interactive
round trip (identity confirm → password → approve with id-map-groups
question). **Disposition: proposed as the next work item.**

## 2026-08-18 — the live harness exists; the first live graphix run found two more

`netidx_admin::testing` (feature `testing`, unix): `TestAdminDomain`
founds a REAL CA + admin server by driving `create_vaulted_ca` with a
rule-based `SetupAnswerer` (the same founding ceremony `ca init`
runs), serves it on loopback with mDNS off, one domain per process.
`tests/admin_domain.rs` is the admin plane's first live integration
test in ANY frontend: roster, queue, delegations, servers, the
session-cache semantics (a password session is a CREDENTIAL HOLDER —
verification happens at `cache_session`, which is also why the graphix
`connect` now calls it: verify-at-connect + the cached token is what
keeps every later op quiet), wrong-password refusal at the right
layer, and PasswordChangeRequired routing. The package's `e2e.rs`
then drives the full ceremony chain FROM GRAPHIX against the live
domain: ConfirmIdentity answered via `answer(q.id, `Confirm(true))`,
the password question answered, `Done(target)`, a roster query, and
a quiet remote ceremony over the cached session. Two graphix findings
fell out of writing it:

1. **Partial struct patterns don't infer from a known scrutinee**:
   `` select ev { `Secret({id, ..}) => .. } `` refuses ("will never
   match") even though the payload type is fully known — the pattern
   types as a one-field exact struct. The annotated form
   (`S as {x, ..}`) and whole-payload bind (`` `Secret(q) => q.id ``)
   both work; the annotation is redundant ceremony the TUI would pay
   at every event dissection. **Graphix work item: seed the partial
   pattern's struct type from the arm's scrutinee type.**

2. **An explicit type predicate on an ABSTRACT type is a
   typechecker-accepted dead arm**: `` select r { Target as t => .. } ``
   compiles, but `is_a` (correctly, per the jul17a ruling) refuses to
   claim a value it can't verify, so the arm NEVER matches and the
   wildcard silently wins — the exact dead-arm class the typechecker
   normally refuses. The designed dissector for `[T, Error]` unions is
   `?`/`$` (and it reads better). **Design question for Eric: refuse
   explicit abstract predicates at compile time (my lean — the trap is
   silent), or make carrier-abstracts verifiable by linking the
   graphix abstract to its Rust registration uuid.** Pinned by
   `abstract_type_predicate_is_dead_at_runtime` (flips on either fix).
