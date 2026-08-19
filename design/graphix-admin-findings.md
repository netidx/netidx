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
   at every event dissection. **FIXED in graphix (601cde69, same day —
   Eric: both worth fixing): select's typecheck completes inferred
   partial predicates from the scrutinee at any nesting depth and
   realigns the compiled binder's field indexes (which were latently
   wrong — `y` in a completed `` `A({y, ..}) `` read slot 0 before).
   A partial matching several union members refuses with "annotate the
   member you mean". The e2e's payload-bind form stays (it reads
   fine), but `` `Secret({id, ..}) `` now works.**

2. **An explicit type predicate on an ABSTRACT type is a
   typechecker-accepted dead arm**: `` select r { Target as t => .. } ``
   compiles, but `is_a` (correctly, per the jul17a ruling) refuses to
   claim a value it can't verify, so the arm NEVER matches and the
   wildcard silently wins — the exact dead-arm class the typechecker
   normally refuses. The designed dissector for `[T, Error]` unions is
   `?`/`$` (and it reads better). **FIXED in graphix (601cde69):
   refused at compile time with the `?`/`$` guidance — Eric weighed
   match-by-id and refusal; id-matching loses (hidden non-Abstract
   reps have no id, and shared carriers like `Ceremony<A>` vs
   `Ceremony<B>` would claim wrong parameterizations — the halfway
   reading jul17a killed). The pin flipped to
   `abstract_type_predicate_refused_at_compile`.**

## 2026-08-19 — the Question/result split, and what depending on tui found

Slice D1 groundwork: the TUI needs ONE modal question pump serving
every ceremony, but `Event<'r>` differs per ceremony only in
`` `Done(Result<'r, _>) ``. Select does no union subtraction (a
fall-through arm keeps the full scrutinee type, verified), so the
split lives in the package instead — `Event<'r>` is now
`[Question, `Done(Result<'r, AdminError>)]` where `Question` is the
`'r`-free 14-variant union, with graphix-level accessors
`questions(c)` (variant-rebuild strip, written once in mod.gx) and
`result(c)`. Every frontend wants this seam; the strict CLI's
scripted answerer would consume it too.

Three graphix findings fell out, all fixed there same day:

1. **Under-declared stdlib package deps** (88a2be92): the admin
   package is the first external consumer of graphix-package-tui, and
   registration immediately failed — tui's packed browser calls
   `str::dirname` but declared str only as a dev-dependency, and http
   declared no array dep at all despite rest.gx using array::concat.
   Nothing caught this because the shell registers the whole stdlib.
   Audited all packed sources; those two were the only gaps.

2. **Set-contains refused a set's own members as residue** (f89df949):
   the `result` accessor's shape — a select whose declared union
   return mixes a typed arm with `never()` —

   ```graphix
   let result = |c: Ceremony<'r>| -> Result<'r, AdminError>
     select events(c) { `Done(r) => r, _ => never() };
   ```

   failed to typecheck: the bare-tvar residue arm covered rhs members
   only with individual non-bare lhs members, so a member equal to
   the whole lhs set (or the lhs's own `'r` cell) landed in the
   residue and the occurs check refused `'r := ['r, ...]`. Both
   faces now covered reflexively; monotone fix, all gates green.

3. **No union subtraction in select** (logged, not requested): the
   direct form `select ev { `Done(_) => never(), q => q }` cannot
   type as `Question`. The variant-rebuild idiom is fine at package
   scale (write once); noting in case a keymap-heavy TUI makes the
   per-use cost real.

## 2026-08-19 (later) — text entry, and the silent-write callable bug

The connect flow needs text and secret entry; the tui package had no
editor widget at all (the Rust TUI hand-rolls ~1.4k lines of line
editing in answer.rs). Per the no-workarounds rule this went into
graphix-package-tui, not the app: **`tui::line_edit`** (d0c4fb46), a
pure-graphix module — `state`/`handle`/`view` with cursor movement,
boundary-correct deletes, and `#mask` for secrets. The reverse-video
cursor exposed `Modifier` as a two-variant stub; extended to
ratatui's full set.

Writing line_edit's harness test then uncovered the best find of the
campaign so far: **every `*st <- v` write reached through an
embedder-compiled callable was silently dropped** (fixed, 9f9e01d0).
The dispatch path every GUI/TUI handler takes creates callee
instances lazily at their first real event, and a handler's select
arms sleep until then — by which time the `&state` reference value
(delivered once, at the callable's init) only exists in the standing
store, which ConnectDeref never consulted. No error, no log: typing
did nothing. The identical program driven without a callable worked,
which is why nothing in the existing corpus ever caught it — run!
fixtures and the fuzzer never dispatch through compile_callable.
Pinned at the right layer (graphix-tests lib_tests/callable.rs,
verified red on the unfixed compiler). The write-side fix mirrors
Deref's standing read, which had been fixed for the READ side long
ago — the asymmetry comment in Deref::update was already pointing at
this hole.
