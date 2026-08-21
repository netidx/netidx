# Graphix findings from the admin TUI campaign

The running log required by `design/graphix-admin.md`. One dated entry
per wall: what we were trying to do, what happened, disposition
(graphix issue filed / fixed in graphix / consciously accepted). The
rule: no entry, no workaround — if the port routed around something,
it's logged here first.

Compile-time milestones (log at every ~1k lines of `.gx`):

| date | .gx lines | `--check` time | notes |
|------|-----------|----------------|-------|
| 2026-08-21 | 909 | reg 417ms / pump-callsite 593ms | DEV build (unoptimized). Registration = all stdlib + package packed-AST decode + typecheck; the 593ms is ONE `pump(...)` call site's per-callsite instance elaboration of the ~300-line pump body. No wall yet; watch the per-callsite number as the app main grows. |

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

## 2026-08-21 — the question pump: 378 lines in, the first live TUI round trip

Phase D's first real slice: `netidx_admin::tui` (the package's first
submodule directory) with the shared modal question pump — all 14
`Question` kinds as self-answering modals (line_edit text/secret,
choice/domain lists, confirm toggles, identity gestures with the
identicon rendered from `identicon()`'s cells), `answer` refusals
re-prompting in place, presentation events accumulating in
notes/progress/code channels. `tui/mod.gx` (~360 lines) is the largest
single `.gx` file written to date. The lab loop worked as designed:
`TuiTestHarness` (now public — graphix 8cc305b7, prerequisite 3 closed)
drives a REAL `connect` ceremony against `TestAdminDomain` from key
events to `Done(target)`: identity modal → `a` → masked password entry
→ Enter → modal closes, result lands. The reactive idioms the design
hoped for all held up: one `select q` observer feeding modal state, a
reactively-precomputed `composed` answer sampled by Enter, connects
from key-handler lambdas, `&`-refs keeping keystroke updates out of
the widget tree. Three graphix findings below fell out; the rest of
the friction was self-inflicted documented gotchas (`///` in a `.gx`,
unescaped `[` in string literals).

## 2026-08-21 — reserved-word diagnostics at package scale (3rd instance)

`let ok = answer(id, a)?` — `ok` is a reserved literal word — inside a
select arm inside the 378-line file reported ``Unexpected ` ` `` at the
ENCLOSING `select` statement's head, four lines above the offender,
with "can't use keyword as a function or variable name" buried among
four merged Unexpecteds and an Expected list of statement keywords.
Same signature for an unescaped-`[` interpolation error (reported at
the `select` head two lines above the string). Diagnosing took a
ten-step bisection against a standalone graphix binary; at admin-TUI
scale every such error costs that. Strengthens the open 2026-08-18
diagnostics finding: the refusal reason AND the offending position
both need to survive the combine merge. **Disposition: graphix work
item (diagnostics), still open.**

## 2026-08-21 — slice patterns carry no exhaustiveness credit

`select acc { [] => .., [init.., last] => .. }` over `Array<T>` is
refused ("missing match cases") even though empty + nonempty is
exhaustive — slice patterns contribute nothing to the coverage union,
so every array select needs a `_` or bind catch-all (the whole corpus
already obeys this; the CLAUDE.md quick-ref's 4-arm slice example
would not compile). The message is its own finding: "type mismatch
\[\] does not contain Array<...>" — the `[]` is the empty SET TYPE
(the union of zero pattern types) rendered as if it were an empty
array pattern. **Disposition: FIXED in graphix (2026-08-22, Eric:
"slice patterns should contribute to exhaustiveness"): unguarded
slice arms whose element patterns match anything pool their LENGTH
claims (exact `[a, b]` = its length; head/tail rest forms = a
minimum), and a pool whose lengths cover ℕ covers each scrutinee
array member that EVERY pool arm's type predicate contains — runtime
dispatch is type-gated per arm, so a differently-typed slice arm is
a hole, not coverage. Guarded arms and refutable-element arms claim
nothing, and the refusal now names the failure: the first uncovered
length, the missing rest form, or why an arm was excluded. Dead-arm
analysis is length-precise too (Eric's follow-up: no dead arms left
behind while there is no installed base) — a slice arm whose whole
range is matched by earlier covering arms is refused, a member whose
lengths complete subtracts from the residual so a trailing wildcard
behind a full ladder dies, and the bool literal pair now subtracts
as well; the whole existing corpus survives because real-world
slice+wildcard selects are partial ladders. The empty-coverage
message reads "no unguarded arm irrefutably covers T" instead of
"\[\] does not contain T". The package's fold is back to the natural
`[] / [init.., last]` spelling.**

## 2026-08-21 — type names resolve differently at def-site and use-site

The wall of the day, three faces, one root: TYPE-name resolution
after the defining module's compile does not see the module's `use`
aliases (value names are fine):

1. An interface type whose DEFINITION uses a gxi `use` alias
   (`Pump.layers: Array<overlay::Layer>` under `use tui::overlay`)
   registers fine but fails "undefined type overlay::Layer in
   netidx_admin::tui" the moment a CONSUMER touches the field.
2. A module-private `.gx`-only type (`Blocking`) used in a public
   lambda's body annotations fails "undefined type Blocking" at the
   consumer's per-callsite instance elaboration — private types
   don't survive into the instance's resolution env.
3. A bare use-imported type name in a body annotation (`Array<Line>`
   under `use tui`) fails the same way at instance elaboration.

In every case the def-site compile ACCEPTED the spelling, so the
package author learns about it only when the first consumer breaks —
the worst possible place. Interim (consciously accepted, logged
first): the tui module fully qualifies every type annotation
(`tui::Line`, `tui::Tui`, `tui::input_handler::Event`, ...) and
`Blocking` moved into the interface. **Disposition: graphix work
item — either instance/consumer type resolution honors the defining
module's `use`s and private types (the env_independent_typerefs
carried-cell design suggests seeding the body annotations' TypeRef
cells at def compile), or the def-site refuses what the use-site
can't resolve. The silent asymmetry is the bug.**
