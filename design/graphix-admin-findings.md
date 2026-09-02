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
| 2026-08-31 | 1799 | reg 1.13s / app-main 2.68s | DEV build. The app-main number is the pump+remote composition (`milestone_timing`, now a permanent ignored test): two instance elaborations of the ~700-line remote body + pump. 2x the lines, ~4.5x the elaboration cost — no wall, but the growth is super-linear; re-measure at 3k. |

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
can't resolve. The silent asymmetry is the bug. UPDATE 2026-08-22:
Eric ruled this the last straw for the open-style module system —
the fix is a full transition to a Rust-2018-style use system
(explicit imports, self/super/package roots, renames, wildcards,
position-independent mod, and one materialized per-module namespace
table that every resolution consults). Design:
graphix design/module_system.md; the three faces above become the
red→green regression fixtures of its P3.**

**FIXED 2026-08-22 (graphix branch `module-system`): the use system
landed — resolution consults the defining module's namespace table
(a global registry keyed by scope path, exempt from lexical
restore), so instance elaboration sees the def module's imports and
private types. All three faces are green fixtures
(`finding1_sig_alias`, `finding1_private_type_in_body`,
`finding1_imported_body_annotation` in graphix-tests, and they
fuse). This package migrated the same day: the tui module's parent
types arrive by `use super::{…}`, tui-root helpers by
`use tui::{line, span, style}`, widget modules by the
`use tui::<w>::{self, *}` idiom; all 19 package tests pass.**

## 2026-08-31 — the abstract-predicate pin flipped again (nominal), and the nameless diagnostic

Resuming Phase D against a week of graphix landings (module system,
nominal abstract types, traits, List, or-patterns): the baseline run
was 18/19, the one failure our own
`abstract_type_predicate_refused_at_compile` pin. Not a regression —
the rule CHANGED under it: nominal abstract types (graphix
2026-08-22) made `Target as t` a legal exact tag test (`AbstractId`
comparison), so the 08-18 compile refusal this pin asserted was
deliberately deleted. Verified both faces against current graphix:
over `[Target, i64]` the predicate compiles and dispatches nominally;
over disjoint `42` it is refused as a dead arm. The pin is now
`abstract_type_predicate_is_nominal` (both faces).

The dead-arm refusal had its own wart: it printed ``pattern abstract
will never match i64`` — `Type::Abstract` carries only the
path-derived id, so Display had no name to print. At admin-TUI scale
every abstract-typed select would pay that. **Disposition: FIXED in
graphix (same day): a process-global `AbstractId → name` registry
filled at `AbstractId::of` (every mint goes through it, packed decode
included since registration re-typechecks), consulted by `Type`'s
Display — the refusal now reads ``pattern Target will never match
i64``, and parameterized abstracts print `Name<params>` instead of
`<abstract#id>`.**

## 2026-08-31 — or-pattern @-captures refuse the keymap idiom Rust accepts

First real consumer of or-patterns (landed yesterday), exactly where
the design doc predicted: keymaps. The natural rewrite of the pump's
duplicated arms —

```graphix
kk@ `Up | kk@ `Char("k") => { sel <- max(0, (kk ~ sel) - 1); `Stop }
```

— is refused: "or-pattern alternatives must bind kk at exactly equal
types (first alternative: `Up, here: `Char(string))". The message is
excellent (names the bind and both types). But the refusal itself is
stricter than the orthodoxy it aims for: in Rust `x @ A(_) | x @ B(_)`
is LEGAL, because Rust bindings don't narrow — the binding gets the
scrutinee's (enum) type. Graphix narrows the capture to its
alternative's variant singleton, so "exactly equal types" refuses the
form orthodox Rust code writes. Even the or-patterns design doc's own
syntax example (``t@ `D(_) | t@ `E(_) => f(t)``) is ill-typed by its
own rule. The idiom that works — and what the TUI now uses — is
sampling an OUTER binding as the trigger (`k ~ sel`, the handler's
own parameter), so no capture is needed:

```graphix
`Up | `Char("k") => { sel <- max(0, (k ~ sel) - 1); `Stop }
```

**Disposition: language-design question filed for Eric — should an
@-capture in an or-arm type as the UNION of its per-alternative
types (the exact type of what it can capture, of which Rust's rule is
the degenerate case)? Same-binds equality would then govern only
structural payload binds. Interim: the outer-binding idiom, which
reads fine; the design doc example needs fixing either way.**

## 2026-08-31 — the change-password routing gap (package API, not graphix)

Porting the remote tab surfaced a hole in the package's layer 2, not
in graphix: `AdminError.PasswordChangeRequired` exists so frontends
can route to the change-password flow, but when `connect`'s ceremony
fails with it there is no `Target` in hand — and `change_password`
takes one. The Rust TUI routes with the verified connection it built
internally; the graphix `connect` ceremony consumed the name and
password through the answerer, so the frontend holds nothing to
reconnect with. The package needs a from-scratch change-password
ceremony (`#server`, optional `#admin`, asking old/new passwords via
the answerer — the same flow the strict CLI necessarily has).
**Disposition: package work item (Rust bridging layer, permitted),
next slice. The remote tab shows the error as a toast until then.**

## 2026-08-31 — the panel screens: variant-of-union coverage, and the rectangle hole behind it

The remote tab's screen type is the natural nested state machine —
`Screen = [`Connect, `Menu, `Panel(PanelKind)]` with `PanelKind` a
5-member union — and the natural per-panel select over it refused:

```graphix
select screen {
  `Connect => ..., `Menu => ...,
  `Panel(`Queue) => ..., /* … all five … */ `Panel(`Certs) => ...
}
```

"missing match cases: [.., `Panel(`Certs), .., `Panel(`Servers)] does
not contain [`Connect, `Menu, `Panel(PanelKind)]" — five arms jointly
exhaust the member, but coverage asked each scrutinee member to be
covered by a SINGLE arm predicate; only slice length-ladders and the
bool pair had pooling. Adjudicating the fix surfaced a second, worse
bug sitting exactly opposite: `Type::union`'s Variant×Variant arm
merged same-tag variants COMPONENT-WISE at any arity, so the coverage
union of `` `P(`A,`X) `` and `` `P(`B,`Y) `` arms was the RECTANGLE
`` `P([`A,`B],[`X,`Y]) `` — and a select whose arms cover only the
diagonal compiled cleanly on main, leaving `` `P(`A,`Y) `` to fall
through every arm at runtime. (The panel case refused rather than
mis-accepting only because set-CHAINED unions skip the pairwise merge
— the two bugs masked each other's territory.)

**Disposition: both FIXED in graphix (same day):**

1. `contains` gained the distribution law: after the single-member
   and prim walks refuse, a set covers a product-headed rhs member
   (variant/tuple/struct) when same-shaped members cover every
   argument position but one in full and their pooled remaining
   position covers it — sound by rectangularity, run as a pure probe
   over a cell-free scrutinee side (commits nothing; strictly
   monotone). `[`P(A), `P(B)] ⊇ `P([A, B])` now holds everywhere
   contains is asked, not just in select.
2. `union`'s variant merge now requires at most ONE differing
   position (`union_identical` per slot) — the exact-merge condition;
   a diagonal pair stays a two-member set, and the diagonal select is
   refused as non-exhaustive.

Pins: `select_variant_union_payload_exhausts`,
`select_variant_union_rect_exhausts` (both fuse — the JIT handles the
nested-variant arms natively), `select_tuple_union_member_exhausts`,
and the negative `select_variant_union_diagonal_rejected`. The remote
tab's panel selects compile as written.

## 2026-08-31 — finding 1's true residual: the instance body typechecked under the caller's env

The remote tab's first full test run failed "undefined type Toast" at
per-callsite instance elaboration — Toast being a module-private type
used as a UNION MEMBER (`[Toast, null]`) in a body annotation, its
struct connected from a nested handler lambda. Minimized to 12 lines:
bare private annotations (`let y: P = x`) survive, `[P, null]` dies.
The mechanism, root-caused with a new `GXDBG_TYPEREF=1` probe: the
def gate's probe walks answer `[P, null] ⊇ null` WITHOUT expanding
`P`, so no def-time walk cell-fills that ref (bare annotations get
filled, which is the only reason the 08-22 module-system fix's
fixtures passed) — and the instance body's typecheck0/1 ran under the
CALLER's env, where the defining module's private typedefs are gone.
Every un-filled body ref resolved against the wrong world; worse, one
that happened to resolve there could silently mean a DIFFERENT type.

**Disposition: FIXED in graphix (same day): `GXLambda` snapshots the
def-side env its body was compiled under (it was already restored for
the body COMPILE — the init's `with_restored`) and now restores it
around the body's `typecheck0`/`typecheck1` drives too; args stay
caller-side. `TypeRef::with_scope` additionally carries an
already-filled resolution into the re-scoped cell (filled = the
name's final target, env-independent by design). Pinned red→green as
`finding1_private_type_union_member` (it fuses). The remote tab's
`Toast`/`Act`/`Screen`/`PanelKind` private types stand as written.**

## 2026-08-31 — the phantom event replay: the wake-forced init view re-raised past events

The find of the campaign so far, and the remote tab's blocker: any
ceremony flow with two consecutive modal questions broke, because
answering the first left its Enter STANDING in the handler's params,
and when the next question woke a fresh select arm, the arm's call
sites materialized under the wake-forced init view — which delivered
those standing values as FIRED. The phantom Enter routed into the
freshly opened Secret modal's `enter` and submitted it with an empty
password ("login refused" with no password modal ever rendered).
`GXDBG_CS` showed the smoking gun (`text_keys … argfired=true` with no
key dispatched), and the same uncommanded dispatch was latent in the
EXISTING pump test — its standing 'a' just routed to a harmless
fallback, which is why the first live round trip never caught it.
Minimized to 30 lines of pure graphix at the callable layer.

**Disposition: FIXED in graphix (Eric's ruling: present-but-stale).**
Three seams, each found by running the live ceremonies against the
previous fix:

1. `Ref`/`Deref` standing reads upgrade to fresh only under a GENUINE
   init view — the wake-forced view (`event.wake_init`) reads stale.
   Things born at init (constants, first productions) still fire;
   things REPLAYED into a woken subtree never do.
2. A present scrutinee with NO retained selection still routes: the
   select chain runs on a depth-0 first consult with STALE wake binds
   (selection is a value question; the guard-flip wake keeps its
   aug03 FIRED — a guard's fire is a genuine event).
3. `ByRef` seeds its cell from a present-but-stale child as a
   standing STALE entry (the fired-only gate left a woken modal's
   `&handle` cell empty and the tui input_handler never received its
   callable — the identity modal ate every key).

Pinned red→green at the callable layer
(`arm_wake_delivers_standing_args_stale`); the full workspace suite,
the 452-program findings corpus, and both live-domain TUI tests are
green. The remote tab now drives connect → menu → queue → roster end
to end under the harness. A semantics change of this depth needs a
fuzz soak before it counts as landed.

## 2026-08-31 — test-side: the AdminName default pre-seeds the modal

The harness typed the admin name into a field already holding the
question's DEFAULT (the OS username) — submitting "ericroot". Correct
pump behavior (defaults are the point); the test now clears the field
first. Worth remembering for scripted drivers: a Text modal's editor
starts at the default, cursor at end.

## 2026-08-31 — @-captures union across or-alternatives (ruled, built)

Closing the morning's or-pattern question: Eric ruled the capture
types as the UNION of the alternatives' narrowed types — Graphix
narrows captures where Rust binds at the enum type, so the
exactly-equal rule refused the keymap idiom
``kk@ `Up | kk@ `Char("k")`` that orthodox Rust accepts. Built in
graphix same day (a reused capture leaf widens the shared binding;
payload binds keep exact equality; the shape fuses). The pump's
keymaps keep the outer-binding sampling they were refactored to —
both idioms are now legal, and new code can use whichever reads
better.

## 2026-09-02 — strict fusion reached the package: two pure builtins stopped fusing

Resuming the port after graphix's strict-fusion flip (2026-09-01) and
the fastcall sweep (09-02), the package's own suite showed three
fixtures failing their `FuseExpect::Jit` annotation — `parse_fingerprint`
and `identicon` are `Sync` + `STATELESS` builtins that fused under the
old rules and, under strict fusion, fuse only with a registered fast
fn. The bidirectional harness is what caught it: an external package
re-annotated nothing and still saw the regression the day it rebuilt.

**Disposition: fixed in the package (one `FASTCALL` per builtin, the
eval delegating to the same fn through `fast_eval`) — exactly the
one-line opt-in the performance model advertises. Positive finding for
the model: a package author outside the stdlib gets the fused site
with no compiler involvement. Worth a line in the `#[native]` chapter:
a pure builtin that does not register a fast fn is a node-walk
boundary by rule, and the harness will say so.**

## 2026-09-02 — test-side: the domain fixture's config lock outlives its drop

With a fourth live-domain test in the package, `TestAdminDomain` began
failing to start ("acquiring the test config lock"): the fixture holds
its serialization guard as a struct field and aborts its daemon task in
`Drop`, but the daemon's config-dir lock is released only when the
aborted task actually unwinds — after the guard is gone and the next
fixture is already trying. **Disposition: netidx test fixture, not
graphix — `start_with_password` now retries the lock for up to ten
seconds.** Also on the test side: a graphix setup program that
`connect`s leaves a cached session for its admin in the process, and a
later `connect` that names nobody rides it (correct package behavior —
the TUI test's setup logged in as root and the tab then connected as
root with no questions). The fixture gained `mint_role_admin`, which
mints over a password session and caches nothing.

## 2026-09-02 — the change-password route, and what starts a ceremony

The slice: `change_password_at` (a from-scratch password session that
replaces a reset password; the finding of 08-31), `#glyph` on both
session ceremonies (a CA fingerprint confirmed once rides along, so the
follow-on flow skips the gesture), `info` (what a `Target` is, for the
menu title), the tab's routing of a refused connect into the ceremony,
and `c` on the roster for the session's own password. Both live tests
drive it end to end — through the package API in graphix, and through
the tab's modals.

The graphix e2e program stalled twice, and both stalls were the same
package-design fact seen from two sides. The ceremony builtins started
on the delivery of their trailing positional (`server`, or the
`Target`) and read whatever the labeled args held at that moment —
CachedArgs-style builtins, by contrast, wait until every argument is
present. So `connect(#password: one_time, listen)` with an async
`one_time` started at init on the literal address and asked for the
password; and `change_password_at(#glyph: must_change ~ glyph, …)` with
a never-fired `glyph` started never, silently (the sample was bottom).
In the TUI both read as "the pump asks a question you expected the
program to have answered" — a stall only where nothing answers.
**Disposition: package fix — one `Trigger` (ceremony.rs) shared by every
ceremony builtin: the trailing positional is still the trigger, and a
start now waits until every argument is present, a trigger that fires
while another argument is undelivered or bottom starting the ceremony
when that one arrives. No graphix change: `~` sequencing and the
bottom-in-bottom-out rule behaved exactly as ruled; the asymmetry was
ours.** The glyph itself is the second lesson — captured as a bare
`Fingerprint` from a select that may never fire, it must be typed
`[Fingerprint, null]` and defaulted, the way the tab already wrote it;
whether a connect asks the gesture at all depends on whether the
domain's cert is the user CA dir's (`resolve_identity` verifies against
the local CA silently), which the fixture now reports as
`gesture_expected` so the TUI tests branch on it instead of assuming.

## 2026-09-02 — "shut for another 29m": datetime − datetime, and what durations look like

The servers panel's read-gate column wants "how long until this member
answers again" — `until − now`. `datetime - datetime` is refused by the
typechecker ("Number does not contain datetime"), and that is the
2026-07-12 ruling working as designed: arithmetic is
`fn('a: Number, 'a) -> 'a` and datetime/duration arithmetic lives in
`sys::time` functions. But `sys::time` had `add`/`sub` (datetime ±
duration), `add_dur`/`sub_dur`, `scale` — and no datetime − datetime,
the one every elapsed/remaining computation needs. Three more things
surfaced on the same probe: the five `sys::time` functions were pure
`Sync` builtins without fast fns (missed by the 09-02 fastcall sweep —
their fixtures were annotated `None`, which is why nothing flagged it);
duration literals accept only `ns`/`us`/`ms`/`s`, so `duration:3.h` is
an opaque "Unexpected `(`" parse error two tokens away; and a duration
interpolates as `1800.s`, so a TUI formats its own "30m".

**Disposition: `sys::time::diff(later, earlier) -> duration` added
(saturating at zero — durations are unsigned), the five time functions
given fast fns, their fixtures flipped to `Jit` plus two `diff` pins
(graphix). The literal units and the duration format are consciously
accepted for this slice (the panel writes seconds and formats in
graphix) and noted for the parser: minute/hour units would read
better, and an unknown unit deserves a diagnostic that names it. Eric
raised the real question — arithmetic as traits, like `Eq`/`Ord` — and
the answer is "after traits v2": `datetime - datetime -> duration` is
heterogeneous, which needs trait type parameters and an associated
`Output`, neither of which traits v1 has; a homogeneous `Arith` trait
would cover only duration + duration.**

## 2026-09-02 — `never()` arms leave a select's type open (third sighting, an idiom to name)

The perms panel's `let r = setperm_r?; r.changed` was refused with
"expected struct not `['_a, '_b, {changed, version}]`": `setperm_r` is
a `select run { `SetPerm(e) => … set_perm(…), _ => never() }` with a
nested `null as _ => never()`, `never()` is `fn() -> 'a`, and by the
free-member rule a select's type is the union of its arms with free
members kept free — so the value carries two unbound members and a
field access cannot pick the struct. The three earlier result selects
in the same file (`roster_r`, `resetpw_r`, `rmadmin_r`) never touched
a field, which is the only reason they compiled. **Disposition: the
documented answer — annotate the binding (`let setperm_r:
Result<RecordedEdit, AdminError> = select …`) — applied to the four
result selects; consciously accepted, and worth a line in the book's
select chapter: a `never()` arm needs the binding annotated when the
value's shape is used downstream.** The alternative, `never()` typed as
an absorbed bottom rather than a fresh variable, is a typing ruling for
another day.
