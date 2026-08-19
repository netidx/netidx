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
