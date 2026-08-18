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
