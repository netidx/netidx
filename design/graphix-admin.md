# graphix-package-netidx-admin + the Graphix admin TUI

Status: **Designed** (package layers 1–2, placement, phasing) /
**Sketched** (ceremony protocol details, TUI module structure).
Port state (2026-09-02): phases A–C done; D — the remote tab complete,
the Local tab landed (detection, status, the in-process lifecycle
actions, the local admin panels through `tui::panels`, the Services
surface `tui::services`, the app shell `tui::app`); the install /
uninstall / join / restore actions wait on phases 4–5. The running
state lives in the findings log's ledger.

## Objective — read this first

**The primary objective is to improve Graphix, not to ship a TUI.** The
admin TUI is the most ambitious application anyone has attempted in the
language (the largest `.gx` program in existence today is 209 lines;
this will be thousands), and we are building it to discover what a real
user building a real application will hit: compile-time walls,
diagnostic quality, missing widgets, ergonomic gaps, and outright bugs
in the compiler, the JIT, and the stdlib. The TUI is the instrument.

That ordering implies a working rule with no exceptions:

> **No workarounds.** When something isn't nice — an awkward idiom, a
> slow compile, a confusing error, a missing capability — we stop, file
> a finding, and fix it in Graphix (or consciously decide it's fine),
> *then* continue. We never route around a Graphix deficiency to keep
> the port moving.

The subtle failure mode to watch for: quietly moving logic into the
package's **Rust** layer because expressing it in Graphix was painful.
The Rust layer's job is bridging (types, IO, the `Answerer` seam) and
nothing else; every piece of *decision or presentation logic* that
lands in Rust instead of Graphix is a finding we papered over. When in
doubt, the logic goes in `.gx` and the pain gets written down.

Findings go in `design/graphix-admin-findings.md` (this repo), one
dated entry per wall: what we were trying to write, what happened, and
the disposition (graphix issue filed / fixed / consciously accepted).
Compile time is measured and logged at every size milestone (1k, 2k,
4k… lines) — the graphix typechecker-performance history says walls
appear suddenly and must be caught early.

## What exists (survey)

- **`netidx_admin::ops`** (`ops/mod.rs`): ~75 public functions
  restructured as **query + action** pairs — queries return structured
  rows, actions take the row's out-of-band **code** as argument, codes
  are always recomputed locally. Built explicitly so "the strict CLI,
  the TUI, and Atlas share one implementation and the library never
  touches a terminal." A Graphix frontend is the fourth consumer of a
  seam designed for exactly this.
- **`netidx_admin::answer::Answerer`**: the question/progress seam the
  interactive ceremonies (`plan::{install, enroll, ca_setup, bundle,
  server_setup, service, delegation}`) drive. Questions are identified
  by the closed `Field` enum, each with a `FieldInfo` (flag, label,
  help) — the engine owns the decision logic, the frontend owns
  presentation.
- **The ratatui TUI** (`netidx-tools/src/admin/tui/`, ~11.2k lines):
  already bridges `Answerer` into an event protocol — every question
  arrives as a `UiRequest` over a channel and is answered over a
  `oneshot` (`tui/answer.rs:59`). This is the shape a reactive
  language consumes natively; the Rust TUI hand-rolls the modal queue,
  pending `VecDeque`, busy flag, and redraw plumbing that Graphix's
  dataflow gives structurally.
- **The browser precedent** (`netidx-tools/src/browser/mod.rs`, 44
  lines): `ShellBuilder` + internal source + `NetConfig::Ready` — the
  proven embedding pattern. The browser's Graphix source ships as a
  module *inside* graphix-package-tui, which is also the model for
  where the admin TUI's source should live (below).
- **Graphix package bridging vocabulary** (graphix repo,
  `stdlib/graphix-package-sys/`): `Rt::watch_var` for streams,
  `Rt::watch` + `CustomBuiltinType` for events carrying reply
  channels, `Rt::spawn_var` for one-shot async results, opaque types
  for handles (`process.gxi`'s `Proc` is the template), `defpackage!`
  for registration.

## Placement

The package lives in **this repo** (`netidx/graphix-package-netidx-admin/`),
not graphix `stdlib/`:

- It versions with `netidx-admin` (0.32.x, moving fast), not with the
  graphix shell.
- It becomes the **first real external package**, dogfooding the
  package manager's external-package path (`packages.toml` v2
  `[packages]` table), which no real package exercises today.
- No dependency cycle: the workspace already path-deps
  `graphix-{shell,rt,compiler,package,package-core}` (root
  `Cargo.toml:46-50`); the new crate adds `graphix-derive` and deps on
  `netidx-admin` + `netidx-admin-proto` in-workspace.

Crate name `graphix-package-netidx-admin` (the naming convention);
Graphix module name **`netidx_admin`** — the package name is the
top-level module by construction, and a package that can in theory be
mixed with any other package doesn't get to claim a name as generic as
`admin` (Eric, 2026-08-18).

Platform: the crate compiles everywhere. Unix-only surface
(daemon/local-socket/CA ops) is declared in the `.gxi` unconditionally
and returns a typed `` `Unsupported `` error at runtime on other
platforms — the interface can't be cfg'd, and honest runtime errors
beat a platform-forked API.

## The API — three layers

### Layer 1: the data model

`netidx-admin-proto` and ops row types cross as Graphix structs and
variants via `netidx-derive` `FromValue`/`IntoValue`, declared in the
`.gxi`. Representative:

```graphix
type Fingerprint = { code: string, glyph: string };
type CaIdentity = { domain: string, roles: Array<string>, fp: Fingerprint };
type EnrollmentRequest = {
  code: string,        // the out-of-band code — the only handle actions take
  kind: string,
  san: string,
  requested: datetime,
};
type AdminInfo = { name: string, may_manage_admins: bool, /* … */ };
type ServerDrift = { server: string, current: bool, /* … */ };
```

Handles are opaque: `type Session;` (an authenticated remote admin
connection), `type Target;` (`netidx_admin::local(cfg_path)` on unix /
`netidx_admin::remote(session)`), `type Ceremony<'r>;` (layer 3). Private
fields like the per-request RPC id stay behind the code-as-id rule —
the Graphix surface addresses rows only by code, same as every other
frontend.

Identicon rendering: the fingerprint's 8×8 identicon is the security
gesture's visual half. The package exposes it as data —
`val identicon: fn(fp: Fingerprint) -> Array<Array<u8>>` (color
indices) — and the TUI renders it (canvas or styled spans). Rendering
in Rust would be a workaround; the identicon is presentation logic.

### Layer 2: queries and actions (plain async builtins)

The whole non-interactive `ops` surface, one builtin per function,
`Rt::spawn_var` one-shots returning `Result`:

```graphix
val list_queue: fn(t: Target) -> Result<Array<EnrollmentRequest>, Error<`Admin(string)>>;
val approve: fn(t: Target, #code: string) -> Result<null, Error<`Admin(string)>>;
val list_admins: fn(t: Target) -> Result<Array<AdminInfo>, Error<`Admin(string)>>;
val drift: fn(t: Target) -> Result<Array<ServerDrift>, Error<`Admin(string)>>;
// … roster, perms, id_map, delegation, revoke, servers, service, slots
```

Error type: a union, not a bare string —
`` [`Admin(string), `PasswordChangeRequired(string), `Unsupported] `` —
because frontends must *route* on `PasswordChangeRequired`
(`ops/mod.rs:52`: "routing on the text of an error is the coupling
that breaks the first time someone rewords it"). This is also the
showcase for Graphix typed-error routing: the TUI's `catch` +
`select` on the error variant opens the change-password screen.

Discovery is a stream, not a query: `val discover: fn() -> AdminDomain`
delivering each mDNS-browsed admin domain as it appears (the
`sys::fs::watch` pattern) — continuous browse is native dataflow.

### Layer 3: ceremonies (the `Answerer` bridge)

The design centerpiece. Each interactive flow is a builtin returning
an opaque `Ceremony<'r>` (`'r` = the flow's typed result):

```graphix
val install: fn(#role: Role, #dry_run: bool) -> Ceremony<InstallReport>;
val join: fn(#dry_run: bool) -> Ceremony<JoinReport>;
val connect: fn(#server: string) -> Ceremony<Session>;   // identity confirm + password
val renew: fn() -> Ceremony<RenewReport>;
// … add_parent, backup, restore, uninstall, change_password
```

Starting a ceremony spawns the op's future with a package-side
`Answerer` whose methods surface as a typed event stream:

```graphix
type Event<'r> = [
  // blocking — the engine is awaiting an answer keyed by `id`
  `Text({ id: u64, field: FieldInfo, default: [string, null], required: bool }),
  `Secret({ id: u64, field: FieldInfo }),
  `Choice({ id: u64, field: FieldInfo, choices: Array<string>, default: [string, null] }),
  `Confirm({ id: u64, field: FieldInfo, default: bool }),
  `SelectAdminDomain({ id: u64, domains: Array<{ domain: string, identity: CaIdentity }> }),
  `SelectParent({ id: u64, rows: Array<ParentRow> }),
  `Announce({ id: u64, title: string, body: string }),
  `AnnounceIdentity({ id: u64, body: string, fp: Fingerprint }),
  `ConfirmIdentity({ id: u64, identity: CaIdentity }),
  `OneTimeSecret({ id: u64, kind: OneTimeSecretKind, password: string }),
  // non-blocking — presentation only
  `VerificationCode({ purpose: string, fp: Fingerprint }),
  `ClearVerificationCode,
  `Progress({ stage: Stage, message: string, duration: [duration, null] }),
  `Note(string),
  `Warn(string),
  // terminal
  `Done(Result<'r, Error<`Admin(string)>>)
];

val events: fn(c: Ceremony<'r>) -> Event<'r>;
val answer: fn(c: Ceremony<'r>, #id: u64, a: Answer) -> null;

type Answer = [
  `Text([string, null]), `Secret(string), `Choice(string), `Confirm(bool),
  `Domain([`Discovered(i64), `Manual, `PollMore]),
  `Parents(Array<i64>),
  `Ack,          // Announce / AnnounceIdentity / OneTimeSecret
  `Cancel        // abort the ceremony (drops the reply — the op errors out)
];
```

This is deliberately a **1:1 reification of `tui/answer.rs`'s
`UiRequest`** — the protocol the TUI already proved out — with the
`oneshot` replaced by `answer(#id, …)`. Mechanics mirror
`sys::net`'s `NetState`: `Answerer` methods send `CustomBuiltinType`
events through `Rt::watch` to the ceremony's bind; `answer` resolves
the held oneshot; a kind-mismatched or stale-id answer is an error
event, and `` `Cancel `` is the Esc gesture.

The alternative considered — questions as calls into Graphix
*callback* lambdas (`install(#text: |q| …)`, resolving each oneshot on
the callback result's first production) — is rejected: modal queueing,
pending-while-busy, and the render loop are frontend policy that the
event stream leaves in Graphix where it belongs; callbacks would bury
the queue inside the package and constrain every frontend to one
interaction shape. (A `ceremony::scripted` helper answering from a
`Map<string, string>` of flag→value can be layered *in Graphix* for
tests — the strict-CLI answerer's behavior as a library function.)

Secrets: passwords cross as ordinary Graphix strings. The `Secret`
zeroize discipline ends at the boundary — the operator types the
password into a Graphix text input, so the string exists in the UI
regardless. Accepted, recorded here.

## The TUI

**Source lives in the package**, as Graphix modules
(`src/graphix/tui/…` → `netidx_admin::tui`), exactly as the netidx browser
ships inside graphix-package-tui. This solves multi-file structure
(a multi-thousand-line program is modules with `.gxi` interfaces, not
one embedded string) and makes the TUI itself installable Graphix
source a user can read — the best documentation of "how to build a
real app" we could produce. The `netidx admin` binary embed is then a
one-liner program (`use netidx_admin::tui; tui::main()`) through the
browser's 44-line `ShellBuilder` pattern; CLI params (config root,
`--server`, …) are seeded through `setup_context` into libstate and
exposed as `netidx_admin::params()`.

Port order — best-fit material first, hardest platform dependencies
last:

1. **Remote panel** (pure ops consumer: tables/lists over layer-2
   queries, code-verification modals, the connect ceremony).
2. **Services + drift + admin-domains panels.**
3. **Local tab** (detected-install status, lifecycle actions).
4. **Install/join/restore ceremonies** (layer 3 full modal flows).
5. **Privileged handoff + $EDITOR flows** (needs terminal
   suspend/resume, below).

The Rust TUI stays until the Graphix one reaches parity, then is
**dropped** — netidx-admin has not shipped, so no deprecation window.

## Known prerequisite Graphix work (findings already in hand)

Discovered by reading the Rust TUI before writing any code; each is a
graphix-repo work item, to be done *there*, not worked around here:

1. **Modal/overlay widget for graphix-package-tui.** The admin TUI is
   modal-driven to its core; the tui package has no
   stack/popup/Clear-overlay equivalent (the gui package has `stack`,
   tui has nothing). Prerequisite for phase 1.
2. **Terminal suspend/resume.** `tui/privileged.rs` suspends ratatui
   (raw mode + alternate screen) to run `sudo`/`su` children and
   `$EDITOR` on the real terminal, then resumes. Needs a tui-package
   capability composing with `sys::process` Inherit stdio.
   Prerequisite for phase 5 only.
3. **Synthetic key-event injection** for `input_handler`, so a test
   harness can lab-drive the TUI (navigate panels, answer modals,
   assert resulting state). Complements — never replaces — looking at
   the rendered output and testing that real terminal events work.
   Wanted early so the test suite grows with the program instead of
   being retrofitted.
4. **Program-scale compile performance.** 209 lines → thousands is a
   20–50× jump on the biggest program the compiler has ever seen.
   Not a known bug — a known *unknown*, measured continuously per the
   findings discipline above.

Expected-but-unconfirmed friction worth watching for explicitly:
or-patterns in `select` (deferred feature; a keymap-heavy TUI will
price its absence), styled-text ergonomics at scale, and whatever the
JIT does with a program two orders of magnitude past its test corpus.

## Phasing

- **A — package, layers 1+2** (data model, queries/actions, discovery
  stream, `Session`/`Target`/connect). Tested against the
  `netidx-admin/tests/e2e.rs` harness pattern. Independently useful:
  scripted admin + live dashboards in Graphix.
- **B — ceremony bridge** (layer 3) + a scripted-answerer test driving
  `join --dry-run` end-to-end from Graphix.
- **C — graphix-repo prerequisites** (modal/overlay; synthetic
  key-event injection; suspend/resume
  deferred to E).
- **D — TUI port**, panels in the order above, findings log running
  throughout.
- **E — privileged/editor flows**, then parity review, then delete
  `netidx-tools/src/admin/tui/`.

A and C can proceed in parallel (different repos). B blocks D.

## Resolved questions (Eric, 2026-08-18)

1. Module name: **`netidx_admin`** — see Placement.
2. `Ceremony<'r>` stays as designed. Whether a parameterized abstract
   type + generic `Event<'r>` survives the builtin boundary is
   empirical ("I hope it does, I guess we'll see") — if it doesn't,
   that's a finding to fix in Graphix, not a cue to redesign around it.
3. Synthetic key events: yes — prerequisite item 3 above. They drive
   the lab; visual review and real-terminal event testing still happen.
4. Port order: implementer's discretion; the order above is the
   default, not a mandate.
