# perms convergence

## The problem

Permissions are pushed to every member of a resolver cluster and never
reconciled. Of the seven things the admin plane syncs between admin servers,
this is the only one with no repair path at all:

| Synced state | Repaired if a host missed it |
|---|---|
| Admin domain map | Pull — `refresh_map_cache`, 30s version check |
| CRL | Pull — `distribute_crl` on each renewal scan |
| id-map | Model + version + reconcile (`design/id-map-convergence.md`) |
| **perms** | **Nothing** |
| Resolver topology | Not repaired, but `facts_match` refuses the register — loud |
| Read gate | Reported back; deliberately not repaired, it is operator state |
| Service control | N/A — a command, not state |

It is also the worst one to be missing, because permissions are the
authorization decision itself, and because of what happens next.

### It is not just "the miss is not repaired"

The CA keeps no copy of a cluster's perms. `handle_read_perms`
(`admin_server/permissions.rs:207`) walks the cluster's registered members and
returns **the document from the first one that answers**. So a stale member is
not merely stale — it is a candidate source of truth.

The reachable sequence:

1. Members A and B both hold `P1`.
2. `perms set X` → `P2`. B is down. A takes it; B stays at `P1`.
3. B returns. A is now down, or simply later in the (server-id-sorted) order.
4. `perms set Y` reads from B — `P1` — applies `Y` to it, and propagates
   `P1 + Y` to the whole cluster.
5. `X` is gone everywhere. Nothing failed. Nothing was reported.

A missed edit can therefore be read back as truth and re-propagated as a
silent regression. That is a different and worse failure than the id-map's,
which could only leave one host behind.

Two smaller problems fall out of the same root:

- **Lost update.** `ops::perms::set_entry` / `remove_entry` are
  read-modify-write against whichever member answered. Two admins editing
  concurrently can clobber each other, and the window is a full network round
  trip wide.
- **`admin perms show` is a coin flip** under partial failure. It reports one
  member's document without saying it is one member's document.

### What is *not* a problem

The enforcement half already converges. The resolver polls the mtimes of its
config **and its `include_permissions` files** every `POLL_INTERVAL` (30s,
`netidx-tools/src/resolver_server.rs:103`) and reloads. So writing a member's
perms file is sufficient; nothing needs a restart.

Which means the line `report_peers` prints today — *"restart the resolver
server(s) to load the new perms"* — is wrong, and has been telling operators
to do something unnecessary. Fix that with this work.

## Design: the CA holds the document

The same shape as the id-map, minus its hard part. Perms are genuinely uniform
across a cluster's members — there is no per-host component the way uids are —
so there is **no diff step**. The CA holds the document, hosts report which
version they have, anyone behind gets the document pushed.

### The model

Per **resolver cluster**, not per admin domain — this is the structural
difference from the id-map. One admin domain has many clusters
(`ResolverClusterEntry`), each with its own perms and its own members.

```rust
struct PermsModel { version: u64, doc: String }   // canonical JSON
// stored keyed by ResolverClusterId
```

`version: 0` means "never established", carrying the same rule as the id-map:
a CA that has lost this knows nothing, and knowing nothing must never mean
"push an empty document at everyone".

Store it in the CA store beside `id-map-model.json`, one file per cluster or
one file keyed by cluster id — the latter is simpler and clusters are few.

### Establishing it

A cluster's model is established by the first `edit_perms`, which carries a
complete document the operator has just looked at. Before that, reads fall
back to polling members exactly as today.

This is the whole adoption story, and it is safe: the document that becomes
authoritative is the one an operator reviewed and propagated. There is no
migration pass, and — since nothing is released — no installed cluster that
predates the model.

### Versioning

Mirrors what the id-map now does, so there is one idiom rather than two:

- `ApplyPermsEditRequest` gains `version: Option<u64>`. A member records it
  beside its perms file on success. `None` keeps the existing meaning: an
  operation that does not by itself make the host current.
- `RegisterRequest` gains `perms_version: Option<u64>`. One number suffices —
  a member belongs to exactly one cluster (`AdminServerEntry.cluster`).
- `AdminServerEntry` gains `reported_perms_version: Option<u64>`, beside
  `reported_read_gate` and `reported_id_map_version`.

### Reconcile

In `handle_register`, next to the id-map check that is already there: if the
member's cluster has an established model and the member's reported version is
below it, push the document with `Some(model.version)`.

That is the entire reconciler. No diff, no ordering, no multi-step repair —
which also means no "only the last step may claim the version" subtlety. One
push, one claim.

Convergence bound is the same ~30s facts poll, and a member in sync costs one
integer comparison.

### What else falls out

- **`set_entry` / `remove_entry` read the model**, not a member. The
  read-modify-write collapses into the CA, under the state write lock that
  already serializes `record_in_model`. The lost-update window closes.
- **The regression path closes**, because step 4 above reads the model rather
  than whichever member answered.
- **`show_perms` should report the model** once established, and say so. What
  a *particular member* holds is a different question, worth its own answer
  when someone is chasing drift — see open questions.

## Implementation phases

Smaller than the id-map's, and the first two are the same shape:

1. **Model + storage.** `PermsModel` keyed by cluster in the CA store; write it
   in `handle_edit_perms` after authorization and before the fanout, so a
   member that misses the push is afterwards just a version behind. Nothing
   reads it yet.
2. **Version reporting.** `version` on `ApplyPermsEditRequest`, members record
   it beside the perms file, `perms_version` on `RegisterRequest`,
   `reported_perms_version` on `AdminServerEntry`.
3. **Reconcile + read from the model.** The `handle_register` branch, plus
   pointing `ops::perms`' read at the model. This is where the regression path
   and the lost update actually close, so it is the phase worth reviewing
   hardest.
4. **Fix the stale advice.** `report_peers` should stop telling operators to
   restart resolvers. Independent of the rest; could land first.

## Verification

Unit: a model established by an edit; a member behind gets the document; a
member at the version gets nothing; an unestablished model proposes nothing.
The `set_entry` path reads the model rather than a member once established.

Lab, in the 3-site WAN lab, through the installed service:

1. **The regression path, first.** Two members. Stop B, `perms set X`, start B,
   stop A, `perms set Y`. Confirm on the current code that `X` is lost — the
   bug is worth seeing before it is fixed — then confirm the fix keeps both.
2. Stop a member, make several perms edits including a removal, bring it back,
   confirm it converges within ~30s without an operator command, and that the
   *resolver* enforces the new perms (subscribe as an identity whose access was
   removed) rather than merely holding the right file.
3. Concurrent `perms set` from two admins against the same cluster; confirm
   both entries survive.
4. Confirm a member at the current version causes no push (watch the audit log
   stay quiet across several polls).
5. Drive each twice.

## Open questions

- **Should `perms show` report the model, per-member state, or both?** The
  model answers "what should it be", which is usually the question. Per-member
  answers "who is lagging", which is what you want when something is wrong.
  Reporting only the model would hide a member that is behind — which the map
  now knows about, via `reported_perms_version`, so a drift column in the
  roster may be the better home for it.
- **Should the same drift be visible for the id-map?** Same question, same
  answer presumably, and it argues for one "who is behind" view covering both
  rather than two.
- **One file or per-cluster files** in the CA store. Backup/restore has to
  carry whichever it is.
