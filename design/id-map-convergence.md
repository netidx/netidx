# id-map convergence

## The problem

An id-map edit reaches every id-map host that is up at the time. A host that
is down is left behind, and nothing brings it back.

Trace, as the code stands:

1. `handle_edit_id_map` takes `id_map_targets(&map)` — every admin server in
   the map with `Role::IdMap` and `ServerState::Registered`. A host that is
   merely *down* is still `Registered`, so it is a target.
2. `push_id_map_edit` to it fails, or times out after `PUSH_TIMEOUT` (10s).
3. It gets a `PeerResult` carrying the error. The response is still
   `Ok(IdMapPropagationOk)` — the hosts that took the edit did take it.
4. The CLI prints `the hosts now DISAGREE … re-run <command> once they are
   back to converge`.

Re-running does converge, because every applier arm reports `changed: false`
rather than erroring when it finds nothing to do. But it is entirely
operator-driven, and nothing in the system repairs it:

- `reconcile_identities_to_target` (`admin_server/issuance.rs:799`) replays
  identity registrations to one host, but its only caller is `handle_register`
  gated on `current.state == ServerState::Enrolled`
  (`admin_server/topology.rs:857`) — the *first* registration after enrollment.
  A host that was already `Registered`, went down, and came back re-registers
  while already `Registered`, so it does not fire.
- `ReconcileCa` fans out the CA identity, the map, the CRL, and topology. It
  carries no id-map state.
- The `pending_pushes` retry loop (`admin_server/runtime.rs:637`) covers only
  issuance-time registrations — a sign that committed but whose push never
  confirmed. An operator's edit is not in that recovery set.

So the two writers to the same file have different durability: **a
registration the CA pushes after signing is retried automatically; an
operator's `admin id-map` edit is not.**

### Why it matters more than tidiness

The divergence is directionally unsafe. `RemoveMember` / `RemoveIdentity` are
how access is taken away. A host that missed one keeps authorizing with the
old membership when it returns, indefinitely, until someone notices. An `add`
that failed to land merely fails closed.

## Requirement

An id-map host that missed any change — because it was down, restarted from an
old backup, or enrolled before the change was made — converges to agreement
without an operator noticing and re-running anything.

"Agreement" is on **names and group membership only**. uids stay per-host and
are deliberately not part of it (`IdMapEdit`'s wire docs; that is why an edit
propagates as an operation and not as a document).

## Design: an authoritative model, reconciled by version

The CA keeps the authoritative *shape* of the admin domain's id-map, hosts
report which version of it they hold, and the CA reconciles any host that is
behind. Convergence becomes a property of the state, not of a delivery log.

### The model

Durable at the CA, alongside the issued-record store:

```rust
struct IdMapModel {
    version: u64,
    groups: BTreeSet<String>,
    identities: BTreeMap<String, IdentityShape>,   // san -> shape
}
struct IdentityShape { primary_group: String, groups: BTreeSet<String> }
```

No uids, no gids. That is the whole point — the model says *who is in what*,
each host answers *what number it calls them locally*.

The CA already holds most of this implicitly: `IssuedRecord.groups`
(`ca_store.rs:148`) is what `reconcile_identities_to_target` replays. The model
makes it explicit and extends it to the things an issuance never knew about —
groups created on their own, and membership changed after signing.

### Where it is written

Every applied `IdMapEdit` updates the model and bumps `version`, in the same
write that decides the edit is authorized — so the model cannot drift from what
was actually propagated. Concretely, in `handle_edit_id_map` after
`authorize_id_map_edit` succeeds and before the fanout, and in
`push_registrations` for the CA's own post-sign registration.

The model is CA state. A host holds only its own map file plus the version it
last applied.

### Versioning

This mirrors the admin domain map's existing `MapVersion` / `GetMapVersion` /
`refresh_map_cache` pattern rather than inventing a second idiom.

- `ApplyIdMapEditRequest` gains `version: u64` — the model version this edit
  produces. A host that applies it records that number beside its id-map file.
- `RegisterRequest` gains `id_map_version: Option<u64>` — what the host holds.
  `None` from a host with no id-map role, or one that has never applied
  anything.
- `AdminServerEntry` gains `reported_id_map_version: Option<u64>`, exactly as
  it already carries `reported_read_gate`.

A host that fails to apply an edit does not record the version, so it stays
behind and the next register picks it up.

### The trigger already exists

`spawn_facts_poll` (`admin_server/runtime.rs:65`) has every non-CA host call
`report_facts` every `MAP_REFRESH_INTERVAL` (30s), which re-registers. So
`handle_register` runs for every id-map host twice a minute already.

Widen its existing reconcile hook: instead of firing only when
`state == Enrolled`, fire whenever

```
entry.roles.contains(Role::IdMap) && req.id_map_version < Some(model.version)
```

The version compare is what keeps this cheap — a host in sync costs one
integer comparison per poll, not a map fetch.

Convergence bound: **≤ ~30s after the host comes back**, with no new timer, no
new task, and no new wire round trip in the steady state.

### The reconcile step

For a host that is behind, `reconcile_id_map_to_target(server, addr)`:

1. `GetIdMap` from that host (the request already exists) — the actual state,
   uids and all.
2. Diff against the model, ignoring uids, producing an ordered
   `Vec<IdMapEdit>`:
   - groups in the model but not on the host → `AddGroup`
   - identities in the model but not on the host, or whose primary group or
     group set differs → `AddIdentity` (idempotent, keeps an existing uid)
   - memberships on the host not in the model → `RemoveMember`
   - identities on the host not in the model → `RemoveIdentity`
   - groups on the host not in the model → `RemoveGroup`
   Order matters: create groups before identities that reference them, drop
   memberships and identities before the groups they hold, or the applier's own
   invariants reject the batch.
3. Push each through the existing `push_id_map_edit`, then record the model
   version on success.

Steps 2 and 3 are pure functions of (model, host map) — testable without a
network, which is where most of the behaviour should be pinned.

### Pruning is required, and is safe here

Healing a missed *removal* means the reconciler must delete what the model does
not have. That is destructive if the model could be incomplete, so state
plainly why it cannot be: the model starts empty at CA init and every write to
any host's id-map goes through it. There is no path that writes a host's map
without the CA — that was the point of deleting `component id-map`.

Two caveats to encode rather than assume:

- **Scope is one admin domain.** A delegated child runs its own CA with its own
  model. The reconciler must only ever touch hosts in its own map.
- **Never prune during first adoption.** If a model is ever introduced over
  hosts that predate it, the first pass must adopt what it finds rather than
  delete it. Not a concern for this change — nothing is released, so the model
  is empty-at-genesis by construction — but a `model_established: bool` (or a
  version of 0 meaning "not yet authoritative") keeps the rule in the code
  rather than in this document.

### What it subsumes

One convergence mechanism instead of several:

- Operator edits missed by a down host — the case that prompted this.
- `pending_pushes`' id-map role: the sign-then-crash case is just another host
  behind the model version. That loop can lose its id-map duty (it should keep
  the record for the issuance itself).
- `reconcile_identities_to_target`'s `Enrolled`-only special case: a newly
  enrolled host is a host at version `None`, which is behind. The dedicated
  path folds into the general one.
- A host restored from an older backup.

## Rejected alternative: a pending-edit log

Persist each failed `(IdMapEdit, unconfirmed targets)` and replay FIFO per
target. Smaller, and closer to `pending_pushes`.

Rejected because it only heals the failure mode it was built for. It does
nothing for a host restored from a backup or otherwise divergent, it needs its
own garbage collection, and it can wedge: one edit that errors permanently on
one target (say `RemoveGroup` for a group that host still has a member of,
because it also missed the `RemoveMember` ordering) blocks every edit behind
it, and the queue never drains. A state diff has no ordering to get wrong
across time and no queue to block.

The one thing the log does better is auditability — "which edit was missed" is
explicit. The audit log already records every `edit-id-map` operation, so that
is covered.

## Implementation phases

1. **Model + storage.** `IdMapModel` in the CA store with its atomic write, and
   the update in `handle_edit_id_map` / `push_registrations`. No behaviour
   change yet; the model is written and never read. Tests: the model after a
   sequence of edits equals the map those edits produce on one host.
2. **Version reporting.** `version` on `ApplyIdMapEditRequest`, hosts record it
   beside the map, `id_map_version` on `RegisterRequest`,
   `reported_id_map_version` on `AdminServerEntry`. Still no reconcile — but
   `admin id-map show` and the map can now *report* who is behind, which is
   worth having on its own.
3. **The diff.** `reconcile_id_map_to_target`, pure-function core first, with
   the ordering invariants as tests: a group is created before an identity
   naming it, a membership dropped before its group.
4. **The trigger.** Widen `handle_register`'s hook to the version compare;
   retire the `Enrolled` special case and `pending_pushes`' id-map duty.

Phases 1–2 are independently useful and independently reviewable. 3 has all the
sharp edges and none of the network. 4 is the small one that turns it on.

## Verification

Unit: the diff's ordering invariants; prune only what the model lacks; a host
at the current version produces an empty diff (no needless pushes).

Lab, in the 3-site WAN lab, through the installed service:

1. Stop an id-map host. Run `add-group`, `add-user`, `add-member`, then
   `remove-member` and `remove-user` — several edits, including removals,
   while it is down. Confirm the CLI reports it as not updated each time.
2. Bring it back. Without touching it, confirm within ~30s that its map matches
   the others in names and membership, and that its uids are unchanged for
   identities it already had.
3. The removal case specifically: confirm the identity removed while it was
   down is gone, not merely absent from the others.
4. Restore a host from a backup taken before a batch of edits; confirm it
   converges the same way.
5. Drive each of these **twice** — the config-authoritative lab's finding was
   that the second application is where things break.

## Open questions

- **Should a host behind the model refuse to serve id-map queries?** It is
  knowingly authorizing on stale data, and the resolver has a read gate for
  exactly this shape of problem. Refusing is safer and noisier; serving stale
  is available and quiet. This is a policy call.
- **Where does the model live** — a file in the CA dir beside the issued
  records, or a section of an existing one? Affects backup/restore, which must
  carry it.
- **Should `admin id-map show` report the model** rather than one host's map,
  with per-host drift shown separately? It would answer "what is the admin
  domain's id-map" instead of "what does this one host think", which is
  usually the real question.
