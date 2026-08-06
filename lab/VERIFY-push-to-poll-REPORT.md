# Lab verification — the CA is authoritative, members converge by polling

Branch `netidx-admin`, 3-site WAN lab (HQ 192.168.50.0/24, EU 192.168.60.0/24,
either side of a netem-shaped router). Everything driven through the installed
`netidx@root.service`, never a hand-started daemon.

Binaries built from a pristine export of git HEAD, and each host's binary probed
for a string only the newest commit under test emits before any assertion was
made. (A previous run of this lab tested a binary built from a working tree a
concurrent agent had reverted; see `reference-verify-deployed-binary-has-the-change`.)

## Build-out

Fresh install of a two-level hierarchy, all four hosts wiped first:

| host | address | role |
|---|---|---|
| resolver-hq-a | 192.168.50.11 | CA + resolver + id-map, cluster `/` |
| resolver-hq-b | 192.168.50.12 | resolver + id-map, cluster `/` |
| resolver-eu-a | 192.168.60.15 | resolver + id-map, cluster `/eu` (delegated child) |

Confirmed working:

- **Phase 2 — the CA records an enrolling host's whole config.**
  `/root/.config/netidx/ca/desired-configs.json` appeared at the CA holding
  hq-b's complete `file::Config` at version 1 — member block, TLS cert/key
  paths, id-map socket, `include_permissions`, tuning.
- **Delegation.** `/eu` requested and approved; both HQ members took the child
  referral and eu-a took the parent referral naming both HQ addresses.
- **The renderer.** After the delegation, hq-b's and eu-a's stored configs both
  advanced to version 2 with the topology folded in, and both hosts installed it
  and stamped `resolver.json.version: 2`. This is the poll path doing the work,
  not the push — a push writes no version stamp.
- **The id-map converges.** All three hosts hold a byte-identical, number-free
  id-map stamped v1, matching the CA's model. eu-a reached it on its own poll
  after enrolling.

## Findings

### 1. Drift deadlocks convergence — a member that missed a change can never be repaired (critical)

`admin_domain::register` (`netidx-admin/src/admin_domain.rs:655`) refuses a
registration whose reported resolver facts differ from the CA's cluster:

```
the CA refused the registration: reported resolver configuration drifts
from the CA-approved resolver cluster
```

But the register response is the *only* thing that delivers the corrective
config. So drift → register refused → no config delivered → drift persists,
forever, retrying every 30s.

This is precisely the case the polling architecture exists to serve: a member
that was down when topology moved comes back with stale `children`, which is
drift, and is refused. Lab step 3 ("add a child cluster with one member down;
confirm it picks up its referrals on return") could not have passed.

**Reproduced.** hq-b's `children` was emptied and its version stamp removed —
the same state a member that missed a delegation is in. It never recovered; the
journal shows the refusal repeating on every poll.

There is a second door to the same deadlock, and it is the more likely one in
practice: a host whose `resolver.json` will not load reports no facts at all
(`local_resolver_data`, `topology.rs:402`), which hits
`bail!("the approved Resolver role must report resolver facts")`. **A host with
an unloadable resolver config can never be handed a working one.**

The check made sense when the CA pushed config — refusing to record facts it had
not authorized. Under polling it is backwards. And it protects nothing
structurally: `register` never adopts the reported facts into the map. It
records `addr`, `state`, and the reported versions, and nothing else. The facts
are used only as a gate, so refusing buys no invariant — it only denies repair.

**Principle:** a member's report may never be grounds to refuse it the thing that
would fix it. Reports are observations; grants are the CA's.

### 2. The CA never reports its own applied versions

`own_ca_entry` (`netidx-admin/src/admin_server/topology.rs:800`) hardcodes

```rust
reported_read_gate: None,
reported_id_map_version: None,
reported_perms_version: None,
```

so the CA's own map entry always claims it has applied nothing. `admin drift`
shows the CA permanently `BEHIND`:

```
8528c8c5-...  192.168.50.11:4565  (never edited)  none < v1 BEHIND
```

while the CA's disk holds exactly the v1 id-map its own model describes. Drift
is now the operator's primary signal that an edit has landed everywhere, so a
host that can never report itself current means the report never reads clean.

### 3. The CA holds no desired config for itself

The CA host never enrolls, so nothing ever calls `DesiredConfigs::set` for it.
`desired_config::render` returns `None` on its first line (`stored.get(server)?`),
so `converge_self`'s config half is dead code on every deployment.

Confirmed: `desired-configs.json` contains hq-b and eu-a and not the CA, and the
CA is the one host with no `resolver.json.version` stamp.

The consequence is that `push_topology`'s doc — "This is an optimization, not
the mechanism ... a server that misses this ends up with exactly the same
document within a poll interval" — is false for exactly one host, the one
holding the CA. For it, the self-push *is* the mechanism, with no repair path
behind it.

### 4. Config drift is invisible

`RegisterRequest.config_version` is sent on every poll and then dropped:
`AdminServerEntry` has no `reported_config_version`, so nothing records it.
`admin drift` reports PERMS and ID-MAP but not the resolver config — the one
piece of state the CA now completely owns.

### 5. Orphaned doc comment

`topology.rs:826` carries a doc comment belonging to a deleted function
("Whether a host reporting `reported` is behind the CA's id-map model...") on
`handle_register`, cut off mid-sentence at "A CA with no model".
