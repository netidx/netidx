# Lab verification — the CA is authoritative, members converge by polling

Branch `netidx-admin`, 3-site WAN lab (HQ 192.168.50.0/24, EU 192.168.60.0/24,
either side of a netem-shaped router). Everything driven through the installed
`netidx@root.service`, never a hand-started daemon. Every change driven twice.

Binaries built from a pristine export of git HEAD. Freshness was checked by
**comparing the md5 of the deployed binary against the build output** — string
probing had already misled me once this session (I probed for a string that was
in an uncommitted change and concluded the deploy had failed). Hash comparison
is exact and has no such failure mode.

| host | address | role |
|---|---|---|
| resolver-hq-a | 192.168.50.11 | CA + resolver + id-map, cluster `/` |
| resolver-hq-b | 192.168.50.12 | resolver + id-map, cluster `/` |
| resolver-eu-a | 192.168.60.15 | resolver + id-map, cluster `/eu` (delegated child) |
| resolver-ap-a | 192.168.60.16 | resolver + id-map, cluster `/ap` (delegated child) |
| publisher-hq | 192.168.50.17 | client, own cert |
| workstation-eu | 192.168.60.18 | client, own cert |

## The verification list, all six steps

1. **Edits with everything up, then a cross-site subscribe.** Perms and id-map
   edits reached every member within a poll; the `/` cluster members ended up
   holding byte-identical documents. Cross-site subscribe covered in step 7
   below.
2. **Several edits, including removals, with a member down.** hq-b was stopped,
   five edits made (id-map adds, a group removal, perms set and remove), then
   restarted. It reached perms v3 and id-map v5 **from one register, with no
   operator action**. This is the case the push plane could not handle.
3. **A child cluster added with a member down.** `/ap` was requested and
   approved while hq-b was stopped — it missed the delegation entirely. On
   return it had both `/eu` and `/ap` referrals within one poll, config v3, with
   no `reconcile-ca` and no operator step.
4. **Read gate through the desired config.** Shut on hq-b: the member's file
   went to `read_gated: Yes`, `reported_read_gate` followed (`reads shut`), and
   **it survived a restart of that member**. Opened again, both directions
   applied twice.
5. **CA relocation with a member down.** The CA's admin server was moved to a
   second address while hq-b was stopped. On return hq-b failed to reach the old
   address four times, searched its known peers pinned to the home CA
   fingerprint, and recorded the new one by itself:

   ```
   the CA at Some(192.168.50.11:4565) has been unreachable for 4 passes;
     asking 2 known peer(s) where it moved
   the CA has moved to 192.168.50.211:4565; recorded
   ```

   Then relocated back with every member up, which exercises the `ApplyCaState`
   push — all three members followed.
6. **ControlService still works.** Remote `activation status` and a targeted
   `restart resolver` against one member by server ID: only the resolver's pid
   moved (7818 → 7968), the admin-server and id-map units were untouched.
7. **Cross-site data plane on the number-free id-map.** publisher-hq published
   `/data/x` at HQ; `/data` denies the `users` group and grants `engineering`.
   workstation-eu at the EU site, across the WAN, subscribed successfully as an
   `engineering` member and was `Denied` as a `users` member — the group
   membership from the number-free id-map is what decided it, with no uid
   anywhere in the chain.

## Findings

### 1. Drift deadlocked convergence — FIXED (`cd33d0f2`)

`admin_domain::register` refused a registration whose reported resolver facts
differed from the CA-approved cluster. But the register response is the only
thing that delivers a corrected config, so drift → refused → never repaired →
still drifting, every 30s forever.

That is exactly the case polling exists to serve: a member down when topology
moves comes back with stale `children`, which *is* drift. Step 3 above could not
have passed. A second door was worse — a host whose `resolver.json` will not
load reports no facts at all and hit `the approved Resolver role must report
resolver facts`, so a host with a broken config could never be handed a good
one.

The check protected nothing: `register` never adopts reported facts into the
map. Drift is now recorded, not refused.

### 2. The CA never reported its own applied state — FIXED (`cd33d0f2`)

`own_ca_entry` hardcoded every `reported_*` field to `None`, so `admin drift`
showed the CA permanently `BEHIND` while its disk held the current model. The CA
now files its report through the same `register` every other host uses.

### 3. The CA held no desired config for itself — FIXED (`cd33d0f2`)

The CA never enrolls, so nothing was stored for it, `desired_config::render`
returned `None` on its first line, and `converge_self`'s config half was dead
code on every deployment. It now adopts its own installed document once.

### 4. Config drift was invisible — FIXED (`cd33d0f2`)

`RegisterRequest.config_version` was sent every poll and dropped. `admin drift`
now has a CONFIG column and reports drift separately from lag.

### 5. A perms edit silently lost the previous one — FIXED (`389a3602`)

The most serious finding. Two `perms set` commands seconds apart, both
reporting ok, and the first grant silently gone:

```
netidx admin perms set /lost carol swl --at /   -> ok, version N
netidx admin perms set /lost dave  swl --at /   -> ok, version N+1
netidx admin perms show --at /                  -> only dave
```

Every `perms set` / `perms remove` is a read-modify-write, and on the CA host
the read returned this host's perms *file*, which trails the model by up to a
poll interval. The remote path already avoids this and its comment describes
this exact failure; the local path returned before reaching it.

A local read now prefers the CA's model when this host holds the CA. Regression
test drives the real entry point and was confirmed to go red without the fix.
Re-verified in the lab afterwards: a second edit correctly reported "no change"
without dropping the first.

### 6. "INCONSISTENT until every peer is updated" — FIXED (`935e65ba`)

Approving a delegation with a member down told the operator the cluster was
inconsistent and to re-run the command. Neither is true: step 3 proved the
member converges by itself. The message now says so.

### 7. An id-map change does not take effect for up to an hour — NEEDS A DECISION

**The one finding with a security dimension, and it is not fixed.**

`UserDb::ifo` (`netidx/src/resolver_server/auth.rs:147`) caches each identity's
group membership for `id_map_timeout`, **default 3600s**
(`resolver_server/config.rs:469`), and nothing invalidates it when the id-map
changes.

Demonstrated: with `/data` denying `users` and granting `engineering`, the
subscriber's group was toggled between the two four times. The id-map reached
every host each time (v15, v17, v19, v21 — `admin drift` reported every server
current) and **the data plane never changed**: the subscribe succeeded in all
four states, including the two where the identity was in `users` and must have
been denied. Restarting the resolvers cleared the cache and the same identity
was immediately `Denied`.

So an operator who revokes someone's group membership, watches `admin drift`
report it converged everywhere, and concludes the revocation is live, can be
wrong by up to an hour. The tooling actively asserts the opposite of the truth.

Two candidate fixes, and the choice is a real design decision:

- **Invalidate on change.** Have the id-map daemon report a generation with each
  answer and have `UserDb` drop entries from an older one. Correct, but it is a
  change to the id-map socket protocol.
- **Shorten the default** for `IdMapType::Socket` only. Cheap, but it trades a
  smaller window for more lookups and does not make revocation prompt.

The hour may well be deliberate for the *platform* mapper, where `/bin/id` is
expensive and the source changes rarely. It looks wrong for the netidx
id-mapper, which the admin plane edits and converges on purpose. Worth your
call rather than mine.

### 8. Cluster members disagree on perms until the first edit — NEEDS A DECISION

Before any perms edit, the two members of cluster `/` held *different*
documents:

```
.11  "/": { "resolver-hq-a.netidx.test": "swlpd", "users": "swl" }
.12  "/": { "resolver-hq-b.netidx.test": "swlpd", "users": "swl" }
```

`template/resolver.rs:291` seeds each host's perms with **its own** TLS name
granted `swlpd` at the base, so the same subscription authorizes differently
depending on which member a client reaches — silently, from install until
someone makes a perms edit.

And the first edit resolves the divergence by taking one host's document as the
model and propagating it. In the lab hq-b **lost** its own `swlpd` grant and
gained hq-a's, as a side effect of an operator granting `/data` to
`engineering`. That is both a silent revocation for hq-b and a silent privilege
expansion for hq-a, on every member.

The plan's Phase 2 called for seeding the model at cluster creation, which was
not done. But seeding from `perms::default_seed` alone would strip every
member's self-grant, so the fix has to also add each member's self-entity to the
cluster model at enrollment and drop it at removal. That makes every resolver's
cert granted at the cluster base on every member — defensible, since cluster
members are already mutually trusted components, but it is a decision about
privilege distribution rather than a clear-cut bug, so I have left it for you.

### 9. `read-gate` demands `--server` while the shared help says it defaults — nit

`netidx admin resolver read-gate` fails with `setting a read gate requires
--server <ADMIN-SERVER>` when run on the CA host, though the shared `--server`
help says it "Defaults to this host's own admin server". Every other admin
command falls back to the local control socket via `resolve_admin_target`;
`set_read_gate` takes a `SocketAddr` and cannot. Either the fallback should be
added or the help should stop promising it.

## Notes for the next pass

- `strings` is not installed on the Debian guests. Compare binary hashes
  instead — exact, and it cannot be fooled by a probe string that only exists
  in an uncommitted change.
- The working tree is shared with other agents; `desired_config.rs` was deleted
  under me mid-session for the second time. `redeploy-head.sh` builds from
  `git archive HEAD` for this reason.
- A backgrounded publisher started over ssh is killed when the session ends.
  Use `systemd-run --unit=...`.
