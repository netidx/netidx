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

The last three fixes (compare-and-swap on edits, the socket id-map cache, and
the CA owning cluster permissions) were verified against a **clean install**,
not the domain the earlier findings came from, so nothing rests on state a
previous run had left behind.

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

That closed the window on the CA host but not the class — think-time in
`$EDITOR` is unbounded, and two admins can read the same version from anywhere.
So the class is closed by compare-and-swap (`33e92df6`): a read returns the
version it hands out, an edit hands it back as `base_version`, and the CA
records the document only if that still names the current version, answering
`Stale` with what *is* current otherwise. The comparison is inside the same
write lock as the write it guards.

`perms set` / `perms remove` rebase onto the returned document and reapply, up
to 4 times — their intent means the same thing against a new base. A `$EDITOR`
document is not rebased and must not be: what the operator submitted is the
intent, so the CLI prints both documents and refuses.

Lab-verified: three `perms set` commands back to back, all three entries
present afterwards.

### 6. "INCONSISTENT until every peer is updated" — FIXED (`935e65ba`)

Approving a delegation with a member down told the operator the cluster was
inconsistent and to re-run the command. Neither is true: step 3 proved the
member converges by itself. The message now says so.

### 7. An id-map change took up to an hour to be enforced — FIXED (`9a2be7a3`)

`UserDb::ifo` (`netidx/src/resolver_server/auth.rs:147`) caches each identity's
group membership for `id_map_timeout` and nothing invalidates it when the
id-map changes. At the flat 3600s default that meant a revocation could take an
hour, while `admin drift` reported the id-map converged on every host the whole
time — the tooling asserting the opposite of what was being enforced.

Demonstrated: with `/data` denying `users` and granting `engineering`, the
subscriber's group was toggled between the two four times. The id-map reached
every host each time (v15, v17, v19, v21, all reported current) and **the data
plane never changed**: the subscribe succeeded in all four states, including
the two where the identity was in `users` and had to be denied. Restarting the
resolvers cleared the cache and the same identity was immediately `Denied`.

The default now comes from the source. `Command` forks `/bin/id`, which may go
out to SSSD or AD and reflects a directory nobody here administers — an hour is
right. `Socket` is a round trip to a local daemon holding the map in memory,
and that map is what the admin plane edits, so the cache is purely the delay
between revoking a group and enforcing it — a minute, the same order as the
30s poll that delivered the change. An explicit setting still wins on either.

Re-measured in the lab against a clean install, with the publisher granted by
name so only the subscriber's group moved:

```
move subscriber to `users`        -> Denied after 89s
move it back to `engineering`     -> allowed again after 81s
```

30s to converge plus up to 60s of cache, both directions. Previously neither
direction happened at all without restarting the resolvers.

This bounds the staleness rather than removing it. Making revocation prompt
needs the daemon to report a generation and `UserDb` to drop older entries,
which is a change to the id-map socket protocol — still open, and still a
reasonable thing to want. Installs that already wrote `"id_map_timeout": 3600`
keep it until re-rendered.

### 8. Cluster members disagreed on perms until the first edit — FIXED (`75c97b87`)

Before any perms edit, the two members of cluster `/` held *different*
documents:

```
.11  "/": { "resolver-hq-a.netidx.test": "swlpd", "users": "swl" }
.12  "/": { "resolver-hq-b.netidx.test": "swlpd", "users": "swl" }
```

`template/resolver.rs:291` seeded each host's perms with **its own** TLS name
granted `swlpd` at the base — a resolver uses its own certificate as a client,
so it needs rights at the base — which meant the same subscription authorized
differently depending on which member it reached, silently, from install until
someone made a perms edit.

And the first edit resolved the divergence by propagating one host's document
to everyone. In the lab hq-b **lost** its own grant and gained hq-a's, as a
side effect of granting `/data` to `engineering`: a silent revocation for one
host and a silent extension of the other's rights across the cluster.

Eric's ruling: the CA is authoritative for permissions, self-grants are just
permissions the CA adds and propagates normally, and a non-CA admin server
never owns permissions. So:

- the CA adopts its own installed document as the cluster's, once, giving a
  cluster a document from the moment it exists rather than from the first edit;
- each member's own grant is added to that document as it enrols.

The grant is *derived* from what the CA is issuing rather than requested by the
enrollee — the only entry a host could legitimately ask for is the one for the
identity the CA is about to give it, and the CA already knows that — so an
enrolling host cannot ask for anything on someone else's behalf.

Verified against a clean install: after hq-b joined, the cluster's one document
held both grants, and both hosts converged on it.

```
"/": { "users": "swl",
       "resolver-hq-a.netidx.test": "swlpd",
       "resolver-hq-b.netidx.test": "swlpd" }
```

Still open: removing a server leaves its grant in the document. It is inert —
the identity's certificate is revoked, so it no longer authenticates — but it
is untidy.

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
