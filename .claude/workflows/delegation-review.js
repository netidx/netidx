export const meta = {
  name: 'delegation-review',
  description: 'Adversarial multi-dimension review of the resolver hierarchy delegation feature',
  phases: [
    { title: 'Review', detail: 'one reviewer per dimension over the delegation source' },
    { title: 'Verify', detail: 'adversarially refute each finding; keep only confirmed' },
  ],
}

// The files that make up the delegation feature. Reviewers are told to read
// these (and anything they reference) before reporting.
const FILES = [
  'netidx-conf/src/conf_proto.rs           — delegation wire messages + ResolverAddr/ReferralEdit + serde',
  'netidx-conf/src/delegation_store.rs      — on-disk queue/approved/denied store',
  'netidx-conf/src/conf_server.rs           — handlers: request/poll/list/approve/deny + apply_referral_edit + push_to_cluster_peers + approve_delegation_prepare',
  'netidx-conf/src/conf_client.rs           — pinned client fns + delegation_code fingerprint + push_referral_edit',
  'netidx-conf/src/resolver.rs              — ResolverConfig::resolver_addrs',
  'netidx-conf/src/template/mod.rs          — set_parent_referral',
  'netidx-tools/src/conf/delegation.rs      — add-parent / review-delegation CLI + delegate_under_parent',
  'netidx-tools/src/conf/roles/resolver.rs  — resolver subcommand wiring',
  'netidx-tools/src/conf/init.rs            — install-time child branch (run_resolver, authchoice_to_info)',
  'netidx/src/resolver_server/config.rs     — children-overlap validation (recently fixed: Path::is_parent)',
]

const CONTEXT = `
You are reviewing the **resolver hierarchy delegation** feature in the netidx
Rust workspace (cwd is the repo root). Background:

- A child resolver is delegated a namespace subtree (e.g. /eu) by a parent
  resolver. The parent stores a \`children[/eu]\` referral; the child stores a
  \`parent\` referral. Referrals route across the boundary transparently.
- Delegation is a TOPOLOGY operation only — it exchanges \`ResolverAddr\`s
  (address + data-plane auth) and does NOT enroll certificates. It assumes ONE
  shared trust domain (one CA, one krb5 realm).
- The ceremony mirrors the existing CA enrollment queue: the child
  \`request_delegation\` (queues) + polls; the parent admin authenticates with
  the CA vault password and \`list/approve/deny\`s, matching an out-of-band
  "request code" (a fingerprint over (path, child addrs+auth)) by human judgment.
- A parent is frequently a CLUSTER of peer resolver servers (member_servers).
  An approval edit MUST reach every cluster peer via a server-to-server
  \`ApplyReferralEdit\` push (peer-cert-gated). The push must be LOUD on a down
  peer (the cluster is then inconsistent) and IDEMPOTENT so a re-run re-syncs.
- The approving host must hold BOTH ca + resolver roles (admin auth + the
  config to edit + its own address). \`child\`/\`parent\` are \`Vec<ResolverAddr>\`.

Feature source files:
${FILES.map(f => '  - ' + f).join('\n')}

Read the files relevant to your dimension (and anything they call) with your
tools before reporting. Cite exact file:line. Judge against what the code
ACTUALLY does, not what comments claim. Prefer a few high-confidence findings
over many speculative ones. The request code ceremony is integrity, not
authentication — do not report "the code doesn't authenticate the child" as a
bug; the trust anchor is the admin's out-of-band judgment (this is by design).
`

const DIMENSIONS = [
  {
    key: 'security',
    prompt: `${CONTEXT}

DIMENSION: SECURITY & AUTHORIZATION. Look for: missing or bypassable admin
auth on list/approve/deny; the peer-cert gate on ApplyReferralEdit (can a
non-conf-server peer push an edit? is the SAN check sound?); whether the
request code binds exactly the (path, child) that gets committed (request
confusion / TOCTOU between code-display and approve); trusting wire-supplied
values that should be recomputed locally (e.g. the request code, the peer
identity); path validation (can a child request the root, a relative path, or
a path that escapes the parent's subtree?); password/secret handling.`,
  },
  {
    key: 'cluster',
    prompt: `${CONTEXT}

DIMENSION: CLUSTER CONSISTENCY & CONCURRENCY. This is the highest-stakes area.
Look for: the self-exclusion logic in push_to_cluster_peers (does it correctly
identify "self" by member.ip == my_listen.ip, and is the my_conf_port
derivation sound when peers run different conf ports?); whether a down peer is
reported LOUDLY vs swallowed; idempotency of re-approve and ApplyReferralEdit
(does re-sync converge, or duplicate/drift?); the resolver_edit_lock — is every
writer (approve, deny, apply_referral_edit) under it? any read-modify-write of
resolver.json that races an operator edit?; crash-window ordering in
approve_delegation_prepare (local-apply / commit / push order — can a crash
leave a half state?); the Pending→commit vs Approved→resync vs Denied→error
state machine.`,
  },
  {
    key: 'edgecases',
    prompt: `${CONTEXT}

DIMENSION: ERROR HANDLING & EDGE CASES. Look for: unwrap/expect/panic on
attacker- or peer-influenced input in the handlers; the TTL/prune behavior of
the delegation store (expired requests, MAX_PENDING overflow); empty/duplicate
child address lists; what happens when resolver.json is missing, malformed, or
Local-only at approve time; the poll Unknown vs Pending vs terminal mapping;
behavior when the same path is requested twice, or two different children
request the same path; partial-write / atomic-move correctness in
delegation_store; the install-time child branch when the parent denies or the
request expires mid-install.`,
  },
  {
    key: 'protocol',
    prompt: `${CONTEXT}

DIMENSION: PROTOCOL & SERIALIZATION COMPAT. Look for: serde back-compat of the
new Request/Response variants (do added enum variants break older peers? is
#[serde(default)] used where needed?); the delegation_code canonical
serialization — is it byte-stable and computed identically on both sides
(ordering of the child Vec, field order, no timestamps/ids leaking in)?; any
mismatch between what the client sends and the server expects; the round-trip
test coverage.`,
  },
  {
    key: 'cli',
    prompt: `${CONTEXT}

DIMENSION: CLI CORRECTNESS & OPERATOR UX. Look for: does add-parent correctly
refuse an already-parented resolver and a Local-only resolver?; does the
review loop surface per-peer push failures loudly with a clear "fix + re-run to
re-sync" message?; the install-time child branch (authchoice_to_info, Local
bail, the parent referral landing in ParentRef before render); the "restart
your resolver server" notices (structural referral edits are inert until
restart — is this said loudly on BOTH sides?); glyph-confirm before any trust
decision; correctness of describe_child / code display vs what's approved.`,
  },
]

const FINDINGS_SCHEMA = {
  type: 'object',
  properties: {
    findings: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          title: { type: 'string', description: 'one-line summary' },
          file: { type: 'string', description: 'path:line' },
          severity: { type: 'string', enum: ['critical', 'high', 'medium', 'low'] },
          explanation: { type: 'string', description: 'concrete argument for why this is a real bug, with the code path' },
          suggested_fix: { type: 'string' },
        },
        required: ['title', 'file', 'severity', 'explanation', 'suggested_fix'],
        additionalProperties: false,
      },
    },
  },
  required: ['findings'],
  additionalProperties: false,
}

const VERDICT_SCHEMA = {
  type: 'object',
  properties: {
    real: { type: 'boolean', description: 'true only if this is a genuine defect that should be fixed' },
    reasoning: { type: 'string', description: 'cite the code you checked; explain why real or refuted' },
  },
  required: ['real', 'reasoning'],
  additionalProperties: false,
}

phase('Review')
const reviewed = await pipeline(
  DIMENSIONS,
  d => agent(d.prompt, { label: `review:${d.key}`, phase: 'Review', schema: FINDINGS_SCHEMA }),
  (review, d) =>
    parallel((review?.findings ?? []).map(f => () =>
      // Two independent skeptics, each prompted to REFUTE. A finding
      // survives only if neither can refute it (both vote real).
      parallel([0, 1].map(i => () =>
        agent(
          `${CONTEXT}\n\nA reviewer reported this finding about the delegation feature:\n` +
          `TITLE: ${f.title}\nLOCATION: ${f.file}\nSEVERITY: ${f.severity}\n` +
          `CLAIM: ${f.explanation}\n\n` +
          `Your job is to REFUTE it. Read the cited code and surrounding context with your ` +
          `tools. Is this actually a defect, or does existing code/validation/design already ` +
          `handle it? Default to real=false if the claim is speculative, already mitigated, ` +
          `or by-design. Only real=true if you independently confirm a genuine defect.`,
          { label: `verify:${d.key}:${i}`, phase: 'Verify', schema: VERDICT_SCHEMA }
        )
      )).then(votes => {
        const v = votes.filter(Boolean)
        const real = v.length > 0 && v.every(x => x.real)
        return { ...f, dimension: d.key, confirmed: real, verdicts: v }
      })
    ))
)

const all = reviewed.flat().filter(Boolean)
const confirmed = all.filter(f => f.confirmed)
const rank = { critical: 0, high: 1, medium: 2, low: 3 }
confirmed.sort((a, b) => (rank[a.severity] ?? 9) - (rank[b.severity] ?? 9))
log(`reviewed ${all.length} candidate findings; ${confirmed.length} confirmed after adversarial verification`)

return {
  confirmed_count: confirmed.length,
  candidate_count: all.length,
  confirmed,
}
