# Container

- Full ACID transactions flag. Add an optional flag to wrap each Txn
  in a Sled transaction so that transactions are fully ACID. Document
  that it has negative performance implications.

- Add a Txn RPC that allows writing multiple values atomically, or
  just change set-data so it can take multiple values.

# Resolver

- DNS hostname support for resolver-server addresses. Let client and
  resolver configs name resolver servers by hostname instead of raw IP,
  resolved at connect time, including over federated referrals (a
  backward-compatible trailing `dnsaddrs` field on the wire `Referral`).
  Designed but deferred — see design/dns-resolver-addresses.md.

# TLS / CA

- CA server. A daemon on the CA box that receives a CSR over the wire,
  signs it (subject to per-admin SAN/validity policy), and returns the
  signed cert + CA cert — eliminating the manual CSR/cert file shuffle
  in the `--auth tls` join flow. LUKS-style keyslot vault for the CA key
  (multiple revocable admins), syncthing-style identicon verification of
  the CA identity, runs as an optional activation unit. See
  design/ca-server.md.
  - DONE: the engine (`netidx-conf`): `fingerprint`, `ca_vault`,
    `ca_proto`, `ca_join`, `ca_server` + `Ca::from_pem` /
    `Ca::init_vaulted` + algorithm-aware CSR check; the server returns
    the full trusted bundle. Tested, incl. real-TLS end-to-end.
  - DONE: the CLI (`ca init`/`serve`/`admin`/`fingerprint`/`join`,
    vaulted CA + identicon + serving cert) and the `conf init` join
    branch (resolver/publisher/workstation generate paths offer the CA
    server, seeded with the upstream resolver IP). Smoke-tested via PTY.
  - DONE: `ca init --with-server` drops the `ca` activation unit and,
    standalone, offers to register the system service via the single
    shared `service::offer` entry point. `ServiceNeed` (merge:
    System>User>None) is the composition seam.
  - DONE: one entry point for CA creation — `create_vaulted_ca`, shared
    by `ca init` and the `conf install resolver` "create a new CA"
    branch (vault + identicon + "set up the CA server?"), with
    vault-aware open/detect. The resolver writes the `ca.unit` into its
    own activation dir so its single system-service offer supervises it.
  - TODO: `ca migrate` (v1 single-password CA → vault).
