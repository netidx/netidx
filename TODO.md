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
