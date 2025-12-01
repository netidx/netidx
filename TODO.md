# Container

- Full ACID transactions flag. Add an optional flag to wrap each Txn
  in a Sled transaction so that transactions are fully ACID. Document
  that it has negative performance implications.

- Add a Txn RPC that allows writing multiple values atomically, or
  just change set-data so it can take multiple values.
