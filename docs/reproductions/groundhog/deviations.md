# Groundhog deviations and truth boundary

The implementation truth boundary is:

> Groundhog core typed-state and reserve-commit reimplementation over MBE transaction semantics.

Implemented:

- unordered block-start snapshot semantics;
- typed nonnegative integer, byte-string, and ordered-set modifications;
- transaction-atomic reservation and rollback;
- pre-consensus candidate filtering;
- fixed-block all-or-nothing validation;
- deterministic materialization;
- worker-parallel transaction interpretation;
- no serial fallback.

Not reproduced:

- the original WASM virtual machine;
- the original C++ concurrent trie and garbage collector;
- the original contract database and RPC rate limiter;
- every storage-object extension found in later source revisions;
- cross-shard Groundhog execution.

The supported experiment boundary is one shard with `cross_shard_ratio = 0`. Groundhog changes application state semantics by replacing sequential nonce updates with ordered-set replay protection. Its results are therefore marked `groundhog_typed_commutative_snapshot_v1` and are comparison-limited against sequential-state methods.
