# Serial Mechanism Spec

The Serial mechanism executes the block transaction list in original block order with one worker.

Inputs:

- immutable base state snapshot
- block metadata and ordered transactions
- transaction semantics and access resolver
- deterministic runtime configuration
- node ID and shard ID

Outputs:

- ordered receipts
- transaction deltas
- final state delta
- state root before
- state root after
- receipt root
- execution plan
- execution plan digest
- generic execution metrics
- per-transaction execution trace

The mechanism is legacy-faithful. It preserves account initialization behavior, validation order, nonce update behavior, receipt ordering, and state root calculation from the legacy engine.

The execution plan digest is evidence only in this stage. It is not included in PBFT messages, block hashes, finality semantics, or cross-shard protocol messages.
