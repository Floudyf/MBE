# Groundhog mechanism specification

Groundhog treats a block as an unordered set of transactions. Every transaction reads the same block-start snapshot and emits typed state modifications. A transaction is valid for the block only when all of its modifications can be reserved atomically together with the already accepted modifications.

## Typed state operations

The MBE reproduction implements the three paper-level object families:

1. Nonnegative integer set-add: all modifications declare the same snapshot base value. Positive and negative aggregates are tracked separately; the block-start base must cover the full negative aggregate, so same-block credits cannot fund same-block withdrawals. Materialization applies both aggregates and must remain within int64 range.
2. Byte-string set: concurrent writes coexist only when they set the same value.
3. Ordered set: insert, clear-through-threshold, and capacity increase are merged deterministically. The maximum clear threshold controls final materialization; inserts at or below it are absent from the committed set but still retain duplicate and capacity evidence during the block. Duplicate hashes and capacity overflow conflict.

## Candidate assembly

```text
reserve FIFO candidate window
→ interpret candidates in parallel against one snapshot
→ process reservations in deterministic candidate order
→ commit all modifications of a successful candidate
→ roll back all modifications of a conflicting candidate
→ release deferred candidates to the mempool
→ keep selected candidates reserved until block commit or proposal failure
```

## Fixed-block execution

```text
interpret every fixed-block transaction against one snapshot
→ reserve typed modifications in deterministic block representation order
→ any constraint conflict rejects the complete block
→ no partial state is returned
→ no serial fallback is allowed
→ materialize all typed objects in deterministic key order
```

Application-invalid MBE transactions are included as failed no-op receipts so the existing MBE logical-transaction lifecycle can terminate. Groundhog typed-state conflicts are deferred during candidate assembly and reject a fixed block during validation.
