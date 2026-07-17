# Serial Correctness Invariants

The `serial_block_executor` must preserve these invariants:

- Transaction order equals the block transaction order.
- Worker count equals 1.
- Receipt order equals transaction order.
- Legacy and new Serial produce the same success and error for every transaction.
- Legacy and new Serial produce the same `ExecutionCost`.
- Legacy and new Serial produce the same `StateKeys` in receipts.
- Legacy and new Serial produce the same `StateRootAfterTx` sequence.
- Legacy and new Serial produce the same receipt root.
- Legacy and new Serial produce the same final state snapshot.
- Legacy and new Serial produce the same final state root.
- Failed transactions preserve legacy side effects such as account initialization.
- State deltas are applied deterministically.
- Plan digests are deterministic for identical input.
- `plan_digest_consistent` must be true across nodes that commit the same block.
- No fallback to the legacy engine is allowed after the block executor is selected.

Declared and observed access sets have distinct meanings:

- Declared access is a pre-execution conservative statement.
- Observed access is recorded from actual reads and writes.
- `StateKeys` are workload/routing/receipt compatibility fields and must not be substituted for observed access.
