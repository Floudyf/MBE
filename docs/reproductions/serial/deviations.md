# Serial Deviations

Serial is an internal MBE reference, so there is no external-paper deviation list.

Known stage boundary:

- The plan digest is not part of block hash or consensus messages in this stage.
- Future mechanisms such as Batch-SI may require consensus-time binding of the execution plan. That question is intentionally left to the corresponding reproduction stage.
- The legacy engine initializes missing sender and receiver accounts before validation. The new Serial executor preserves this behavior for equivalence even when a transaction later fails.
- The legacy receipt `StateKeys` come from the transaction payload and are not a complete observed access set. This stage adds observed access evidence without changing legacy receipt compatibility.
- Real-cluster drain waits are workload-aware. This does not change PBFT-style
  finality, durable commit semantics, terminal accounting, or cross-shard
  closure. It replaces an under-sized fixed proposal timeout and fixed drain
  window with bounded estimates plus a no-progress watchdog.
- Node process runtime duration can exceed the user-facing experiment duration
  when needed to complete drain. This is a runtime closure budget, not a change
  to workload size, transaction semantics, block execution semantics, or
  finality criteria.
