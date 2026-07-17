# Block-STM Acceptance Matrix

Status: planned acceptance matrix before implementation.

## Unit and Kernel Tests

- MVMemory read latest lower-index version.
- MVMemory returns base value when no lower write exists.
- MVMemory returns ESTIMATE for invalidated unresolved writes.
- Captured reads validate successfully against unchanged visible versions.
- Captured reads fail validation when a lower-index write changes a read key.
- Abort increments incarnation and removes the old stable output.
- Dependency wait and resume work for read-after-estimate.
- Worker counts 1, 2, 4, and 8 are deterministic.
- Hot sender/nonce sequence triggers abort and re-execution.
- Independent senders execute with low abort count.
- All receipt and state roots equal Serial.

## V5 Plugin Tests

- `block_stm` manifest is category `block_executor`.
- `block_stm` supports `real_cluster`.
- Compatibility validates worker count.
- Formal RunGroup can compare Serial and Block-STM while keeping workload, topology, seed, and non-execution plugins fixed.
- Results expose Block-STM metrics and artifacts generically.

## Real-Cluster Tests

- 1 shard, 4 nodes, synthetic workload, Serial vs Block-STM.
- 4 shards, 16 nodes, mixed intra-shard and cross-shard workload.
- Dataset original workload with no synthetic fallback.
- State root, receipt root, plan digest, terminal count, pending queues, and orphan process gates remain strict.

## Stop Conditions

If Block-STM cannot match Serial equivalence for current transfer semantics, stop and report the exact invariant failure. Do not replace Block-STM with a static conflict graph, wave batching, or declared-access-only executor.
