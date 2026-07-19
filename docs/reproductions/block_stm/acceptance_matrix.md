# Block-STM Acceptance Matrix

Status: kernel, plugin, and V5 execution-method closure acceptance are wired.

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
- Scheduler ordered execute queue drives speculative task issue order.

Current focused evidence:

- `go test ./realism/execution ./realism/execution/blockstm -run "TestBlockSTM|TestScheduler|TestDependency|TestCommutative" -v`
- `go test ./...`
- `python scripts/v5_execution_methods_closure_acceptance.py`
- `python scripts/v5_execution_methods_closure_acceptance.py --workload-source dataset-original --tx-count 100`
- `python scripts/v5_execution_methods_closure_acceptance.py --workload-source dataset-derived --tx-count 100`

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

The V5 execution-method closure script runs the current platform comparison
matrix:

- Hash + Serial
- Hash + Block-STM
- MetaTrack + Serial
- MetaTrack + Block-STM

The script rejects a run unless all four methods share workload, seed, topology,
and non-method conditions; all submitted transactions become terminal; there is
no fallback and no orphan process; state roots and plan digests are consistent;
Hash and MetaTrack routing assignments differ; MetaTrack methods produce
runtime scheduler, remote-state, and aggregation evidence; and Block-STM methods
emit serial-equivalence artifacts with `serial_equivalent=true`.

For synthetic crafted workloads the closure script requires observable routing,
track, and aggregation differences because those transactions contain explicit
commutative hot-update cases. For registered dataset workloads, the same script
does not manufacture synthetic fast-track or aggregation evidence. Dataset mode
instead verifies that each of the four methods actually runs the materialized
canonical workload, preserves the dataset truth label and no-fallback replay
summary, reaches terminal finality for every submitted transaction, and keeps
Block-STM serial equivalence true.

## Stop Conditions

If Block-STM cannot match Serial equivalence for current transfer semantics, stop and report the exact invariant failure. Do not replace Block-STM with a static conflict graph, wave batching, or declared-access-only executor.
