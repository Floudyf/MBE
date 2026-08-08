# Batch-SI mapping to MBE

| Batch-SI concept | MBE implementation |
|---|---|
| Read/write sets | immutable `tx.SignedTransaction.AccessList` |
| AWRT | private map in `executor/realism/execution/batch_si_executor.go` |
| WRBP | private `batchSIWRBPPartition` |
| OFAS | private `batchSIOFASOrder` |
| Consensus-bound schedule | `block.ExecutionPlanEnvelope` with `batch_si_execution_plan_v1` |
| Candidate deferral | `ConsensusExecutionPlanningResult.Deferred` -> mempool `ReleaseReserved` |
| Batch snapshot | private copy of the preceding batch's working state |
| Intra-batch workers | private Batch-SI worker pool |
| Deterministic commit | private ordered delta materialization, followed by MBE normal commit |
| Formal method | `hash_batch_si` |
| Ablations | four Batch-SI-owned method profiles |

## Runtime boundary

The shared V5 runtime only detects an optional `ConsensusExecutionPlanner`, carries its block and deferral result, and invokes semantic verification. It contains no AWRT, WRBP, OFAS, or Batch-SI execution logic.

## Topology boundary

Batch-SI currently requires `cross_shard_ratio=0`. A multi-shard topology is permitted only when each transaction is shard-local; each shard runs Batch-SI independently.

## Worker/process boundary

- `nodes` determines independent node processes: one node equals one OS process.
- `worker_count` is the maximum number of node-internal Batch-SI transaction workers.
- changing workers does not change node or process count.
- Serial remains effectively single-worker even when a common experiment worker value is selected.
