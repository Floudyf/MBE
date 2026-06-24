# Executor

The Go 1.26.1 replay executor preserves the V0 streaming `trace.jsonl.gz` path and virtual-time latency model; it must not use `time.Sleep` for simulated latency.

V1.2 adds a small, single-chain sharded-execution module boundary while retaining the V0 `core.Replay(config, trace, out)` API and output files:

- `state_sharding`: `phi(key) -> state shard`, the persistent state location.
- `routing`: `M_t(key) -> execution shard`, the batch-side execution route; it does not migrate state or change `phi`.
- `execution_sharding`: `psi_t(tx) -> execution shard`, the shard that receives a transaction for execution.

Only the default hash modules are implemented. Co-access/MetaTrack routing, dual-track execution, hot-update aggregation, baselines, and cross-chain mechanisms are intentionally outside V1.2.
