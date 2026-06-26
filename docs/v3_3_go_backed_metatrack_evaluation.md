# V3.3 Go-backed MetaTrack Evaluation

## Scope

V3.3 absorbs the earlier V3.2b / V3.2.5 Go-backed parity stage and then adds a controlled MetaTrack ablation smoke path on the Go-backed runtime.

V3.3.1 follows V3.3 as a research-chain role separation correction. It keeps the V3.3 Go-backed MetaTrack smoke path but makes the single-chain runtime explicitly distinguish consensus domain, execution shard, state storage unit, state placement, execution routing, and remote state access.

Gate A:

- Go-backed minimal runtime parity with the V3.2 Python reference runtime.
- Same V3 runtime artifact schema: used profiles, runtime log, summary CSV/JSON, report, block log, tx results, and state commit log.
- `truth_label: modular_runtime`.
- `runtime_mode` clearly identifies the Go-backed runtime.

Gate B:

- Go-backed MetaTrack plugin combinations: `baseline_hash_only`, `co_access_only`, `co_access_dual_track`, and `full_MetaTrack`.
- Fair ablation over identical workload, seed, ChainProfile, hardware profile, submit rate, block config, consensus config, and network profile.
- Only `ShardingPlugin`, `ExecutionSchedulerPlugin`, `StateAccessPlugin`, and `CommitPlugin` may differ.
- In V3.3.1, `co_access_sharding` is execution-side routing. It does not migrate persistent state placement.

## Runnable Profile

```text
configs/v3/experiments/metatrack_go_backed_ablation_smoke.yaml
```

This profile is a V3.3 smoke / controlled evaluation profile. It is not a paper-scale performance profile.

In V3.3.1 it runs on `single_chain_research_default`, which has one consensus domain, four logical execution shards, four logical state storage units, disabled/planned committee and epoch lifecycle, fixed state placement, and a fixed network model.

## Go Executor Mode

```text
go run ./cmd/replay -mode v3-runtime \
  -chain-profile <path> \
  -plugin-profile <path> \
  -plugin-profile-id <id> \
  -experiment-profile <path> \
  -output <dir>
```

The V3 mode does not start Fabric, Docker, network.sh, frontend, public-chain clients, or dual-chain services.

## Outputs

Per Go runtime run:

- `used_chain_profile.yaml`
- `used_plugin_profile.yaml`
- `used_experiment_profile.yaml`
- `runtime.log`
- `summary.csv`
- `summary.json`
- `report.md`
- `block_log.csv`
- `tx_results.csv`
- `state_commit_log.csv`

Aggregate MetaTrack smoke run:

- `metatrack_summary.csv`
- `metatrack_summary.json`
- `metatrack_latency.csv`
- `metatrack_mechanism_metrics.csv`
- `metatrack_ablation_report.md`

V3.3.1 adds role-separated fields to the same artifacts:

- `block_log.csv`: `consensus_domain_id`, `validator_count`, `execution_shard_count`, `state_storage_unit_count`.
- `tx_results.csv`: `consensus_domain_id`, `execution_shard_id`, `home_state_unit_ids`, `accessed_state_unit_ids`, `remote_state_unit_count`, `cross_state_unit_access`, `state_locality_hit`.
- `state_commit_log.csv`: `state_storage_unit_id`, `execution_shard_id`, `is_remote_commit`, `placement_policy`, `routing_plugin`.
- `metatrack_mechanism_metrics.csv`: `execution_shard_count`, `state_storage_unit_count`, `cross_state_unit_access_count`, `remote_state_fetch_count`, `state_locality_ratio`, `execution_shard_load_balance`, `state_unit_load_balance`.

The legacy `shard_id` field remains as a compatibility alias for `execution_shard_id`.

## Non-goals

V3.3 and V3.3.1 do not implement Fabric-backed validation, Fabric peer changes, Docker/Fabric automation, frontend integration, dual-chain runtime, MetaFlow, AFS/FDA, production PBFT/HotStuff, real multi-machine networking, committee lifecycle, state migration, or production blockchain behavior.

Fabric-backed validation remains V3.4. Current V3-final remains frontend integration after V3.4.
