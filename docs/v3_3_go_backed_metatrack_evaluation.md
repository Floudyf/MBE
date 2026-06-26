# V3.3 Go-backed MetaTrack Evaluation

## Scope

V3.3 absorbs the earlier V3.2b / V3.2.5 Go-backed parity stage and then adds a controlled MetaTrack ablation smoke path on the Go-backed runtime.

Gate A:

- Go-backed minimal runtime parity with the V3.2 Python reference runtime.
- Same V3 runtime artifact schema: used profiles, runtime log, summary CSV/JSON, report, block log, tx results, and state commit log.
- `truth_label: modular_runtime`.
- `runtime_mode` clearly identifies the Go-backed runtime.

Gate B:

- Go-backed MetaTrack plugin combinations: `baseline_hash_only`, `co_access_only`, `co_access_dual_track`, and `full_MetaTrack`.
- Fair ablation over identical workload, seed, ChainProfile, hardware profile, submit rate, block config, consensus config, and network profile.
- Only `ShardingPlugin`, `ExecutionSchedulerPlugin`, `StateAccessPlugin`, and `CommitPlugin` may differ.

## Runnable Profile

```text
configs/v3/experiments/metatrack_go_backed_ablation_smoke.yaml
```

This profile is a V3.3 smoke / controlled evaluation profile. It is not a paper-scale performance profile.

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

## Non-goals

V3.3 does not implement Fabric-backed validation, Fabric peer changes, Docker/Fabric automation, frontend integration, dual-chain runtime, MetaFlow, AFS/FDA, production PBFT/HotStuff, or production blockchain behavior.

Fabric-backed validation remains V3.4. Current V3-final remains frontend integration after V3.4.
