# V3.2 Minimal Single-chain Modular Runtime

## Goal

V3.2 adds the first runnable modular research chain path: a deterministic Python backend reference runtime for one chain. Its purpose is to stabilize runtime semantics, profile contracts, and artifact schemas before Go-backed parity.

## Supported Pipeline

```text
synthetic workload
-> fifo_pool
-> time_or_count_block_producer
-> simple_leader
-> hash_sharding
-> serial_execution
-> direct_fetch
-> normal_commit
-> basic_metrics
```

The runtime is single-process logical multi-node. It derives logical node ids from `ChainProfile.deployment` and `ChainProfile.node`, records `proposer_node`, `consensus_plugin`, and finalized block timestamps, and uses deterministic virtual time. It must not use `time.sleep` to simulate latency.

## Runnable Profile

Only this V3.2 experiment profile is runnable:

```text
configs/v3/experiments/single_chain_runtime_smoke.yaml
```

It uses:

- `chain_x_default`
- `v3_2_minimal_single_chain`
- synthetic workload
- `truth_label: modular_runtime`
- `backend_type: modular_research_chain`

## Outputs

The smoke runtime writes:

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

It must not write MetaTrack mechanism metrics, MetaFlow events/control decisions, or Fabric validation artifacts.

## Non-goals

V3.2 does not implement:

- Go-backed runtime parity.
- Go executor changes.
- Frontend integration.
- MetaTrack V3.3 plugin evaluation.
- co-access sharding.
- dual-track execution.
- access-list prefetch.
- hot update aggregation commit.
- Fabric validation.
- Docker/Fabric/network.sh execution.
- dual-chain runtime.
- MetaFlow.
- AFS/FDA.

Smoke results validate the minimal runtime pipeline only. They are not Fabric live execution, not MetaTrack full evaluation, and not final paper-scale performance evidence.
