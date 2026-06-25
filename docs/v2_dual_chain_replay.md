# V2.5 Dual-chain Replay Engine

V2.5 adds a local virtual-time dual-chain replay engine. It consumes the V2.4 cross-chain trace schema and produces replay-derived timing artifacts for two chain profiles.

V2.5 does not execute a production cross-chain bridge. It does not implement V2.6 protocol baselines, MetaFlow, committee bridge, Pending Pool, Docker/Fabric startup, network.sh, public-chain live nodes, or archive-node ingestion.

## Inputs

- `configs/experiments/v2_dual_chain_sample.yaml`
- `trace/samples/v2_cross_trace_small.jsonl`
- `trace/samples/v2_multi_chain_trace_meta.json`

The sample config is runnable only because it explicitly declares `stage: V2.5`, `status: runnable`, and local `backend_type: local_virtual` chain profiles. The V2.0 planning file `configs/topologies/v2_dual_chain_planned.yaml` remains `status: planned` and `runnable: false`.

## Chain Profiles

Each chain profile declares:

- `chain_id`
- `role`
- `backend`
- `backend_type`
- `block_interval_ms`
- `finality_depth`
- `capabilities`

V2.5 supports `local_virtual` and `trace_replay` backend types. Live Fabric and EVM backends are V3 planned placeholders only.

## Timing Rules

V2.5 uses virtual time only:

- `expected_commit_time_ms = max(observed_commit_time_ms, ceil(submit_time_ms / block_interval_ms) * block_interval_ms)`
- `expected_finality_time_ms = expected_commit_time_ms + block_interval_ms * finality_depth`
- `finality_wait_time_ms = expected_finality_time_ms - expected_commit_time_ms`
- `stage_latency_ms = expected_finality_time_ms - submit_time_ms`

The engine never uses wall-clock sleep to simulate latency.

## Outputs

A V2.5 replay run writes:

- `metadata.json`
- `used_config.yaml`
- `used_config.json`
- `dual_chain_summary.csv`
- `dual_chain_summary.json`
- `stage_metrics.csv`
- `runtime.log`
- `report.md`

The summary includes cross transaction counts, final status counts, average and p99 end-to-end latency, finality wait time, source/target wait time, chain speed imbalance, backend types, chain profile parameters, and `data_truth_label`.

## API and CLI

Backend API:

- `GET /api/v2/chain-backends`
- `GET /api/v2/dual-chain/sample-config`
- `POST /api/v2/dual-chain/replay`

CLI:

```powershell
python scripts/v2_5_dual_chain_replay.py --config configs/experiments/v2_dual_chain_sample.yaml --out .cache/v2_dual_chain_manual
```

The API records runs through the V2.2 job/artifact manager. The CLI writes the same artifacts to the requested output directory.

## Non-goals

V2.5 does not implement V2.6 cross-chain protocol baselines. It replays stage records that already exist in the trace.

V2.5 does not implement dual-chain live deployment, multi-server Fabric, FabricLiveBackend, EVMLiveBackend, real event listeners, public-chain RPC ingestion, MetaFlow, committee bridge, or Pending Pool.

Synthetic replay remains synthetic replay. Schema validation and virtual-time replay are not real on-chain execution.
