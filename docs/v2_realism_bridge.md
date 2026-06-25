# V2.9 Realism Bridge / Chain-backed Calibration

## Goal

V2.9 adds a realism bridge between the V2 local replay platform and future V3 live backends. It reads existing chain-backed trace data, compares observed timing with local replay timing, and writes calibration artifacts.

V2.9 is chain-backed calibration. It is not a real-time blockchain execution platform.

## Why This Bridge Exists

V2 is a V3-ready local replay platform, not a one-off simulator. V2.9 shows how replay parameters can be checked against chain-backed observations before V3 replaces local backends with live backends.

## Chain-backed Trace Calibration

The Fabric smoke calibration config reads:

- `.cache/fabric_smoke/latest/trace.jsonl.gz`
- `.cache/fabric_smoke/latest/trace_meta.json`

These files must already exist. The web API and CLI do not start Docker, Fabric, `network.sh`, or chaincode deployment.

If the Fabric smoke trace is missing, V2.9 returns a blocked result with:

```powershell
python scripts/v1_fabric_smoke.py --strict --channel mbechannel --out .cache/fabric_smoke/latest
```

This command is a manual CLI hint, not an automatic web action.

## Calibration Config

Configs live in `configs/calibration/`:

- `v2_synthetic_calibration_sample.yaml`
- `v2_fabric_smoke_calibration.yaml`

Default truth labels:

- synthetic sample: `data_truth_label = synthetic_replay`, `backend_type = local_virtual`, `calibration_truth = synthetic_observation_sample`
- Fabric smoke calibration: `data_truth_label = fabric_chain_backed_trace_replay`, `backend_type = trace_replay`, `calibration_truth = chain_backed_observation`

## Observed vs Replay Comparison

The runner aligns records by `stage_id`, then `tx_id`, then `cross_tx_id`, then `record_id`. It computes:

- `commit_error_ms`
- `finality_error_ms`
- `latency_error_ms`
- absolute error aggregates
- matched and unmatched counts

If observed timing fields are missing, the runner reports warnings instead of inventing measurements.

## Output Artifacts

V2.9 reuses the V2.2 job/artifact manager. A run writes:

- `metadata.json`
- `used_config.yaml`
- `used_config.json`
- `calibration_summary.csv`
- `calibration_summary.json`
- `replay_vs_observed.csv`
- `calibration_report.md`
- `runtime.log`

The artifact allowlist is extended only for those calibration outputs.

## API And CLI

API endpoints:

- `GET /api/v2/calibration/configs`
- `GET /api/v2/calibration/fabric-smoke/status`
- `POST /api/v2/calibration/run`

CLI example:

```powershell
python scripts/v2_9_realism_bridge.py --config configs/calibration/v2_synthetic_calibration_sample.yaml --out .cache/v2_9_calibration/latest
```

## Data Truth Label

Synthetic calibration sample is not real chain execution.

Fabric smoke calibration uses existing Fabric smoke trace. The trace source is chain-backed, but the current V2.9 run is calibration/replay analysis only. The web UI does not control Fabric.

## Non-goals

V2.9 does not implement FabricLiveBackend, EVMLiveBackend, distributed runner, real-time chain listener, V3 live backend, MetaFlow, real committee bridge, real signatures, MintCert, RefundCert, FinalityProof, or Pending Pool.

V2.9 does not start Docker, Fabric, `network.sh`, public-chain live nodes, or archive-node clients.

## Relationship To V3 And V2-final

V3 is reserved for multi-server deployment, live backend implementations, monitoring, and production-like long-running experiments.

V2-final Frontend Consolidation should later organize V0/V1/V2 workflows, hide developer debug details, localize the UI, and present calibration reports cleanly. V2.9 does not do that frontend consolidation.
