# V2.8 Sweep / Report

## Goal

V2.8 adds the local batch experiment layer for the V3-ready replay platform. It runs repeatable sweeps over existing V2.5 dual-chain replay and V2.6 cross-chain protocol baseline replay, aggregates results, and writes report artifacts.

V2.8 is local sweep/report only. It is not real chain execution, not Fabric execution, not public-chain replay, and not a production cross-chain bridge.

## Sweep Types

- `v2_baseline_sweep`: runs one dual-chain replay case plus the V2.6 baseline protocols.
- `v2_chain_speed_imbalance_sweep`: varies target chain block interval and finality depth.
- `v2_protocol_baseline_sweep`: compares `lock_mint_serial`, `lock_mint_pipeline`, `fixed_window_baseline`, and `committee_bridge_basic`.
- `v2_window_size_sweep`: varies `window_size` for `fixed_window_baseline`.
- `v2_committee_delay_sweep`: varies `committee_delay_ms` for `committee_bridge_basic`.

`committee_delay_ms` is a local baseline parameter. It is not real committee signature time.

## Sweep Config

Sweep configs live in `configs/sweeps/`. Each config declares:

- `version: v2`
- `stage: v2.8`
- `data_truth_label: synthetic_replay`
- `backend_type: local_virtual`
- `protocol_truth: local_baseline_model`
- `runner.sleep_enabled: false`

Live backends, MetaFlow, Fabric live execution, EVM live execution, Docker, Fabric, and `network.sh` are not allowed in V2.8 sweep configs.

## Case Expansion

The sweep runner expands config parameter lists into deterministic cases named `case_000001`, `case_000002`, and so on. Each case records the sweep id, protocol name, source/target chain parameters, data truth label, backend type, and local protocol truth.

## Metrics Aggregation

Each case writes a stable row into `sweep_summary.csv` and `sweep_summary.json`. The stable columns include:

- `sweep_id`
- `case_id`
- `case_type`
- `protocol_name`
- `data_truth_label`
- `backend_type`
- `protocol_truth`
- chain timing and finality parameters
- success, timeout, refund, and failure counts
- latency, pending, finality wait, and chain imbalance fields

Missing fields are left blank to keep the schema stable across dual-chain replay and protocol baseline cases.

## Report Generation

The report generator writes `sweep_report.md` with:

- scope and non-goals
- data truth and backend truth
- sweep config summary
- Markdown summary table
- conservative observations
- artifact list

Observations are descriptive and should not be read as production bridge claims.

## Output Artifacts

V2.8 reuses the V2.2 job/artifact manager. A sweep run is stored under `.cache/v2_jobs/{run_id}/` and may contain:

- `metadata.json`
- `used_config.yaml`
- `used_config.json`
- `sweep_summary.csv`
- `sweep_summary.json`
- `sweep_report.md`
- `runtime.log`
- `case_artifacts_index.json`

The artifact allowlist is extended only for `sweep_report.md` and `case_artifacts_index.json`.

## API And CLI

API endpoints:

- `GET /api/v2/sweeps`
- `GET /api/v2/sweeps/{sweep_id}`
- `POST /api/v2/sweeps/run`

CLI example:

```powershell
python scripts/v2_8_sweep_report.py --config configs/sweeps/v2_baseline_sweep.yaml --out .cache/v2_8_sweeps/latest
```

The CLI and API use the same `SweepRunner`.

## Data Truth

Default V2.8 sweeps use:

- `data_truth_label = synthetic_replay`
- `backend_type = local_virtual`
- `protocol_truth = local_baseline_model`

The input is the V2.4 synthetic schema sample. Execution is local virtual-time replay or local protocol baseline replay. It is not real chain execution, not Fabric execution, not public-chain replay, not a production bridge, and not MetaFlow.

## Non-goals

V2.8 does not implement MetaFlow, a real committee bridge, real signature committees, MintCert, RefundCert, FinalityProof, or Pending Pool.

V2.8 does not start Docker, Fabric, `network.sh`, public-chain live nodes, or archive-node clients.

V2.8 does not implement FabricLiveBackend, EVMLiveBackend, V3 live backends, or V2.9 realism bridge.

V2.9 may later add Realism Bridge / Chain-backed Calibration. V3 is reserved for multi-server, live backend, and production-like deployment work.
