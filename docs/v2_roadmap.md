# V2 Roadmap: Modular Multi-Source and Cross-Chain Replay Platform

## 1. V2 Positioning

V1 has completed the single-chain runnable experiment platform:

```text
V1 = single-chain runnable experiment platform
```

V1 already includes:

```text
V0: default single-chain experiment closure
V1.1: topology-first experiment wizard
V1.2: executor sharded-execution prototype enhancement
V1.3: workload / trace enhancement
V1.4: Fabric chain-backed trace smoke runner
V1.5: co-access routing
V1.6: dual-track execution
V1.7: hot update aggregation
V1.8: baseline / sweep / report
V1-final: frontend / backend acceptance UI
V1-final-plus: interactive workload / ablation / trace-source selector
```

V1 already supports:

```text
synthetic controlled replay
existing trace replay
Fabric chain-backed trace replay
interactive workload selector
interactive ablation preset / custom toggles
Go executor replay
routing / dual-track / aggregation metrics
V1 sweep / report / download
Fabric smoke CLI/WSL runner
```

V1 still does not include:

```text
formal cross-chain
MetaFlow
committee bridge
Pending Pool
dual-chain replay engine
multi-chain replay engine
production-grade Fabric
multi-server Fabric
live public-chain access
```

V2 is not a replacement for V1. It extends the V1 base into a modular, multi-trace-source, dual-chain/cross-chain replay experiment platform. The goal is to provide the local replay and virtual-time substrate for later MetaFlow, cross-chain protocol experiments, and multi-chain topology experiments.

V2 keeps V1 as the runnable single-chain base. V2 extends the platform along three axes: plugin-based experiment composition, multi-source trace ingestion, and dual-chain/cross-chain replay. It remains a local experimental platform rather than a production blockchain or production cross-chain bridge.

V3 is the stage that may consider multi-server real deployment, production-grade Fabric, real multi-chain networks, and a complete MetaFlow implementation.

## 2. What V2 Should Achieve

After V2 is complete, the platform should support:

1. Plugin Registry + Experiment Composer
2. topology selector: `single_chain` / `dual_chain` / `multi_chain planned`
3. trace source selector: `synthetic` / `Fabric chain-backed` / `existing trace` / `public-chain imported trace`
4. workload selector
5. chain profile selector
6. protocol selector
7. dual-chain replay
8. cross-chain baseline protocols
9. cross-chain metrics
10. V2 sweep / report
11. frontend multi-chain experiment console

## 3. What V2 Does Not Promise

V2 does not promise:

```text
production-grade cross-chain bridge
real multi-server Fabric
live public-chain node access
complete MetaFlow protocol
economic security proof
automatic Docker/Fabric startup
synthetic replay described as real on-chain data
```

## 4. V2 Stage Plan

### V2.1 Plugin Registry + Composer 2.0

Goal:

```text
Promote the hard-coded V1 workload / routing / execution / commit / trace source / topology choices into a plugin registry and Composer rules.
```

Runnable output:

```text
Frontend and backend can list plugins.
Composer can generate valid experiment config.
Config Validator can classify runnable / planned / invalid.
```

Likely files:

```text
backend/app/main.py
backend/app/v2_plugins.py
configs/plugins/
configs/topologies/
frontend/src/App.tsx
frontend/src/api.ts
docs/v2_plugin_registry.md
tests/test_v2_plugin_registry.py
```

Non-goals:

```text
No dual-chain replay engine.
No cross-chain protocol.
No MetaFlow.
No Fabric/Docker startup.
```

Validation:

```powershell
$env:PYTHONPATH = (Get-Location).Path
python -m pytest tests -q
python -m pytest backend/tests -q
python scripts/v0_sanity.py
cd executor
go test ./...
cd ..
cd frontend
npm.cmd run build
cd ..
git diff --check
```

Commit message:

```text
Add V2.1 plugin registry composer
```

### V2.2 Experiment Job Manager + Artifact Manager

Goal:

```text
Upgrade one-off latest runs into run_id/job status/history artifact management.
```

Runnable output:

```text
Every experiment has a run_id.
Frontend can inspect running/success/failed.
Frontend can inspect stdout/stderr/runtime log.
Frontend can download the run config, summary, and report.
```

Likely files:

```text
backend/app/jobs.py
backend/app/artifacts.py
backend/app/main.py
frontend/src/App.tsx
frontend/src/api.ts
tests/test_v2_jobs.py
docs/v2_job_artifact_manager.md
```

Non-goals:

```text
No Celery/Redis.
No distributed task queue.
No multi-user permission system.
```

Commit message:

```text
Add V2.2 job artifact manager
```

### V2.3 Trace Source Expansion

Goal:

```text
Expand trace sources so synthetic, Fabric chain-backed, existing trace, and public-chain imported trace can enter the platform through one interface.
```

Runnable output:

```text
Existing trace import is supported.
Fabric smoke trace status is recognized.
Public-chain trace adapter skeleton exists.
Public-chain trace defaults to semantic_unknown.
```

Likely files:

```text
trace/converter/
configs/trace_sources/
backend/app/main.py
tests/test_v2_trace_sources.py
docs/v2_trace_sources.md
```

Non-goals:

```text
No live public-chain node connection.
No archive node requirement.
No forced recovery of Ethereum storage semantics.
No public-chain trace as the main commutative-delta experiment.
```

Commit message:

```text
Add V2.3 trace source expansion
```

### V2.4 Multi-chain Trace Schema

Goal:

```text
Add a multi-chain / cross-chain trace schema that represents the stages of one cross-chain transaction.
```

Runnable output:

```text
cross_trace.jsonl.gz schema
multi_chain_trace_meta.json schema
schema validation tests
synthetic cross-chain trace sample
```

Required fields:

```text
cross_tx_id
stage
source_chain
target_chain
submit_time
commit_time
finality_time
status
asset_id
amount
timeout_deadline
stage_latency_ms
```

Likely files:

```text
trace/schema/cross_chain_trace.schema.json
trace/samples/
tests/test_v2_cross_trace_schema.py
docs/v2_multi_chain_trace_schema.md
```

Non-goals:

```text
No dual-chain replay engine.
No real cross-chain protocol.
```

Commit message:

```text
Add V2.4 multi-chain trace schema
```

### V2.5 Dual-chain Replay Engine

Goal:

```text
Implement a local virtual-time dual-chain replay engine.
```

Runnable output:

```text
Independent virtual clocks for chain_A and chain_B.
Different block interval / finality depth.
Source/target chain queue delay.
Finality wait metrics.
```

Likely files:

```text
executor/crosschain/
executor/core/
configs/experiments/
tests
docs/v2_dual_chain_replay.md
```

Non-goals:

```text
No production cross-chain bridge.
No multi-server Fabric.
No complete MetaFlow protocol.
```

Commit message:

```text
Add V2.5 dual-chain replay engine
```

### V2.6 Cross-chain Protocol Baselines

Goal:

```text
Implement cross-chain replay baseline protocols.
```

Protocol baselines:

```text
lock_mint_serial
lock_mint_pipeline
fixed_window_baseline
committee_bridge_basic
```

Runnable output:

```text
Compare cross-chain end-to-end latency, pending count, timeout/refund count, and finality wait.
```

Metrics:

```text
cross_tx_count
cross_success_count
cross_timeout_count
cross_refund_count
avg_e2e_latency_ms
p99_e2e_latency_ms
pending_count
finality_wait_time_ms
source_wait_time_ms
target_wait_time_ms
chain_speed_imbalance
```

Non-goals:

```text
No complete MetaFlow protocol.
No real signature committee.
No production bridge security model.
```

Commit message:

```text
Add V2.6 cross-chain protocol baselines
```

### V2.7 Multi-chain / Cross-chain UI

Goal:

```text
Connect topology, chain profile, cross-chain protocol, and metrics to the frontend.
```

Frontend areas:

```text
Topology selector
Chain A / Chain B parameter panel
Protocol selector
Cross-chain metrics panel
Cross-chain report viewer
Data truth label
```

Non-goals:

```text
No real Fabric startup.
No public-chain connection.
No multi-user backend.
```

Commit message:

```text
Add V2.7 multi-chain cross-chain UI
```

### V2.8 V2 Baseline / Sweep / Report

Goal:

```text
Generate sweeps and reports for dual-chain / cross-chain replay.
```

Sweeps:

```text
dual_chain_baseline_sweep
chain_speed_imbalance_sweep
finality_depth_sweep
cross_chain_window_sweep
protocol_baseline_sweep
```

Outputs:

```text
sweep_summary.csv
sweep_summary.json
report.md
runtime logs
artifact downloads
```

Commit message:

```text
Add V2.8 cross-chain sweep report
```

## 5. Recommended V2 Execution Order

```text
V2.1 → V2.2 → V2.3 → V2.4 → V2.5 → V2.6 → V2.7 → V2.8
```

Do not skip V2.1/V2.2 and jump directly into cross-chain work. Do not start with MetaFlow. Do not start with real multi-server Fabric.
