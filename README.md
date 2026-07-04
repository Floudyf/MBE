# MBE - Metaverse Blockchain Experiment Platform

MBE is evolving from a local modular research-chain runtime toward a configurable node-topology emulator-like runtime for metaverse blockchain experiments.

Current stage: V3-final Fault, Observability, and Reproducibility Closure.
Latest runtime capability: deterministic fault injection MVP, observability summary, final artifact catalog, reproducibility guide, experiment manual, and paper experiment mapping.
Runtime truth: v3_final_emulator_closure_not_production_system.
Next stage: V3 maintenance only; do not start V4 unless explicitly requested.

## Current Status

Current stage: V3-final Fault, Observability, and Reproducibility Closure.
Latest runtime capability: deterministic fault injection MVP, observability summary, final artifact catalog, reproducibility guide, experiment manual, and paper experiment mapping.
Current capability: deterministic fault injection, local observability summary, component health status, final artifact catalog, reproducibility bundle, experiment manual, and paper experiment mapping.
Runtime truth: v3_final_emulator_closure_not_production_system.

V3.5, V3.6, V3.7, V3.8, V3.9, V3.10, V3.10.1, V3.11, V3.12, V3.13, and V3-final are closed. V3-final adds deterministic local fault injection, node failure/recovery/network delay/drop/target congestion/Relay fault observation artifacts, local observability summaries, component health status, final artifact catalog, reproducibility manifest/guide/manual, and paper experiment mapping. It is not multi-server deployment, not a production cluster, not production PBFT / HotStuff / Raft, not production fault tolerance, not production monitoring, not a Byzantine adversary model, not BlockEmulator backend, not Fabric/EVM live backend, and not paper-grade performance evidence.
Next stage: V3 maintenance only; do not start V4 unless explicitly requested.

After V3-final, the compressed V3 roadmap is documented in `docs/v3_remaining_roadmap_after_v3_10_1.md`. New feature work should be treated as maintenance unless the user explicitly starts V4.

## V3.5 Route

- V3.5.1 Logical Node Topology Runtime: frontend topology config, backend validation, single-process logical nodes, and node/network/message artifacts.
- V3.5.2 Local Multi-process Launcher Preview: generate launcher preview artifacts from topology.
- V3.5.3 Local Node Process Runtime: add local process role entry points.
- V3.5.4 V3.5 Closure: align README/docs/skill/frontend/backend stage wording and validation. Complete.

V3.5 is node topology and local launcher foundations. It is not Fabric/EVM live backend work, not real TCP/PBFT, and does not claim full BlockEmulator compatibility.

## V3.6 / V3.7 Planning

- V3.6.1 implemented: configurable `NetworkAdapter` with localhost TCP typed message preview.
- V3.6.2 implemented: consensus-light proposal/vote preview over NetworkAdapter typed messages and V3.6 closure.
- V3.7.1 implemented: configurable `ConsensusRuntimePlugin`, with `blockemulator_aligned_pbft_preview` as one selectable PBFT state machine preview rather than the only consensus path.
- V3.7.2 implemented: PBFT preview over the selected NetworkAdapter path plus V3.7 closure.
- V3.8 implemented: CrossShardProtocol skeleton and closure with relay_preview artifacts.
- V3.9 implemented: State Authenticity Layer MVP with persistent state backend, Merkle/MPT-like roots, proof verification, and witness artifacts.
- V3.10 implemented: Benchmark / Experiment Template Hardening with benchmark templates, baseline profiles, local sweep runner, reproducibility manifest, and benchmark report artifacts.
- V3.10.1 implemented: Frontend UX and Chinese Console Cleanup with simplified navigation, HelpTip explanations, run progress feedback, and lightweight result chart preview.
- V3.11 implemented: CrossShardProtocol Relay MVP with SourceLock, RelayCertificate, target commit, source finalization, timeout/refund/abort paths, and Relay MVP artifacts.
- V3.12 implemented: Runtime Realism Closure with local_multi_process dry_run/smoke, max_local_processes guard, node lifecycle artifacts, localhost TCP process path records, shard/committee/epoch MVP artifacts, and light reconfiguration plan.
- V3.13 implemented: Metaverse Experiment Suite Closure with metaverse workload catalog, scenario templates, baseline matrix, multi-seed/sweep MVP, and paper table/figure data export scaffold.
- V3-final implemented: Fault, Observability, and Reproducibility Closure with deterministic fault injection MVP, observability summary, component health status, final artifact catalog, reproducibility guide, experiment manual, and paper experiment mapping.

V3.6, V3.7, V3.8, V3.9, V3.10, V3.10.1, V3.11, V3.12, V3.13, and V3-final are closed. Do not start V4 unless explicitly requested.

## Historical V0 Scope

V0 established the platform skeleton:

- basic frontend
- FastAPI backend
- experiment composer
- default plugin package
- `asset_hotspot` synthetic workload
- MockChain
- streaming `trace.jsonl.gz`
- Go executor with virtual clock replay
- basic metrics
- CI sanity test

V0 uses the default MockChain single-chain component package. It does not include Fabric, EVM, PBFT, HotStuff, DAG consensus, complex routing/execution, cross-chain protocols, Grafana, multi-user permissions, or distributed deployment.

Runtime versions remain aligned with the project instructions: Python 3.12.x, Go 1.26.1, Node.js 22 LTS, React 18.x, TypeScript 5.x, FastAPI 0.115.x, and Uvicorn 0.30.x.

## Repository Layout

- `frontend/`: React/Vite frontend.
- `backend/`: FastAPI API, composer, run control, and artifact management.
- `workload/` and `trace/`: synthetic workload and streaming trace support.
- `chain/mockchain/`: V0 MockChain backend.
- `executor/`: Go replay executor and V3 modular runtime.
- `configs/`: experiment configs, plugin lists, profiles, and topology defaults.
- `docs/`: implementation plans, stage docs, validation notes, and truth boundaries.

## Python Development And Tests

From the repository root:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements-dev.txt
```

Run the asset hotspot workload test:

```powershell
python -m pytest tests/workload/test_asset_hotspot.py -q
```

## V0 Backend Control Plane

Install backend dependencies and start FastAPI:

```powershell
python -m pip install -r backend/requirements.txt
python -m uvicorn backend.app.main:app --reload
```

Health check:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/health
```

Run the default V0 experiment and view the summary:

```powershell
Invoke-RestMethod -Method Post http://127.0.0.1:8000/api/v0/experiments/v0_default_asset_hotspot/run
Invoke-RestMethod http://127.0.0.1:8000/api/v0/experiments/v0_default_asset_hotspot/summary
```

Generated `experiments/runs/` artifacts are local outputs and are ignored by Git.

## Frontend

The frontend uses Node.js 22 LTS, React 18, TypeScript 5, and Vite.

```powershell
cd frontend
npm install
npm run dev
```

The default Vite URL is usually `http://127.0.0.1:5173`, connected to the local backend at `http://127.0.0.1:8000`.

## V0 End-to-End Sanity Check

Run from the repository root:

```powershell
python scripts/v0_sanity.py
```

The sanity check regenerates the default `asset_hotspot` trace, runs Go replay, and checks required trace, summary, latency, and runtime log artifacts. This remains a regression validation even though the active project stage is V3-final Fault, Observability, and Reproducibility Closure.

## Windows One-Click Startup

From the repository root, run:

```powershell
.\start_mbe.bat
```

The script checks `.venv` and `frontend/node_modules`, starts backend/frontend PowerShell windows, and opens the local frontend when services are ready.

## Artifact Downloads

After a run completes, the frontend artifact panel shows available summary, log, profile, node-level, launcher preview, and local node process preview files. V3.5 artifacts include:

- `node_address_table.csv`
- `topology.json`
- `launch_nodes_windows.bat`
- `launch_nodes_linux.sh`
- `launcher_readme.md`
- `node_process_status.csv`
- `node_process_manifest.json`
- `node_process_log_sample.log`
- `address_table.json`
- `multi_process_manifest.json`
- `node_process_log.csv`
- `node_lifecycle_log.csv`
- `network_message_log.csv`
- `node_process_status.json`
- `local_multi_process_summary.json`
- `shard_assignment_log.csv`
- `committee_assignment_log.csv`
- `committee_summary.json`
- `epoch_log.csv`
- `reconfiguration_plan.json`
- `reshard_plan_log.csv`
- `reconfiguration_summary.json`
- `tcp_adapter_status.csv`
- `network_send_log.csv`
- `network_receive_log.csv`
- `typed_message_log.csv`
- `consensus_network_log.csv`
- `pbft_network_summary.json`
- `cross_shard_tx_log.csv`
- `cross_shard_message_log.csv`
- `relay_preview_log.csv`
- `cross_shard_status.csv`
- `cross_shard_summary.json`
- `relay_state_machine_log.csv`
- `source_lock_log.csv`
- `relay_certificate_log.csv`
- `relay_proof_verification_log.csv`
- `target_verification_log.csv`
- `target_commit_log.csv`
- `source_finalize_log.csv`
- `cross_shard_timeout_refund_log.csv`
- `cross_shard_failure_log.csv`
- `relay_mvp_summary.json`
- `state_storage_log.csv`
- `state_version_log.csv`
- `state_root_log.csv`
- `state_proof_log.csv`
- `state_proof_verification_log.csv`
- `witness_log.csv`
- `witness_verification_log.csv`
- `state_authenticity_summary.json`
- `benchmark_template_catalog.json`
- `baseline_profile_catalog.json`
- `benchmark_plan.json`
- `benchmark_run_index.csv`
- `sweep_matrix.csv`
- `sweep_summary.csv`
- `sweep_summary.json`
- `aggregate_summary.csv`
- `baseline_comparison.csv`
- `reproducibility_manifest.json`
- `benchmark_report.md`
- `benchmark_summary.json`
- `fault_injection_config.json`
- `fault_injection_log.csv`
- `node_failure_log.csv`
- `node_recovery_log.csv`
- `network_fault_log.csv`
- `target_congestion_log.csv`
- `relay_fault_observation_log.csv`
- `fault_injection_summary.json`
- `observability_summary.json`
- `observability_timeline.csv`
- `component_health_summary.csv`
- `runtime_component_status.json`
- `final_artifact_catalog.json`
- `final_artifact_catalog.md`
- `v3_final_reproducibility_manifest.json`
- `v3_reproducibility_guide.md`
- `v3_experiment_manual.md`
- `v3_paper_experiment_mapping.md`
- `v3_final_summary.json`

These launcher, node process, local multi-process runtime, NetworkAdapter, PBFT-over-network, cross-shard skeleton, Relay MVP, state authenticity, committee/epoch, metaverse workload, benchmark hardening, fault injection, observability, and reproducibility files are preview / MVP artifacts only. They do not prove production networking, production PBFT, full Byzantine safety, production fault tolerance, production monitoring, multi-server deployment, production cluster behavior, BlockEmulator backend behavior, complete Relay/Broker/2PC/Monoxide, production atomic cross-shard commit, Byzantine-secure relay, Ethereum-compatible MPT, production database durability, full stateless execution, complete cross-shard state proof protocol, large-scale distributed benchmark, performance superiority, or paper-grade benchmark evidence.
