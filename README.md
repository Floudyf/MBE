# MBE - Metaverse Blockchain Experiment Platform

MBE is evolving from a local modular research-chain runtime toward a configurable node-topology emulator-like runtime for metaverse blockchain experiments.

Current stage: V3.8 CrossShardProtocol Skeleton Closure.
Latest runtime capability: configurable CrossShardProtocol skeleton with relay_preview artifacts.
Runtime truth: cross_shard_protocol_skeleton_not_atomic_cross_shard_commit.
Next stage: V3.9 StateStorage / StateProof Hardening.

## Current Status

Current stage: V3.8 CrossShardProtocol Skeleton Closure.
Latest runtime capability: configurable CrossShardProtocol skeleton with relay_preview artifacts.
Current capability: `cross_shard_protocol` configuration with `none` and runnable `relay_preview` skeleton, cross-shard transaction detection preview, relay preview messages, cross-shard artifacts, and summary metrics under Routing/Sharding.
Runtime truth: cross_shard_protocol_skeleton_not_atomic_cross_shard_commit.

V3.5, V3.6, V3.7, and V3.8 are closed. V3.8 adds a configurable CrossShardProtocol entry and `relay_preview` skeleton artifacts while keeping CrossShardProtocol under Routing/Sharding. It remains not complete Relay, not Broker, not 2PC, not atomic cross-shard commit, not cross-shard state proof, not rollback/timeout recovery, not Fabric/EVM live backend, not BlockEmulator backend, and not a paper-grade benchmark.
Next stage: V3.9 StateStorage / StateProof Hardening. V3.9 has not started.

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
- V3.9 planned: StateStorage / StateProof Hardening.

V3.6, V3.7, and V3.8 are closed. V3.9 remains a roadmap item only.

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

The sanity check regenerates the default `asset_hotspot` trace, runs Go replay, and checks required trace, summary, latency, and runtime log artifacts. This remains a regression validation even though the active project stage is V3.8 CrossShardProtocol Skeleton Closure.

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

These launcher, node process, NetworkAdapter, PBFT-over-network, and cross-shard skeleton files are preview artifacts only. They do not prove production networking, production PBFT, full Byzantine safety, a real multi-process network runtime, BlockEmulator backend behavior, complete Relay/Broker/2PC, atomic cross-shard commit, cross-shard state proof, or paper-grade benchmark evidence.
