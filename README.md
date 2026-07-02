# MBE - Metaverse Blockchain Experiment Platform

MBE is evolving from a local modular research-chain runtime toward a configurable node-topology emulator-like runtime for metaverse blockchain experiments.

Current stage: V3.6.2 V3.6 Closure.
Latest runtime capability: configurable NetworkAdapter with consensus-light over typed message runtime.
Runtime truth: network_adapter_consensus_light_preview_not_real_pbft.
Next stage: V3.7 ConsensusRuntime and BlockEmulator-aligned PBFT Preview.

## Current Status

Current stage: V3.6.2 V3.6 Closure.
Latest runtime capability: configurable NetworkAdapter with consensus-light over typed message runtime.
Current capability: consensus-light proposal/vote preview over the selected NetworkAdapter typed message path.
Runtime truth: network_adapter_consensus_light_preview_not_real_pbft.

V3.5 is closed. V3.6 is closed. V3.6.1 added a selectable NetworkAdapter surface with `in_memory_message_bus` compatibility and `localhost_tcp_preview` typed message preview artifacts. V3.6.2 adds consensus-light proposal/vote preview over the selected adapter and closes V3.6. It remains not real PBFT, not HotStuff/Raft, not production networking, not Fabric/EVM live backend, not BlockEmulator backend, not a real cross-shard protocol, and not a paper-grade benchmark.
Next stage: V3.7 ConsensusRuntime and BlockEmulator-aligned PBFT Preview. V3.7 has not started.

## V3.5 Route

- V3.5.1 Logical Node Topology Runtime: frontend topology config, backend validation, single-process logical nodes, and node/network/message artifacts.
- V3.5.2 Local Multi-process Launcher Preview: generate launcher preview artifacts from topology.
- V3.5.3 Local Node Process Runtime: add local process role entry points.
- V3.5.4 V3.5 Closure: align README/docs/skill/frontend/backend stage wording and validation. Complete.

V3.5 is node topology and local launcher foundations. It is not Fabric/EVM live backend work, not real TCP/PBFT, and does not claim full BlockEmulator compatibility.

## V3.6 / V3.7 Planning

- V3.6.1 implemented: configurable `NetworkAdapter` with localhost TCP typed message preview.
- V3.6.2 implemented: consensus-light proposal/vote preview over NetworkAdapter typed messages and V3.6 closure.
- V3.7 planned: configurable `ConsensusRuntime`, with `blockemulator_aligned_pbft_preview` as one selectable consensus plugin rather than the only consensus path.
- V3.8 planned: CrossShardProtocol skeleton.

V3.6 is closed. V3.7 and V3.8 remain roadmap items only.

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

The sanity check regenerates the default `asset_hotspot` trace, runs Go replay, and checks required trace, summary, latency, and runtime log artifacts. This remains a regression validation even though the active project stage is V3.6.2.

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

These launcher, node process, and NetworkAdapter files are preview artifacts only. They do not prove production networking, real PBFT, a real multi-process network runtime, BlockEmulator backend behavior, or paper-grade benchmark evidence.
