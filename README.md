# MBE - Metaverse Blockchain Experiment Platform

MBE is evolving from a local modular research-chain runtime toward a configurable node-topology emulator-like runtime for metaverse blockchain experiments.

Current stage: V3.5.2 Local Multi-process Launcher Preview.
Latest completed runtime capability: V3.5.1 configurable single-process logical node topology with node/network/message artifacts.
Current V3.5.2 capability: local multi-process launcher preview artifacts generated from topology.
Next stage: V3.5.3 Local Node Process Runtime.

## Current Status

Current stage: V3.5.2 Local Multi-process Launcher Preview.
Latest completed runtime capability: V3.5.1 configurable single-process logical node topology with node/network/message artifacts.
Current capability: launcher preview artifacts generated from logical node topology.
Runtime truth: launcher preview generated from logical node topology.

Not real TCP, not a real multi-process runtime, not real PBFT, not BlockEmulator backend, not Fabric/EVM live backend, not paper-grade benchmark.
Next stage: V3.5.3 Local Node Process Runtime.

## V3.5 Route

- V3.5.1 Logical Node Topology Runtime: frontend topology config, backend validation, single-process logical nodes, and node/network/message artifacts.
- V3.5.2 Local Multi-process Launcher Preview: generate launcher preview artifacts from topology.
- V3.5.3 Local Node Process Runtime: add local process role entry points.
- V3.5.4 V3.5 Closure: align README/docs/skill/frontend/backend stage wording and validation.

V3.5 is node topology and local launcher foundations. It is not Fabric/EVM live backend work and does not claim full BlockEmulator compatibility.

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

The sanity check regenerates the default `asset_hotspot` trace, runs Go replay, and checks required trace, summary, latency, and runtime log artifacts. This remains a regression validation even though the active project stage is V3.5.2.

## Windows One-Click Startup

From the repository root, run:

```powershell
.\start_mbe.bat
```

The script checks `.venv` and `frontend/node_modules`, starts backend/frontend PowerShell windows, and opens the local frontend when services are ready.

## Artifact Downloads

After a run completes, the frontend artifact panel shows available summary, log, profile, node-level, and launcher preview files. V3.5.2 launcher preview artifacts are:

- `node_address_table.csv`
- `topology.json`
- `launch_nodes_windows.bat`
- `launch_nodes_linux.sh`
- `launcher_readme.md`

These launcher files are preview artifacts only. They do not prove real TCP networking, real PBFT, a real multi-process runtime, or BlockEmulator backend behavior.
