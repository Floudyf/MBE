# V3.4.11 Stage / Version / Frontend / Docs Closure

## 1. Scope

This stage only closes stage/version/frontend/docs/skill alignment. It does not add a new runtime mechanism.

## 2. Current Stage

```text
current_stage = V3.4.11 closure
latest_runtime_stage = V3.4.10 controlled smoke runner
runtime_truth = local Go-backed modular research chain Draft Smoke
next_stage = V3.5 node-level emulator skeleton
```

## 3. What V3.4.9 Added

V3.4.9 added the MetaTrack ablation templates.

## 4. What V3.4.10 Added

- controlled smoke runner
- five MetaTrack presets
- run_index.csv
- aggregate_summary.csv
- ablation_report.md
- realism_readiness.json
- realism_readiness.md
- backend controlled smoke API
- frontend controlled smoke panel
- test_v3_4_10_controlled_smoke.py

## 5. What V3.4.11 Does

- align frontend wording
- align backend stage fields
- align README / docs
- align Codex skill
- clarify truthfulness boundaries
- run validation

## 6. Can Claim

- local Go-backed modular research chain Draft Smoke
- deterministic controlled MetaTrack smoke comparison
- module-level artifacts
- realism readiness report
- research prototype / emulator-like local runtime

## 7. Cannot Claim

- paper-grade benchmark
- real on-chain execution
- Fabric/EVM live backend
- BlockEmulator backend
- real multi-node network
- real PBFT/HotStuff/Raft
- real cross-shard protocol
- real proof/witness/MPT/state root
- persistent KV

## 8. Validation

V3.4.11 validation commands:

```powershell
C:\Users\飛\AppData\Local\Programs\Python\Python312\python.exe -m pytest backend/tests/test_v3_4_10_controlled_smoke.py -q
C:\Users\飛\AppData\Local\Programs\Python\Python312\python.exe -m pytest backend/tests -q
C:\Users\飛\AppData\Local\Programs\Python\Python312\python.exe -m pytest tests -q
cd executor
go test ./...
cd ..
cd frontend
npm.cmd run build
cd ..
C:\Users\飛\AppData\Local\Programs\Python\Python312\python.exe scripts/v0_sanity.py
git diff --check
git status --short
```

Recorded V3.4.11 validation results:

- `pytest backend/tests/test_v3_4_10_controlled_smoke.py -q`: passed, 1 passed.
- `pytest backend/tests -q`: passed, 273 passed, 1 Starlette/httpx2 deprecation warning.
- `pytest tests -q`: passed, 24 passed.
- `go test ./...`: passed after permissioned rerun because the sandbox could not write the Go build cache.
- `npm.cmd run build`: passed after permissioned rerun because the sandbox could not create `frontend/dist/assets`; Vite reported the existing CJS Node API deprecation warning.
- `scripts/v0_sanity.py`: passed.
- `git diff --check`: passed with only line-ending conversion warnings.
- `git status --short`: expected V3.4.11 modified files before commit.

## 9. Next Stage

The next stage is V3.5 node-level emulator skeleton. Enter V3.5 only after V3.4.11 closure is clean and validated.
