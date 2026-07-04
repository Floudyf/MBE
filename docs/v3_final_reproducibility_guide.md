# V3-final Reproducibility Guide

## 1. Stage

Current stage is V3-final Fault, Observability, and Reproducibility Closure.

## 2. Environment

Use Python 3.12.x, Go 1.26.1, Node.js 22 LTS, React 18.x, TypeScript 5.x, FastAPI 0.115.x, and Uvicorn 0.30.x.

## 3. Entrypoints

Primary entrypoints are the V3 Composer frontend, `backend/app/main.py`, `backend/app/services/v3_composer_draft_runner.py`, and `executor/v3runtime/`.

## 4. Minimal Run

Open the V3 Composer, validate a draft, run Draft Smoke, and inspect `summary.json`, `v3_final_summary.json`, `fault_injection_summary.json`, `observability_summary.json`, and `v3_final_reproducibility_manifest.json`.

## 5. Fault Configuration

Use `fault_profile=none` for no-op fault artifacts or `mixed_fault` for representative deterministic local node/network/Relay observations.

## 6. Observability Configuration

Use `observability_level=basic` for summary outputs and `detailed` for additional component timeline rows.

## 7. Reproducibility Bundle

Keep generated config/profile files, summaries, final catalog, manifest, guide, manual, and paper mapping together with the commit hash.

## 8. Controlled Smoke

The controlled smoke runner executes representative MetaTrack presets and copies final closure artifacts from a representative child run.

## 9. Artifact Review

Use `final_artifact_catalog.json` to identify which artifacts exist, which stage added them, and which truth boundary applies.

## 10. Validation

Run frontend build, Go tests, backend tests, existing tests, V0 sanity, and `git diff --check` before committing.

## 11. Non-goals

Do not interpret outputs as production networking, production consensus, production monitoring, Byzantine adversary analysis, or paper-grade performance evidence.

## 12. Truth Boundary

V3-final implements a local emulator closure with deterministic fault injection, observability, and reproducibility artifacts. It is not multi-server deployment, not a production cluster, not production PBFT / HotStuff / Raft, not BlockEmulator backend, not Fabric/EVM live backend, and does not prove paper-grade performance.
