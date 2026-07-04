# V3-final Experiment Manual

## 1. Purpose

Use V3-final to run controlled local emulator experiments with deterministic workload, topology, fault, observability, and reproducibility artifacts.

## 2. Select Template

Choose a runnable V3 Composer template and preset. Keep planned-only modules disabled unless explicitly testing validation.

## 3. Configure Runtime

Use `logical_single_process` for the stable default path or `local_multi_process` for the local process MVP. `local_multi_process` is local-machine only.

## 4. Configure Metaverse Suite

Enable the V3.13 metaverse suite when synthetic metaverse workload metadata, baseline matrix, sweep, or paper export scaffold artifacts are needed.

## 5. Configure Fault Injection

Set `fault_injection_enabled=true` and choose a deterministic `fault_profile`. Use `mixed_fault` for representative local closure smoke.

## 6. Run Draft Smoke

Validate the draft first. Then run Draft Smoke and inspect the result overview, raw metrics, and artifact groups.

## 7. Inspect Outputs

Review `summary.json`, `fault_injection_summary.json`, `observability_summary.json`, `runtime_component_status.json`, `final_artifact_catalog.json`, and `v3_final_summary.json`.

## 8. Boundaries

V3-final is a local emulator prototype. It is not multi-server deployment, not a production cluster, not production PBFT / HotStuff / Raft, not production fault tolerance, not production monitoring, not BlockEmulator backend, not Fabric/EVM live backend, and not paper-grade evidence.
