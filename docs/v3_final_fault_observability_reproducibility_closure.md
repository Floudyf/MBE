# V3-final Fault, Observability, and Reproducibility Closure

## 1. V3-final Goal

V3-final closes V3 as a local emulator prototype by adding deterministic fault injection, local observability summaries, final artifact cataloging, and reproducibility documentation around the existing Go-backed Draft Smoke path.

## 2. Stage Metadata Alignment

Backend V3 stage metadata is sourced from `backend/app/services/v3_runtime_topology.py::stage_metadata()`. The frontend V3 Composer page reads backend preview metadata and falls back to V3-final labels only when metadata is unavailable.

## 3. Fault Injection MVP

New topology fields include `fault_injection_enabled`, `fault_profile`, `fault_seed`, `fault_start_round`, `fault_duration_rounds`, `failed_node_count`, `message_delay_ms`, `message_drop_ratio`, `target_congestion_ratio`, and `relay_fault_mode`.

Supported profiles are `none`, `node_failure`, `node_recovery`, `network_delay`, `network_drop`, `target_congestion`, `relay_fault`, and `mixed_fault`.

This is deterministic local metadata generation. It is not production fault tolerance and not a Byzantine adversary model.

## 4. Observability

`observability_enabled` and `observability_level=basic|detailed` control local observability outputs. V3-final writes summary, timeline, component health, and runtime component status artifacts.

This is not production monitoring.

## 5. Reproducibility

`reproducibility_bundle_enabled`, `paper_mapping_enabled`, and `final_artifact_catalog_enabled` control final closure outputs: manifest, guide, experiment manual, paper mapping, and artifact catalog.

This is a reproducibility bundle, not a paper-grade result claim.

## 6. Artifacts

Fault artifacts: `fault_injection_config.json`, `fault_injection_log.csv`, `node_failure_log.csv`, `node_recovery_log.csv`, `network_fault_log.csv`, `target_congestion_log.csv`, `relay_fault_observation_log.csv`, `fault_injection_summary.json`.

Observability artifacts: `observability_summary.json`, `observability_timeline.csv`, `component_health_summary.csv`, `runtime_component_status.json`.

Final artifacts: `final_artifact_catalog.json`, `final_artifact_catalog.md`, `v3_final_reproducibility_manifest.json`, `v3_reproducibility_guide.md`, `v3_experiment_manual.md`, `v3_paper_experiment_mapping.md`, `v3_final_summary.json`.

## 7. Summary Metrics

V3-final adds `v3_final_enabled`, `stage_alignment_ok`, `frontend_backend_alignment_truth`, `v3_final_truth`, fault event counts, observability component counts, and reproducibility artifact availability metrics.

## 8. Frontend Changes

The V3 Composer page uses backend stage metadata, adds fault/observability/reproducibility controls in Runtime Topology, shows final metrics in result panels, and groups final artifacts for download.

## 9. Truth Boundary

V3-final implements a local emulator closure with deterministic fault injection, observability, and reproducibility artifacts. It is not multi-server deployment, not a production cluster, not production PBFT / HotStuff / Raft, not BlockEmulator backend, not Fabric/EVM live backend, and does not prove paper-grade performance.

中文口径：V3-final 实现的是本地 emulator 原型闭环，用于增强节点生命周期、网络消息、分片、委员会、epoch、故障、观测和复现的可观测性。它不是多服务器部署，不是生产级集群，也不是生产级共识网络。

## 10. Validation Commands

```powershell
git diff --check
cd frontend
npm.cmd run build
cd ..
cd executor
go test ./...
cd ..
$env:PYTHONPATH = (Get-Location).Path
python -m pytest backend/tests -q
python -m pytest tests -q
python scripts/v0_sanity.py
```
