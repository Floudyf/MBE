# V5.0 Real Experiment Platform Master Plan

## 1. Purpose

V5 defines the next architecture for MBE after the current V4.3.6.1 baseline. The target is a metaverse blockchain research platform where a user can design a fully pluginized blockchain method, save it as a reusable method template, preview and simulate it, run it on a real local multi-process multi-shard cluster, and close formal paper artifacts from the same experiment definition.

This is a planning and skill reset document only. It does not claim that V5.1 or V5.2 is implemented.

> Status update: V5.1 is implemented and verified as the local real-cluster runtime foundation. V5.2 formal software closure is implemented and verified through Gate A/B, an 8-child real RunGroup, a completed 16-node/4-shard/10000-transaction Child, and persisted 12-child matrix compilation. The 12-child high-load paper matrix remains a long-running experiment and is not represented as executed evidence.

## 2. Current Code Review

The current repository baseline is `6faaf8d Align saved templates with run matrix`.

Current implemented foundations:

- `README.md` documents the V3/V4 truth boundary, though it previously still described V4 as the next planning direction.
- `backend/app/models/experiment_flow.py`, `backend/app/services/experiment_flow_service.py`, and `backend/app/api/experiment_flow.py` expose experiment-flow catalog, run-plan preview, matrix preview, V4 request derivation, and selected-row execution.
- `backend/app/models/v3_saved_config.py` and `backend/app/services/v3_saved_config_store.py` define and persist `V3SavedConfig`; saved method templates already reuse this store.
- `backend/app/models/v3_metatrack_formal_benchmark.py` and `backend/app/services/v3_metatrack_formal_benchmark_runner.py` implement a runnable V3 formal MetaTrack benchmark path. It builds a formal matrix, generates experiment and plugin profiles, calls Go `cmd/replay -mode v3-runtime`, extracts metrics, aggregates results, computes confidence intervals, emits chart data, and writes reproducibility artifacts.
- `backend/app/services/v3_go_runtime_runner.py` confirms the formal path is backed by the Go V3 runtime via `go run ./cmd/replay -mode v3-runtime`.
- `backend/app/models/v4_realism.py`, `backend/app/api/v4_realism.py`, and `backend/app/services/v4_realism_runner.py` expose V4 realism smoke status, smoke execution, summary, and artifacts through `/api/v4/realism/*`.
- `frontend/src/pages/V3ComposerPage.tsx`, `frontend/src/components/experiment/MethodPipelineWorkbench.tsx`, and `backend/app/services/v3_composer_catalog.py` show the current 11-module Composer and saved-template workflow.
- `frontend/src/pages/RunExperimentPage.tsx` reads saved methods through experiment-flow, supports preset/custom topology, `nodes`, `shards`, `validators_per_shard`, `tx_count`, `seed`, `repeat_count`, and expands matrix rows.
- `frontend/src/App.tsx` still keeps result/artifact views centered on V2 run registry plus separate V4 realism details.
- `executor/cmd/mbe-node/main.go` has an independent node entrypoint with `--run-mode server`.
- `executor/cmd/mbe-client/main.go` currently generates signed transaction JSONL and client logs; it does not yet submit a real workload over RPC.
- `executor/cmd/mbe-supervisor/main.go` can generate a V4 plan or run V4.2/V4.3 smoke, but it is not yet a full real-cluster orchestrator with child-process lifecycle, PID management, health checks, workload scheduling, fault injection, stop/reap, and artifact collection.
- `executor/realism/p2p`, `tx`, `mempool`, `execution`, `state`, `storage`, `xshard`, and `faults` contain real foundations for local TCP, signed transactions, per-node mempool, deterministic execution, state/storage persistence, cross-shard state-machine evidence, and fault delay/drop evidence.

## 3. Current Real Capabilities

MBE can launch the React + FastAPI platform.

The V3 formal MetaTrack benchmark runner is runnable, but its truth boundary is local modular emulator / logical runtime evidence. It is not the final real multi-process sharded chain.

The V4.3 realism smoke has signed transactions, sender/public-key binding, per-node mempool, localhost TCP, PBFT-style messages, deterministic execution, state/block/receipt/tx-index persistence, state-root consistency, cross-shard state-machine evidence, fault delay/drop evidence, and a BlockEmulator CSV bridge.

The V4.3 smoke is not yet the final real cluster. Its main block-commit path creates multiple `RuntimeV41` objects in one Go process, mostly under shard `s0`. Cross-shard flow is strong acceptance/evidence work, but it is not yet one continuous runtime where all shards continuously produce blocks and all intra-shard/cross-shard transactions share one general path.

## 4. Key Gaps

1. No unified Plugin Catalog + Manifest + Schema + Compatibility + Runtime Factory spans all modules and all backends.
2. Method templates do not yet drive V4/V5 real-node runtime plugin loading. `V4RealismSmokeRequest` has topology and smoke flags, but no complete plugin selection snapshot.
3. No full real-cluster orchestrator exists. `mbe-supervisor` does not yet start and manage one OS process per logical node.
4. Run Experiment executes mainly `quick_validation` and `v4_realism_validation`; main, comparison, ablation, workload sensitivity, and topology scaling remain preview-only.
5. Result Center is not unified across V3 formal runs, V4 realism runs, future V5 real-cluster runs, run groups, and child runs.

## 5. V5 Target

V5 builds a real experiment platform for metaverse blockchain research:

```text
Experiment Design
-> Saved Method Template
-> ExperimentSpec
-> Compatibility Validation
-> CompiledRunPlan
-> Preview / Simulation / Real Cluster
-> Run Group / Child Runs
-> Unified Result Center
-> Reproducibility and Paper Artifacts
```

Preview and simulation remain useful. Formal paper-candidate execution must use `real_cluster`. If `real_cluster` fails to start, it must fail visibly and must not fall back to simulation or V4 smoke. Simulation output must never be auto-marked as formal paper evidence.

## 6. Only Two V5 Stages

V5 has only two outward stages:

1. `V5.1 Real Plugin-Driven Multi-Process Multi-Shard Runtime`
2. `V5.2 Real Formal Experiment and Result Closure`

Do not create public V5.1.1, V5.1.2, or V5.2.1 stages. Internal work packages are allowed inside the stage documents, but public reporting should keep the two-stage boundary.

The old planned ideas `V4.3.6.2 Formal Runner Dispatch` and `V4.3.6.3 Result Center Integration` are absorbed into V5.2.

## 7. Architecture Summary

V5.1 builds the platform substrate:

- unified plugin manifest store and catalog API;
- generic frontend category/selector/schema rendering;
- compatibility engine;
- immutable `ExperimentSpec`;
- immutable `CompiledRunPlan`;
- real multi-process cluster supervisor;
- Go Interface + Factory Registry plugin runtime;
- continuous multi-shard block/consensus/execution/commit runtime;
- real workload client submission;
- cross-shard protocol inside the same continuous runtime.

V5.2 uses the V5.1 substrate to run formal experiment groups:

- Preview, Simulation, and Real Cluster modes;
- main, comparison, ablation, sensitivity, topology scaling, and fault/recovery experiments;
- persistent run groups and child runs;
- unified result registry and result center;
- dynamic plugin metrics;
- statistical aggregation and confidence intervals;
- paper figure/table data and reproducibility ZIP;
- strict Paper Candidate gate.

## 8. Truth Boundary

Truth labels must be explicit:

```text
v3_final_light_runtime_baseline
v4_realism_smoke_regression
v5_preview_only
v5_simulation_logical_runtime
v5_real_cluster_candidate
v5_paper_candidate_real_cluster
```

Do not call a run `real_cluster` unless every logical node is an independent OS process with its own PID, port, mempool, consensus state, data directory, storage, and logs.

## 9. Migration Principles

- Preserve V3 logical/formal runtime as `simulation`.
- Preserve V4 realism smoke as historical validation and regression evidence.
- Add V5 `real_cluster` as a new formal backend.
- Reuse `V3SavedConfig`; do not create a parallel template store.
- Adapt current 11-module Composer records into the unified V5 Plugin Catalog through compatibility mapping.
- Do not delete old APIs early; wrap them through facades/adapters and deprecate deliberately.
- Keep existing backend tests, Go tests, frontend build, V3 formal regression, and V4 realism regression passing in implementation rounds.

## 10. Risks And Resource Boundary

Windows is the primary development environment. V5 must not rely on Go `.so` dynamic plugins. Use Go interfaces, a factory registry, manifests, and runtime configuration.

The local real cluster must include resource estimates and guardrails. A formal `real_cluster` failure must produce clear diagnostics, process cleanup evidence, and no automatic downgrade.

## 11. Done For V5.0

V5.0 is complete when the V5 master plan, V5.1 plan, V5.2 plan, migration/truth-boundary plan, V5 skill, and README status wording exist and only documentation/skill/README files changed.
