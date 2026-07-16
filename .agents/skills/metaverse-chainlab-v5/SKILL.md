# metaverse-chainlab-v5

## 0. Purpose

This skill governs V5 work for MBE: Real Experiment Platform.

V5 builds a metaverse blockchain research platform where method design, saved templates, experiment specs, compatibility validation, simulation, real local multi-process multi-shard execution, unified results, and paper artifacts share one truthful workflow.

V5 is not implemented just because this skill exists.

## 1. Mandatory Start Rule

Every V5 work round must start with:

```powershell
cd F:\Metaverse_Blockchain_Env
git status --short
git log -1 --oneline
```

If the worktree is dirty, stop and report unless the user explicitly says the existing changes are expected and authorizes continuing.

Every V5 work round must read:

- `README.md`
- `.agents/skills/metaverse-chainlab-v5/SKILL.md`
- the relevant V5 stage document
- the relevant current code and docs before changing files

Planning comes before implementation. For a new stage, update docs/skill before code.

## 2. Current Code Boundary

Current implemented baseline:

- V3 formal MetaTrack benchmark runner is a runnable local modular emulator / logical-runtime path.
- V4.3 realism smoke has signed tx, sender/public-key binding, per-node mempool, localhost TCP, PBFT-style messages, deterministic execution, persistence, state-root consistency, cross-shard evidence, fault delay/drop evidence, and BlockEmulator CSV bridge.
- Current V4.3 smoke primarily creates multiple runtime objects inside one Go supervisor process. It is not final one-node-one-OS-process real cluster.
- `mbe-node --run-mode server` exists.
- `mbe-supervisor` can generate plans and run V4.2/V4.3 smoke, but is not yet a full cluster orchestrator.
- Run Experiment executes mainly `quick_validation` and `v4_realism_validation`; main/comparison/ablation/workload-sensitivity/topology-scaling are preview-only.
- Saved templates must continue to reuse `V3SavedConfig`.

## 3. Only Two V5 Stages

V5 has only:

1. `V5.1 Real Plugin-Driven Multi-Process Multi-Shard Runtime`
2. `V5.2 Real Formal Experiment and Result Closure`

Do not create public V5.1.1, V5.1.2, V5.2.1, or similar outward stages.

Former planned `V4.3.6.2 Formal Runner Dispatch` and `V4.3.6.3 Result Center Integration` belong inside V5.2.

## 4. V5.1 Goal

V5.1 builds a unified `ExperimentSpec` driven runtime with:

- full-module plugin catalog;
- dynamic frontend catalog/schema configuration;
- backend compatibility engine and compiler;
- immutable `CompiledRunPlan`;
- real local multi-process supervisor;
- Go Interface + Factory Registry runtime plugins;
- continuous multi-shard block/consensus/execution/commit;
- cross-shard protocol inside the same runtime;
- real workload client submission;
- evidence-grade process/network/state/artifact acceptance.

V5.1 implementation status: complete as a local research runtime after the required 8-node, 16-node, four-method difference, backend/Go/frontend, and E2E gates pass. V5.2 software closure is also implemented and verified through Gate A/B, an 8-child real RunGroup, and one completed 16-node/4-shard/10000-transaction Child. This does not claim production security, production PBFT, or completion of the long-running 12-child paper matrix.

## 5. V5.2 Goal

V5.2 uses V5.1 to execute full formal experiment matrices and close:

- Preview / Simulation / Real Cluster modes;
- main, comparison, ablation, sensitivity, scaling, fault/recovery experiments;
- persistent run groups and child runs;
- unified run registry and result center;
- plugin metrics display;
- aggregation, confidence intervals, paper table/figure data;
- reproducibility ZIP;
- Paper Candidate gate.

Before a V5.2 formal run group or matrix may execute, close and record two
mandatory gates:

- Gate A: category-specific runtime plugin interfaces and factories drive
  runtime behavior; plugin identifiers may occur in manifests/registration but
  not as main-path algorithm switches in node, client, or supervisor code.
- Gate B: finality and throughput are derived from raw real-runtime lifecycle
  events through durable commit/finalization or refund, never from TCP send
  latency or synthetic summary rows.

Use `scripts/v5_2_plugin_behavior_gate.py` to close Gate A and
`scripts/v5_2_finality_metric_acceptance.py` as the focused Gate B check.
Gate A/B, the 8-child RunGroup, and the completed single-child correctness gate
are required for V5.2 software closure. The 12-child paper matrix may be
compiled and persisted without execution; unexecuted rows are never completed
or Paper Candidates. A Paper Candidate still requires a completed real-cluster
child and all per-child truth checks.

## 6. Plugin Principle

All modules use one extension model. Categories:

- workload
- transaction_admission
- txpool
- sharding
- routing
- block_producer
- consensus
- network
- execution
- scheduler
- state_access
- state_storage
- cross_shard
- commit
- fault_injection
- metrics
- observability

Every plugin manifest must include `plugin_id`, `category`, `version`, `display_name`, `description`, `implementation_status`, `supported_backends`, `config_schema`, `default_config`, `capabilities`, `requirements`, `conflicts`, `metrics`, `runtime_factory` or `runtime_adapter`, and `truth_boundary`.

Adding a plugin should mean implementing an interface, registering a factory, adding a manifest, and adding tests. It should not require rewriting core frontend pages, supervisor flow, ExperimentSpec structure, compiler flow, or result-center core logic.

## 7. Frontend Rule

Frontend plugin configuration must follow:

```text
Plugin Catalog API
-> Generic Plugin Category Panel
-> Generic Plugin Selector
-> Generic Schema Form Renderer
-> Compatibility Feedback
-> Saved Template / ExperimentSpec
```

Avoid large `if (pluginId === "...")` branches in core pages. UI extensions are optional enhancements; generic schema form support is mandatory.

## 7.1 Workload Data Plane Rule

The V5 workload data plane is a Run-level concern, never a Method Design variable. Method profiles continue to exclude `workload` and `fault_injection`. Runnable workloads include `deterministic_signed_synthetic` and registered dataset sources compiled through `canonical_trace_replay`.

Dataset selection must be driven by a dataset registry, manifest, adapter, and generic workload configuration, not dataset-ID conditionals in `V5FormalRunPage`. A dataset source must carry provenance, deterministic selection, materialized hash, no-fallback status, and artifact snapshot. Open/hash/schema/count failure must fail the Child; it must not fall back to synthetic, V4 smoke, V1 replay, or Simulation. Full raw/canonical/materialized data is never committed to Git, and artifacts must not expose local absolute paths or credentials.

The implemented canonical contract is `mbe_workload_record_v1`. Source-specific fields belong in dataset adapters or canonical record `metadata`; the compiler and Go iterator consume generic sender, operation, state key, routing key, and skew key fields. Decentraland is the first formal real-data adapter, and future data sources should be added by manifest + adapter or by files accepted by `canonical_csv_v1`.

Paper Candidates using a dataset must record dataset ID, source hash, variant, materialized hash, realized skew, and actual cross-shard ratio. Keep the Go Interface + Factory Registry model. Do not modify consensus, finality definitions, or cross-shard protocol semantics unless a minimal replay adapter is proven necessary and tested.

The completed workload data plane implementation used internal sequential checkpoints:

- Checkpoint A: Dataset and Materialization
- Checkpoint B: Compiler and Real Runtime
- Checkpoint C: Frontend, Results, and Artifacts
- Checkpoint D: Full Acceptance

These checkpoints are not outward V5.3/V5.2.1/V6 stages.

The UI must show implementation status, backend support, parameter ranges, dependencies, conflicts, truth boundary, and metrics. Formal Real Cluster requires all selected plugins to support `real_cluster`.

## 8. Backend Rule

Use layered services:

- Plugin Manifest Store
- Plugin Catalog Service
- Compatibility Engine
- ExperimentSpec
- Experiment Compiler
- Cluster Orchestrator
- Runtime Adapter / Factory Registry

The orchestrator handles process lifecycle only. It must not embed MetaTrack/PBFT/hash-routing behavior through plugin-name `if/else` logic.

Reuse `V3SavedConfig`, existing Composer Drafts, and existing method templates. Do not create a parallel template store.

## 9. Go Runtime Rule

Windows is primary. Do not plan Go `.so` dynamic plugins.

Use:

```text
Interface + Factory Registry + Plugin Manifest + Runtime Configuration
```

Target interfaces include:

- `WorkloadPlugin`
- `AdmissionPlugin`
- `TxPoolPlugin`
- `ShardingPlugin`
- `RoutingPlugin`
- `BlockProducerPlugin`
- `ConsensusPlugin`
- `NetworkPlugin`
- `ExecutionPlugin`
- `SchedulerPlugin`
- `StateAccessPlugin`
- `StateStoragePlugin`
- `CrossShardPlugin`
- `CommitPlugin`
- `FaultPlugin`
- `MetricsPlugin`
- `ObservabilityPlugin`

Startup:

```text
node_config
-> plugin profile
-> registry lookup
-> factory instantiate
-> parameter validation
-> dependency injection
-> runtime start
```

## 10. Execution Backends

Preview:

- configuration, compatibility, matrix, resource estimate only.

Simulation:

- V3 logical runtime;
- fast screening and debugging;
- not automatic paper evidence.

Real Cluster:

- V5.1 real multi-process runtime;
- required for formal Paper Candidate;
- no automatic fallback to simulation or V4 smoke.

## 11. Real Cluster Requirements

Formal `real_cluster` requires one logical node per independent OS process, with independent PID, TCP port, mempool, consensus state, data directory, state/block/receipt/tx-index, and logs.

Supervisor responsibilities:

- resource estimate;
- port allocation;
- node config compilation;
- start N `mbe-node` child processes;
- process manifest and PID recording;
- health/network/committee readiness checks;
- start real `mbe-client`;
- monitoring;
- fault policy;
- termination;
- graceful stop;
- forced reap;
- orphan check;
- artifact collection and validation.

Startup failure must fail the run. Do not fall back.

## 12. Continuous Multi-Shard Requirement

V5.1 must run all shards continuously:

- each shard has committee, leader, validators, mempool, consensus rounds, block height, and state root;
- multiple shards run at the same time;
- intra-shard and cross-shard transactions coexist in one runtime.

Cross-shard path:

```text
SourceLock
-> RelayCertificate
-> TargetVerify
-> TargetCommit
-> SourceFinalize
```

Failure path:

```text
Timeout
-> Refund / Abort
```

Certificates and cross-shard state messages must travel through the real node network and shard consensus/commit paths.

## 13. Truth Labels

Use:

```text
v3_final_light_runtime_baseline
v3_formal_simulation_logical_runtime
v4_realism_smoke_regression
v5_preview_only
v5_simulation_logical_runtime
v5_real_cluster_candidate
v5_paper_candidate_real_cluster
```

Do not claim V5 real_cluster before independent process evidence exists. Do not claim current V4 smoke is independent OS multi-process. Do not claim V3 formal is V5 real cluster.

## 14. Validation Rules

Docs/config-only:

```powershell
git diff --check
git status --short
```

Python/backend modified:

```powershell
$env:PYTHONPATH = (Get-Location).Path
python -m pytest tests -q
python -m pytest backend/tests -q
python scripts/v0_sanity.py
```

Frontend modified:

```powershell
cd frontend
npm.cmd run build
cd ..
```

Go modified:

```powershell
cd executor
go test ./...
cd ..
```

Do not commit if validation cannot be completed.

## 15. Git Rules

Do not push unless the user explicitly asks.

Commit only when explicitly requested or when the current work round asks for a commit and validation passes. Do not commit generated artifacts, caches, local run outputs, frontend build output, or large traces.

## 16. Final Report Format

Every V5 work round final report must include:

```text
1. 当前基线 commit
2. 当前代码真实能力审查结论
3. 当前最关键的五个缺口
4. V5 总目标
5. V5.1 定义
6. V5.2 定义
7. 全模块插件类别
8. 前端动态配置原则
9. 后端分层原则
10. Go Runtime 插件原则
11. 多进程真实运行定义
12. 持续多分片定义
13. Simulation / Real Cluster 边界
14. V3/V4/V5 迁移关系
15. 新增文件列表
16. 修改文件列表
17. git diff --check 结果
18. 是否 commit
19. commit hash
20. ready_to_commit=true/false
21. push=false
22. 本轮没有修改的业务代码范围
23. 下一步应该进入的阶段
```
