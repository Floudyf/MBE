# AGENTS.md

## Project

This repository implements MBE, a metaverse blockchain experiment platform. The current stable product baseline is the V5 Formal Experiment Workflow: V5 Method Design, runnable saved Method Profiles, Formal RunGroups, local independent-process `real_cluster` execution, results, artifacts, and explicit truth boundaries. V3-final and V4 realism paths remain preserved historical compatibility and regression baselines.

The next active implementation direction, only when explicitly requested by the user, is the V5 Workload Data Plane. It extends the completed V5 Formal Experiment Workflow with registered datasets, deterministic materialization, real dataset replay, and derived skew workloads. V3 and V4 remain historical compatibility and regression baselines with their original truth boundaries.

## Current Baseline And Next Direction

Stable baseline:

```text
V5 Formal Experiment Workflow = synthetic formal workflow / local real_cluster / results and reproducibility baseline
```

The next explicitly authorized implementation is:

```text
V5 Workload Data Plane implementation
```

This is not V5.3, V5.2.1, or V6. It must preserve V3/V4 truth boundaries and must not silently route dataset work to the synthetic workload.

The implemented workload data plane uses a generic `mbe_workload_record_v1`
canonical contract. Decentraland is the first formal real-data adapter, and
additional sources should be added by manifest + adapter or by providing a
file accepted by `canonical_csv_v1`. Runtime nodes, consensus, state storage,
cross-shard protocol, and result collection consume only the generic workload
record and do not know the original source format.

## V5 Workload Data Plane Safety

- Workload is a Run condition, not a Method Design variable. Saved V5 methods continue to exclude `workload` and `fault_injection`.
- Full CSV, canonical traces, and materialized workloads do not enter Git. Raw data is read-only; materializer output belongs in `.cache/workloads`.
- Artifacts expose dataset IDs or relative logical paths only, never local absolute paths. API/RPC credentials never enter code, documentation, data, manifests, or artifacts.
- Dataset open/hash/schema/count failures fail the Child. There is no fallback to `deterministic_signed_synthetic`, V4 smoke, V1 replay, or Simulation.
- Source-specific fields belong in dataset adapters and record `metadata`, not
  in the core Go iterator or compiler contract.
- Implement in order: dataset validation -> canonical conversion -> materialization -> ExperimentSpec -> compiler -> Go iterator -> frontend -> E2E. Read `docs/v5_workload_data_plane_design.md` before implementation.

## Non-negotiable Workflow

Every MBE work round must follow this order:

1. Check the worktree state.
2. Read the relevant README, docs, and skill file.
3. Plan the stage boundary.
4. Update docs/skill before code when opening a new stage.
5. Implement only the requested stage.
6. Run the relevant validation commands.
7. Report changed files, validation results, git status, and whether anything was not completed.

Planning comes before implementation. Do not jump into code before the stage has a clear written scope.

## Git And Safety Rules

Before modifying files, run:

```powershell
cd F:\Metaverse_Blockchain_Env
git status --short
```

If the worktree is dirty, stop and report unless the user explicitly says the existing changes are expected and authorizes continuing on top of them.

Codex may commit only when explicitly asked and after validation. Codex must not push unless the user explicitly asks for push.

Do not overwrite user changes. Do not commit generated artifacts, caches, local run outputs, large traces, or frontend build output.

## Runtime And Language Versions

Keep the existing project constraints unless the user explicitly approves a change:

- Python: 3.12.x
- Go: 1.26.1
- Node.js: 22 LTS
- React: 18.x
- TypeScript: 5.x
- FastAPI: 0.115.x
- Uvicorn: 0.30.x

Keep dependency manifests and local development tooling aligned with these constraints when the corresponding module is modified.

## V3 Boundary

V3-final remains the stable light-runtime and experiment-console baseline. V3 may receive bounded maintenance patches only when explicitly requested. V3 must not claim:

- production blockchain capability;
- production PBFT / HotStuff / Raft;
- production networking;
- multi-server deployment;
- Byzantine adversary security;
- Fabric/EVM live backend;
- BlockEmulator replacement;
- paper-final performance superiority.

V3 preview/MVP artifacts must remain labeled as preview/MVP/local emulator evidence.

## V4 Historical Direction

V4 was opened as an explicitly requested historical stage. Its four-round plan is retained for compatibility and truth-boundary review:

```text
Round 1: docs / skill / roadmap reset only.
Round 2: real node foundation: signed tx, client, node, per-node mempool.
Round 3: real networking, block proposer, PBFT-style consensus runtime, block commit.
Round 4: deterministic execution, persistent state, cross-shard state machine, recovery/faults, frontend Realism Mode.
```

V4 aims to meet and exceed BlockEmulator-style runtime realism. It should not copy BlockEmulator code. It may study BlockEmulator concepts, interfaces, logs, and acceptance targets.

## V4 Runtime Truth Labels

Use explicit truth labels in docs, metadata, UI, and reports:

```text
v3_final_light_runtime_baseline
v4_real_node_foundation
v4_real_p2p_consensus_commit
v4_real_state_cross_shard_recovery
v4_realism_runtime_candidate
```

Do not label a component as real unless its logs and tests are produced by actual runtime behavior rather than deterministic row generation.

## V4 Target Transaction Lifecycle

The long-term V4 transaction lifecycle is:

```text
real trace / live client
-> signed transaction
-> node RPC submit
-> per-node mempool admission
-> shard routing
-> P2P tx gossip
-> shard leader block proposal
-> PBFT-style PrePrepare / Prepare / Commit / ViewChange
-> deterministic execution
-> state db / state root / receipt
-> durable block/state/receipt/tx-index commit
-> cross-shard SourceLock / RelayCertificate / TargetCommit / SourceFinalize or Refund
-> metrics / proof / receipt / frontend result
```

## V4 Non-goals In Early Rounds

Do not implement everything in one round. Early V4 rounds must not overclaim:

- production PBFT;
- production Byzantine security;
- complete Ethereum-compatible MPT;
- Fabric/EVM live backend;
- large-scale cloud deployment;
- paper-final performance superiority;
- full public-chain semantic compatibility;
- complete metaverse platform integration.

## Validation Commands

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

If validation cannot be completed, do not commit. Report the blocker.

## Final Report Format

Every work round final report must include:

```text
1. 本轮阶段
2. 实现内容
3. 新增/修改文件
4. 未实现内容
5. 阶段边界检查
6. Data truth / backend truth 影响
7. Artifacts / outputs
8. 兼容性
9. 测试与验证结果
10. git status
11. commit hash
12. 是否 push：必须说明未 push，除非用户明确要求 push
```
