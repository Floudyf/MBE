# metaverse-chainlab-v3

## 0. Current V3 Scope Realignment

As of the V3.4 runtime self-check, current V3 acceptance is realigned to:

- `V3.0 Planning Scaffold`: complete.
- `V3.1 Profile Layer`: complete.
- `V3.2 Minimal Single-chain Modular Runtime`: historical Python backend reference semantics.
- `V3.2b` / `V3.2.5 Go-backed Minimal Runtime / Go parity`: absorbed into V3.3.
- `V3.3 MetaTrack Plugin Evaluation`.
- `V3.3.1 Research-chain Role Separation`.
- `V3.3.2 Single-chain Modular Composer Profile / Experiment Templates`.
- `V3.3.3 Single-chain Composer Frontend MVP`.
- `V3.3.4 Composer Chinese Localization and Snake Layout Polish`.
- `V3.3.5a Interactive Single-chain Composer Draft UI`.
- `V3.3.5b Backend Draft Validation and Draft Smoke Run`.
- `V3.3.6 Draft Run Result UX and History Management`.
- `V3.3.7 Boundary, Documentation, and Skill Closure`.
- `V3.4.0 Runtime Self-check and Scope Realignment`.
- `V3.4.1 Runtime Plugin Hardening: FIFO TxPool`.
- `V3.4.2 Runtime Plugin Hardening: BlockProducer`.
- `V3.4.3 Runtime Plugin Hardening: Consensus-light`.
- `V3.4.4 Single-module Experiment Templates`.
- `V3.5 Fabric-backed Validation for MetaTrack`.
- `V3-final Frontend Integration and Acceptance`.

V3.4 series goal: harden critical foundation modules in the V3 Go-backed modular research chain runtime into observable runtime behavior before any Fabric-backed validation. V3.4 remains a local modular research chain runtime. It is not Fabric live execution.

Every V3.4.x runtime hardening substage must include corresponding frontend alignment. When runtime adds an artifact, summary metric, or module truth boundary, frontend artifact grouping, result summary, history detail, and module detail must align in the same implementation stage. Runtime must not output a new artifact that the frontend cannot download or explain.

Fabric-backed validation is deferred to V3.5. V3.5 remains small-scale black-box validation / calibration for MetaTrack. It is not a Fabric peer kernel modification and not the main experiment runtime.

Deferred / future scope:

- Minimal dual-chain runtime.
- MetaFlow Protocol Plugin and AFS/FDA.
- Multi-process / multi-machine network emulator behavior.

Existing MetaFlow profiles remain planned preview profiles and must not become runnable unless a later user request explicitly reopens that scope.

## 1. Scope

This skill governs V3 work for MBE.

V3 builds a MetaTrack-oriented modular plugin chain runtime with a later Fabric-backed validation layer and frontend acceptance. V3 reuses V2 experiment management, artifacts, sweeps, reports, calibration, and frontend shell. MetaFlow dual-chain protocol evaluation is retained only as planned preview / future roadmap material in the current V3 scope.

V3 positioning:

```text
V3 = Modular Plugin Chain Runtime with Fabric-backed Validation
V3 = 面向 MetaTrack 的模块化插件链实验平台，并带 Fabric 链支持验证；MetaFlow 保留为 planned preview / future roadmap。
```

Fabric-backed validation is a validation layer, not the V3.4.1 runtime hardening target. V3.4 runtime hardening is a prerequisite for calibrating a stable and observable modular runtime in V3.5.

## 2. V3 Non-goals

V3 is not:

- Not a production blockchain.
- Not a production cross-chain bridge.
- Not a public-chain deployment platform.
- Not a Fabric peer internal patch.
- Not a replacement for Fabric.
- Not a multi-public-chain production system.
- Not allowed to present `local_virtual`, replay, or Draft Smoke results as real-chain results.
- Not allowed to present smoke-level results as final paper evidence.

V3 must not claim production security, production availability, public-chain deployment, or complete Fabric replacement.

## 3. Stage Rules

Only one V3 stage may be implemented per round. Do not jump stages. Do not implement future planned components early.

Current V3.3 note: V3.3 absorbs the earlier V3.2b / V3.2.5 Go-backed parity stage. Gate A is Go-backed minimal runtime parity. Gate B is Go-backed MetaTrack plugin combinations and fair ablation. Gate C is the single-chain Composer Draft loop: frontend draft, backend validate-draft, backend run-draft-smoke, result display, local history, and artifact downloads. Do not implement Fabric validation, dual-chain runtime, MetaFlow, AFS, or FDA in V3.3.

Current V3.3.6 + V3.3.7 note: the latest stable V3.3 surface is a single-chain modular research chain composer. It supports composer preview, frontend Draft configuration, backend `validate-draft`, backend `run-draft-smoke`, Draft run history under `.cache/v3_draft_runs/`, and artifact download. Draft Smoke is a local single-configuration smoke path for debugging, demonstration, and configuration traceability. It is not a formal paper experiment, not Fabric-backed, not MetaFlow, and not dual-chain.

V3 stages:

- `V3.0 Planning Scaffold`: docs and skill only. Defines stage roadmap, boundaries, profiles, fair baseline policy, and acceptance checklist. No code.
- `V3.1 Profile Layer`: only ChainProfile / PluginProfile / ExperimentProfile schema, loader, validator, preview, and planned/runnable guard.
- `V3.2 Minimal Single-chain Modular Runtime`: historical minimal single-chain modular research chain runtime target.
- `V3.3 MetaTrack Plugin Evaluation`: only MetaTrack plugin combinations and fair single-chain evaluation on the Go-backed runtime.
- `V3.3.1 Research-chain Role Separation`: only role-separated single-chain runtime abstractions and artifacts for consensus domain, execution shard, state storage unit, placement, routing, and remote state access.
- `V3.3.2 Single-chain Modular Composer Profile / Experiment Templates`: only template/profile/composer preview/fairness scope metadata for single-chain experiments.
- `V3.3.3 Single-chain Composer Frontend MVP`: only the first readable V3 Composer page.
- `V3.3.4 Composer Layout Polish`: only localization and fixed snake layout polish.
- `V3.3.5a Interactive Composer Draft UI`: only frontend local Draft configuration and local validation.
- `V3.3.5b Backend Draft Validation and Draft Smoke`: only backend authority validation and single Draft Smoke run.
- `V3.3.6 Draft Run Result UX and History`: only result display, local history management, and artifact browsing for Draft Smoke.
- `V3.3.7 Boundary and Skill Closure`: only docs, boundary wording, and skill updates.
- `V3.4.0 Runtime Self-check and Scope Realignment`: only self-check, documentation, and roadmap/skill realignment.
- `V3.4.1 Runtime Plugin Hardening: FIFO TxPool`: only real FIFO TxPool runtime behavior, observability, and frontend alignment.
- `V3.4.2 Runtime Plugin Hardening: BlockProducer`: only BlockProducer hardening after TxPool is observable, plus frontend alignment for new producer artifacts/metrics.
- `V3.4.3 Runtime Plugin Hardening: Consensus-light`: only lightweight truthful consensus model hardening, not PBFT/HotStuff/Raft, plus frontend alignment for consensus-light status/metrics.
- `V3.4.4 Single-module Experiment Templates`: only single-module experiment templates, fairness validation, and frontend alignment for those templates.
- `V3.5 Fabric-backed Validation for MetaTrack`: only Fabric-backed observation, calibration, and small-scale black-box validation after V3.4 runtime hardening. Do not patch Fabric peer internals.
- `V3-final Frontend Integration and Acceptance`: only current-scope frontend integration, acceptance report, artifact browsing, and boundary presentation.

Stage constraints:

```text
V3.0 only docs and skill.
V3.1 only profile layer.
V3.2 only minimal single-chain runtime semantics.
V3.3 only MetaTrack plugins/evaluation and Composer Draft Smoke.
V3.4.0 only runtime self-check and scope realignment.
V3.4.1 only FIFO TxPool hardening and frontend alignment.
V3.4.2 only BlockProducer hardening and frontend alignment.
V3.4.3 only Consensus-light hardening and frontend alignment.
V3.4.4 only single-module experiment templates and frontend alignment.
V3.5 only Fabric-backed validation after V3.4 runtime hardening.
V3-final only frontend integration and acceptance report for current V3 scope.
```

## 4. V3.4.1 Runtime Plugin Hardening: FIFO TxPool Boundary

V3.4.1 allows:

- Implement a real FIFO TxPool runtime object.
- Support `Admit`, `SelectForBlock`, dedup, capacity, and reject/backpressure.
- Make BlockProducer select transactions from TxPool instead of directly slicing transactions.
- Add `txpool_log.csv`.
- Make `queue_wait_ms` derive from TxPool wait statistics instead of being fixed to 0.
- Add or realign summary TxPool metrics.
- Reserve or connect artifact allowlist, Draft Smoke history, and frontend display for `txpool_log.csv`.
- Keep V3 Draft Smoke and the existing MetaTrack four combinations runnable.

V3.4.1 frontend alignment requires:

- `frontend/src/api.ts`: add or remain compatible with `txpool_admitted_count`, `txpool_rejected_count`, `txpool_peak_size`, `txpool_avg_wait_ms`, `txpool_p95_wait_ms`, and `queue_wait_ms`.
- `frontend/src/components/v3/DraftRunResultPanel.tsx`: show average queue wait, peak pool size, admitted count, rejected count, and TxPool artifact availability.
- `frontend/src/components/v3/ArtifactGroups.tsx`: include `txpool_log.csv` under Runtime queue logs, TxPool logs, or Chain runtime logs. Do not label it as Fabric or paper-grade evidence.
- `frontend/src/components/v3/DraftRunHistoryPanel.tsx`: show or link historical runs containing `txpool_log.csv`; treat old runs without it as legacy missing artifacts, not errors.
- `frontend/src/components/v3/ModuleDetailPanel.tsx`: distinguish `fifo_pool` as runtime-realized after V3.4.1 implementation, while `priority_pool`, `hotspot_aware_pool`, and `fee_based_pool` remain planned.
- `frontend/src/components/v3/ModuleCard.tsx`: distinguish configured runnable, runtime-supported, preview-only, and planned.
- `frontend/src/pages/V3ComposerPage.tsx`: preserve or strengthen boundary wording: single-chain, Go Runtime, Smoke experiment, non-Fabric, non-MetaFlow, non-PBFT / HotStuff / Raft, and non-multi-node network. It may add TxPool runtime hardening, FIFO pool only, and local modular runtime.

V3.4.1 frontend non-goals:

- No full real-time dashboard.
- No WebSocket live monitor.
- No drag-and-drop freeform composer.
- No multi-user permission system.
- No formal result database.
- No Fabric live status console.
- No MetaFlow frontend.
- No dual-chain frontend.
- No cross-shard relay / broker / 2PC frontend.
- Do not present Draft Smoke as paper-grade result.

V3.4.1 forbids:

- Real PBFT / HotStuff / Raft implementation.
- Fabric / EVM live backend.
- Fabric Docker / `network.sh` automatic startup.
- MetaFlow.
- Dual-chain runtime.
- Cross-chain bridge.
- Relay / broker / 2PC cross-shard protocol.
- Dynamic resharding.
- Committee lifecycle.
- State migration.
- State root / persistent KV / Merkle proof / snapshot.
- Multi-process / multi-machine networking.
- Describing Draft Smoke as formal paper experiment evidence or real-chain execution.

## 5. Mandatory Start Check

Every V3 round must start with:

```powershell
cd F:\Metaverse_Blockchain_Env
git status --short
```

If `git status --short` is not empty, stop and report unless the user explicitly identifies the existing changes as expected and authorizes continuing on top of them. Do not overwrite user changes.

## 6. No Push Rule

Codex may commit only when explicitly asked and after validation. Codex must not push unless the user explicitly asks for push.

Codex must check `git status --short` before each V3 work round. If the worktree is dirty and the user did not explicitly allow continuing on top of those changes, stop and report.

## 7. Data Truth Rules

V3 must preserve and extend data truth labels:

```text
synthetic_replay
existing_trace_replay
fabric_chain_backed_trace_replay
fabric_live_validation
modular_runtime
modular_runtime_calibrated
public_chain_imported_trace_semantic_unknown
planned_cross_chain_replay
```

Definitions:

- `synthetic_replay`: local generated workload and replay. Not real chain execution.
- `existing_trace_replay`: replay of an existing trace file. No chain is launched.
- `fabric_chain_backed_trace_replay`: replay or analysis of an existing Fabric trace. The web/API layer does not start Fabric.
- `fabric_live_validation`: small-scale validation on a real Fabric network. This is validation, not the main V3 runtime and not a Fabric peer patch.
- `modular_runtime`: result produced by the V3 modular research chain runtime.
- `modular_runtime_calibrated`: V3 modular runtime result using a Fabric calibration profile.
- `public_chain_imported_trace_semantic_unknown`: imported public-chain trace with unknown semantics; no default reliable access-set, delta, or commutativity semantics.
- `planned_cross_chain_replay`: planned cross-chain replay marker; never runnable by itself.

## 8. Backend / Runtime Types

V3 backend/runtime types:

- `local_virtual`: V2 local virtual-time backend. Not real chain execution.
- `trace_replay`: replay backend over existing trace. Does not launch a chain.
- `modular_research_chain`: V3 self-developed modular research chain runtime.
- `fabric_validation`: Fabric-backed validation backend for observation/calibration, not the main experiment kernel.
- `fabric_live_planned`: planned Fabric live backend. Not runnable until its stage explicitly implements it.
- `evm_live_planned`: planned EVM live backend. Not runnable until its stage explicitly implements it.

Backend truth must be displayed in UI, metadata, reports, and artifacts. Planned backend types must not have run buttons or execution paths.

## 9. Fair Baseline Rules

MetaTrack V3.3 comparisons must use identical workload, seed, ChainProfile, hardware profile, block config, consensus config, submit rate, and network profile. Only the following plugin classes may differ:

```text
ShardingPlugin
ExecutionSchedulerPlugin
StateAccessPlugin
CommitPlugin
```

Runtime Hardening / Single-module Test fairness rules:

1. TxPool single-module test:
   Only `TxPoolPlugin` may differ. Workload, BlockProducer, Consensus, Routing, Execution, StateAccess, StateStorage, Commit, Metrics, seed, submit rate, block config, and network profile must be fixed.

2. BlockProducer single-module test:
   Only `BlockProducer` may differ. Workload, TxPool, Consensus, Routing, Execution, StateAccess, StateStorage, Commit, Metrics, seed, submit rate, and network profile must be fixed.

3. Consensus-light single-module test:
   Only `ConsensusPlugin` may differ. Workload, TxPool, BlockProducer, Routing, Execution, StateAccess, StateStorage, Commit, Metrics, seed, submit rate, block config, and network profile must be fixed.

Forbidden fairness shortcuts:

- Do not create advantage by changing workload.
- Do not create advantage by changing seed.
- Do not create advantage by changing submit rate.
- Do not create advantage by changing block config, network profile, or hardware profile.
- Do not create advantage by hiding metrics or artifacts in the frontend.
- Do not present smoke-level results as paper-grade results.

## 10. Artifact Rules

Every V3 modular runtime run after V3.4.1 should include at least:

```text
used_chain_profile.yaml/json
used_plugin_profile.yaml/json
used_experiment_profile.yaml/json
runtime.log
summary.csv/json
report.md
block_log.csv
tx_results.csv
state_commit_log.csv
txpool_log.csv
```

`txpool_log.csv` is a V3.4.1 runtime hardening artifact. It belongs to the local modular runtime / Draft Smoke artifact set. It is not paper-grade final evidence by itself. It must explain TxPool admit, select, reject, queue wait, and pool size changes.

Frontend artifact rules:

- `txpool_log.csv` must be downloadable from Draft Smoke current results and history when present.
- Old Draft Smoke runs that do not contain `txpool_log.csv` must remain viewable.
- Artifact grouping, summary preview, and history detail must align with every new V3.4.x runtime artifact.
- Runtime must not produce a new artifact that the frontend cannot download or explain.

MetaTrack aggregate runs additionally output:

```text
metatrack_summary.csv/json
metatrack_latency.csv
metatrack_mechanism_metrics.csv
```

Fabric validation runs, deferred to V3.5, additionally output:

```text
fabric_validation_summary.csv/json
fabric_tx_results.csv
fabric_commit_latency.csv
fabric_validation_report.md
```

MetaFlow runs remain deferred / future scope and may not become runnable in V3.4.

Do not commit generated artifacts, caches, run directories, large traces, or frontend build output.

Draft Smoke artifacts are temporary local artifacts only:

```text
.cache/v3_draft_runs/<run_id>/
```

They may include `composer_draft.json`, `normalized_draft.json`, `draft_validation.json`, generated experiment/plugin profiles, runtime logs, summaries, transaction/state logs, and, after V3.4.1, `txpool_log.csv`. These files must not be copied into `configs/` and must not be committed to Git.

Built-in smoke vs Draft Smoke:

- built-in smoke runs the existing MetaTrack Go-backed smoke path;
- Draft Smoke runs exactly the current Composer Draft single configuration;
- Draft Smoke must not auto-expand the MetaTrack ablation matrix;
- Draft Smoke must not be described as paper-grade evidence.

## 11. V3.4.1 Frontend Acceptance

After V3.4.1 code implementation:

- Draft Smoke result panel shows TxPool summary metrics.
- Artifact area can download `txpool_log.csv`.
- History can show runs with `txpool_log.csv`; older runs without it do not crash.
- TxPool module detail explains FIFO pool as the hardening target and other TxPool plugins as planned.
- Page still clearly says non-Fabric, non-MetaFlow, non-PBFT, and non-multi-node network.
- `npm.cmd run build` passes.

## 12. Validation Commands

Docs/config-only:

```powershell
git diff --check
git status --short
```

Python backend modified:

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

If validation cannot be completed, do not commit and report the blocker.

## 13. Final Report Format

Every V3 final report must include:

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
12. 是否 push：必须说明未 push
```

## 14. Strict Truthfulness

Do not claim V3 runtime exists before V3.2.
Do not claim MetaTrack V3 experiment exists before V3.3.
Do not claim V3.4.1 implements Fabric validation.
Do not claim Fabric validation exists before V3.5.
Do not claim FIFO TxPool hardening makes MBE a full BlockEmulator-like emulator.
Do not claim TxPool hardening provides real multi-node or real network execution.
Do not claim PBFT / HotStuff / Raft before their actual runtime implementation.
Do not claim `txpool_log.csv` is paper-grade evidence by itself.
Do not claim frontend display of TxPool metrics makes Draft Smoke a formal result database or paper-grade result.
Do not claim MetaFlow exists in current V3.4 scope.
Do not claim production bridge support at any point.

Planning docs may describe future goals, interfaces, artifacts, and stage boundaries. They must not present planned capabilities as implemented or runnable.
