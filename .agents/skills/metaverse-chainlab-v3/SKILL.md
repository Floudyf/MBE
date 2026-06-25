# metaverse-chainlab-v3

## 1. Scope

This skill governs V3 work for MBE.

V3 builds a modular plugin chain runtime with Fabric-backed validation. V3 supports MetaTrack single-chain plugin evaluation and MetaFlow dual-chain protocol evaluation. V3 reuses V2 experiment management, artifacts, sweeps, reports, calibration, and frontend shell.

V3 positioning:

```text
V3 = Modular Plugin Chain Runtime with Fabric-backed Validation
V3 = 面向 MetaTrack + MetaFlow 的模块化插件链实验平台，并带 Fabric 链支持验证。
```

V3 keeps V2 as the experiment organization, replay, sweep, report, calibration, and frontend foundation. V3 adds the modular plugin chain runtime layer that V2 intentionally did not implement.

## 2. V3 Non-goals

V3 is not:

- Not a production blockchain.
- Not a production cross-chain bridge.
- Not a public-chain deployment platform.
- Not a Fabric peer internal patch.
- Not a replacement for Fabric.
- Not a multi-public-chain production system.
- Not allowed to present `local_virtual` or replay results as real-chain results.
- Not allowed to present smoke-level results as final paper evidence.

V3 must not claim production security, production availability, public-chain deployment, or complete Fabric replacement.

## 3. Stage Rules

Only one V3 stage may be implemented per round. Do not jump stages. Do not implement future planned components early.

V3 stages:

- `V3.0 Planning Scaffold`: docs and skill only. Defines stage roadmap, boundaries, profiles, fair baseline policy, and acceptance checklist. No code.
- `V3.1 Profile Layer`: only ChainProfile / PluginProfile / ExperimentProfile schema, loader, validator, preview, and planned/runnable guard.
- `V3.2 Minimal Single-chain Modular Runtime`: only minimal single-chain modular research chain runtime: `NodeRuntime`, `TxPool`, `BlockProducer`, `ConsensusPlugin`, `ExecutionSchedulerPlugin`, `StateAccessPlugin`, `CommitPlugin`, `MetricsCollector`.
- `V3.3 MetaTrack Plugin Evaluation`: only MetaTrack plugin combinations and fair single-chain evaluation on the V3.2 runtime.
- `V3.4 Fabric-backed Validation for MetaTrack`: only Fabric-backed observation, calibration, and small-scale black-box validation. Do not patch Fabric peer internals.
- `V3.5 Minimal Dual-chain Runtime`: only two-chain runtime representation for source lock, target mint, finality wait, pending count, complete/refund paths.
- `V3.6 MetaFlow Protocol Plugin and AFS/FDA`: only MetaFlow protocol plugins, AFS/FDA control, B/D/T parameters, and comparison against baselines.
- `V3-final Frontend Integration and Acceptance`: only frontend integration, acceptance report, artifact browsing, and boundary presentation.

Stage constraints:

```text
V3.0 only docs and skill.
V3.1 only profile layer.
V3.2 only minimal single-chain runtime.
V3.3 only MetaTrack plugins/evaluation.
V3.4 only Fabric-backed validation.
V3.5 only minimal dual-chain runtime.
V3.6 only MetaFlow protocol and AFS/FDA.
V3-final only frontend integration and acceptance report.
```

## 4. Mandatory Start Check

Every V3 round must start with:

```powershell
cd F:\Metaverse_Blockchain_Env
git status --short
git log --oneline -20
```

If `git status --short` is not empty, stop and report. Do not overwrite user changes.

## 5. No Push Rule

Codex may commit after validation. Codex must not push. User manually pushes.

## 6. Data Truth Rules

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

## 7. Backend / Runtime Types

V3 backend/runtime types:

- `local_virtual`: V2 local virtual-time backend. Not real chain execution.
- `trace_replay`: replay backend over existing trace. Does not launch a chain.
- `modular_research_chain`: V3 self-developed modular research chain runtime.
- `fabric_validation`: Fabric-backed validation backend for observation/calibration, not the main experiment kernel.
- `fabric_live_planned`: planned Fabric live backend. Not runnable until its stage explicitly implements it.
- `evm_live_planned`: planned EVM live backend. Not runnable until its stage explicitly implements it.

Backend truth must be displayed in UI, metadata, reports, and artifacts. Planned backend types must not have run buttons or execution paths.

## 8. Fair Baseline Rules

MetaTrack comparisons must use identical workload, seed, ChainProfile, hardware profile, block config, consensus config, submit rate, and calibration profile. Only the following plugin classes may differ:

```text
ShardingPlugin
ExecutionSchedulerPlugin
StateAccessPlugin
CommitPlugin
```

MetaFlow comparisons must use identical source ChainProfile, target ChainProfile, workload arrival sequence, finality profile, timeout baseline, hardware profile, and network profile. Only the following may differ:

```text
CrossChainProtocolPlugin
control policy
B / D / T adaptation logic
```

No experiment may use different traces, chain states, submit rates, seeds, hardware settings, or calibration settings to create artificial advantage.

## 9. Artifact Rules

Every V3 run must output:

```text
used_chain_profile.yaml/json
used_plugin_profile.yaml/json
used_experiment_profile.yaml/json
runtime.log
summary.csv/json
report.md
```

MetaTrack runs additionally output:

```text
metatrack_summary.csv/json
metatrack_latency.csv
metatrack_mechanism_metrics.csv
block_log.csv
tx_results.csv
state_commit_log.csv
```

Fabric validation runs additionally output:

```text
fabric_validation_summary.csv/json
fabric_tx_results.csv
fabric_commit_latency.csv
fabric_validation_report.md
```

MetaFlow runs additionally output:

```text
metaflow_summary.csv/json
metaflow_events.csv
protocol_results.csv
control_decisions.csv
metaflow_vs_baselines_report.md
```

Do not commit generated artifacts, caches, run directories, large traces, or frontend build output.

## 10. Validation Commands

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

## 11. Final Report Format

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

## 12. Strict Truthfulness

Do not claim V3 runtime exists before V3.2.
Do not claim MetaTrack V3 experiment exists before V3.3.
Do not claim Fabric validation exists before V3.4.
Do not claim MetaFlow exists before V3.6.
Do not claim production bridge support at any point.

V3.0 planning docs may describe future goals, interfaces, artifacts, and stage boundaries. They must not present planned capabilities as implemented or runnable.
