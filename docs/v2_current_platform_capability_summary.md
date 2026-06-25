# V2 Current Platform Capability Summary

## 1. 当前平台一句话定位

V2 当前是一个 V3-ready local replay / sweep / calibration 实验平台。

它已经把 V1 single-chain runnable base 扩展为可组织实验、管理 run/artifact、接入多 trace source、校验 cross-chain trace schema、执行 dual-chain virtual-time replay、执行 cross-chain protocol baseline、批量 sweep/report、以及 chain-backed calibration 的本地实验平台。

它还不是：

- 模块化插件链 runtime。
- 真实多节点链系统。
- 生产级跨链桥。
- `FabricLiveBackend` / `EVMLiveBackend`。
- MetaFlow 完整实现。

## 2. 当前阶段总览

### V0

- 阶段目标：闭合默认单链实验链路。
- 已实现能力：FastAPI backend、React frontend、synthetic workload、`trace.jsonl.gz`、Go replay executor、virtual clock、summary/latency/runtime artifacts。
- 主要文件 / 模块：`backend/app/main.py`、`workload/`、`trace/writer/`、`executor/core/`、`scripts/v0_sanity.py`。
- 可运行入口：`POST /api/v0/experiments/v0_default_asset_hotspot/run`、`python scripts/v0_sanity.py`。
- 输出 artifacts：`config.yaml`、`trace.jsonl.gz`、`trace_meta.json`、`summary.csv`、`latency.csv`、`runtime.log`。
- 限制 / 非目标：只覆盖默认 MockChain 单链闭环，不包含 V1/V2/V3 机制。

### V1

- 阶段目标：形成 single-chain runnable experiment platform，为 MetaTrack 单链机制实验提供基础。
- 已实现能力：topology-first 配置思路、enhanced workload/trace、Fabric smoke trace、co-access routing、dual-track execution、hot update aggregation、baseline/sweep/report、V1-final-plus interactive workload / ablation / trace-source selector。
- 主要文件 / 模块：`configs/experiments/v1_*.yaml`、`configs/sweeps/v1_8_baselines.yaml`、`executor/routing/`、`executor/execution_sharding/`、`executor/commit/`、`scripts/v1_8_sweep.py`、`scripts/v1_fabric_smoke.py`。
- 可运行入口：V1 custom run API、V1 sweep/report API、`python scripts/v1_8_sweep.py`、Fabric smoke CLI。
- 输出 artifacts：`summary.csv`、`latency.csv`、`runtime.log`、`report.md`、sweep summary files、V1 run config files where available。
- 限制 / 非目标：不是 formal cross-chain；default tiny trace / smoke runs 不能作为论文最终性能结论。

### V2.0

- 阶段目标：建立 V2 planning scaffold、路线、架构和边界。
- 已实现能力：V2 roadmap、architecture、boundaries、planned dual-chain topology、V2 skill。
- 主要文件 / 模块：`docs/v2_roadmap.md`、`docs/v2_architecture.md`、`docs/v2_boundaries.md`、`configs/topologies/v2_dual_chain_planned.yaml`、`.agents/skills/metaverse-chainlab-v2/SKILL.md`。
- 可运行入口：无。规划文档与 planned config 不可执行。
- 输出 artifacts：无 runtime artifacts。
- 限制 / 非目标：不实现 V2 业务逻辑。

### V2.1

- 阶段目标：Plugin Registry + Composer 2.0。
- 已实现能力：plugin declaration、registry loading、composer preview、config validator、`runnable/planned/experimental/invalid` 分类、data truth labels、planned topology guard。
- 主要文件 / 模块：`configs/plugins/v2_plugin_registry.yaml`、`backend/app/services/plugin_registry.py`、`backend/app/services/config_validator_v2.py`、`backend/app/services/experiment_composer_v2.py`。
- 可运行入口：plugin list API、composer preview API。
- 输出 artifacts：无 experiment runtime artifacts。
- 限制 / 非目标：只做 preview/validation，不运行 dual-chain 或 protocol。

### V2.2

- 阶段目标：Experiment Job Manager + Artifact Manager。
- 已实现能力：`run_id`、job metadata、run history、artifact listing/download、V1 latest compatibility layer。
- 主要文件 / 模块：`backend/app/services/job_manager.py`、`backend/app/services/artifact_manager.py`、`backend/app/services/run_id.py`。
- 可运行入口：`GET /api/v2/runs`、`GET /api/v2/runs/{run_id}`、artifact APIs。
- 输出 artifacts：由各 stage 写入 `.cache/v2_jobs/{run_id}/`。
- 限制 / 非目标：文件系统 job manager，不是 Celery/Redis/distributed queue。

### V2.3

- 阶段目标：Trace Source Expansion。
- 已实现能力：统一 trace source layer、trace source config、existing trace workspace-bound validation、Fabric smoke trace readiness check、public-chain imported trace skeleton with semantic_unknown。
- 主要文件 / 模块：`configs/trace_sources/v2_trace_sources.yaml`、`backend/app/services/trace_source_service.py`、`backend/app/services/trace_source_validator.py`。
- 可运行入口：`GET /api/v2/trace-sources`、`GET /api/v2/trace-sources/{source_id}`、`POST /api/v2/trace-sources/validate`。
- 输出 artifacts：无 experiment runtime artifacts。
- 限制 / 非目标：不连接 public-chain live node，不要求 archive node，不启动 Docker/Fabric。

### V2.4

- 阶段目标：Multi-chain Trace Schema。
- 已实现能力：cross-chain stage-record schema、multi-chain trace meta schema、small sample trace/meta、streaming validator。
- 主要文件 / 模块：`trace/schema/v2_cross_chain_trace.schema.json`、`trace/schema/v2_multi_chain_trace_meta.schema.json`、`trace/samples/`、`trace/validator/cross_chain_trace_validator.py`。
- 可运行入口：validator functions and tests。
- 输出 artifacts：sample files only。
- 限制 / 非目标：schema validation is not replay execution; sample trace is not an experiment result。

### V2.5

- 阶段目标：Dual-chain Replay Engine。
- 已实现能力：`ChainBackend` interface、`LocalVirtualBackend`、`TraceReplayBackend`、planned live backend placeholder、dual-chain virtual-time replay、finality/wait/imbalance metrics、V2.2 job/artifact integration。
- 主要文件 / 模块：`backend/app/services/chain_backend.py`、`backend/app/services/dual_chain_replay.py`、`backend/app/services/dual_chain_metrics.py`、`configs/experiments/v2_dual_chain_sample.yaml`。
- 可运行入口：`GET /api/v2/chain-backends`、`GET /api/v2/dual-chain/sample-config`、`POST /api/v2/dual-chain/replay`、`python scripts/v2_5_dual_chain_replay.py`。
- 输出 artifacts：`dual_chain_summary.csv/json`、`stage_metrics.csv`、`runtime.log`、`report.md`、`used_config.yaml/json`。
- 限制 / 非目标：local virtual-time replay only; not real chain execution; live backends remain planned。

### V2.6

- 阶段目标：Cross-chain Protocol Baselines。
- 已实现能力：`CrossChainProtocol` interface、protocol state/action/event/result data structures、protocol runner、`lock_mint_serial`、`lock_mint_pipeline`、`fixed_window_baseline`、`committee_bridge_basic` local baseline。
- 主要文件 / 模块：`backend/app/services/cross_chain_protocol.py`、`backend/app/services/cross_chain_protocols.py`、`backend/app/services/protocol_replay.py`、`backend/app/services/protocol_metrics.py`、`configs/experiments/v2_cross_chain_protocol_sample.yaml`。
- 可运行入口：`GET /api/v2/cross-chain/protocols`、`GET /api/v2/cross-chain/sample-config`、`POST /api/v2/cross-chain/protocol-replay`、`python scripts/v2_6_cross_chain_protocol_replay.py`。
- 输出 artifacts：`protocol_summary.csv/json`、`protocol_results.csv`、`protocol_events.csv`、`runtime.log`、`report.md`、`used_config.yaml/json`。
- 限制 / 非目标：baseline models only; not production bridge; MetaFlow not implemented; committee bridge is not a real signature/proof system。

### V2.7

- 阶段目标：Multi-chain / Cross-chain UI with Data Truth Labels。
- 已实现能力：V2 dashboard access to plugins/composer, trace sources, chain backends, V2.5 replay, V2.6 protocol replay, run history/artifacts, truth/backend/status badges。
- 主要文件 / 模块：`frontend/src/components/V2Dashboard.tsx`、`frontend/src/api.ts`、`frontend/src/App.tsx`。
- 可运行入口：frontend V2 dashboard。
- 输出 artifacts：uses backend run artifacts。
- 限制 / 非目标：UI integration only; no new replay engine or protocol。

### V2.8

- 阶段目标：V2 Baseline / Sweep / Report。
- 已实现能力：batch sweep configs, deterministic case expansion, V2.5/V2.6 runner reuse, metrics aggregation, Markdown report generation, V2.2 job/artifact integration。
- 主要文件 / 模块：`backend/app/services/sweep_runner_v2.py`、`backend/app/services/sweep_report_v2.py`、`configs/sweeps/v2_*.yaml`、`scripts/v2_8_sweep_report.py`。
- 可运行入口：`GET /api/v2/sweeps`、`GET /api/v2/sweeps/{sweep_id}`、`POST /api/v2/sweeps/run`、CLI script。
- 输出 artifacts：`sweep_summary.csv/json`、`sweep_report.md`、`runtime.log`、`case_artifacts_index.json`、`used_config.yaml/json`。
- 限制 / 非目标：local sweep/report only; not real chain experiments。

### V2.9

- 阶段目标：Realism Bridge / Chain-backed Calibration。
- 已实现能力：calibration configs, Fabric smoke trace status reuse, chain-backed trace adapter, observed-vs-replay comparison, calibration metrics/report, V2.2 job/artifact integration。
- 主要文件 / 模块：`backend/app/services/chain_backed_trace_adapter.py`、`backend/app/services/calibration_runner_v2.py`、`backend/app/services/calibration_report_v2.py`、`configs/calibration/`、`scripts/v2_9_realism_bridge.py`。
- 可运行入口：`GET /api/v2/calibration/configs`、`GET /api/v2/calibration/fabric-smoke/status`、`POST /api/v2/calibration/run`、CLI script。
- 输出 artifacts：`calibration_summary.csv/json`、`replay_vs_observed.csv`、`calibration_report.md`、`runtime.log`、`used_config.yaml/json`。
- 限制 / 非目标：not FabricLiveBackend; web does not start Fabric/Docker/network.sh; Fabric trace must already exist。

### V2-final

- 阶段目标：Frontend Consolidation。
- 已实现能力：platform overview, experiment-type pages, run history/artifact page, boundary page, developer mode, Chinese-first UI, data truth/backend badges。
- 主要文件 / 模块：`frontend/src/App.tsx`、`frontend/src/styles.css`、`docs/v2_final_frontend_consolidation.md`。
- 可运行入口：frontend experiment console。
- 输出 artifacts：uses backend artifacts。
- 限制 / 非目标：frontend expression only; no new mechanisms。

### V2-final acceptance fix

- 阶段目标：审计 V1 metrics / V1 ablation / V2 result-artifact binding and fix acceptance-level UI/contract issues。
- 已实现能力：documented V1 smoke-level findings, frontend formatting and semantics cleanup, V2.5/V2.6 run metadata binding fix, regression tests。
- 主要文件 / 模块：`docs/v2_final_acceptance_findings.md`、`backend/tests/test_v2_final_acceptance_metrics.py`、small metadata/UI binding changes。
- 可运行入口：existing V1/V2 frontend/API flows。
- 输出 artifacts：none beyond existing run artifacts。
- 限制 / 非目标：did not change Go executor or fabricate metric differences。

## 3. 当前平台架构分层

### Frontend Experiment Console

- 已实现什么：V0/V1/V2 pages, Chinese-first experiment flow, data truth/backend/status badges, run history and artifact downloads, developer mode。
- 用于哪些实验：V1 single-chain, V1 ablation, V2.5 dual-chain replay, V2.6 protocol baseline, V2.8 sweep/report, V2.9 calibration。
- 是否 V3 可复用：可复用为 V3 experiment console shell。
- 还缺什么：V3 deployment/live-backend controls, richer report browsing, experiment profile editor, production safety gates。

### FastAPI Backend

- 已实现什么：V0/V1/V2 APIs, run orchestration, config previews, trace validation, replay/sweep/calibration endpoints。
- 用于哪些实验：all current runnable experiments。
- 是否 V3 可复用：可复用 API structure and artifact access pattern。
- 还缺什么：distributed runtime control, live node management, plugin runtime lifecycle, auth/tenancy if needed。

### Plugin Registry / Composer

- 已实现什么：V2 plugin registry, plugin status/reason/capabilities, composer preview, validator classification。
- 用于哪些实验：experiment preview and planned/runnable guard。
- 是否 V3 可复用：可复用为 V3 plugin catalog/control-plane seed。
- 还缺什么：runtime plugin loading, plugin lifecycle, version negotiation, deterministic plugin replacement in a live chain runtime。

### Trace Source Layer

- 已实现什么：synthetic, existing_trace, Fabric chain-backed trace, public-chain imported trace skeleton; workspace-bound validation。
- 用于哪些实验：V1 trace selection, V2 validation, V2.9 calibration。
- 是否 V3 可复用：可复用 as trace ingestion and truth-label gate。
- 还缺什么：live trace collectors, richer semantic decoders, chain-backed workload capture, public-chain import pipeline beyond skeleton。

### Schema / Validator Layer

- 已实现什么：V0/V1 trace schema pieces, V2 cross-chain trace schema/meta schema, streaming validator。
- 用于哪些实验：V2.5/V2.6/V2.8/V2.9 sample and validation flows。
- 是否 V3 可复用：可复用 for V3 trace interchange and report reproducibility。
- 还缺什么：schema for live block logs, tx pool events, consensus events, state access logs, plugin-level telemetry。

### Job / Artifact Manager

- 已实现什么：file-system run_id, metadata, run listing, artifact allowlist/download。
- 用于哪些实验：V1 custom compatibility, V2.5/V2.6/V2.8/V2.9。
- 是否 V3 可复用：可复用 as local artifact manager and report distribution layer。
- 还缺什么：long-running job lifecycle, cancellation, distributed workers, storage retention policy, multi-user access if required。

### Single-chain V1 Runner

- 已实现什么：synthetic/existing/Fabric-trace replay into Go executor, V1 ablation/sweep/report。
- 用于哪些实验：MetaTrack-style single-chain mechanism experiments。
- 是否 V3 可复用：可复用 as single-chain baseline and validation harness。
- 还缺什么：larger workloads, fair chain profiles, chain-backed validation at experiment scale, mechanism-sensitive latency model。

### Dual-chain Replay Layer

- 已实现什么：local virtual-time replay over V2.4 trace schema with chain profiles and finality metrics。
- 用于哪些实验：V2.5, V2.8 dual-chain cases, V2.9 comparison substrate。
- 是否 V3 可复用：可复用 as deterministic reference runner and calibration target。
- 还缺什么：real dual-chain execution, live backend replacement, cross-chain runtime events。

### Cross-chain Protocol Baseline Layer

- 已实现什么：baseline protocol interface and local replay runner for four baselines。
- 用于哪些实验：V2.6, V2.8 protocol sweeps。
- 是否 V3 可复用：interface and result schema can be reused。
- 还缺什么：MetaFlow, real proof/signature systems, Pending/finality window logic, live observed protocol validation。

### Sweep / Report Layer

- 已实现什么：config-driven sweeps, case expansion, stable summary columns, Markdown reports。
- 用于哪些实验：V2.8 local replay/baseline comparisons。
- 是否 V3 可复用：可复用 for batch experiment organization and reporting。
- 还缺什么：chain-backed/live sweep execution, fairness controls, experiment replication policy, richer statistical analysis。

### Calibration / Realism Bridge Layer

- 已实现什么：synthetic calibration sample, existing Fabric smoke trace calibration, observed-vs-replay comparison。
- 用于哪些实验：V2.9 realism bridge。
- 是否 V3 可复用：可复用 for V3 live backend calibration and sanity checks。
- 还缺什么：live observation ingestion, multi-chain chain-backed calibration, calibration parameter fitting beyond simple suggestions。

### Go Executor

- 已实现什么：streaming trace replay, virtual time, V1 routing/dual-track/aggregation counters and metrics。
- 用于哪些实验：V0, V1, V1 sweep/report。
- 是否 V3 可复用：可复用 as single-chain replay executor / baseline oracle。
- 还缺什么：modular research chain runtime, live node behavior, pluggable consensus/sharding/execution/commit pipeline。

## 4. 当前能做哪些实验

### 4.1 单链机制实验 MetaTrack / V1

当前能运行 V1 single-chain replay。它支持：

- `synthetic_replay`
- `existing_trace_replay`
- `fabric_chain_backed_trace_replay`
- workload selector
- ablation preset/custom toggles
- trace source selector

当前 tiny synthetic / smoke run 的 latency 可能过窄。`avg_latency_ms`、`p95_latency_ms`、`p99_latency_ms` 相同不是前端 bug；acceptance fix 已确认这是 source artifact 的结果。默认 smoke-level V1 结果不能作为论文最终性能结论。

输出 artifacts：

- `summary.csv`
- `latency.csv`
- `runtime.log`
- `report.md`
- `used_config.yaml/json` where available

### 4.2 单链消融对比

当前支持：

- `baseline_hash_only`
- `co_access_only`
- `co_access_dual_track`
- `full_v1`

这些配置会传递不同 routing / dual-track / aggregation settings。当前默认小 trace 可能导致 TPS/P99 完全相同，`aggregation_ratio` 可能为 0。这是 smoke-level / validation-level，用来验证配置链路和产物生成，不是最终机制效果实验。

### 4.3 双链回放实验 V2.5

当前支持 `LocalVirtualBackend` / `TraceReplayBackend` and dual-chain virtual-time replay。它支持 chain profile、block interval、finality depth、finality wait、source/target wait、chain speed imbalance 等 metrics。

它不是真实链执行。

输出 artifacts：

- `dual_chain_summary.csv/json`
- `stage_metrics.csv`
- `runtime.log`
- `report.md`
- `used_config.yaml/json`

### 4.4 跨链协议基线 V2.6

当前支持：

- `lock_mint_serial`
- `lock_mint_pipeline`
- `fixed_window_baseline`
- `committee_bridge_basic`

MetaFlow 仍是 planned / not implemented。`committee_bridge_basic` 是本地 baseline model，不是真实委员会桥，不包含真实 signatures、MintCert、RefundCert 或 FinalityProof。

输出 artifacts：

- `protocol_summary.csv/json`
- `protocol_results.csv`
- `protocol_events.csv`
- `runtime.log`
- `report.md`
- `used_config.yaml/json`

### 4.5 批量对比与报告 V2.8

当前支持：

- `v2_baseline_sweep`
- `v2_chain_speed_imbalance_sweep`
- `v2_protocol_baseline_sweep`
- `v2_window_size_sweep`
- `v2_committee_delay_sweep`

它支持 `sweep_summary.csv/json` and `sweep_report.md`。这些是 local sweep/report，不是真实链实验。

### 4.6 真实链轨迹校准 V2.9

当前支持：

- synthetic calibration sample。
- 读取已有 Fabric smoke trace 做 chain-backed calibration。

它不启动 Fabric / Docker / `network.sh`，也不是 `FabricLiveBackend`。Fabric trace 缺失时只返回 manual CLI hint。

输出 artifacts：

- `calibration_summary.csv/json`
- `replay_vs_observed.csv`
- `calibration_report.md`
- `runtime.log`
- `used_config.yaml/json`

## 5. Data Truth Labels 与 Backend Types 当前状态

当前已有 data truth labels：

- `synthetic_replay`
- `existing_trace_replay`
- `fabric_chain_backed_trace_replay`
- `public_chain_imported_trace_semantic_unknown`
- `planned_cross_chain_replay`

当前 backend types：

- `local_virtual`
- `trace_replay`
- `fabric_live_planned`
- `evm_live_planned`

含义必须保持清楚：

- `synthetic_replay` 不是真实链。
- `fabric_chain_backed_trace_replay` 是网页回放已有 Fabric trace，不是网页启动 Fabric。
- `local_virtual` 不是真实链。
- `fabric_live_planned` / `evm_live_planned` 仍未实现。

## 6. 当前平台距离“模块化插件链 runtime”还缺什么

V2 已有一些雏形：

- Plugin Registry / Composer。
- ChainBackend Interface。
- Protocol Interface。
- Job / Artifact Manager。
- Sweep / Report。
- Calibration。
- Frontend Experiment Console。

但当前还没有真正的 modular research chain runtime。缺口包括：

- `ChainProfile` runtime semantics: 当前 profile 主要服务 replay/backend 参数，还不是 live chain runtime 的完整 profile。
- `PluginProfile`: 尚未定义可热替换/可组合的 runtime plugin profile。
- `ExperimentProfile`: 当前已有 configs/sweeps/calibration，但还没有统一的 V3 experiment profile runtime。
- 真实节点进程: 没有 node process lifecycle。
- 交易池 `TxPool`: 没有真实 tx admission、mempool ordering、backpressure。
- 区块生成 `Block Producer`: 没有真实 block production loop。
- 共识插件 `ConsensusPlugin`: 没有插件化共识执行。
- 分片插件 `ShardingPlugin`: 没有真实插件化 state/execution sharding runtime。
- 执行调度插件 `ExecutionSchedulerPlugin`: V1 executor 有机制原型，但不是 V3 runtime plugin。
- 状态访问插件 `StateAccessPlugin`: 没有统一 state store/read-write-set runtime。
- 提交插件 `CommitPlugin`: V1 hot-update aggregation 是 replay prototype，不是 live commit pipeline。
- 跨链协议插件 `CrossChainProtocolPlugin`: V2.6 有 protocol interface，但不是 V3 live protocol plugin runtime。
- 真实状态存储: 没有 durable blockchain state store。
- 真实区块日志: 没有 live block/event log。
- 插件替换机制: 没有 runtime-level plugin swap/load lifecycle。
- 同链环境公平 baseline runner: V2.8 有 local sweep runner，但还不是 live chain fairness harness。

## 7. 当前平台距离“MetaTrack 链支持公平实验”还缺什么

当前 V1 smoke 能验证链路，但还不能证明论文级机制效果。MetaTrack fair experiment 仍需要：

- 更大 workload。
- 可控 hotspot / co-access / aggregation trace。
- 机制敏感 virtual-time latency model。
- 同一 chain profile 下的 baseline 对比。
- 同一 calibration profile 下的 MetaTrack 对比。
- chain-backed workload 采集。
- Fabric real-chain validation。
- MetaTrack mechanism metrics 完整输出。

尤其需要避免把 tiny trace 下相同 TPS/P99 或 zero aggregation ratio 当作最终机制结论。

## 8. 当前平台距离“MetaFlow 双链协议实验”还缺什么

V2.6 只有 protocol baselines。MetaFlow 当前未实现。

MetaFlow 双链协议实验仍缺：

- MetaFlow protocol skeleton。
- `B` / `D` / `T` 参数。
- AFS 控制。
- FDA 控制。
- Pending / finality window。
- `control_decisions.csv`。
- 三种工况实验。
- MetaFlow vs baselines comparison。
- 双链 chain-backed / live-observed validation。

V2.5/V2.6/V2.8/V2.9 提供 substrate、baseline、sweep 和 calibration 资产，但不是 MetaFlow 实现。

## 9. V2 到 V3 可复用资产

- 前端实验中心：可作为 V3 experiment console 的页面骨架，继续区分 V1/V2/V3、run history、artifacts、boundaries、developer mode。
- Job / Artifact Manager：可复用 run_id、metadata、artifact allowlist/download 模式。
- run_id 机制：可复用于 V3 long-running experiments 的 identity layer。
- artifact 下载 API：可复用为 V3 reports/logs/configs 下载入口。
- Data Truth Labels：可复用为 V3 live/synthetic/chain-backed/public-chain 结果标注规则。
- Trace Source Layer：可复用为 V3 trace ingestion and validation gate。
- Cross-chain Trace Schema：可复用为 V3 cross-chain event interchange format seed。
- Dual-chain Replay Metrics：可复用为 V3 live backend comparison and calibration metrics。
- Protocol Baseline Interface：可复用为 future `CrossChainProtocolPlugin` boundary seed。
- Sweep / Report Runner：可复用为 V3 batch experiment/report framework。
- Calibration Runner：可复用为 V3 live backend calibration and replay-vs-observed comparison。
- V2 Docs / Boundaries：可复用为 V3 planning safety and truth-label requirements。

## 10. V2 到 V3 不应复用或不能直接复用的地方

- tiny V1 smoke latency: 不能作为性能结论或 V3 calibration baseline。
- 默认 V1 ablation 结果: 只能验证链路，不能直接作为 MetaTrack fairness evidence。
- `local_virtual` 结果: 不能冒充真实链。
- planned backend: 不能当成 runnable。
- old V2Dashboard developer details: 可以保留 developer mode，但不能作为最终实验用户体验。
- `committee_bridge_basic`: 只能作为 local baseline model，不能当 production bridge。
- public-chain imported trace skeleton: 不能默认携带 reliable access-set / delta / commutative semantics。

## 11. 建议给后续人工分析的问题清单

进入 V3 前建议人工决定：

- V3 是否自研 modular research chain runtime？
- V3 的 `ChainProfile` 应包含哪些属性？
- V3 插件边界如何划分？
- MetaTrack 主实验是否采用 modular runtime + Fabric validation？
- MetaFlow 是否作为 `CrossChainProtocolPlugin`？
- Fabric 在 V3 中是主实验内核还是 validation backend？
- 哪些实验必须是真实链验证？
- 哪些实验可以是 prototype evaluation？
- V3 是否需要 multi-server deployment first，还是 single-machine multi-node runtime first？
- V3 live backend 的安全闸门如何防止网页误启动 Docker/Fabric/public-chain clients？
- V3 报告如何区分 synthetic replay、chain-backed calibration、live backend observation 和 production-like deployment？
