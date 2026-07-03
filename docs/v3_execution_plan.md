# V3 Execution Plan

## V3.4.11 Current Status

V3.4.11 closure is complete. V3.4.9 introduced MetaTrack ablation templates, V3.4.10 introduced the controlled smoke runner, and V3.4.11 aligned stage/version/frontend/docs/skill wording and validation.

This status does not claim paper-grade benchmark evidence, real chain execution, Fabric/EVM live backend, BlockEmulator backend, real multi-node networking, real PBFT/HotStuff/Raft, real cross-shard protocol, or real proof/witness/MPT/state root.

## V3.5.4 Current Status

Current stage is V3.5.4 V3.5 Closure. V3.5.1 added frontend topology config, backend topology validation, single-process logical node generation, node/network/consensus message artifacts, and node topology summary metrics. V3.5.2 added launcher preview artifacts from topology. V3.5.3 added a local node process preview entry point that can load topology, identify a node by `node_id`, validate role/shard, and write node-local status/log artifacts. V3.5.4 closes README/docs/skill/frontend/backend stage wording and validation. It is not real TCP, not a real multi-process network runtime, not real PBFT, not HotStuff/Raft, not Fabric/EVM live backend, not BlockEmulator backend, not a real cross-shard protocol, and not a paper-grade benchmark.

V3.5 route:

- V3.5.1 Logical Node Topology Runtime.
- V3.5.2 Local Multi-process Launcher Preview.
- V3.5.3 Local Node Process Runtime.
- V3.5.4 V3.5 Closure. Complete.

V3.5 node topology and local launcher foundations are closed. Fabric-backed validation is deferred to a later stage unless explicitly reopened. The next major stage is V3.6 TCP Adapter and Consensus Hardening.

## V3.6.1 Current Status

V3.6.1 is complete. V3.6.1 adds a configurable NetworkAdapter concept with `in_memory_message_bus` compatibility and `localhost_tcp_preview` typed message preview. It writes `tcp_adapter_status.csv`, `network_send_log.csv`, `network_receive_log.csv`, and `typed_message_log.csv`, and adds summary metrics for adapter selection, TCP preview status, listen/send/receive counts, typed message count, and network error count.

V3.6.1 does not implement real PBFT, HotStuff/Raft, BlockEmulator-aligned PBFT, real cross-shard protocol, Fabric/EVM live backend, or paper-grade benchmark evidence.

## V3.6.2 Current Status

Current stage is V3.6.2 V3.6 Closure. V3.6.2 connects consensus-light to the selected NetworkAdapter typed message path. With `in_memory_message_bus`, it records proposal/vote typed messages on the in-memory path. With `localhost_tcp_preview`, it records the same consensus-light proposal/vote preview over the localhost TCP typed message preview path. It writes `consensus_network_light_log.csv` and `network_consensus_summary.json`, appends `proposal_preview` and `vote_preview` to typed message/send/receive logs, and adds summary metrics for `consensus_over_network_enabled`, `consensus_runtime_selected`, `proposal_preview_count`, `vote_preview_count`, `light_quorum_reached_count`, `consensus_network_error_count`, and `consensus_network_path`.

V3.6 is closed after V3.6.2. This closure does not implement PBFT PrePrepare/Prepare/Commit, BlockEmulator-aligned PBFT, HotStuff/Raft, real cross-shard protocol, Fabric/EVM live backend, production networking, or paper-grade benchmark evidence.

## V3.7.1 Current Status

Current stage is V3.7.1. V3.7.1 introduces a configurable `ConsensusRuntimePlugin` concept while keeping consensus selectable. `simple_leader`, `poa_light`, and `pbft_light_model` remain lightweight/model-based paths. `blockemulator_aligned_pbft_preview` is added as one optional runtime that writes deterministic PBFT state machine preview artifacts.

V3.7.1 writes `pbft_state_log.csv`, `pbft_message_log.csv`, `quorum_log.csv`, and `finalized_block_log.csv`. It adds summary metrics for `consensus_runtime_selected`, `pbft_view`, `pbft_sequence`, `pbft_preprepare_count`, `pbft_prepare_count`, `pbft_commit_count`, `pbft_quorum_reached_count`, `pbft_finalized_block_count`, `pbft_consensus_latency_ms`, `pbft_preview_enabled`, and `pbft_quorum_threshold`.

V3.7.1 is not production PBFT, not full PBFT over localhost TCP, not full Byzantine safety, not view-change hardening, not stable checkpointing, not signature/verification hardening, not HotStuff/Raft, not a real cross-shard protocol, not Fabric/EVM live backend, not a BlockEmulator backend, and not paper-grade benchmark evidence. It is followed by V3.7.2 BlockEmulator-aligned PBFT over NetworkAdapter + V3.7 Closure.

## V3.7.2 Current Status

V3.7.2 is complete as V3.7 Closure. It connects the optional `blockemulator_aligned_pbft_preview` runtime to the selected V3.6 `NetworkAdapter` typed message path. With `in_memory_message_bus`, PBFT preview messages are logged on the deterministic in-memory typed message path. With `localhost_tcp_preview`, PBFT preview messages are logged on the localhost TCP typed message preview path.

V3.7.2 writes `consensus_network_log.csv` and `pbft_network_summary.json`, while preserving `pbft_state_log.csv`, `pbft_message_log.csv`, `quorum_log.csv`, `finalized_block_log.csv`, `typed_message_log.csv`, `network_send_log.csv`, and `network_receive_log.csv`. It adds summary metrics for `pbft_over_network_enabled`, `pbft_network_path`, `pbft_network_message_count`, `pbft_network_error_count`, `pbft_preprepare_network_count`, `pbft_prepare_network_count`, `pbft_commit_network_count`, `pbft_finalized_network_count`, and `pbft_network_quorum_reached_count`.

V3.7 is closed after V3.7.2. This closure does not implement production PBFT, full Byzantine safety, full view-change hardening, stable checkpointing, signature/verification hardening, HotStuff/Raft, real cross-shard protocol, Fabric/EVM live backend, BlockEmulator backend, or paper-grade benchmark evidence.

## V3.8 Current Status

Current stage is V3.8 CrossShardProtocol Skeleton Closure. V3.8 adds a configurable `cross_shard_protocol` entry under Routing/Sharding, supports runnable `none` and `relay_preview`, keeps `broker_preview` and `two_phase_commit_preview` as planned-only options, detects cross-shard transactions from Routing/Sharding preview records, emits `cross_shard_relay` skeleton messages for relay preview, and writes cross-shard artifacts.

V3.8 writes `cross_shard_tx_log.csv`, `cross_shard_message_log.csv`, `relay_preview_log.csv`, `cross_shard_status.csv`, and `cross_shard_summary.json`. It adds summary metrics for `cross_shard_protocol_selected`, `cross_shard_tx_count`, `cross_shard_ratio`, `cross_shard_message_count`, `relay_preview_count`, `cross_shard_completed_count`, `cross_shard_failed_count`, and `cross_shard_avg_latency_ms`.

V3.8 is closed after this skeleton. It does not implement complete Relay, Broker, 2PC, atomic cross-shard commit, cross-shard locking, rollback, timeout recovery, cross-shard state proof, Merkle proof/witness, Fabric/EVM live backend, BlockEmulator full cross-shard mechanism, production sharding, or paper-grade benchmark evidence.

## V3.8 CrossShardProtocol Skeleton and Closure

CrossShardProtocol belongs under Routing/Sharding as a sub-capability:

```text
routing_policy / shard_mapping / cross_shard_detection / cross_shard_protocol
```

The main transaction flow must remain:

```text
Workload -> TxPool -> BlockProducer -> ConsensusRuntime -> CommitteeEpoch -> Routing/Sharding -> Execution -> StateAccess -> StateStorage -> Commit -> MetricsReport
```

CrossShardProtocol must not become a new main-flow card, and V3.8 must not refactor the V3 Composer page or left navigation.

## V3.9 Planning Entry

V3.9 was originally planned as StateStorage / StateProof Hardening. It is now closed as V3.9 State Authenticity Layer MVP Closure, with the scope and truth boundary recorded below.

## V3.9 Current Status

Current stage is V3.9 State Authenticity Layer MVP Closure. V3.9 adds selectable `state_backend` support for `memory_kv`, runnable `persistent_kv`, runnable `merkle_trie_mvp`, and planned-only `ethereum_mpt_compatible`. It writes deterministic state roots, generates and verifies state proofs, produces stateless witness artifacts, and records state authenticity summary metrics.

V3.9 writes `state_storage_log.csv`, `state_version_log.csv`, `state_root_log.csv`, `state_proof_log.csv`, `state_proof_verification_log.csv`, `witness_log.csv`, `witness_verification_log.csv`, and `state_authenticity_summary.json`. It adds summary metrics for `state_backend_selected`, `persistent_state_enabled`, `state_root_enabled`, `state_root_count`, `state_key_count`, `state_update_count`, `state_proof_generated_count`, `state_proof_verified_count`, `state_proof_failed_count`, `witness_generated_count`, `witness_verified_count`, `witness_failed_count`, and `state_authenticity_error_count`.

V3.9 is closed after this MVP. It does not implement Ethereum-compatible MPT, production database durability, full stateless execution, full stateless blockchain, complete cross-shard state proof protocol, fraud proof / validity proof, atomic cross-shard verified commit, Fabric/EVM live backend, BlockEmulator backend, or paper-grade benchmark evidence.

## V3.9 State Authenticity Layer MVP and Closure

StateProof and Witness belong under StateAccess / StateStorage / Commit as sub-capabilities. They must not become new main-flow cards.

The main transaction flow must remain:

```text
Workload -> TxPool -> BlockProducer -> ConsensusRuntime -> CommitteeEpoch -> Routing/Sharding -> Execution -> StateAccess -> StateStorage -> Commit -> MetricsReport
```

## V3.10 Current Status

Current stage is V3.10 Benchmark / Experiment Template Hardening Closure. V3.10 adds a benchmark template catalog, baseline profile catalog, local controlled sweep runner MVP, multi-seed repeatability, reproducibility manifest, benchmark run index, sweep summaries, baseline comparison output, and benchmark report artifacts.

V3.10 writes `benchmark_template_catalog.json`, `baseline_profile_catalog.json`, `benchmark_plan.json`, `benchmark_run_index.csv`, `sweep_matrix.csv`, `sweep_summary.csv`, `sweep_summary.json`, `aggregate_summary.csv`, `baseline_comparison.csv`, `reproducibility_manifest.json`, `benchmark_report.md`, and `benchmark_summary.json`. It adds summary metrics for `benchmark_template_selected`, `baseline_profile_selected`, `benchmark_run_count`, `sweep_parameter_count`, `repeat_count`, `benchmark_artifact_count`, `baseline_comparison_count`, `reproducibility_manifest_available`, `benchmark_report_available`, and `paper_grade_benchmark`.

V3.10 is closed after this hardening round. It does not implement complete Relay / Broker / 2PC, production PBFT / HotStuff / Raft, Ethereum-compatible MPT, Fabric/EVM live backend, BlockEmulator backend, real large-scale distributed benchmark, performance superiority over BlockEmulator, or paper-grade benchmark evidence.

## V3.10 Benchmark / Experiment Template Hardening and Closure

Benchmark templates, baselines, sweeps, and reproducibility manifest belong to the experiment control layer / result layer and must not become new main-flow cards.

The main transaction flow must remain:

```text
Workload -> TxPool -> BlockProducer -> ConsensusRuntime -> CommitteeEpoch -> Routing/Sharding -> Execution -> StateAccess -> StateStorage -> Commit -> MetricsReport
```

## V3.10.1 Current Status

Current stage is V3.10.1 Frontend UX and Chinese Console Cleanup Closure. V3.10.1 follows V3.10 benchmark hardening and reorganizes the frontend into a Chinese V3 experiment console with simplified navigation, progressive HelpTip explanations, run progress feedback, and lightweight result chart preview.

V3.10.1 is an interface cleanup stage. It does not add new chain/runtime protocol semantics, does not modify Go runtime behavior, does not change V3.10 benchmark truth, and does not start V3.11.

## V3.10.1 Frontend UX and Chinese Console Cleanup

Frontend navigation, HelpTip, chart preview, Chinese labels, and visual cleanup belong to the UI presentation layer. They must not become new runtime modules.

The main transaction flow remains:

```text
Workload -> TxPool -> BlockProducer -> ConsensusRuntime -> CommitteeEpoch -> Routing/Sharding -> Execution -> StateAccess -> StateStorage -> Commit -> MetricsReport
```

V3.10.1 keeps Benchmark in the experiment control / result layer, CrossShardProtocol under Routing/Sharding, and StateProof / Witness under StateAccess / StateStorage / Commit. It does not add new main-flow cards.

## V3.11 Planned

V3.11 is planned as CrossShard Protocol Hardening. It has not started. V3.11 should not be entered unless explicitly requested. V3.10.1 frontend cleanup does not change runtime semantics, and V3.10 benchmark artifacts must not be treated as paper-grade evidence.

## V3.6 / V3.7 Planning

V3.6 is NetworkAdapter and TCP Typed Message Runtime. V3.6.1 starts with a configurable `NetworkAdapter`, supports `in_memory_message_bus` and `localhost_tcp_preview`, adds typed `MessageEnvelope` logs, and keeps TCP as preview only. V3.6.2 adds Consensus-light over NetworkAdapter and closes V3.6. V3.6 does not implement real PBFT, HotStuff/Raft, real cross-shard protocol, Fabric/EVM live backend, or paper-grade benchmark claims.

V3.7 is ConsensusRuntime and BlockEmulator-aligned PBFT Preview. V3.7.1 is implemented as configurable ConsensusRuntime with optional PBFT state machine preview artifacts. V3.7.2 is implemented as PBFT preview over NetworkAdapter plus V3.7 closure. V3.7 does not hardcode PBFT as the only consensus, does not copy BlockEmulator code, and does not claim production PBFT.

V3.8 is implemented as CrossShardProtocol Skeleton Closure. It stays separate from V3.6 networking and V3.7 PBFT preview work.
V3.9 is implemented as State Authenticity Layer MVP Closure. It strengthens StateAccess / StateStorage / Commit with persistent state backend MVP, Merkle/MPT-like roots, proof verification, and witness artifacts.
V3.10 is implemented as Benchmark / Experiment Template Hardening Closure. V3.10.1 is implemented as Frontend UX and Chinese Console Cleanup Closure. V3.11 is planned as CrossShard Protocol Hardening.

Planned stage list extension:

- V3.6 NetworkAdapter and TCP Typed Message Runtime.
- V3.7 ConsensusRuntime and BlockEmulator-aligned PBFT Preview.
- V3.8 CrossShardProtocol Skeleton Closure.
- V3.9 State Authenticity Layer MVP Closure.
- V3.10 Benchmark / Experiment Template Hardening Closure.
- V3.10.1 Frontend UX and Chinese Console Cleanup Closure.
- V3.11 CrossShard Protocol Hardening.

The main transaction flow should remain:

```text
Workload -> TxPool -> BlockProducer -> ConsensusRuntime -> CommitteeEpoch -> Routing/Sharding -> Execution -> StateAccess -> StateStorage -> Commit -> MetricsReport
```

RuntimeTopology / NodeProcessRuntime / NetworkAdapter belong to the runtime support layer and should not be inserted into the main transaction flow. CrossShardProtocol belongs under Routing/Sharding as a sub-capability and must not become a new main-flow card. StateProof and Witness belong under StateAccess / StateStorage / Commit as sub-capabilities and must not become new main-flow cards. Benchmark templates, baselines, sweeps, and reproducibility manifest belong to the experiment control layer / result layer and must not become new main-flow cards. Frontend navigation, HelpTip, chart preview, and Chinese labels belong to the UI presentation layer and must not become runtime modules.

## 0.1 V3.3 Go-backed MetaTrack Update

V3.3 absorbs the earlier V3.2b / V3.2.5 Go-backed minimal runtime parity stage.

- Gate A: Go-backed minimal runtime parity with the V3.2 Python reference runtime.
- Gate B: Go-backed MetaTrack plugin combinations and fair ablation.
- Gate C: single-chain Composer Draft loop with frontend Draft, backend `validate-draft`, backend `run-draft-smoke`, result display, local history, and artifact downloads.
- V3.3 smoke / controlled results are not final paper-scale performance evidence.
- V3.3 does not implement Fabric validation, frontend final acceptance, dual-chain runtime, MetaFlow, AFS, or FDA.

## 0.2 V3.3.1 Research-chain Role Separation Update

V3.3.1 is a platform abstraction correction stage after the Go-backed MetaTrack smoke path. It explicitly separates `ConsensusDomain`, committee / epoch placeholders, `ExecutionShard`, `StateStorageUnit`, `StatePlacement phi(key) -> state_storage_unit_id`, `ExecutionRouting M_t(tx/key) -> execution_shard_id`, and `RemoteStateAccess`.

The implementation remains lightweight: single-process logical runtime, fixed `consensus_0`, `simple_leader`, disabled/planned committee and epoch lifecycle, logical execution shards, and logical memory-backed state storage units.

V3.3.1 is not Fabric validation, not frontend final acceptance, not MetaFlow, not dual-chain or cross-chain runtime, not AFS/FDA, not PBFT/HotStuff, not a real multi-machine network, and not state migration. MetaTrack co-access routing changes execution-side routing only; it does not migrate persistent state placement.

## 0.3 V3.3.2 Single-chain Modular Composer Update

V3.3.2 adds the single-chain ExperimentTemplate and ComposerPreview metadata layer. It introduces `ExperimentTemplate`, `ModuleGraph`, `ModuleStatus`, `PluginMatrix`, `VariableModuleScope`, `ComposerPreview`, and `FairnessScope`.

The single-chain module graph is:

```text
Workload -> TxPool -> BlockProducer -> Consensus -> CommitteeEpoch -> Routing -> Execution -> StateAccess -> StateStorage -> Commit -> MetricsReport
```

Module status is limited to `fixed`, `variable`, `disabled`, `planned`, and `output`. V3.3.2 does not implement frontend UI, Fabric validation, MetaFlow, dual-chain runtime, PBFT/HotStuff, runnable committee lifecycle, runnable dynamic resharding, or runnable state migration.

## 0.4 V3.4 Runtime Hardening Realignment

The V3.4 runtime self-check in `docs/v3_4_runtime_self_check.md` found that V3.3 can run single-chain Composer Draft Smoke, but several foundation modules are still local logical, partially-runnable, or config-only:

- TxPool is config-only / catalog-only; `AdmitTimeMS = SubmitTimeMS`.
- `queue_wait_ms` is fixed to 0.
- There is no `txpool_log.csv`.
- BlockProducer is `cutBlocks(txs, chain)` logical slicing, not selection from TxPool.
- Consensus is the local `simple_leader` model; PBFT / HotStuff / Raft are planned only.
- StateStorage is memory map / memory_kv style with no state root, persistent KV, or snapshot.
- There is no real multi-process, multi-node, or network communication.

Therefore V3.4 no longer immediately enters Fabric-backed validation. V3.4 is now a runtime hardening and closure series. After V3.4.11 closure, the next stage is V3.5 node-level emulator skeleton; Fabric/EVM live backend work remains future scope unless explicitly reopened.

## 0. Current Scope Realignment

Current V3 acceptance is now MetaTrack-oriented single-chain modular runtime + V3.4 runtime hardening/controlled smoke closure + V3.5 node-level emulator skeleton + frontend acceptance:

- V3.0 Planning Scaffold: complete.
- V3.1 Profile Layer: complete.
- V3.2 Minimal Single-chain Modular Runtime.
- V3.2b / V3.2.5 Go-backed Minimal Runtime / Go parity: absorbed into V3.3.
- V3.3 MetaTrack Plugin Evaluation.
- V3.3.1 Research-chain Role Separation.
- V3.3.2 Single-chain Modular Composer Profile / Experiment Templates.
- V3.3.3 Single-chain Composer Frontend MVP.
- V3.3.4 Composer Chinese Localization and Snake Layout Polish.
- V3.3.5a Interactive Single-chain Composer Draft UI.
- V3.3.5b Backend Draft Validation and Draft Smoke Run.
- V3.3.6 Draft Run Result UX and History Management.
- V3.3.7 Boundary, Documentation, and Skill Closure.
- V3.4.0 Runtime Self-check and Scope Realignment.
- V3.4.1 Runtime Plugin Hardening: FIFO TxPool.
- V3.4.2 Runtime Plugin Hardening: BlockProducer.
- V3.4.3 Runtime Plugin Hardening: Consensus-light.
- V3.4.4 Single-module Experiment Templates.
- V3.4.9 MetaTrack Ablation Templates.
- V3.4.10 Controlled Smoke Runner.
- V3.4.11 Stage / Version / Frontend / Docs Closure.
- V3.5 Node-level Emulator Skeleton.
- V3.6 NetworkAdapter and TCP Typed Message Runtime.
- V3.7 ConsensusRuntime and BlockEmulator-aligned PBFT Preview.
- V3.8 CrossShardProtocol Skeleton Closure.
- V3.9 State Authenticity Layer MVP Closure.
- V3.10 Benchmark / Experiment Template Hardening.
- V3-final Frontend Integration and Acceptance.

Deferred / future work:

- Minimal dual-chain runtime.
- MetaFlow Protocol Plugin and AFS/FDA.
- Multi-process / multi-machine network emulator behavior.

Current V3-final does not require dual-chain runtime or MetaFlow. Existing MetaFlow preview profiles remain planned / not runnable.

Every V3.4.x runtime hardening substage must include corresponding frontend alignment. When runtime adds an artifact, summary metric, or module truth boundary, the frontend artifact grouping, result summary, history detail, and module detail must be aligned in the same implementation stage. Runtime must not output a new artifact that the frontend cannot download or explain.

## 1. V3 背景与动机

V2 已经形成 V3-ready local replay / sweep / calibration 实验平台。它能组织实验、管理 run/artifact、执行 V1 single-chain replay、执行 V2.5 dual-chain replay、执行 V2.6 protocol baseline、生成 V2.8 sweep/report，并通过 V2.9 做 chain-backed calibration。

V3 的动机不是继续扩展本地 replay，而是在 V2 实验组织层之上建立自研的模块化研究链 runtime，让 MetaTrack 与 baseline 在同一条研究链环境中公平替换插件。V3.4 先把 runtime 基础模块做成可观测行为并完成 controlled smoke closure；V3.5 enters node topology and local launcher foundations. Fabric-backed validation is deferred to a later stage unless explicitly reopened.

## 2. V2 当前能力基础

V2 可复用资产包括：

- Frontend Experiment Console。
- FastAPI Backend。
- Plugin Registry / Composer。
- Trace Source Layer。
- Schema / Validator Layer。
- Job / Artifact Manager。
- Single-chain V1 Runner。
- Dual-chain Replay Layer。
- Cross-chain Protocol Baseline Layer。
- Sweep / Report Layer。
- Calibration / Realism Bridge Layer。
- Go Executor。
- Data Truth Labels。
- Backend Types。

V2 仍缺少：

- ChainProfile / PluginProfile / ExperimentProfile runtime semantics。
- 真实节点进程。
- TxPool。
- Block Producer。
- ConsensusPlugin。
- ShardingPlugin。
- ExecutionSchedulerPlugin。
- StateAccessPlugin。
- CommitPlugin。
- CrossChainProtocolPlugin。
- 真实状态存储。
- 真实区块日志。
- 插件替换机制。
- 同链环境公平 baseline runner。
- Fabric real-chain validation pipeline。

## 3. V3 总体定位

```text
V3 = Modular Plugin Chain Runtime with Fabric-backed Validation
V3 = 面向 MetaTrack 的模块化插件链实验平台，并带 Fabric 链支持验证。
```

Fabric-backed validation is a validation layer, not the goal of V3.4.1 runtime hardening. V3.4 runtime hardening is a prerequisite for Fabric validation. After V3.4.1, the platform is still not a production chain, not a multi-node network emulator, and not Fabric live execution.

V3 不是：

- 纯 replay 平台。
- 纯 simulation 平台。
- Fabric 内核修改版。
- 生产级联盟链。
- 生产级跨链桥。
- 多公链跨链平台。
- 公网链部署平台。
- 多进程 / 多机器网络 emulator 的完成形态。

V3 的主实验内核是 `modular research chain runtime`。Fabric 的定位是 validation backend，不是主实验内核。

## 4. V3 与 V2 的平滑过渡关系

V3 不推倒 V2。V3 保留 V2 的实验组织层，并新增模块化插件链 runtime。

过渡逻辑：

```text
V2.1 Plugin Registry / Composer
  -> V3 PluginProfile and plugin catalog

V2.2 Job / Artifact Manager
  -> V3 run_id, artifact, report, and history foundation

V2.3 Trace Source Layer
  -> V3 chain-backed trace ingestion and validation gate

V2.5 ChainBackend / dual-chain replay
  -> V3 ChainProfile and runtime boundary reference

V2.8 Sweep / Report
  -> V3 fair baseline runner and report generator

V2.9 Calibration / Realism Bridge
  -> future chain-backed validation / calibration after node-level emulator foundations are explicit

V2-final Frontend Console
  -> V3 experiment console shell
```

## 5. V3 总体架构

```text
Frontend Experiment Console
        |
FastAPI Control Plane
        |
V3 Profile Layer
        |
Modular Research Chain Runtime
        |
Plugin Layer
  - TxPoolPlugin
  - BlockProducer
  - ConsensusPlugin
  - ShardingPlugin
  - ExecutionSchedulerPlugin
  - StateAccessPlugin
  - CommitPlugin
        |
Metrics / Artifact / Report Layer
        |
Future Fabric-backed Validation Layer (deferred; not part of V3.5 node topology / launcher work)
```

V3 is designed to fairly replace module plugins inside one research-chain environment. Fabric is for observation, calibration, and small-scale validation after runtime hardening.

## 6. V3 阶段路线

### V3.0 Planning Scaffold

目标：建立 V3 skill、docs、边界、阶段路线、profile 定义、公平 baseline 政策。只写文档，不实现代码。

### V3.1 Profile Layer

目标：定义 `ChainProfile`、`PluginProfile`、`ExperimentProfile` 的 schema / loader / validator / preview。

### V3.2 Minimal Single-chain Modular Runtime

目标：实现最小单链模块化研究链 runtime semantics：

- `NodeRuntime`
- `TxPool`
- `BlockProducer`
- `ConsensusPlugin`
- `ExecutionSchedulerPlugin`
- `StateAccessPlugin`
- `CommitPlugin`
- `MetricsCollector`

第一版只要求 single-machine multi-node 或 single-process logical multi-node，不要求多服务器。

### V3.3 MetaTrack Plugin Evaluation

目标：在 Go-backed modular runtime 上实现 MetaTrack 所需插件组合：

- `hash_sharding`
- `co_access_sharding`
- `serial_execution`
- `dual_track_execution`
- `direct_fetch`
- `access_list_prefetch`
- `normal_commit`
- `hot_update_aggregation_commit`

运行：

- `baseline_hash_only`
- `co_access_only`
- `co_access_dual_track`
- `full_MetaTrack`

所有 baseline 与 full_MetaTrack 必须在同一 workload、同一 seed、同一 ChainProfile、同一 block config、同一 consensus config 下运行，只允许替换 `ShardingPlugin`、`ExecutionSchedulerPlugin`、`StateAccessPlugin`、`CommitPlugin`。

### V3.4.0 Runtime Self-check and Scope Realignment

目标：完成 runtime self-check，确认当前模块真实状态，把路线从直接 Fabric-backed validation 调整为 V3.4 runtime hardening series，并把 Fabric validation 延后到未来阶段，除非后续明确重新打开。

产物：

- `docs/v3_4_runtime_self_check.md`
- 更新后的 skill / execution plan。

### V3.4.1 Runtime Plugin Hardening: FIFO TxPool

目标：

- 把 TxPool 从 config-only / catalog-only 推进为可观测 runtime object。
- 让交易先进入 TxPool，再由 BlockProducer 选择出块。
- 输出 `txpool_log.csv`。
- 让 `queue_wait_ms` 来源于真实等待时间统计。

后续实现阶段允许修改的业务代码范围建议限制在：

- `executor/v3runtime/runtime.go`
- `executor/v3runtime/runtime_test.go`
- `backend/app/services/artifact_manager.py`
- `backend/app/services/v3_draft_run_history.py`
- `frontend/src/api.ts`
- `frontend/src/components/v3/ArtifactGroups.tsx`
- `frontend/src/components/v3/DraftRunResultPanel.tsx`
- `frontend/src/components/v3/DraftRunHistoryPanel.tsx`
- `frontend/src/components/v3/ModuleDetailPanel.tsx`
- `frontend/src/components/v3/ModuleCard.tsx`
- `frontend/src/pages/V3ComposerPage.tsx`

本阶段非目标：

- PBFT / HotStuff / Raft。
- Fabric / EVM live backend。
- MetaFlow。
- dual-chain runtime。
- cross-chain bridge。
- cross-shard relay / broker / 2PC。
- dynamic resharding。
- committee lifecycle。
- state root / persistent KV。
- multi-process / network。

Frontend alignment:

- `frontend/src/api.ts` must add or remain compatible with TxPool summary fields: `txpool_admitted_count`, `txpool_rejected_count`, `txpool_peak_size`, `txpool_avg_wait_ms`, `txpool_p95_wait_ms`, and `queue_wait_ms`.
- `DraftRunResultPanel.tsx` must display TxPool metrics, including average queue wait, peak pool size, admitted count, rejected count, and TxPool artifact availability.
- `ArtifactGroups.tsx` must include `txpool_log.csv` under Runtime queue logs, TxPool logs, or Chain runtime logs, and must not label it as Fabric or paper-grade evidence.
- `DraftRunHistoryPanel.tsx` must display or link historical runs containing `txpool_log.csv`; older runs without it must be treated as legacy missing artifact, not an error.
- `ModuleDetailPanel.tsx` must distinguish `fifo_pool` as runtime-realized after V3.4.1 implementation, while `priority_pool`, `hotspot_aware_pool`, and `fee_based_pool` remain planned.
- `ModuleCard.tsx` should distinguish configured runnable, runtime-supported, preview-only, and planned status so a selector option is not mistaken for runtime behavior.
- `V3ComposerPage.tsx` must preserve or strengthen boundary wording: single-chain, Go Runtime, Smoke experiment, non-Fabric, non-MetaFlow, non-PBFT / HotStuff / Raft, and non-multi-node network. It may add TxPool runtime hardening, FIFO pool only, and local modular runtime.

Frontend non-goals:

- no full real-time dashboard;
- no WebSocket live monitor;
- no drag-and-drop freeform composer;
- no multi-user permission system;
- no formal result database;
- no Fabric live status console;
- no MetaFlow frontend;
- no dual-chain frontend;
- no cross-shard relay / broker / 2PC frontend;
- no paper-grade framing for Draft Smoke.

### V3.4.2 Runtime Plugin Hardening: BlockProducer

目标：在 TxPool 可观测之后，把 BlockProducer 从逻辑切块推进为可解释的虚拟时间 producer。BlockProducer 应从 TxPool selection 生成 block，并保留 `block_log.csv` 与 `txpool_log.csv` 的互相解释关系。

非目标：真实节点网络、BFT、Fabric live execution、cross-shard protocol。

### V3.4.3 Runtime Plugin Hardening: Consensus-light

目标：在保持 truthfulness 的前提下增强轻量 consensus model 的可观测性，例如 leader/ordering/finality events。不得声称 PBFT / HotStuff / Raft，除非真实实现对应 runtime state machine 和消息/投票语义。

V3.4.3a is a design and BlockEmulator reference-check step. It records the current MBE consensus position and prepares V3.4.3b code boundaries in `docs/v3_4_3_consensus_light_design.md`.

V3.4.3b may add `simple_leader` as the default local model, `poa_light` as a lightweight authority confirmation model, and `pbft_light_model` as a PBFT-style stage, quorum, and message-count model. It may also add `consensus_log.csv` and summary fields for consensus latency, message count, round count, finalized block count, failed block count, and view change count.

The PBFT-light model may borrow BlockEmulator-style Propose / PrePrepare / Prepare / Commit / Finalized stage naming and quorum accounting: `f = (N - 1) / 3`, prepare quorum `2f + 1`, and commit quorum `2f + 1`.

It must not copy or implement BlockEmulator's TCP listener, `networks.Broadcast`, `TcpDial`, goroutine message handler, LevelDB / MPT / BlockChain coupling, CLPA / Broker / Relay coupling, real view-change safety, old request sync, or multi-process node lifecycle.

V3.4.3 remains local Go-backed modular runtime hardening. It is not real PBFT, not HotStuff, not Raft, not Fabric live execution, and not multi-node networking. Fabric-backed validation is deferred to a later stage unless explicitly reopened.

### V3.4.4 Single-module Experiment Templates

目标：新增单模块实验模板和公平校验，让 TxPool、BlockProducer、Consensus-light 等后续模块测试只改变目标插件，其余 workload、seed、submit rate、block/network/hardware profile 和其它模块固定。

### V3.5 Node-level Emulator Skeleton

目标：在 V3.4.11 closure 之后，建立 node-level emulator skeleton 的最小边界与接口。V3.5 不自动进入 Fabric/EVM live backend，不实现真实 PBFT/HotStuff/Raft，不实现真实多节点网络，也不把 Draft Smoke 升级为 paper-grade benchmark。

不改 Fabric peer 内核。Fabric-backed validation 是 validation / calibration layer，不是主实验 runtime。

### V3-final Frontend Integration and Acceptance

目标：前端整合当前 V3 范围内的 MetaTrack 插件链实验、V3.4 runtime hardening / controlled smoke artifacts、V3.5 node-level emulator skeleton 状态、运行记录、报告下载、系统边界与开发者模式。MetaFlow 双链协议实验保留为 planned/deferred preview，不作为当前 V3-final 验收条件。

## 7. MetaTrack 主线

MetaTrack 主线在 V3.3 开始。它必须运行在 modular research chain runtime 上。V3 不声称 MetaTrack 已经修改或嵌入 Fabric peer。

MetaTrack 主线要求：

- 同一 ChainProfile。
- 同一 workload。
- 同一 seed。
- 同一 hardware profile。
- 同一 block/consensus config。
- 同一 submit rate。
- 同一 network profile。
- 只替换 `ShardingPlugin`、`ExecutionSchedulerPlugin`、`StateAccessPlugin`、`CommitPlugin`。

## 8. Runtime Hardening 单模块公平规则

TxPool 单模块测试：

- 只允许 `TxPoolPlugin` 变化。
- 必须固定 Workload、BlockProducer、Consensus、Routing、Execution、StateAccess、StateStorage、Commit、Metrics、seed、submit rate、block config、network profile。

BlockProducer 单模块测试：

- 只允许 `BlockProducer` 变化。
- 必须固定 Workload、TxPool、Consensus、Routing、Execution、StateAccess、StateStorage、Commit、Metrics、seed、submit rate、network profile。

Consensus-light 单模块测试：

- 只允许 `ConsensusPlugin` 变化。
- 必须固定 Workload、TxPool、BlockProducer、Routing、Execution、StateAccess、StateStorage、Commit、Metrics、seed、submit rate、block config、network profile。

禁止通过更换 workload、seed、submit rate、block config、network profile、hardware profile 制造优势。禁止把 smoke-level result 当作 paper-grade result。

## 9. Fabric-backed validation 主线

Fabric 的定位是 validation backend，不是主实验内核。

Fabric-backed validation is deferred to a later stage unless explicitly reopened. When reopened, it may be used for:

- 真实 tx submit / commit latency 观测。
- 真实 block number / tx status 采集。
- chain-backed trace。
- modular runtime calibration。
- 小规模 end-to-end validation。

Fabric 不用于：

- 替换 V3 modular runtime。
- 修改 Fabric peer 内核。
- 声称 MetaTrack 在 Fabric 内核中实现。
- 提供生产级跨链桥证明。
- 绕过 V3.4 runtime hardening。

## 10. Artifacts

Every V3 modular runtime run after V3.4.1 should include:

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

`txpool_log.csv` is a V3.4.1 local modular runtime / Draft Smoke artifact. It is not paper-grade evidence by itself. It must explain TxPool admit, select, reject, queue wait, and pool size changes.

Fabric validation artifacts are deferred to a later stage unless explicitly reopened:

```text
fabric_validation_summary.csv/json
fabric_tx_results.csv
fabric_commit_latency.csv
fabric_validation_report.md
```

## 11. V3 成功标准

V3 成功标准：

- V3.0 文档和 skill 完整，边界清楚。
- V3.1 profile 能 preview 和 validate。
- V3.2 minimal runtime 能运行小 workload 并输出 block/tx/state logs。
- V3.3 MetaTrack baseline/full comparison 遵守公平规则。
- V3.4.0 完成 runtime self-check 和 scope realignment。
- V3.4.1 FIFO TxPool hardening 输出 `txpool_log.csv`，summary 中 queue wait 指标不再硬编码为 0。
- V3.4.1 frontend alignment 显示 TxPool summary 指标、下载 `txpool_log.csv`、兼容旧 history run 缺失 artifact，并继续展示非 Fabric / 非 MetaFlow / 非 PBFT / 非多节点网络边界。
- V3.4.2 BlockProducer 从 TxPool selection 生成 block，并保留 `block_log.csv` 可解释性。
- V3.4.3 Consensus-light 在保持 truthfulness 的前提下增加轻量模型。
- V3.4.4 支持 single-module experiment template 和公平校验。
- V3.5 node-level emulator skeleton 能明确节点级 emulator 的最小接口、边界和非目标。
- V3-final 前端清楚展示 truth labels、backend types、artifacts 和 boundaries。

## 12. V3 非目标

V3 非目标：

- 生产区块链。
- 生产跨链桥。
- 公网链部署平台。
- Fabric peer 内核补丁。
- 替代 Fabric。
- 多公链生产系统。
- 将 local/replay/smoke 结果当作真实链或论文最终性能结论。
- 在 V3.4.1 声称 Fabric validation。
- 在 TxPool hardening 后声称完整 BlockEmulator-like emulator。

## 13. 风险与降级路线

风险：

- runtime 范围过大。
- Fabric validation 过早进入，会校准一个尚未稳定的 logical runtime。
- Fabric validation 环境不稳定。
- MetaTrack 公平性被配置差异污染。
- profile schema 过早复杂化。
- 前端误导 data truth。

降级路线：

- 先 V3.4.1 TxPool hardening，再 V3.4.2 BlockProducer hardening，再考虑 Fabric validation。
- 如果 TxPool hardening 时间不够，只保留 FIFO pool、`txpool_log.csv`、`queue_wait_ms` 三件事，不开放 TxPool 多插件对比。
- 如果前端对齐时间不够，必须至少保留 `txpool_log.csv` 下载、TxPool summary 指标展示、旧 run 缺失 artifact 不崩溃三件事。
- V3.5 node-level emulator skeleton 只建立最小节点级 emulator 骨架，不自动进入 Fabric/EVM live backend。
- MetaFlow / dual-chain runtime 继续保持 planned/deferred preview。

## V3.3.3 Single-chain Modular Composer Frontend MVP

V3.3.3 renders the V3.3.2 `composer_preview`, `module_graph`, `plugin_matrix`, and `fairness_scope` in the frontend as a read-only Single-chain Modular Composer MVP. It adds a narrow V3 composer preview API, template listing, smoke-run alignment, module detail view, Plugin Matrix, Fairness Scope, run level, and artifact grouping.

V3.3.3 does not implement Fabric validation, MetaFlow, dual-chain runtime, PBFT/HotStuff, runnable committee lifecycle, dynamic resharding, state migration, or a drag-and-drop freeform composer editor.

## V3.3.4 Frontend Chinese Localization and Composer Layout Polish

V3.3.4 refines the V3.3.3 frontend only. It localizes the Composer page into Chinese, keeps English IDs as secondary reproducibility labels, and replaces the horizontal module-chain scrollbar with a responsive wrapped / snake-like Composer layout.

V3.3.4 does not change backend runtime semantics, does not change the `composer_preview` schema, and does not implement Fabric validation, MetaFlow, dual-chain runtime, PBFT/HotStuff, runnable committee lifecycle, dynamic resharding, state migration, or a drag-and-drop freeform editor.
