# V3 Execution Plan

## 0. Current Scope Realignment

Current V3 acceptance is now MetaTrack-oriented single-chain modular runtime + Fabric-backed validation + frontend acceptance:

- V3.0 Planning Scaffold: complete.
- V3.1 Profile Layer: complete.
- V3.2 Minimal Single-chain Modular Runtime.
- V3.2b / V3.2.5 Go-backed Minimal Runtime / Go parity: planned after V3.2.
- V3.3 MetaTrack Plugin Evaluation.
- V3.4 Fabric-backed Validation for MetaTrack.
- V3-final Frontend Integration and Acceptance.

Deferred / future work:

- V3.5 Minimal Dual-chain Runtime.
- V3.6 MetaFlow Protocol Plugin and AFS/FDA.

Current V3-final does not require V3.5 or V3.6. Existing MetaFlow preview profiles remain planned / not runnable. V3.2 is a Python backend reference runtime for stabilizing runtime semantics, profile contracts, and artifact schemas; it does not implement Go-backed parity, MetaTrack full evaluation, Fabric validation, dual-chain runtime, MetaFlow, AFS, or FDA.

## 1. V3 背景与动机

V2 已经形成 V3-ready local replay / sweep / calibration 实验平台。它能组织实验、管理 run/artifact、执行 V1 single-chain replay、执行 V2.5 dual-chain replay、执行 V2.6 protocol baseline、生成 V2.8 sweep/report，并通过 V2.9 做 chain-backed calibration。

V3 的动机不是继续扩展本地 replay，而是在 V2 实验组织层之上建立自研的模块化研究链 runtime，让 MetaTrack 与 baseline 在同一条研究链环境中公平替换插件，让 MetaFlow 与 cross-chain baselines 在同一双链环境中公平比较，并用 Fabric-backed observation / calibration / validation 防止实验被理解为纯模拟。

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
V3 = 面向 MetaTrack + MetaFlow 的模块化插件链实验平台，并带 Fabric 链支持验证。
```

V3 不是：

- 纯 replay 平台。
- 纯 simulation 平台。
- 只在本地 trace 上跑的 emulator。
- Fabric 内核修改版。
- 生产级联盟链。
- 生产级跨链桥。
- 多公链跨链平台。
- 公网链部署平台。

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

V2.4 Cross-chain Trace Schema
  -> V3 event schema seed for dual-chain and cross-chain protocol logs

V2.5 ChainBackend / dual-chain replay
  -> V3 ChainProfile and dual-chain runtime reference

V2.6 CrossChainProtocol Interface
  -> V3 CrossChainProtocolPlugin seed

V2.8 Sweep / Report
  -> V3 fair baseline runner and report generator

V2.9 Calibration / Realism Bridge
  -> V3 Fabric-backed validation and replay-vs-observed calibration

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
  - CrossChainProtocolPlugin
        |
Metrics / Artifact / Report Layer
        |
Fabric-backed Validation Layer
```

V3 不是为了支持很多链，而是为了在同一个研究链环境下公平替换模块插件。Fabric 用于真实链观测、校准和补充验证。

## 6. V3 阶段路线

### V3.0 Planning Scaffold

目标：建立 V3 skill、docs、边界、阶段路线、profile 定义、公平 baseline 政策。

只写文档，不实现代码。

### V3.1 Profile Layer

目标：定义 `ChainProfile`、`PluginProfile`、`ExperimentProfile` 的 schema / loader / validator / preview。

V3.1 才开始实现配置层。本轮 V3.0 不实现。

### V3.2 Minimal Single-chain Modular Runtime

目标：实现最小单链模块化研究链 runtime：

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

目标：在 V3.2 runtime 上实现 MetaTrack 所需插件组合：

- `hash_sharding`
- `co_access_sharding`
- `serial_execution`
- `dual_track_execution`
- `normal_commit`
- `hot_update_aggregation_commit`

运行：

- `baseline_hash_only`
- `co_access_only`
- `co_access_dual_track`
- `full_MetaTrack`

所有 baseline 与 full_MetaTrack 必须在同一 workload、同一 seed、同一 ChainProfile、同一 block config、同一 consensus config 下运行，只允许替换插件。

### V3.4 Fabric-backed Validation for MetaTrack

目标：用 Fabric 产生真实 commit latency、tx_id、block_number、status、chain-backed trace，校准 V3 modular runtime，并做小规模真实 Fabric 黑盒验证。

不改 Fabric peer 内核。

### V3.5 Minimal Dual-chain Runtime

目标：在 V3.2 单链 runtime 基础上创建 source_chain X 和 target_chain Y，支持 source lock、target mint、source complete/refund、finality wait、pending count。

服务 MetaFlow。

### V3.6 MetaFlow Protocol Plugin and AFS/FDA

目标：实现 `CrossChainProtocolPlugin`：

- `lock_mint_serial`
- `lock_mint_pipeline`
- `fixed_window_baseline`
- `committee_bridge_basic`
- `metaflow_basic`
- `metaflow_afs_fda`

实现 MetaFlow 参数：

- `B = batch size`
- `D = pending/finality window`
- `T = timeout`
- `AFS = adaptive batch-size control`
- `FDA = finality/pending-window control`

### V3-final Frontend Integration and Acceptance

目标：前端整合当前 V3 范围内的 MetaTrack 插件链实验、Fabric 验证、运行记录、报告下载、系统边界与开发者模式。MetaFlow 双链协议实验保留为 planned/deferred preview，不作为当前 V3-final 验收条件。

## 7. MetaTrack 主线

MetaTrack 主线在 V3.3 开始。它必须运行在 V3.2 modular research chain runtime 上。V3 不声称 MetaTrack 已经修改或嵌入 Fabric peer。

MetaTrack 主线要求：

- 同一 ChainProfile。
- 同一 workload。
- 同一 seed。
- 同一 hardware profile。
- 同一 block/consensus config。
- 只替换 `ShardingPlugin`、`ExecutionSchedulerPlugin`、`StateAccessPlugin`、`CommitPlugin`。

## 8. MetaFlow 主线

MetaFlow 主线在 V3.6 开始。V2.6 baseline 可作为 interface seed，但不是 MetaFlow 实现。

MetaFlow 主线要求：

- 同一 source ChainProfile。
- 同一 target ChainProfile。
- 同一 workload arrival sequence。
- 同一 finality profile。
- 同一 timeout baseline。
- 只替换 `CrossChainProtocolPlugin` 和 control policy。

## 9. Fabric-backed validation 主线

Fabric 的定位是 validation backend，不是主实验内核。

Fabric-backed validation 用于：

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

## 10. V3 成功标准

V3 成功标准：

- V3.0 文档和 skill 完整，边界清楚。
- V3.1 profile 能 preview 和 validate。
- V3.2 minimal runtime 能运行小 workload 并输出 block/tx/state logs。
- V3.3 MetaTrack baseline/full comparison 遵守公平规则。
- V3.4 Fabric-backed validation 能输出 calibration/validation report。
- V3.5 dual-chain runtime remains deferred / future roadmap and is not a current V3-final gate。
- V3.6 MetaFlow basic and AFS/FDA remain deferred / future roadmap and are not current V3-final gates。
- V3-final 前端清楚展示 truth labels、backend types、artifacts 和 boundaries。

## 11. V3 非目标

V3 非目标：

- 生产区块链。
- 生产跨链桥。
- 公网链部署平台。
- Fabric peer 内核补丁。
- 替代 Fabric。
- 多公链生产系统。
- 将 local/replay/smoke 结果当作真实链或论文最终性能结论。

## 12. 风险与降级路线

风险：

- runtime 范围过大。
- Fabric validation 环境不稳定。
- MetaTrack/MetaFlow 公平性被配置差异污染。
- profile schema 过早复杂化。
- 前端误导 data truth。

降级路线：

- V3.2 先做 single-process logical multi-node。
- V3.3 先用 synthetic workload，再接 chain-backed calibration。
- V3.4 Fabric validation 只做小规模黑盒验证。
- V3.5 先表达 minimal two-chain path。
- V3.6 先实现 structured research artifacts，real threshold signatures 可作为未来工作。
