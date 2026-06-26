# V3 Modular Chain Runtime

## 0. V3.2 Runtime Scope

V3.2 implements only a minimal single-chain modular runtime as a Python backend reference runtime:

```text
synthetic workload
-> TxPool
-> BlockProducer
-> simple_leader Consensus
-> hash_sharding
-> serial_execution
-> direct_fetch
-> normal_commit
-> basic_metrics / artifacts
```

V3.2 uses single-process logical multi-node execution. It does not start real node processes, RPC, p2p, Docker, Fabric, network.sh, or the Go executor. Go-backed parity is planned for V3.2b / V3.2.5. MetaTrack full evaluation starts in V3.3. Fabric-backed validation starts in V3.4. Dual-chain runtime and MetaFlow AFS/FDA are deferred / future scope and are not current V3-final acceptance requirements.

## 1. Runtime 目标

V3 modular chain runtime 的目标是在同一条自研模块化研究链上公平替换插件，支撑 MetaTrack 单链机制评估和 MetaFlow 双链协议评估。

Runtime 不是 V2 replay runner 的简单改名。它应拥有最小链路：交易进入、交易池、出块、共识/排序、分片/路由、执行调度、状态访问、提交、日志、指标。

## 2. Runtime 非目标

V3 runtime 非目标：

- 不做生产级区块链。
- 不做生产 p2p。
- 不要求多服务器。
- 不要求真实 Byzantine fault injection。
- 不替代 Fabric。
- 不直接连接公网链。
- 不提供生产跨链桥安全承诺。

## 3. Runtime 最小链路

```text
Client / Workload
  -> TxPool
  -> BlockProducer
  -> ConsensusPlugin
  -> ShardingPlugin / Routing
  -> ExecutionSchedulerPlugin
  -> StateAccessPlugin
  -> CommitPlugin
  -> BlockLog / StateLog / Metrics
```

V3.2 第一版 runtime 可以是 single-machine multi-node 或 single-process logical multi-node。V3.2 不要求多服务器，不要求生产级 p2p，不要求真实 Byzantine fault injection。

## 4. Runtime 组件

### Client / Workload

- 职责：提交交易和 workload arrival sequence。
- 输入：WorkloadProfile、seed、arrival rate、transaction mix。
- 输出：submitted transactions。
- 非职责：不决定分片、执行路径或提交策略。

### TxPool

- 职责：接收交易、排队、去重、暴露 block selection。
- 输入：transactions、pool policy。
- 输出：candidate transaction batch。
- 非职责：不执行交易、不提交状态。

### BlockProducer

- 职责：按 `block_interval_ms`、`max_tx_per_block`、cut policy 生成 block proposal。
- 输入：TxPool batch、ChainProfile block config。
- 输出：block proposal。
- 非职责：不做最终共识安全证明。

### ConsensusPlugin

- 职责：对 block proposal 给出 ordering/finality decision。
- 输入：block proposal、consensus config。
- 输出：ordered/finalized block event。
- 非职责：V3.2 不要求 PBFT/HotStuff 真实容错实现。

### ShardingPlugin / Routing

- 职责：决定 state shard、execution shard、remote access path。
- 输入：transaction access list、chain sharding config。
- 输出：shard assignment、routing metadata。
- 非职责：不执行交易。

### ExecutionSchedulerPlugin

- 职责：决定 serial/parallel/dual-track execution path。
- 输入：ordered transactions、shard metadata、dependency metadata。
- 输出：execution plan、track assignment。
- 非职责：不直接改写状态。

### StateAccessPlugin

- 职责：读写状态、远程访问、prefetch、access-list validation。
- 输入：execution plan、read/write set。
- 输出：state reads/writes、access metrics。
- 非职责：不负责 commit policy。

### CommitPlugin

- 职责：提交状态变更、hot update aggregation、commit log。
- 输入：executed transaction results、state delta。
- 输出：state_commit_log、block commit status。
- 非职责：不改变 workload 或 seed。

### MetricsCollector

- 职责：收集 throughput、latency、queue wait、block commit latency、mechanism metrics。
- 输入：runtime events。
- 输出：summary、logs、reports。
- 非职责：不修饰或伪造实验效果。

## 5. Runtime 事件流

Runtime 应输出稳定事件：

- `tx_submitted`
- `tx_admitted`
- `block_cut`
- `block_ordered`
- `shard_assigned`
- `execution_started`
- `state_read`
- `state_write`
- `commit_started`
- `commit_completed`
- `tx_finalized`
- `block_finalized`

这些事件用于 `block_log.csv`、`tx_results.csv`、`state_commit_log.csv` 和 mechanism metrics。

## 6. Runtime 插件执行顺序

默认顺序：

1. Workload submits transactions。
2. TxPool admits and orders candidates by pool policy。
3. BlockProducer cuts block。
4. ConsensusPlugin orders/finalizes proposal。
5. ShardingPlugin assigns shards and routing。
6. ExecutionSchedulerPlugin builds execution plan。
7. StateAccessPlugin reads/writes required state。
8. CommitPlugin commits state delta。
9. MetricsCollector records events and summaries。

Only plugin-specific stages may differ in fair comparisons.

## 7. Runtime 与 V2 replay runner 的区别

V2 replay runner 读取已有 trace 或 schema sample，计算 virtual-time replay metrics。V3 runtime 应生成 block logs、state commit logs、tx pool behavior、plugin events 和 runtime metrics。

V2 replay 是 deterministic reference and calibration substrate。V3 runtime 是模块化研究链主实验内核。

## 8. Runtime 与 Fabric 的关系

Fabric 是 validation backend，不是 V3 runtime 内核。V3 不修改 Fabric peer，不替换 Fabric orderer，不声称 MetaTrack scheduler 在 Fabric 内核中运行。

Fabric-backed validation 观测真实 submit/commit/block/status，再用于 calibration 和小规模 end-to-end validation。

## 9. Runtime 输出 artifacts

V3 runtime 基础 artifacts：

- `used_chain_profile.yaml/json`
- `used_plugin_profile.yaml/json`
- `used_experiment_profile.yaml/json`
- `runtime.log`
- `summary.csv/json`
- `report.md`
- `block_log.csv`
- `tx_results.csv`
- `state_commit_log.csv`

MetaTrack / MetaFlow / Fabric validation 阶段会追加各自专用 artifacts。

## 10. Runtime 后续阶段实现边界

- V3.1 只实现 profile layer，不实现 runtime。
- V3.2 只实现 minimal single-chain runtime。
- V3.3 才实现 MetaTrack plugins and evaluation。
- V3.4 才实现 Fabric-backed validation。
- V3.5 才实现 minimal dual-chain runtime。
- V3.6 才实现 MetaFlow protocol plugin and AFS/FDA。
