# V3 Fabric-backed Validation

## 1. Fabric 的角色

Fabric 在 V3 中是 validation backend。它用于真实链观测、校准和小规模端到端验证，帮助说明 V3 modular runtime 不是纯 simulation-only evaluation。

## 2. Fabric 不是主实验内核

V3 主实验内核是 modular research chain runtime。Fabric 不负责承载所有 MetaTrack 插件替换，也不作为 MetaFlow 主协议 runtime。

## 3. Fabric 不被改 peer 内核

V3 不修改 Fabric peer 内核，不替换 Fabric orderer，不把 Fabric validation pipeline 写成 V3 CommitPlugin。若不改 Fabric peer，就不能声称 MetaTrack 已经在 Fabric 内核实现。

## 4. Fabric chain-backed trace

Fabric chain-backed trace 应包含：

- tx id。
- submit / commit timestamp where available。
- block number。
- tx status。
- chaincode operation。
- state update observation。
- trace metadata。

Web/API 不得自动启动 Docker/Fabric/network.sh。Fabric trace 可以由手动 CLI/WSL 流程生成。

## 5. Fabric calibration

Fabric calibration 用于：

- 观测真实 commit latency。
- 校准 modular runtime block/commit/finality parameters。
- 计算 replay-vs-observed error。
- 生成 calibration profile。

同一 calibration profile 必须公平应用到 proposed method 和 baselines。

## 6. Fabric real-chain validation

Fabric real-chain validation 是小规模黑盒验证：

- 提交真实 Fabric transactions。
- 观察真实 status。
- 观察 block number。
- 观察 commit latency。
- 对照 modular runtime trend。

它不替代主实验，不承担生产级性能结论。

## 7. 可验证什么

Fabric validation 能验证：

- 真实 tx submit / commit latency。
- 真实 block number。
- 真实 tx status。
- 真实 chaincode 状态更新。
- 真实端到端提交。
- 外部调度策略的小规模趋势。
- 热点聚合语义正确性。

## 8. 不能验证什么

Fabric validation 不能验证：

- Fabric peer 内部执行 MetaTrack dual-track scheduler。
- Fabric orderer 被替换成 V3 ConsensusPlugin。
- Fabric validation pipeline 被替换成 V3 CommitPlugin。
- 不改 Fabric peer 就声称 MetaTrack 已经在 Fabric 内核实现。
- 生产级跨链桥安全。
- 公网链部署能力。

## 9. Artifacts

Fabric validation artifacts:

- `fabric_validation_summary.csv/json`
- `fabric_tx_results.csv`
- `fabric_commit_latency.csv`
- `fabric_block_log.csv`
- `fabric_validation_report.md`
- `used_chain_profile.yaml/json`
- `used_experiment_profile.yaml/json`
- `runtime.log`

## 10. 安全边界

Fabric validation may be allowed only in explicitly authorized stages such as V3.4. Normal frontend/API paths must not start Docker, Fabric, or network.sh unless the current stage explicitly permits it and safety gates are implemented.

Fabric validation must never be described as FabricLiveBackend unless FabricLiveBackend is explicitly implemented in a later approved stage.
