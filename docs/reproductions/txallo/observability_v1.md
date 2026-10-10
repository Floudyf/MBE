# TxAllo 可核验观测闭合 v1 (MBE, HEAD 1dbb8de + TxAlloFix_v1)

## 范围

此补丁**不更改** G/A 核心、历史选择、分片决策、PBFT、Stateful 全节点复制、精确版本、Stateless 远程读写、其他算法。它只增加 TxAllo 动态运行时客户端的阶段计时、客户端实际可执行文件指纹，以及结束后的只读汇总。

## 已存在的权威资料

- `workload/txallo_history_summary.json`, `client/txallo_allocation_summary.json`: G 启动历史、回放分母和 G/A 参数；G 缓存命中必须与 G 本轮执行区分。
- `workload/txallo_mapping_epochs.jsonl`, `workload/txallo_mapping_snapshot_e*.json`: 源高度 epoch、更新算法和版本化映射。
- `client/txallo_transaction_placement.csv`: 每笔账户分片、映射来源、fallback、实际跨片、routing_epoch 和映射状态摘要。
- `nodes/*/txallo_mapping_ack_e*.json`, `nodes/*/txallo_epoch_lifecycle.jsonl`: 全节点 ACK 与提交/终态事件。
- `nodes/*/txallo_replica_commit.jsonl`: 全节点持久状态复制 token、精确值摘要和物理角色。
- `nodes/*/block_execution_summary.json`, `nodes/*/runtime_metrics.json`, `network_message_summary.csv`: 共识、执行、WAL、状态承诺和网络活动；物理耗时必须区分资源和关键路径。
- `backend.app.services.v5_txallo_epoch_benefit_v1`: 已有的动态/冻结 G/固定 hash 反事实跨片结果，属于离线放置比较，**不是** TPS 实测。

## 本次新增

`client/txallo_epoch_timing.jsonl`：每个**被关闭且供后续学习**的源区块 epoch 一条，使用 Go 单调钟产生 `commit_wait_ns`、`replica_wait_ns`、`allocation_update_ns`、`mapping_ack_wait_ns`、`total_barrier_ns`，并带 `source_epoch`、`routing_epoch_before/after`、`transaction_count`、`replica_token_count`、`status`、`failure_phase`。空 epoch 也记；最终部分 epoch 不允许回灌历史，**不会**算一个完成的 A epoch。总屏障时间包含诊断/处理额外开销，不与阶段时间相加重复计算；提交客户端窗口还包含路由、网络发送等其他时间。记录失败阶段时保留错误，不把失败转为成功。若观测文件写入异常，stderr 明确告警；观测器不参与 TxAllo 的可见性或调度。

`client/txallo_client_binary_v1.json`：运行时正在执行的客户端进程可执行文件 SHA256，**不等于**所有 validator 可执行文件哈希，也不能只用 Git HEAD 冒充实际二进制身份。

`aggregate/txallo_evidence_summary.json`：在提取正式指标时（冷归档之前）自动生成。含只读账户动态收益、源证据 SHA256、代表节点物理块成本、网络按消息类型计数以及 epoch 阶段计时。没有原始资料时 `unavailable`，不以 0 代替。`runtime_attempt_counters` 记录认证根候选复用/构建次数，**不等价于 `NewCommitment()` 的精确调用次数**。

## 现有历史 10K 可支持的结论

该次运行已经独立审计证明 10,000 交易的 10,001 账户关系呈单中心星形：中心 m.federation 被访问 10,000 次，10,000 个外围账户各一次。动态 A/G、冻结 G、固定 hash 均为 50.2% 跨片率，18 次更新未在此负载降低跨片率。它不能推广为 A-TxAllo 在其他数据中无效。

## 物理区块去重口径

严格从编译计划选择每个执行分片恰好一个 leader 的 `block_execution_summary.json`；缺任何代表节点则 `unavailable`，不选任意 PBFT validator 后假装精确。`physical_blocks` 仅每执行分片唯一 `block_hash` 计一次。`blocks_with_transaction_instances` 可能含跨片协议实例；`system_delta_drain_blocks` 仅指 `TxList` 为空而有系统增量的区块，不能声称涵盖全部副本物化块；二者不得相加得出总块数。累计执行/commit/WAL 毫秒为跨 leader **资源和**，不是 wall-clock 或最长关键路径。

## 必须继续保留的限制

- 本次没有改 Serial 的毫秒级 `state_commitment_ms` 精度，也没有直接插桩 `NewCommitment()`。
- 网络日志不存在精确 bytes 字段时，字节返回 `null` 而非 0；按消息类型计数不能直接当 TxAllo 独占带宽。
- 此记录没有提供 PBFT 间因果跨度或完整节点运行时二进制指纹；这些属于其他平台级进一步观测。
- 旧实验此前没有采集单调 epoch 阶段计时，不能通过历史交易生命周期倒推出精确等待时间。
- 先以相同 1000、2500、10000 笔数据进行重复测试，明确独立运行样本和完整运行参数。此补丁不承诺任何 TPS 改善。
- 源码版本验证需要保留 `.cache/txallo_fix_v1/apply_receipt.json` 和本次 `.cache/txallo_obs_v1/apply_receipt.json` 的精确源 SHA。基线 HEAD 1dbb8de 本身不足以指明这两个未提交补丁。

## 使用

- 一个动作：双击 `START_HERE.cmd`，内部自动签封、staging、Go/Python 测试、然后 APPLY。
- 手动无写入验证：`powershell -NoProfile -ExecutionPolicy Bypass -File .\VERIFY_ONLY.ps1`
- 回滚：`powershell -NoProfile -ExecutionPolicy Bypass -File .\ROLLBACK_FIX.ps1`。仅恢复此包的 8 个文件；不会回滚 TxAlloFix_v1。
- `aggregate/txallo_evidence_summary.json` 在新运行正式指标提取时自动写入；已有旧实验可在离线 Python 模块里调用 `build_summary(run_dir)`，但不会补造当时没有的阶段计时。
