# Aria MBE Mapping

<!-- MBE_V37_ARIA_FORMAL_RUNTIME_TRUTH -->

## 正式实验路径

Aria 在 MBE 中由 `aria_block_producer` 与 `aria_block_executor` 成对组成。正式实验路径把**一次候选冲突分析批次**绑定到一个 PBFT 区块：

1. 出块节点从 FIFO 交易池取得候选交易；
2. 所有候选交易针对同一个区块开始快照独立执行，保留真实读/写集合；
3. 按 Aria 的最小交易序号读写预留以及 Rule 2 判定本批次可提交集合；
4. 可提交交易进入当前 PBFT 区块；
5. 冲突交易释放回 FIFO 交易池，等待后续 PBFT 区块重新进入新的 Aria 批次；
6. 验证节点对共识绑定的候选证据重新计算选择，并只物化确定选择的交易结果。

因此，正式 MBE 路径不是“一个已经共识的区块内部无限运行多个 Aria epoch 直到全部交易完成”。`AriaExecutor.ExecuteBlock` 中保留的内部多 epoch 模式只用于执行器级回归/兼容，不代表正式实验的共识生命周期。

## MBE 业务语义适配

Aria 复用 MBE 的交易业务执行语义，但不复用其他并发算法的调度逻辑。`retry_nonce_gaps` 用于处理 MBE 账户 nonce 的先后约束：如果较大 nonce 的交易在当前快照上暂时不可执行，而较小 nonce 的前驱仍可在后续批次推进，则该交易被延期。这是 MBE 账户模型适配，不是 Aria 原论文额外提出的一项机制。

## 共享组件

路由、PBFT、TCP 网络、状态存储、持久化提交与工作负载回放继续使用 MBE 公共实现。Aria 的候选选择、冲突规则和执行证据由 Aria 自己的插件实现，不调用 Block-STM、Groundhog、CG、BSX 或 Batch-SI 的算法代码。
