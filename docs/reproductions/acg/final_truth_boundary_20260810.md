# ACG / Nezha truth boundary

<!-- MBE_V37_ACG_TRUTH -->

主要来源：Jiang Xiao 等，*Nezha: Exploiting Concurrency for Transaction Processing in DAG-based Blockchains*, ICDCS 2022，DOI `10.1109/ICDCS54860.2022.00034`。

官方参考仓库：`CGCL-codes/Nezha`。

当前 MBE ACG 路径以论文和 Nezha 官方 artifact 为共同参考，对地址冲突图与层次排序机制进行独立重实现。正式 source type 为：

`paper_and_official_source_reimplementation`

当前核心机制包括：

- 按逻辑地址索引交易读写关系；
- 构建 Nezha 地址冲突关系；
- 按官方参考路径执行 `CreateGraph -> QueuesSort -> DeSS`；
- 由层次排序形成确定性的可执行 wave；
- 不调用 conventional CG、BSX、Batch-SI 或其他并发算法的规划器；
- HS 判定为 victim 的交易在当前候选批次中被延期，而不是生成永久失败结果；
- 延期交易释放回 MBE FIFO 交易池，在后续 PBFT 区块重新进入 ACG 规划。

其中 `HS abort -> FIFO Deferred -> later-block retry` 是 MBE 固定工作负载实验生命周期适配，用于保证所有逻辑交易最终都有明确完成路径；它不是 Nezha 官方 benchmark 本身提出的额外机制。

PBFT、网络、持久化和实验工作负载生命周期继续使用 MBE 公共基础设施，不因此声称与作者完整系统逐行或逐组件相同。

正式 truth boundary：

`nezha_acg_hs_official_reference_mbe_retry_v2`

因此当前实现应描述为：

**Nezha ACG/HS 论文与官方参考实现机制的 MBE 独立重实现 + MBE FIFO 延期重试生命周期适配。**

不能描述为作者源码逐行复制，也不能再标记为仅依据论文描述的 `paper_description_reimplementation`。