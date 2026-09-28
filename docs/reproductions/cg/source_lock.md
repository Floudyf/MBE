# Nezha Conventional CG source lock

<!-- MBE_V37_NEZHA_CG_SOURCE_TRUTH -->

## 当前实现对应的来源

当前 `hash_cg` / `cg_*` 路径是 **Nezha 官方 artifact 中 conventional CG 基线的 MBE 独立重实现**，不是旧文档曾描述的 Euro-Par/Sawtooth 原序单向 DAG 基线。

- 论文：Jiang Xiao, Shijie Zhang, Zhiwei Zhang, Bo Li, Xiaohai Dai, Hai Jin, *Nezha: Exploiting Concurrency for Transaction Processing in DAG-based Blockchains*, ICDCS 2022.
- DOI: `10.1109/ICDCS54860.2022.00034`
- 官方参考仓库：`CGCL-codes/Nezha`
- 锁定提交：`85eaf541591e5f3020dd520cf3b8ee35009d296a`
- 参考范围：`core/classical_graph.go`, `graph/johnsonce.go`, `graph/tarjanscc.go`, `test.go`

## 当前 MBE 机制

1. 按 Nezha conventional CG 规则构建有向冲突多重图，并保留官方实现的 WW 平行边语义与 RW 重复抑制；
2. 用 Tarjan 找强连通分量；
3. 在可处理的环空间内执行完整 Johnson 枚举；
4. 按官方 BreakCycles 的“最大环参与度”规则选择反馈受害交易；
5. 被选为受害者的交易在本次候选中属于算法冲突中止，但 MBE 固定工作负载实验把它释放回 FIFO 交易池，等待后续区块重试，而不是永久丢失；
6. 去环后的依赖就绪前沿由 MBE worker 并行执行；PBFT 本身不改变。

## 大环空间适配

高密度读改写负载可能使 elementary-cycle 数量爆炸。当前正式代码对不可承受的环空间使用显式、确定性的保护：RMW 完全双向团检测、Johnson 遍历/环出现预算以及确定性前缀 BreakCycles 后重新计算强连通分量。该适配必须记录在实验真值中，不能称为“无限制完整 Johnson”。

真值标签：`nezha_cg_official_multigraph_bounded_johnson_retry_mbe_worker_v4`。

## 线程实验边界

`cgPlanningWorkerCount` 固定返回 1；因此 2/4/8/16/32 worker 实验改变的是**去环后的依赖就绪执行并行度**，不是冲突图构建或 Johnson 枚举线程数。论文和图表解释必须保持这一边界。
