# BSX source lock

<!-- MBE_V37_BSX_TRUTH -->

主要来源：Yaron Hay, Roy Friedman, *Batch-Schedule-Execute: On Optimizing Concurrent Deterministic Scheduling for Blockchains*, SRDS 2024；扩展版本 arXiv `2402.05535`。

论文级核心边界：

- 对同质交易构造无向交易冲突图；
- 同一颜色类中的交易两两无冲突，可以并行；
- 图着色产生确定性批次/执行日程；
- 寻找最少颜色/最小延迟的最优着色问题是困难问题。

论文没有给出一个必须采用的作者官方多项式时间着色实现，也没有要求 DSATUR 是唯一正确算法。因此 MBE 使用**确定性的 DSATUR 启发式**作为工程适配，并固定并列规则（饱和度、图度数、原始交易序号），同色交易并行、颜色批次按确定顺序推进。

正式真值标签：`bsx_homogeneous_conflict_graph_coloring_dsatur_v1`。

不得把当前结果描述为“论文规定的 DSATUR”或“保证获得最少颜色的最优 BSX 调度”。它是 BSX 冲突图着色框架上的确定性可复现实例化。
