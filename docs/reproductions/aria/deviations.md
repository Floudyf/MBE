# Aria Deviations and Truth Boundary

<!-- MBE_V37_ARIA_DEVIATIONS_TRUTH -->

## 已复现的核心机制

- 同一批次开始快照；
- 交易私有推测写；
- 真实读写集合捕获；
- 最小交易序号读/写预留；
- RAW、WAR、WAW 判定；
- 默认 Rule 2 重排序规则；
- 只读快速提交；
- 冲突交易保持相对 FIFO 顺序延期。

## 正式 MBE 生命周期适配

正式路径是“一次 Aria 候选批次对应一个 PBFT 区块”。本批次被延期的交易不会在同一个已共识区块内部反复执行，而是回到交易池等待后续区块。执行器内部的多 epoch drain 只保留作局部回归辅助，不作为论文正式实验路径。

`retry_nonce_gaps` 是 MBE 账户 nonce 语义适配，用于把暂时缺少较小 nonce 前驱的交易延期；它不应被描述为 Aria 原论文机制。

## 未复现

- Aria 原系统的分布式数据库协调器与远程数据库预留协议；
- AriaFB 或 Calvin fallback；
- 原系统完整数据库存储引擎；
- 源码逐行同一性。

正式真值标签应理解为：**Aria Rule-2 核心机制在 MBE 交易/共识生命周期上的独立重实现**。
