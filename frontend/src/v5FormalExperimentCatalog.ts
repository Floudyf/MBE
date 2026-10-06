import type { V5FormalSuite } from "./api";

export type FormalSuiteDefinition = {
  id: V5FormalSuite;
  title: string;
  description: string;
  methodMode: "single" | "multiple" | "ablation";
};

export type FormalMethodDefinition = {
  methodId: string;
  title: string;
  description: string;
  family: "stateful" | "stateless" | "metatrack" | "batch_si";
  comparisonVisible: boolean;
  mainVisible: boolean;
  ablationTarget?: "batch_si" | "metatrack";
  isFullVariant?: boolean;
};

export const FORMAL_SUITE_DEFINITIONS: FormalSuiteDefinition[] = [
  { id: "main_experiment", title: "主实验", description: "选择一个研究方案并加载其正式主实验配置。", methodMode: "single" },
  { id: "comparison_experiment", title: "方法对比", description: "在完全一致的负载、拓扑与资源条件下比较多个方案。", methodMode: "multiple" },
  { id: "ablation_experiment", title: "消融实验", description: "先选择研究方案，再选择该方案已经注册的消融变体。", methodMode: "ablation" },
  { id: "workload_sensitivity", title: "负载敏感性", description: "固定系统条件，扫描偏斜、读写比例、到达强度或交易规模。", methodMode: "multiple" },
  { id: "topology_scaling", title: "拓扑与资源扩展", description: "扫描节点、分片、每片验证节点与节点内 Worker 数量。", methodMode: "multiple" },
  { id: "fault_recovery_experiment", title: "故障与恢复", description: "选择一个方法，对比无故障基准与已实现的网络故障场景。", methodMode: "single" },
];

export const FORMAL_METHOD_DEFINITIONS: FormalMethodDefinition[] = [
  { methodId: "hash_serial", title: "Serial", description: "有状态哈希路由的确定性串行参考。", family: "stateful", comparisonVisible: true, mainVisible: false },
  { methodId: "hash_block_stm", title: "Block-STM", description: "乐观并发执行、验证、中止与重执行。", family: "stateful", comparisonVisible: true, mainVisible: true },
  { methodId: "hash_aria", title: "Aria", description: "同快照批量乐观执行与确定性内部纪元。", family: "stateful", comparisonVisible: true, mainVisible: true },
  { methodId: "hash_groundhog", title: "Groundhog", description: "同块快照、类型化状态修改与约束合并。", family: "stateful", comparisonVisible: true, mainVisible: true },
  { methodId: "hash_fabricpp_cg", title: "Fabric++ CG", description: "Sharma 等 SIGMOD 2019：WS(Ti)∩RS(Tj) 冲突图 + Tarjan + Johnson + 最大周期参与度移除 + 可串行化调度。", family: "stateful", comparisonVisible: true, mainVisible: false },
  { methodId: "hash_cg", title: "CG / Nezha", description: "Nezha 作者传统 CG：官方冲突多重图、Tarjan/JohnsonCE/BreakCycles 与 BasicTopologicalSort 参考序列；MBE Worker 仅作为执行资源适配。", family: "stateful", comparisonVisible: true, mainVisible: false },
  { methodId: "hash_acg", title: "ACG / Nezha", description: "Batch-SI 原论文对照组：地址冲突图与层次调度。", family: "stateful", comparisonVisible: true, mainVisible: false },
  { methodId: "hash_bsx", title: "BSX", description: "Batch-SI 原论文对照组：无向冲突图 + 确定性图着色。", family: "stateful", comparisonVisible: true, mainVisible: false },
  { methodId: "stateful_calvin", title: "Calvin", description: "SIGMOD 2012：全局确定性顺序 + 分区常驻状态 + FIFO S/X 锁队列 + READ_RESULT 参与者执行；排序复制层统一适配为 MBE PBFT。", family: "stateful", comparisonVisible: true, mainVisible: false },
  // MBE_OPTME_V22_CATALOG
  // MBE_OPTME_V22_6_STATEFUL_CARD_RESTORE
  { methodId: "stateful_optme", title: "OptME", description: "SC 2024 算法复现：所有验证节点共享一个 optme-global PBFT 排序域；共识后在共同 block-start 状态上运行原 OptME 并行模拟、地址冲突图、层次排序、First-Updater-Wins、重排与二次执行。topology.shards 仅表示逻辑状态分区数，不表示多条 OptME 排序链。", family: "stateful", comparisonVisible: true, mainVisible: true },
  { methodId: "stateful_txallo", title: "TxAllo", description: "ICDE 2023 原论文适配：正式评测窗口前的已提交历史运行完整层次 Louvain + G-TxAllo，并冻结账户→分片映射；A-TxAllo 核心保留，但不把任意交易分块伪装成已提交区块更新。", family: "stateful", comparisonVisible: true, mainVisible: true },
  { methodId: "hash_batch_si", title: "Batch-SI", description: "AWRT + WRBP + OFAS + 批快照并行。", family: "batch_si", comparisonVisible: true, mainVisible: true, ablationTarget: "batch_si", isFullVariant: true },
  { methodId: "hash_batch_si_no_wrbp", title: "w/o WRBP", description: "以顺序分批替代写机会批次回填，验证 WRBP 的批宽贡献。", family: "batch_si", comparisonVisible: false, mainVisible: false, ablationTarget: "batch_si" },
  { methodId: "hash_batch_si_no_ofas", title: "w/o OFAS", description: "使用 Batch-SI 内部完整依赖图排序替代 OFAS。", family: "batch_si", comparisonVisible: false, mainVisible: false, ablationTarget: "batch_si" },
  { methodId: "hash_batch_si_serial_batch", title: "w/o Snapshot Parallelism", description: "保持相同分批与排序，将批内执行改为单 Worker。", family: "batch_si", comparisonVisible: false, mainVisible: false, ablationTarget: "batch_si" },
  { methodId: "hash_batch_si_txid_priority", title: "w/o OFAS Priority", description: "保留 OFAS 正确性规则，仅取消论文读次数优先级。", family: "batch_si", comparisonVisible: false, mainVisible: false, ablationTarget: "batch_si" },
  { methodId: "stateless_calvin", title: "Stateless Calvin", description: "Calvin 无状态兼容适配：保留全局确定性顺序与 S/X 锁调度，状态前驱/生产版本由 PBFT 共识绑定的 Calvin 块顺序生成，再按签名 AccessList 从状态归属分区真实取值并写回；不使用 MetaTrack 双轨、StateReady 或前沿控制。 /* MBE_CALVIN_CONSENSUS_VERSION_PLAN_V34 */", family: "stateless", comparisonVisible: true, mainVisible: false },
  { methodId: "stateless_hash_serial", title: "Stateless Hash + Serial", description: "无状态哈希路由的串行兼容参考。", family: "stateless", comparisonVisible: true, mainVisible: false },
  // MBE_PORYGON_PAPER_REPRO_20260921_V8_REFACTOR
  { methodId: "stateless_porygon", title: "Porygon", description: "ICDE 2024 的 MBE 适配复现：前端仍是一张方案卡，但由 Porygon 准入、账户/对象分片、单排序域路由、TransactionBlock/Witness、流水调度、ESC 执行、状态访问/存储与跨片协调等标准插件拼装；前端分片数映射为 ESC/StateOwner 分区数，所有节点共享一个全局 Ordering/PBFT 域。", family: "stateless", comparisonVisible: true, mainVisible: false },
  { methodId: "stateless_hash_block_stm", title: "Stateless Hash + Block-STM", description: "无状态哈希路由与 Block-STM 后端组合。", family: "stateless", comparisonVisible: true, mainVisible: false },
  { methodId: "stateless_optme", title: "Stateless-OptME", description: "OptME 无状态适配：与 Stateful OptME 共享一个 optme-global PBFT 排序域；AccessList 只限定 H-1 block-start 状态投影，客户端 source order 不再生成 StateVersions，最终状态按逻辑 Home 分区本地物化；OptME 调度仍只来自真实模拟 ReadSet/WriteSet。", family: "stateless", comparisonVisible: true, mainVisible: false },
  { methodId: "stateless_txallo", title: "Stateless-TxAllo", description: "TxAllo 无状态适配：与 TxAllo 使用完全相同的预评测 G-TxAllo 冻结映射；仅把状态承载替换为 MBE 无状态 exact-version 远程获取/写回，PBFT 与 FIFO/串行执行保持共享基线。", family: "stateless", comparisonVisible: true, mainVisible: false },
  { methodId: "metatrack_serial", title: "MetaTrack（初始版）", description: "历史兼容方法；保留旧结果解析，不再作为新实验卡片显示。", family: "metatrack", comparisonVisible: false, mainVisible: false },
  { methodId: "metatrack_full_locality", title: "MetaTrack（当前版）", description: "历史兼容方法；保留旧结果解析，不再作为新实验卡片显示。", family: "metatrack", comparisonVisible: false, mainVisible: false },
{ methodId: "metatrack_latest", title: "Metatrack", description: "完整版本：增量依赖/局部性感知分流 + 有效独立前沿双轨 + V669 依赖闭包安全门下的 N/L 自适应完整 RouteBatch 共识聚合 + 执行域本地 exact-version；正式消融统一固定 RouteBatch=100，保持已验证的流水线工作点。", family: "metatrack", comparisonVisible: true, mainVisible: true, ablationTarget: "metatrack", isFullVariant: true },
  { methodId: "metatrack_unified", title: "历史实验版（V668）", description: "历史兼容：保留原 V668 N/L-only 自适应共识实验及固定100笔 RouteBatch 配置；不进入新的正式消融矩阵。", family: "metatrack", comparisonVisible: false, mainVisible: false },
  { methodId: "metatrack_ab_route", title: "去掉共现矩阵分片", description: "替换整个共现矩阵分片器为历史 frequency/load-only 状态放置：不构建/不使用 pair 共现矩阵、共现邻居扩展或历史 pair affinity；高频状态独立按负载放置，交易再按独立状态放置的 majority/队列负载决定执行 shard。Signed routing、exact-version、StateReady、双轨、V669、状态预取与 PBFT 保持不变。", family: "metatrack", comparisonVisible: false, mainVisible: false, ablationTarget: "metatrack" },
  { methodId: "metatrack_ab_track", title: "去掉双轨", description: "只去掉 Fast/Conservative 双轨：严格按 canonical 顺序执行，一次只允许 1 个 business execution in-flight；即使后续交易已 ready，也必须等待前一交易完成。依赖 DAG、StateReady、分流、V669 共识、状态访问与配置 worker_count 保持不变。", family: "metatrack", comparisonVisible: false, mainVisible: false, ablationTarget: "metatrack" },
  { methodId: "metatrack_ab_cons", title: "去掉共识聚合", description: "只去掉多个 RouteBatch 的 V669 自适应聚合：一个完整 RouteBatch 一个 PBFT 共识窗口；RouteBatch=100 且 PBFT 协议本身不变。", family: "metatrack", comparisonVisible: false, mainVisible: false, ablationTarget: "metatrack" },
  { methodId: "metatrack_ab_state", title: "去掉状态预取", description: "状态访问改为 Home exact-version 路径，以去掉执行域本地预取收益；exact-version、Version Liveness、RouteBatch=100、分流、双轨、V669 共识和 PBFT 保持不变。", family: "metatrack", comparisonVisible: false, mainVisible: false, ablationTarget: "metatrack" },
  { methodId: "metatrack_ab_handoff", title: "历史子消融-无本地版本交接", description: "历史结果兼容项；不再进入新的正式消融矩阵。", family: "metatrack", comparisonVisible: false, mainVisible: false },
  { methodId: "metatrack_exp", title: "实验版（旧）", description: "历史流式实验结果兼容项，不再作为新实验卡片显示。", family: "metatrack", comparisonVisible: false, mainVisible: false },
];

export const BATCH_SI_ABLATION_METHOD_IDS = [
  "hash_batch_si",
  "hash_batch_si_no_wrbp",
  "hash_batch_si_no_ofas",
  "hash_batch_si_serial_batch",
  "hash_batch_si_txid_priority",
] as const;

export const METATRACK_ABLATION_METHOD_IDS = [
  "metatrack_latest",
  "metatrack_ab_route",
  "metatrack_ab_track",
  "metatrack_ab_state",
  "metatrack_ab_cons",
] as const;

export const PARALLEL_WORKER_OPTIONS = [1, 2, 4, 8] as const;

export const WORKER_SCALING_OPTIONS = [2, 4, 8, 16, 32] as const;

export const BATCH_SI_WORKER_SCALING_METHOD_IDS = [
  "hash_batch_si",
  "hash_cg",
  "hash_acg",
  "hash_bsx",
  "hash_aria",
  "hash_groundhog",
] as const;

export function methodDefinition(methodId: string): FormalMethodDefinition | undefined {
  return FORMAL_METHOD_DEFINITIONS.find((item) => item.methodId === methodId);
}
