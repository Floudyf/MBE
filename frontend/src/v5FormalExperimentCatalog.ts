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
  { methodId: "stateful_optme", title: "OptME", description: "SC 2024 算法复现（MBE 多分片集成）：每个物理 PBFT 分片在本地持久状态上，对本分片共识输出执行原 OptME 的并行模拟、地址冲突图、层次排序、First-Updater-Wins、重排与二次执行；多分片拓扑不作为原论文贡献。", family: "stateful", comparisonVisible: true, mainVisible: true },
  { methodId: "stateful_txallo", title: "TxAllo", description: "ICDE 2023 原论文版：只使用评测窗口之前的历史账户交易图运行 G-TxAllo/A-TxAllo，冻结账户→分片映射后执行正式负载。", family: "stateful", comparisonVisible: true, mainVisible: true },
  { methodId: "hash_batch_si", title: "Batch-SI", description: "AWRT + WRBP + OFAS + 批快照并行。", family: "batch_si", comparisonVisible: true, mainVisible: true, ablationTarget: "batch_si", isFullVariant: true },
  { methodId: "hash_batch_si_no_wrbp", title: "w/o WRBP", description: "以顺序分批替代写机会批次回填，验证 WRBP 的批宽贡献。", family: "batch_si", comparisonVisible: false, mainVisible: false, ablationTarget: "batch_si" },
  { methodId: "hash_batch_si_no_ofas", title: "w/o OFAS", description: "使用 Batch-SI 内部完整依赖图排序替代 OFAS。", family: "batch_si", comparisonVisible: false, mainVisible: false, ablationTarget: "batch_si" },
  { methodId: "hash_batch_si_serial_batch", title: "w/o Snapshot Parallelism", description: "保持相同分批与排序，将批内执行改为单 Worker。", family: "batch_si", comparisonVisible: false, mainVisible: false, ablationTarget: "batch_si" },
  { methodId: "hash_batch_si_txid_priority", title: "w/o OFAS Priority", description: "保留 OFAS 正确性规则，仅取消论文读次数优先级。", family: "batch_si", comparisonVisible: false, mainVisible: false, ablationTarget: "batch_si" },
  { methodId: "stateless_calvin", title: "Stateless Calvin", description: "Calvin 无状态兼容适配：保留全局确定性顺序与 S/X 锁调度，状态前驱/生产版本由 PBFT 共识绑定的 Calvin 块顺序生成，再按签名 AccessList 从状态归属分区真实取值并写回；不使用 MetaTrack 双轨、StateReady 或前沿控制。 /* MBE_CALVIN_CONSENSUS_VERSION_PLAN_V34 */", family: "stateless", comparisonVisible: true, mainVisible: false },
  { methodId: "stateless_hash_serial", title: "Stateless Hash + Serial", description: "无状态哈希路由的串行兼容参考。", family: "stateless", comparisonVisible: true, mainVisible: false },
  // MBE_PORYGON_PAPER_REPRO_20260921_V8_REFACTOR
  { methodId: "stateless_porygon", title: "Porygon", description: "ICDE 2024：前端分片数直接映射为执行分片/ESC 数；所有节点共享一个全局 Ordering/PBFT 域，跨 ESC 按 Porygon 单片执行与多片更新语义处理。", family: "stateless", comparisonVisible: true, mainVisible: false },
  { methodId: "stateless_hash_block_stm", title: "Stateless Hash + Block-STM", description: "无状态哈希路由与 Block-STM 后端组合。", family: "stateless", comparisonVisible: true, mainVisible: false },
  { methodId: "stateless_optme", title: "Stateless-OptME", description: "OptME 无状态多分片适配：每个 PBFT 分片在共识后独立运行同一 OptME 核心；AccessList 只限定远程状态投影，调度仍来自真实模拟 ReadSet/WriteSet。", family: "stateless", comparisonVisible: true, mainVisible: false },
  { methodId: "stateless_txallo", title: "Stateless-TxAllo", description: "TxAllo 无状态适配：账户图、G/A-TxAllo 与冻结映射和原版一致，仅将执行状态承载替换为通用无状态远程获取/写回。", family: "stateless", comparisonVisible: true, mainVisible: false },
  { methodId: "metatrack_serial", title: "MetaTrack（初始版）", description: "历史兼容方法；保留旧结果解析，不再作为新实验卡片显示。", family: "metatrack", comparisonVisible: false, mainVisible: false },
  { methodId: "metatrack_full_locality", title: "MetaTrack（当前版）", description: "历史兼容方法；保留旧结果解析，不再作为新实验卡片显示。", family: "metatrack", comparisonVisible: false, mainVisible: false },
{ methodId: "metatrack_latest", title: "Metatrack", description: "完整版本：工作负载/依赖感知分流 + 有效独立前沿双轨 + 原始 V661 关键路径保持共识窗口 + 执行域本地 exact-version。", family: "metatrack", comparisonVisible: true, mainVisible: true, ablationTarget: "metatrack", isFullVariant: true },
  { methodId: "metatrack_unified", title: "实验版", description: "恢复已验证的 v6.5.6.8 自适应共识窗口：保持完整 RouteBatch 投影，只在加入下一批后 N/L（窗口交易数/真实执行前驱最长链）严格提高且不超过 block_size 硬上限时聚合；无经验阈值。其余分流、双轨、StateReady、状态本地化、Version Liveness 与 PBFT 均与 Metatrack 一致。", family: "metatrack", comparisonVisible: true, mainVisible: true },
  { methodId: "metatrack_ab_route", title: "消融-哈希分片", description: "仅将 routing 模块替换为确定性哈希分片；执行、共识窗口、状态访问与 PBFT 保持不变。", family: "metatrack", comparisonVisible: false, mainVisible: false, ablationTarget: "metatrack" },
  { methodId: "metatrack_ab_track", title: "消融-无双轨统一就绪", description: "仅将 execution 模块替换为真正的单轨执行插件：保留同一依赖 DAG、StateReady、Worker、分流、V661 共识窗口与本地 exact-version 状态访问，但删除 Fast/Conservative 轨道判定、双 Ready 队列和 Fast-first；所有已就绪交易进入统一 FIFO Ready 队列。", family: "metatrack", comparisonVisible: true, mainVisible: true, ablationTarget: "metatrack" },
  { methodId: "metatrack_ab_cons", title: "消融-固定批次共识", description: "仅将 block_producer 模块替换为一个完整 RouteBatch 一个 PBFT 单元；PBFT、Version Liveness 与执行路径保持不变。", family: "metatrack", comparisonVisible: false, mainVisible: false, ablationTarget: "metatrack" },
  { methodId: "metatrack_ab_state", title: "消融-Home状态访问", description: "仅将 state_access 模块替换为 Home exact-version；预取、批量传输、Version Liveness、状态裁剪和其它模块保持不变。", family: "metatrack", comparisonVisible: false, mainVisible: false, ablationTarget: "metatrack" },
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
