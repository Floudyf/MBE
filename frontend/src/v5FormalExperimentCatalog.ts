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
  ablationTarget?: "batch_si";
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
  { methodId: "hash_cg", title: "CG", description: "Batch-SI 原论文对照组：完整冲突图 + DAG 层级并行。", family: "stateful", comparisonVisible: true, mainVisible: false },
  { methodId: "hash_acg", title: "ACG / Nezha", description: "Batch-SI 原论文对照组：地址冲突图与层次调度。", family: "stateful", comparisonVisible: true, mainVisible: false },
  { methodId: "hash_bsx", title: "BSX", description: "Batch-SI 原论文对照组：无向冲突图 + 确定性图着色。", family: "stateful", comparisonVisible: true, mainVisible: false },
  { methodId: "hash_batch_si", title: "Batch-SI", description: "AWRT + WRBP + OFAS + 批快照并行。", family: "batch_si", comparisonVisible: true, mainVisible: true, ablationTarget: "batch_si", isFullVariant: true },
  { methodId: "hash_batch_si_no_wrbp", title: "w/o WRBP", description: "以顺序分批替代写机会批次回填，验证 WRBP 的批宽贡献。", family: "batch_si", comparisonVisible: false, mainVisible: false, ablationTarget: "batch_si" },
  { methodId: "hash_batch_si_no_ofas", title: "w/o OFAS", description: "使用 Batch-SI 内部完整依赖图排序替代 OFAS。", family: "batch_si", comparisonVisible: false, mainVisible: false, ablationTarget: "batch_si" },
  { methodId: "hash_batch_si_serial_batch", title: "w/o Snapshot Parallelism", description: "保持相同分批与排序，将批内执行改为单 Worker。", family: "batch_si", comparisonVisible: false, mainVisible: false, ablationTarget: "batch_si" },
  { methodId: "hash_batch_si_txid_priority", title: "w/o OFAS Priority", description: "保留 OFAS 正确性规则，仅取消论文读次数优先级。", family: "batch_si", comparisonVisible: false, mainVisible: false, ablationTarget: "batch_si" },
  { methodId: "stateless_hash_serial", title: "Stateless Hash + Serial", description: "无状态哈希路由的串行兼容参考。", family: "stateless", comparisonVisible: true, mainVisible: false },
  { methodId: "stateless_hash_block_stm", title: "Stateless Hash + Block-STM", description: "无状态哈希路由与 Block-STM 后端组合。", family: "stateless", comparisonVisible: true, mainVisible: false },
  { methodId: "metatrack_serial", title: "MetaTrack", description: "状态共访存路由与双轨执行的完整方案。", family: "metatrack", comparisonVisible: true, mainVisible: true },
  { methodId: "metatrack_block_stm", title: "MetaTrack + Block-STM", description: "MetaTrack 路由与 Block-STM 执行兼容组合。", family: "metatrack", comparisonVisible: true, mainVisible: false },
];

export const BATCH_SI_ABLATION_METHOD_IDS = [
  "hash_batch_si",
  "hash_batch_si_no_wrbp",
  "hash_batch_si_no_ofas",
  "hash_batch_si_serial_batch",
  "hash_batch_si_txid_priority",
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
