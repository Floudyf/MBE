export type DraftModuleStatus = "default" | "fixed" | "variable" | "disabled" | "planned" | "output";
export type DraftPluginStatus = "runnable" | "preview" | "planned";

export type DraftPluginOption = {
  id: string;
  label: string;
  status: DraftPluginStatus;
};

export type ModuleCatalogEntry = {
  moduleId: string;
  label: string;
  description: string;
  defaultPlugin: string;
  required: boolean;
  output?: boolean;
  allowVariable?: boolean;
  allowDisable?: boolean;
  plugins: DraftPluginOption[];
  params?: string[];
  notes?: string[];
};

export const requiredModuleIds = new Set([
  "Workload",
  "TxPool",
  "BlockProducer",
  "Consensus",
  "Routing",
  "Execution",
  "StateAccess",
  "StateStorage",
  "Commit",
  "MetricsReport",
]);

export const optionalModuleIds = new Set([
  "CommitteeEpoch",
  "ShardReconfiguration",
  "StateMigration",
  "FabricValidation",
  "MetaFlow",
]);

export const statusLabels: Record<DraftModuleStatus, string> = {
  default: "默认配置",
  fixed: "固定环境",
  variable: "实验变量",
  disabled: "已关闭",
  planned: "规划中",
  output: "输出模块",
};

export const pluginStatusLabels: Record<DraftPluginStatus, string> = {
  runnable: "可运行",
  preview: "仅预览",
  planned: "规划中",
};

export const composerCatalog: Record<string, ModuleCatalogEntry> = {
  Workload: {
    moduleId: "Workload",
    label: "负载生成",
    description: "生成或读取实验交易输入，决定交易数量、到达率、热点比例和随机种子。",
    defaultPlugin: "synthetic_hotspot",
    required: true,
    allowVariable: true,
    plugins: [
      { id: "default_synthetic", label: "默认合成负载", status: "runnable" },
      { id: "synthetic_hotspot", label: "热点合成负载", status: "runnable" },
      { id: "existing_trace", label: "已有 trace 回放", status: "preview" },
      { id: "saved_workload", label: "已保存 workload", status: "preview" },
      { id: "fabric_observation_trace", label: "Fabric 观测 trace", status: "planned" },
    ],
    params: ["tx_count", "seed", "submit_rate_tps", "hotspot_ratio", "zipf_alpha"],
  },
  TxPool: {
    moduleId: "TxPool",
    label: "交易池",
    description: "接收负载提交的交易，并决定交易进入区块前的排队策略。",
    defaultPlugin: "fifo_pool",
    required: true,
    plugins: [
      { id: "fifo_pool", label: "FIFO 交易池", status: "runnable" },
      { id: "priority_pool", label: "优先级交易池", status: "planned" },
      { id: "hotspot_aware_pool", label: "热点感知交易池", status: "planned" },
      { id: "fee_based_pool", label: "费用优先交易池", status: "planned" },
    ],
  },
  BlockProducer: {
    moduleId: "BlockProducer",
    label: "区块生成",
    description: "按时间或交易数量切分区块，为共识排序提供批次。",
    defaultPlugin: "time_or_count_block_producer",
    required: true,
    plugins: [
      { id: "time_or_count_block_producer", label: "时间或数量出块", status: "runnable" },
      { id: "fixed_size_block", label: "固定大小区块", status: "planned" },
      { id: "adaptive_block_cut", label: "自适应切块", status: "planned" },
    ],
    params: ["block_interval_ms", "max_tx_per_block"],
  },
  Consensus: {
    moduleId: "Consensus",
    label: "共识排序",
    description: "为单链交易批次提供确定性排序。本阶段只使用轻量 leader 排序模型。",
    defaultPlugin: "simple_leader",
    required: true,
    plugins: [
      { id: "simple_leader", label: "简单 leader 排序", status: "runnable" },
      { id: "poa_light", label: "PoA-light authority model", status: "runnable" },
      { id: "pbft_light_model", label: "PBFT-light stage/quorum model", status: "runnable" },
      { id: "pbft_model", label: "PBFT 模型", status: "planned" },
      { id: "pbft", label: "Real PBFT unsupported", status: "planned" },
      { id: "real_pbft", label: "Real PBFT unsupported", status: "planned" },
      { id: "pbft_planned", label: "PBFT 模型", status: "planned" },
      { id: "hotstuff_model", label: "HotStuff 模型", status: "planned" },
      { id: "hotstuff", label: "HotStuff unsupported", status: "planned" },
      { id: "hotstuff_planned", label: "HotStuff 模型", status: "planned" },
      { id: "raft_model", label: "Raft 模型", status: "planned" },
      { id: "raft", label: "Raft unsupported", status: "planned" },
      { id: "committee_consensus", label: "委员会共识", status: "planned" },
    ],
  },
  CommitteeEpoch: {
    moduleId: "CommitteeEpoch",
    label: "委员会 / Epoch",
    description: "描述未来委员会生命周期与 epoch 切换。本阶段只允许关闭或预览，不进入可运行实验变量。",
    defaultPlugin: "disabled",
    required: false,
    allowDisable: true,
    plugins: [
      { id: "disabled", label: "当前关闭", status: "runnable" },
      { id: "none", label: "当前关闭", status: "runnable" },
      { id: "fixed_epoch_placeholder", label: "固定 epoch 占位", status: "preview" },
      { id: "fixed_epoch_planned", label: "固定 epoch 占位", status: "preview" },
      { id: "committee_lifecycle_planned", label: "委员会生命周期", status: "planned" },
      { id: "adaptive_committee_lifecycle", label: "自适应委员会生命周期", status: "planned" },
    ],
    notes: ["当前不允许作为可运行实验变量。"],
  },
  Routing: {
    moduleId: "Routing",
    label: "分片 / 路由",
    description: "决定交易或状态访问被路由到哪个执行分片。co_access_sharding 只改变执行侧路由 M_t，不迁移状态持久位置 φ(key)。",
    defaultPlugin: "hash_sharding",
    required: true,
    allowVariable: true,
    plugins: [
      { id: "hash_sharding", label: "Hash 路由", status: "runnable" },
      { id: "co_access_sharding", label: "共访问路由", status: "runnable" },
      { id: "range_sharding", label: "Range 路由", status: "planned" },
      { id: "dynamic_resharding", label: "动态重分片", status: "planned" },
      { id: "dynamic_resharding_planned", label: "动态重分片", status: "planned" },
    ],
  },
  Execution: {
    moduleId: "Execution",
    label: "交易执行",
    description: "执行排序后的交易，并决定串行、双轨或未来并行调度策略。",
    defaultPlugin: "serial_execution",
    required: true,
    allowVariable: true,
    plugins: [
      { id: "serial_execution", label: "串行执行", status: "runnable" },
      { id: "dual_track_execution", label: "双轨执行", status: "runnable" },
      { id: "parallel_naive", label: "朴素并行", status: "planned" },
      { id: "block_stm_like", label: "Block-STM-like", status: "planned" },
      { id: "block_stm_like_planned", label: "Block-STM-like", status: "planned" },
    ],
  },
  StateAccess: {
    moduleId: "StateAccess",
    label: "状态访问",
    description: "为交易执行提供状态读取路径，可比较直接读取与访问列表预取。",
    defaultPlugin: "direct_fetch",
    required: true,
    allowVariable: true,
    plugins: [
      { id: "direct_fetch", label: "直接读取", status: "runnable" },
      { id: "access_list_prefetch", label: "访问列表预取", status: "runnable" },
      { id: "cache_prefetch", label: "缓存预取", status: "planned" },
      { id: "cache_prefetch_planned", label: "缓存预取", status: "planned" },
      { id: "witness_prefetch", label: "Witness 预取", status: "planned" },
      { id: "witness_prefetch_planned", label: "Witness 预取", status: "planned" },
    ],
  },
  StateStorage: {
    moduleId: "StateStorage",
    label: "状态存储",
    description: "维护状态的持久位置与存储单元模型，当前用于单链本地 replay。",
    defaultPlugin: "hash_state_storage",
    required: true,
    plugins: [
      { id: "memory_kv", label: "内存 KV", status: "runnable" },
      { id: "hash_state_storage", label: "Hash 状态存储", status: "runnable" },
      { id: "lsm_model", label: "LSM 模型", status: "planned" },
      { id: "partitioned_state_storage", label: "分区状态存储", status: "planned" },
    ],
    params: ["storage_unit_count", "placement_policy", "remote_fetch_cost_ms"],
  },
  Commit: {
    moduleId: "Commit",
    label: "状态提交",
    description: "提交执行结果。热点聚合只适用于可交换增量更新，不能对所有交易强行聚合。",
    defaultPlugin: "normal_commit",
    required: true,
    allowVariable: true,
    plugins: [
      { id: "normal_commit", label: "普通提交", status: "runnable" },
      { id: "hot_update_aggregation_commit", label: "热点更新聚合提交", status: "runnable" },
      { id: "batch_commit", label: "批量提交", status: "planned" },
      { id: "batch_commit_planned", label: "批量提交", status: "planned" },
      { id: "two_phase_commit", label: "两阶段提交", status: "planned" },
    ],
  },
  MetricsReport: {
    moduleId: "MetricsReport",
    label: "指标 / 报告",
    description: "汇总 TPS、平均延迟、P95、P99、成功数、失败数和报告产物。",
    defaultPlugin: "basic_metrics",
    required: true,
    output: true,
    plugins: [
      { id: "basic_metrics", label: "基础指标", status: "runnable" },
      { id: "metatrack_metrics", label: "MetaTrack 指标", status: "runnable" },
      { id: "consensus_metrics", label: "共识指标", status: "planned" },
      { id: "sharding_metrics", label: "分片指标", status: "planned" },
      { id: "committee_metrics", label: "委员会指标", status: "planned" },
    ],
  },
};

export function moduleCatalogEntry(moduleId: string): ModuleCatalogEntry {
  return composerCatalog[moduleId] || {
    moduleId,
    label: moduleId,
    description: "本模块来自 composer preview，前端 catalog 暂无额外说明。",
    defaultPlugin: "default",
    required: requiredModuleIds.has(moduleId),
    allowDisable: optionalModuleIds.has(moduleId),
    plugins: [{ id: "default", label: "默认插件", status: "runnable" }],
  };
}

export function pluginOption(moduleId: string, pluginId: string): DraftPluginOption {
  return moduleCatalogEntry(moduleId).plugins.find((plugin) => plugin.id === pluginId) || {
    id: pluginId,
    label: pluginId,
    status: pluginId.includes("planned") ? "planned" : "runnable",
  };
}
