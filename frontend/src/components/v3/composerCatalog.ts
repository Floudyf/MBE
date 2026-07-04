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
  fixed: "已启用",
  variable: "实验变量",
  disabled: "已关闭",
  planned: "规划中",
  output: "输出模块",
};

export const pluginStatusLabels: Record<DraftPluginStatus, string> = {
  runnable: "可运行",
  preview: "预览",
  planned: "规划中",
};

export const composerCatalog: Record<string, ModuleCatalogEntry> = {
  Workload: {
    moduleId: "Workload",
    label: "负载生成",
    description: "生成或读取实验交易输入，控制交易数量、热点比例、到达率和随机种子。",
    defaultPlugin: "synthetic_hotspot",
    required: true,
    allowVariable: true,
    plugins: [
      { id: "synthetic_hotspot", label: "热点合成负载", status: "runnable" },
      { id: "default_synthetic", label: "默认合成负载", status: "runnable" },
      { id: "existing_trace", label: "已有 trace 回放", status: "preview" },
      { id: "saved_workload", label: "已保存 workload", status: "preview" },
      { id: "fabric_observation_trace", label: "Fabric 观测 trace", status: "planned" },
    ],
    params: ["tx_count", "seed", "submit_rate_tps", "hotspot_ratio", "zipf_alpha"],
  },
  TxPool: {
    moduleId: "TxPool",
    label: "交易池",
    description: "接收交易并决定交易进入区块前的排队策略。当前可运行路径以 FIFO 为主。",
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
      { id: "time_or_count_block_producer", label: "时间 / 数量出块", status: "runnable" },
      { id: "fixed_size_block", label: "固定大小区块", status: "planned" },
      { id: "adaptive_block_cut", label: "自适应切块", status: "planned" },
    ],
    params: ["block_interval_ms", "max_tx_per_block"],
  },
  Consensus: {
    moduleId: "Consensus",
    label: "共识排序",
    description: "为交易批次提供确定性排序。PBFT 网络预览是可选插件，不是生产 PBFT。",
    defaultPlugin: "simple_leader",
    required: true,
    plugins: [
      { id: "simple_leader", label: "简单 Leader", status: "runnable" },
      { id: "poa_light", label: "PoA 轻量模型", status: "runnable" },
      { id: "pbft_light_model", label: "PBFT 轻量模型", status: "runnable" },
      { id: "blockemulator_aligned_pbft_preview", label: "PBFT 网络预览", status: "runnable" },
      { id: "pbft", label: "真实 PBFT", status: "planned" },
      { id: "hotstuff", label: "HotStuff", status: "planned" },
      { id: "raft", label: "Raft", status: "planned" },
      { id: "committee_consensus", label: "委员会共识", status: "planned" },
    ],
  },
  CommitteeEpoch: {
    moduleId: "CommitteeEpoch",
    label: "委员会 / Epoch",
    description: "描述未来委员会生命周期与 epoch 切换。当前默认关闭。",
    defaultPlugin: "disabled",
    required: false,
    allowDisable: true,
    plugins: [
      { id: "disabled", label: "不启用", status: "runnable" },
      { id: "none", label: "不启用", status: "runnable" },
      { id: "fixed_epoch_placeholder", label: "固定 Epoch 占位", status: "preview" },
      { id: "committee_lifecycle_planned", label: "委员会生命周期", status: "planned" },
      { id: "adaptive_committee_lifecycle", label: "自适应委员会生命周期", status: "planned" },
    ],
    notes: ["当前不作为可运行实验变量。"],
  },
  Routing: {
    moduleId: "Routing",
    label: "分片 / 路由",
    description: "决定交易或状态访问路由到哪个执行分片。CrossShardProtocol 是本模块下的子能力，不是新的主流程卡片。",
    defaultPlugin: "hash_sharding",
    required: true,
    allowVariable: true,
    plugins: [
      { id: "hash_sharding", label: "Hash 路由", status: "runnable" },
      { id: "metatrack_coaccess_routing", label: "MetaTrack 共访问路由", status: "runnable" },
      { id: "hotspot_aware_routing", label: "热点感知路由", status: "runnable" },
      { id: "co_access_sharding", label: "共访问路由（兼容别名）", status: "preview" },
      { id: "clpa_like_partitioning", label: "CLPA-like 分区", status: "planned" },
      { id: "shardcutter_like_partitioning", label: "ShardCutter-like 分区", status: "planned" },
      { id: "relay_cross_shard", label: "Relay 跨片策略", status: "planned" },
      { id: "broker_cross_shard", label: "Broker 跨片策略", status: "planned" },
      { id: "two_phase_commit", label: "2PC 跨片策略", status: "planned" },
      { id: "dynamic_resharding", label: "动态重分片", status: "planned" },
    ],
  },
  Execution: {
    moduleId: "Execution",
    label: "交易执行",
    description: "执行排序后的交易，并记录串行、轻量并行、双轨等本地模型指标。",
    defaultPlugin: "serial_execution",
    required: true,
    allowVariable: true,
    plugins: [
      { id: "serial_execution", label: "串行执行", status: "runnable" },
      { id: "parallel_light_execution", label: "轻量并行模型", status: "runnable" },
      { id: "metatrack_dual_track_execution", label: "MetaTrack 双轨模型", status: "runnable" },
      { id: "dual_track_execution", label: "双轨执行（兼容别名）", status: "preview" },
      { id: "block_stm_like", label: "Block-STM-like", status: "planned" },
      { id: "calvin_like_model", label: "Calvin-like 模型", status: "planned" },
      { id: "real_optimistic_execution", label: "真实乐观执行", status: "planned" },
      { id: "real_rollback_engine", label: "真实回滚引擎", status: "planned" },
    ],
  },
  StateAccess: {
    moduleId: "StateAccess",
    label: "状态访问",
    description: "为交易执行提供状态读取路径。Proof / Witness 是 V3.9 MVP 产物，不是完整无状态执行。",
    defaultPlugin: "direct_fetch",
    required: true,
    allowVariable: true,
    plugins: [
      { id: "direct_fetch", label: "直接读取", status: "runnable" },
      { id: "remote_state_access_model", label: "远程访问轻量模型", status: "runnable" },
      { id: "cached_state_access", label: "缓存访问轻量模型", status: "runnable" },
      { id: "access_list_prefetch", label: "访问列表预取", status: "runnable" },
      { id: "real_witness_fetch", label: "真实 witness 获取", status: "planned" },
      { id: "real_proof_fetch", label: "真实 proof 获取", status: "planned" },
      { id: "mpt_proof_model", label: "MPT proof 模型", status: "planned" },
      { id: "persistent_kv_access", label: "Persistent KV 访问", status: "planned" },
    ],
  },
  StateStorage: {
    moduleId: "StateStorage",
    label: "状态存储",
    description: "维护状态的存储单元模型。persistent_kv / merkle_trie_mvp 通过状态后端选择控制。",
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
    description: "提交执行结果，记录普通提交、保守提交、热点聚合和约束检查聚合等本地模型。",
    defaultPlugin: "normal_commit",
    required: true,
    allowVariable: true,
    plugins: [
      { id: "normal_commit", label: "普通提交", status: "runnable" },
      { id: "conservative_commit", label: "保守提交", status: "runnable" },
      { id: "hot_update_aggregation", label: "热点更新聚合", status: "runnable" },
      { id: "constraint_checked_aggregation", label: "约束检查聚合", status: "runnable" },
      { id: "hot_update_aggregation_commit", label: "热点更新聚合（兼容别名）", status: "preview" },
      { id: "atomic_reservation_commit", label: "原子预留提交", status: "planned" },
      { id: "batch_commit", label: "批量提交", status: "planned" },
      { id: "real_db_lock_commit", label: "真实 DB 锁提交", status: "planned" },
    ],
  },
  MetricsReport: {
    moduleId: "MetricsReport",
    label: "指标 / 报告",
    description: "汇总 TPS、延迟、成功失败计数、机制指标和报告产物。",
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
    description: "该模块来自 composer preview，前端 catalog 暂无额外说明。",
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
