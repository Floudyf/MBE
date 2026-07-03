export const moduleNames: Record<string, string> = {
  Workload: "负载生成",
  TxPool: "交易池",
  BlockProducer: "区块生成",
  Consensus: "共识排序",
  CommitteeEpoch: "委员会 / Epoch",
  Routing: "分片 / 路由",
  Execution: "交易执行",
  StateAccess: "状态访问",
  StateStorage: "状态存储",
  Commit: "状态提交",
  MetricsReport: "指标 / 报告",
};

export const moduleShortNames: Record<string, string> = {
  Workload: "负载",
  TxPool: "交易池",
  BlockProducer: "出块",
  Consensus: "共识",
  CommitteeEpoch: "委员会",
  Routing: "路由",
  Execution: "执行",
  StateAccess: "访问",
  StateStorage: "存储",
  Commit: "提交",
  MetricsReport: "指标",
};

export const statusLabels: Record<string, string> = {
  default: "默认配置",
  fixed: "模板固定",
  variable: "实验变量",
  disabled: "已关闭",
  planned: "规划中",
  output: "输出模块",
};

export const roleLabels: Record<string, string> = {
  input: "输入",
  environment: "固定环境",
  research_variable: "研究变量",
  planned: "规划中",
  disabled: "已关闭",
  output: "输出",
  validation: "验证",
};

export const tagLabels: Record<string, string> = {
  baseline: "基线",
  proposed: "提出方法",
  MetaTrack: "MetaTrack",
  environment: "环境",
  future: "后续扩展",
  validation: "验证",
  single_chain: "单链",
  output: "输出",
  planned: "规划中",
};

export const templateLabels: Record<string, string> = {
  metatrack_ablation: "MetaTrack 消融实验",
  single_module_execution: "执行模块实验",
  single_module_state_access: "状态访问实验",
  single_module_commit: "提交模块实验",
  consensus_only: "共识协议实验",
  sharding_only: "分片 / 路由实验",
  execution_scheduler_only: "执行调度实验",
  state_access_only: "状态访问实验",
  commit_only: "提交优化实验",
  committee_lifecycle_planned: "委员会生命周期实验（规划中）",
};

export const profileLabels: Record<string, string> = {
  metatrack_go_backed_ablation_smoke: "MetaTrack Go-backed 消融快速验证",
  single_chain_role_separation_smoke: "单链角色拆分快速验证",
  single_chain_composer_preview: "单链 Composer 预览",
};

export const methodLabels: Record<string, string> = {
  baseline_hash_only: "基线：Hash 分片",
  co_access_only: "消融：共访问路由",
  co_access_dual_track: "消融：共访问 + 双轨执行",
  full_MetaTrack: "完整 MetaTrack",
};

export const pluginLabels: Record<string, string> = {
  default_synthetic: "默认合成负载",
  synthetic_hotspot: "热点合成负载",
  existing_trace: "已有 trace 回放",
  saved_workload: "已保存 workload",
  fabric_observation_trace: "Fabric 观测 trace",
  fifo_pool: "FIFO 交易池",
  priority_pool: "优先级交易池",
  hotspot_aware_pool: "热点感知交易池",
  fee_based_pool: "费用优先交易池",
  time_or_count_block_producer: "时间 / 数量出块",
  fixed_size_block: "固定大小区块",
  adaptive_block_cut: "自适应切块",
  simple_leader: "简单 Leader",
  poa_light: "PoA 轻量模型",
  pbft_light_model: "PBFT 轻量模型",
  blockemulator_aligned_pbft_preview: "PBFT 网络预览",
  disabled: "不启用",
  none: "不启用",
  fixed_epoch_placeholder: "固定 Epoch 占位",
  fixed_epoch_planned: "固定 Epoch 占位",
  committee_lifecycle_planned: "委员会生命周期",
  adaptive_committee_lifecycle: "自适应委员会生命周期",
  hash_sharding: "Hash 路由",
  metatrack_coaccess_routing: "MetaTrack 共访问路由",
  co_access_sharding: "共访问路由",
  hotspot_aware_routing: "热点感知路由",
  clpa_like_partitioning: "CLPA-like 分区",
  shardcutter_like_partitioning: "ShardCutter-like 分区",
  relay_cross_shard: "Relay 跨片策略",
  broker_cross_shard: "Broker 跨片策略",
  two_phase_commit: "2PC 跨片策略",
  range_sharding: "Range 路由",
  dynamic_resharding: "动态重分片",
  serial_execution: "串行执行",
  dual_track_execution: "双轨执行",
  parallel_light_execution: "轻量并行模型",
  metatrack_dual_track_execution: "MetaTrack 双轨模型",
  block_stm_like: "Block-STM-like",
  calvin_like_model: "Calvin-like 模型",
  direct_fetch: "直接读取",
  remote_state_access_model: "远程访问轻量模型",
  cached_state_access: "缓存访问轻量模型",
  access_list_prefetch: "访问列表预取",
  real_witness_fetch: "真实 witness 获取",
  real_proof_fetch: "真实 proof 获取",
  mpt_proof_model: "MPT proof 模型",
  persistent_kv_access: "Persistent KV 访问",
  memory_kv: "内存 KV",
  hash_state_storage: "Hash 状态存储",
  lsm_model: "LSM 模型",
  partitioned_state_storage: "分区状态存储",
  normal_commit: "普通提交",
  conservative_commit: "保守提交",
  hot_update_aggregation: "热点更新聚合",
  hot_update_aggregation_commit: "热点更新聚合",
  constraint_checked_aggregation: "约束检查聚合",
  atomic_reservation_commit: "原子预留提交",
  batch_commit: "批量提交",
  real_db_lock_commit: "真实 DB 锁提交",
  basic_metrics: "基础指标",
  metatrack_metrics: "MetaTrack 指标",
  consensus_metrics: "共识指标",
  sharding_metrics: "分片指标",
  committee_metrics: "委员会指标",
};

export function labelFor(mapping: Record<string, string>, id?: string, fallback = "无"): string {
  if (!id) return fallback;
  return mapping[id] || id;
}

export function yesNo(value: unknown): string {
  return value ? "是" : "否";
}
