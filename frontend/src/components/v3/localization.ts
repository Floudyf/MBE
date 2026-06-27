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
  fixed: "固定环境",
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
  consensus_only: "共识协议实验",
  sharding_only: "分片 / 路由实验",
  execution_scheduler_only: "执行调度实验",
  state_access_only: "状态访问实验",
  commit_only: "提交优化实验",
  committee_lifecycle_planned: "委员会生命周期实验（规划中）",
};

export const profileLabels: Record<string, string> = {
  metatrack_go_backed_ablation_smoke: "MetaTrack Go-backed 消融 Smoke",
  single_chain_role_separation_smoke: "单链角色拆分 Smoke",
  single_chain_composer_preview: "单链 Composer 预览",
};

export const methodLabels: Record<string, string> = {
  baseline_hash_only: "基线：哈希分片",
  co_access_only: "消融：共访问路由",
  co_access_dual_track: "消融：共访问 + 双轨执行",
  full_MetaTrack: "完整 MetaTrack",
};

export function labelFor(mapping: Record<string, string>, id?: string, fallback = "无"): string {
  if (!id) return fallback;
  return mapping[id] || id;
}

export function yesNo(value: unknown): string {
  return value ? "是" : "否";
}
