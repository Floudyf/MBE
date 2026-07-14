export const suiteLabel = (value: string) => ({
  main_experiment: "主实验", comparison_experiment: "方法对比实验", ablation_experiment: "消融实验",
  workload_sensitivity: "负载敏感性实验", topology_scaling: "拓扑扩展性实验", fault_recovery_experiment: "故障与恢复实验",
}[value] ?? value);

export const categoryLabel = (value: string) => ({
  transaction_admission: "交易准入", txpool: "交易池", sharding: "分片", routing: "路由", block_producer: "出块器",
  consensus: "共识", network: "网络", execution: "执行", scheduler: "调度", state_access: "状态访问",
  state_storage: "状态存储", cross_shard: "跨片", commit: "提交", metrics: "指标", observability: "可观测性",
  workload: "负载", fault_injection: "故障注入",
}[value] ?? value);

export const roleLabel = (value: string) => ({ main: "完整方法", baseline: "基线方法", ablation: "消融方法", custom: "自定义方法" }[value] ?? value);
export const statusLabel = (value: string) => ({ queued: "排队中", starting: "启动中", running: "运行中", completed: "已完成", completed_with_failures: "完成但存在失败", failed: "失败", cancelled: "已取消", blocked: "已阻止", runnable: "可运行" }[value] ?? value);
export const backendLabel = (value: string) => value === "real_cluster" ? "真实多进程集群" : value;
export const truthLabel = (value: string) => value === "v5_real_cluster_candidate" ? "本地真实集群研究运行时" : value;
export const booleanLabel = (value: unknown) => value === true ? "是" : value === false ? "否" : "—";
export const faultModeLabel = (value: string) => ({ disabled: "无故障", delay_only: "网络延迟", network_drop: "随机丢包" }[value] ?? value);
export const missingLabel = (value: unknown) => value === undefined || value === null || value === "" ? "—" : String(value);
export const blockerLabel = (value: string) => value
  .replace("cross-shard experiments with message loss or node restart are not supported because Relay/SourceFinalize reliable retransmission is not implemented", "跨片实验不支持丢包或节点重启，因为尚未实现 Relay/SourceFinalize 的可靠重传。")
  .replace("cross_shard_ratio requires at least 2 shards", "跨片交易比例要求至少两个分片。")
  .replace("nodes must equal shards * validators_per_shard for V5 committee topology", "节点数必须等于分片数乘以每片验证节点数。")
  .replace("fast_first_scheduler requires dual_track_execution", "快速优先调度器需要双轨执行插件。");
