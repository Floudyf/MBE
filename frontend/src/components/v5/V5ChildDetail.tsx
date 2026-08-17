// MBE_V5_RESULTS_UI_TRUTH_CN_FINAL_20260814_V5
// MBE_V5_RESULTS_UI_FINAL_CN_CLOSURE_20260814_V4
import { useState } from "react";

import { v5RealClusterArtifactURL, type V5FinalityEvidence, type V5FormalChildRun, type V5RealClusterSummary, type V5RuntimeArtifact } from "../../api";
import { formatHash, truthText, valueText } from "../../workloadUi";

const workloadArtifacts = ["workload_manifest_snapshot.json", "workload_source_spec.json", "workload_selection.json", "workload_skew_report.json", "workload_materialization_summary.json", "workload_identity_mapping_summary.json", "workload_replay_summary.json"];
const blockExecutorArtifacts = ["block_execution_summary.json", "execution_plan.jsonl", "transaction_execution_trace.csv", "state_delta_log.csv", "plan_digest_consistency.csv"];
const blockSTMArtifacts = ["block_stm_summary.json", "block_stm_task_trace.csv", "block_stm_validation_trace.csv", "block_stm_abort_trace.csv", "block_stm_dependency_trace.csv", "incarnation_summary.csv", "serial_equivalence.json"];
const metatrackArtifacts = ["metatrack_batch_plan.jsonl", "access_matrix_summary.csv", "state_frequency.csv", "coaccess_matrix_edges.csv", "placement_plan.csv", "transaction_placement.csv", "dependency_graph.csv", "track_classification.csv", "metatrack_scheduler_trace.csv", "predicted_remote_access.csv", "physical_remote_state_operations.csv", "aggregate/replica_deduplicated_remote_operations.csv", "aggregate/remote_state_metrics_summary.json", "aggregation_plan.csv", "logical_physical_update_mapping.csv"];
const preferred = [...workloadArtifacts, ...blockExecutorArtifacts, ...blockSTMArtifacts, ...metatrackArtifacts, "real_cluster_summary.json", "finality_summary.json", "transaction_lifecycle.csv", "transaction_lifecycle.jsonl", "transaction_finality.csv", "latency_distribution.csv", "throughput_windows.csv", "drain_status.json", "client_receipt_log.csv", "compiled_run_plan.json", "supervisor_stdout.log", "supervisor_stderr.log"];
type Row = [label: string, value: unknown, testId?: string];

export default function V5ChildDetail({ child }: { child: V5FormalChildRun | null }) {
  const [query, setQuery] = useState("");
  if (!child) return <section className="final-card wide" data-testid="v5-child-detail"><p className="muted">请选择一个子实验。</p></section>;
  const summary = child.result?.summary;
  const finality = summary?.finality_evidence;
  const metrics = child.metrics ?? {};
  const artifacts = (child.result?.artifacts ?? []).filter((item) => item.name.toLowerCase().includes(query.toLowerCase()));
  return <section className="final-card wide" data-testid="v5-child-detail">
    <h2>子实验详情</h2>
    <h3>运行标识</h3><Grid values={[["子实验 ID", child.child_run_id], ["运行 ID", child.result?.run_id], ["实验类型", suiteText(child.suite_type)], ["方法", displayMethodName(child.method.display_name)], ["方法配置 ID", child.method_config_id, "v5-metric-method-config-id"], ["随机种子", child.seed], ["重复序号", child.repeat_index + 1], ["尝试次数", child.attempt], ["拓扑", `${child.topology_point.nodes}/${child.topology_point.shards}/${child.topology_point.validators_per_shard}`], ["交易数量", child.estimated_transactions], ["状态", statusText(child.status)], ["执行状态", statusText(String(child.execution_status ?? child.result?.summary?.execution_status ?? ""))], ["产物状态", artifactStatusText(String(child.artifact_status ?? child.result?.summary?.artifact_status ?? ""))], ["正式结果", (child.formal_eligibility ?? child.result?.summary?.formal_eligibility) === true ? "可用" : (child.formal_eligibility ?? child.result?.summary?.formal_eligibility) === false ? "不可用" : "未提供"], ["论文候选", child.paper_candidate], ["对比组", child.comparison_group_id], ["扫描点", `${child.scan_variable || "—"}: ${child.scan_value || "—"}`]]} />
    <h3>性能指标</h3><Grid values={[["端到端 TPS", metrics.end_to_end_tps ?? finality?.end_to_end_tps ?? metrics.throughput_tps ?? finality?.throughput_tps, "v5-metric-end-to-end-tps"], ["逻辑终态 TPS", metrics.logical_finality_tps ?? finality?.logical_finality_tps, "v5-metric-logical-finality-tps"], ["完成时长（ms）", metrics.completion_duration_ms ?? finality?.completion_duration_ms, "v5-metric-completion-duration"], ["尾部完成开销（ms）", metrics.tail_completion_overhead_ms ?? finality?.tail_completion_overhead_ms, "v5-metric-tail-completion-overhead"], ["P50 终态延迟", metrics.p50_finality_ms ?? metrics.p50_latency_ms ?? finality?.p50_finality_ms], ["P95 终态延迟", metrics.p95_finality_ms ?? metrics.p95_latency_ms ?? finality?.p95_finality_ms], ["P99 终态延迟", metrics.p99_finality_ms ?? metrics.p99_latency_ms ?? finality?.p99_finality_ms, "v5-metric-p99-finality"], ["已最终确认", metrics.finalized_tx_count], ["生命周期完整", metrics.lifecycle_complete], ["缺失指标", Array.isArray(metrics.missing) ? metrics.missing.join(", ") : metrics.missing]]} />
    <div data-testid="v5-artifact-contract"><h3>产物契约</h3><Grid values={artifactContractRows(summary, metrics)} /></div>
    <WorkloadTruth summary={summary} child={child} />
    <div data-testid="v5-finality-summary"><h3>最终确认指标</h3><Grid values={finalityRows(finality)} /></div>
    <div data-testid="v5-runtime-evidence"><h3>运行真实性证据</h3><Grid values={runtimeRows(summary)} /><h4>区块执行引擎</h4><Grid values={blockExecutorRows(summary)} /><p className="muted">运行阶段：{value(summary?.runtime_stage)}；运行真实性：{value(summary?.runtime_truth)}。生产级区块链：否；生产级 PBFT：否。</p></div>
    <div data-testid="v5-mechanism-metrics"><h3>机制指标</h3><Grid values={mechanismRows(metrics)} /></div>
    {child.error && <p className="file-error">子实验错误：{child.error}</p>}
    <div data-testid="v5-child-artifact-catalog"><h3>子实验运行产物</h3><label><span>搜索产物</span><input aria-label="子实验产物搜索" value={query} onChange={(event) => setQuery(event.target.value)} /></label><ArtifactCatalog artifacts={artifacts} /></div>
  </section>;
}

function suiteText(value: string): string { return ({ main_experiment: "主实验", comparison_experiment: "方法对比", ablation_experiment: "消融实验", workload_sensitivity: "工作负载敏感性", topology_scaling: "拓扑扩展", fault_recovery_experiment: "故障恢复" } as Record<string, string>)[value] ?? value; }
function statusText(value: string): string { return ({ completed: "已完成", completed_with_failures: "部分失败", running: "运行中", queued: "排队中", failed: "失败", cancelled: "已取消", timed_out: "超时", blocked_incompatible_workload: "工作负载不兼容" } as Record<string, string>)[value] ?? (value || "—"); }
function artifactStatusText(value: string): string { return ({ complete: "完整", incomplete: "不完整", pending: "等待中", unavailable: "不可用" } as Record<string, string>)[value] ?? (value || "—"); }
function displayMethodName(value: string): string {
  const lower = value.toLowerCase();
  if (lower.includes("address conflict graph") || lower.includes("acg/nezha")) return "ACG/Nezha";
  if (lower.includes("batch-schedule-execute") || lower.includes("(bsx)")) return "BSX";
  if (lower.includes("conflict graph") && !lower.includes("address")) return "CG";
  if (lower.includes("batch-si")) return "Batch-SI";
  if (lower.includes("groundhog")) return "Groundhog";
  if (lower.includes("aria")) return "Aria";
  if (lower.includes("block-stm")) return "Block-STM";
  if (lower.includes("serial")) return "Serial";
  return value.replace(/Stateful Hash/gi, "有状态 Hash").replace(/Stateless Hash/gi, "无状态 Hash").replace(/with Block-STM backend/gi, "+ Block-STM");
}

function Grid({ values }: { values: Row[] }) { return <dl className="stage-flow-kpis">{values.map(([label, item, testId]) => <div key={label} data-testid={testId ?? `v5-metric-${slug(label)}`}><dt>{label}</dt><dd>{value(item)}</dd></div>)}</dl>; }
function value(item: unknown): string { return item === undefined || item === null || item === "" ? "—" : typeof item === "boolean" ? (item ? "是" : "否") : typeof item === "number" ? item.toLocaleString(undefined, { maximumFractionDigits: 3 }) : String(item); }
function finalityRows(item: V5FinalityEvidence | undefined): Row[] { return [["已提交", item?.submitted_unique_tx_count, "v5-metric-submitted"], ["全局终态", item?.terminal_unique_tx_count, "v5-metric-terminal"], ["未完成", item?.incomplete_unique_tx_count, "v5-metric-incomplete"], ["已最终确认", item?.finalized_unique_logical_tx_count], ["片内已提交", item?.intra_shard_committed_unique_count, "v5-metric-intra-committed"], ["片内终态", item?.intra_shard_terminal_unique_count], ["跨片请求", item?.cross_shard_requested_unique_count, "v5-metric-cross-requested"], ["跨片目标提交", item?.cross_shard_target_committed_unique_count], ["跨片最终确认", item?.cross_shard_finalized_unique_count, "v5-metric-cross-finalized"], ["跨片退款", item?.cross_shard_refunded_unique_count], ["跨片失败", item?.cross_shard_failed_unique_count], ["指标真实性", item?.metric_truth], ["TCP 发送延迟已排除", item?.tcp_send_latency_excluded]]; }
function runtimeRows(source: V5RealClusterSummary | undefined): Row[] { return [["可提交", source?.ready_to_commit], ["每逻辑节点独立 OS 进程", source?.one_node_one_os_process], ["独立 TCP 端口", source?.independent_tcp_ports], ["全部分片活跃", source?.all_shards_active], ["每分片多个区块", source?.per_shard_multiple_blocks], ["真实客户端提交", source?.real_client_submission], ["真实跨片网络", source?.real_cross_shard_network], ["PBFT 风格消息", source?.real_pbft_style_messages], ["真实签名交易", source?.real_signed_tx], ["持久化状态", source?.persistent_state], ["插件驱动运行时", source?.plugin_driven_runtime], ["状态根一致", source?.state_root_consistent, "v5-metric-state-root-consistent"], ["无静默回退", source?.no_fallback, "v5-metric-no-fallback"], ["孤儿进程", source?.orphan_process_count, "v5-metric-orphan-processes"], ["实际 / 预期进程", source ? `${value(source.distinct_process_count)} / ${value(source.expected_process_count)}` : undefined], ["分片数量", source?.shard_count], ["分片区块", source?.shard_blocks ? JSON.stringify(source.shard_blocks) : undefined]]; }
function blockExecutorRows(source: V5RealClusterSummary | undefined): Row[] { return [["区块执行器 ID", source?.block_executor_id, "v5-metric-block-executor-id"], ["区块执行器一致", source?.block_executor_consistent, "v5-metric-block-executor-consistent"], ["执行计划摘要一致", source?.plan_digest_consistent, "v5-metric-plan-digest-consistent"], ["状态根一致", source?.state_root_consistent, "v5-metric-state-root-consistent-block"]]; }
function artifactContractRows(summary: V5RealClusterSummary | undefined, metrics: Record<string, unknown>): Row[] {
  const missing = Array.isArray(metrics.missing_expected_artifacts) ? metrics.missing_expected_artifacts : Array.isArray(summary?.missing_expected_artifacts) ? summary?.missing_expected_artifacts : [];
  const contract = record(summary?.artifact_contract) ? summary?.artifact_contract as Record<string, unknown> : {};
  return [
    ["产物契约状态", metrics.artifact_contract_status ?? summary?.artifact_contract_status ?? contract.artifact_contract_status, "v5-artifact-contract-status"],
    ["预期产物数", metrics.expected_artifact_count ?? contract.expected_artifact_count, "v5-artifact-contract-expected"],
    ["实际产物数", metrics.actual_artifact_count ?? contract.actual_artifact_count, "v5-artifact-contract-actual"],
    ["缺失预期产物", missing.length ? missing.join(", ") : "无", "v5-artifact-contract-missing"],
    ["非预期产物数", metrics.unexpected_artifact_count],
  ];
}
function mechanismRows(metrics: Record<string, unknown>): Row[] {
  return [
    ["工作线程数", metrics.worker_count],
    ["最大并行宽度", metrics.maximum_parallel_width],
    ["中止事件数", metrics.abort_count],
    ["重执行次数", metrics.reexecution_count],
    ["依赖等待次数", metrics.dependency_wait_count],
    ["验证失败次数", metrics.validation_failure_count],
    ["规划调度事件数", metrics.planning_scheduler_event_count, "v5-metric-planning-scheduler-event-count"],
    ["运行时调度事件数", metrics.runtime_scheduler_event_count, "v5-metric-runtime-scheduler-event-count"],
    ["Leader 调度事件数", metrics.leader_scheduler_event_count],
    ["Replica 调度事件数", metrics.replica_scheduler_event_count],
    ["唯一逻辑调度决策数", metrics.unique_logical_scheduling_decision_count, "v5-metric-unique-logical-scheduling-decision-count"],
    ["阻塞逻辑交易数", metrics.blocked_logical_tx_count],
    ["唤醒逻辑交易数", metrics.wakeup_logical_tx_count],
    ["依赖等待事件数", metrics.dependency_wait_event_count],
    ["工作窃取尝试数", metrics.work_steal_attempt_count],
    ["工作窃取成功数", metrics.work_steal_success_count],
    ["物理远程操作数", metrics.physical_remote_operation_count, "v5-metric-physical-remote-operation-count"],
    ["物理远程读取数", metrics.physical_remote_fetch_count],
    ["物理远程写回数", metrics.physical_remote_writeback_count],
    ["物理远程失败数", metrics.physical_remote_failed_count],
    ["副本去重远程操作数", metrics.replica_deduplicated_remote_operation_count, "v5-metric-replica-deduplicated-remote-operation-count"],
    ["副本去重远程读取数", metrics.replica_deduplicated_remote_fetch_count],
    ["副本去重远程写回数", metrics.replica_deduplicated_remote_writeback_count],
    ["每逻辑交易远程读取数", metrics.remote_fetches_per_logical_tx],
    ["每逻辑交易远程写回数", metrics.remote_writebacks_per_logical_tx],
    ["每逻辑交易远程操作数", metrics.remote_operations_per_logical_tx],
    ["副本放大因子", metrics.replica_amplification_factor],
    ["远程读取副本放大因子", metrics.remote_fetch_replica_amplification_factor],
    ["远程写回副本放大因子", metrics.remote_writeback_replica_amplification_factor],
    ["聚合组数", metrics.aggregation_group_count],
    ["已执行逻辑交易数", metrics.executed_logical_transaction_count],
    ["已执行交易实例数", metrics.executed_transaction_instance_count],
    ["聚合前物理操作数", metrics.pre_aggregation_physical_op_count, "v5-metric-pre-aggregation-physical-op-count"],
    ["聚合后物理操作数", metrics.post_aggregation_physical_op_count, "v5-metric-post-aggregation-physical-op-count"],
    ["聚合键数", metrics.aggregated_key_count],
    ["聚合逻辑增量数", metrics.aggregated_logical_delta_count],
    ["节省物理操作数", metrics.physical_ops_saved_count, "v5-metric-physical-ops-saved-count"],
    ["聚合削减比例", metrics.aggregation_reduction_ratio],
    ["逻辑更新数（旧指标）", metrics.logical_update_count_deprecated ?? metrics.logical_update_count, "v5-metric-logical-update-count-deprecated"],
    ["物理更新数（旧指标）", metrics.physical_update_count_deprecated ?? metrics.physical_update_count],
    ["调度事件数（旧指标）", metrics.scheduler_event_count],
    ["远程状态访问数（旧指标）", metrics.remote_state_access_count],
    ["远程状态读取数（旧指标）", metrics.remote_state_read_count],
    ["远程状态写应用数（旧指标）", metrics.remote_state_write_apply_count],
    ["串行等价", metrics.serial_equivalent],
  ];
}
function ArtifactCatalog({ artifacts }: { artifacts: V5RuntimeArtifact[] }) {
  const ordered = [...artifacts].sort((a, b) => rank(a.name) - rank(b.name) || a.name.localeCompare(b.name));
  const key = ordered.filter((item) => rank(item.name) !== Number.MAX_SAFE_INTEGER);
  const other = ordered.filter((item) => rank(item.name) === Number.MAX_SAFE_INTEGER);
  return <>{key.length ? <ArtifactTable title="关键产物" artifacts={key} /> : null}<details><summary>其他高级产物（{other.length}）</summary>{grouped(other).map(([title, items]) => <ArtifactTable key={title} title={title} artifacts={items} />)}</details>{!ordered.length && <p className="muted">没有匹配的真实运行产物。</p>}</>;
}
function ArtifactTable({ title, artifacts }: { title: string; artifacts: V5RuntimeArtifact[] }) { return <section><h4>{title}</h4><div className="table-wrap"><table><thead><tr><th>产物</th><th>产物角色</th><th>真实性范围</th><th>生成器 / 模式版本</th><th>字节数</th><th>下载</th></tr></thead><tbody>{artifacts.map((item) => <tr key={item.download_url}><td>{item.name}</td><td title={item.artifact_role ?? item.truth_category ?? "raw_artifact"}>{artifactRoleText(item.artifact_role ?? item.truth_category ?? "raw_artifact")}</td><td title={item.truth_scope ?? item.truth_category ?? "unspecified"}>{artifactTruthText(item.truth_scope ?? item.truth_category ?? "unspecified")}</td><td>{[item.producer, item.schema_version].filter(Boolean).join(" / ") || "—"}</td><td>{item.size_bytes}</td><td><a href={v5RealClusterArtifactURL(item.download_url)}>下载</a></td></tr>)}</tbody></table></div></section>; }
function rank(name: string): number { const index = preferred.indexOf(name); return index >= 0 ? index : Number.MAX_SAFE_INTEGER; }
function grouped(items: V5RuntimeArtifact[]): Array<[string, V5RuntimeArtifact[]]> { const labels = ["客户端", "节点", "日志 / Supervisor", "其他"]; return labels.map((label) => [label, items.filter((item) => category(item.name) === label)] as [string, V5RuntimeArtifact[]]).filter(([, current]) => current.length); }
function category(name: string): string { if (workloadArtifacts.includes(name) || workloadArtifacts.some((item) => name.endsWith(`/${item}`))) return "工作负载证据"; if (blockSTMArtifacts.includes(name) || blockSTMArtifacts.some((item) => name.endsWith(`/${item}`))) return "Block-STM 证据"; if (metatrackArtifacts.includes(name) || metatrackArtifacts.some((item) => name.endsWith(`/${item}`))) return "MetaTrack 证据"; if (name.startsWith("client/")) return "客户端"; if (name.startsWith("nodes/")) return "节点"; if (name.startsWith("logs/") || name.startsWith("supervisor")) return "日志 / Supervisor"; return "其他"; }
function slug(value: string): string { return value.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/(^-|-$)/g, ""); }

function WorkloadTruth({ summary, child }: { summary: V5RealClusterSummary | undefined; child: V5FormalChildRun }) {
  const replay = record(summary?.workload_replay_summary) ? summary?.workload_replay_summary as Record<string, unknown> : {};
  const plan = record(child.result?.summary) ? child.result?.summary as Record<string, unknown> : {};
  const truth = replay.truth_label ?? child.result?.summary?.runtime_truth;
  return <div data-testid="v5-workload-truth"><h3>工作负载真实性</h3>
    <p className="muted">{truthText(truth)}</p>
    <WorkloadGrid values={[
      ["工作负载插件 (workload plugin)", replay.dataset_id ? "canonical_trace_replay" : "deterministic_signed_synthetic"],
      ["来源类型 (source type)", sourceTypeText(replay.dataset_id ? "dataset" : "synthetic")],
      ["数据集 ID (dataset_id)", replay.dataset_id],
      ["工作负载变体 (variant)", replay.variant_id],
      ["真实性标签 (truth label)", truthLabelText(truth)],
      ["请求 / 实际交易数 (requested / actual tx count)", `${valueText(replay.expected_count ?? child.estimated_transactions)} / ${valueText(replay.read_count ?? child.estimated_transactions)}`],
      ["随机种子 (seed)", child.seed],
      ["源数据摘要 (source hash)", formatHash(replay.source_sha256)],
      ["规范化摘要 (canonical hash)", formatHash(plan.canonical_sha256)],
      ["物化负载摘要 (materialized hash)", formatHash(replay.materialized_sha256)],
      ["目标偏斜参数 α (target alpha)", plan.target_alpha],
      ["后验偏斜诊断 (realized skew)", JSON.stringify(plan.realized_skew ?? {})],
      ["类别分布 (category distribution)", JSON.stringify(plan.category_counts ?? {})],
      ["源数据重复行比例 (duplicate source-row ratio)", plan.duplicate_source_row_ratio],
      ["期望跨分片 (expected cross-shard)", `${valueText(replay.expected_cross_shard_count)} / ${valueText(replay.expected_cross_shard_ratio)}`],
      ["实际跨分片 (actual cross-shard)", `${valueText(replay.actual_cross_shard_count)} / ${valueText(replay.actual_cross_shard_ratio)}`],
      ["签名验证通过数 (signature verification)", replay.signature_pass_count],
      ["Nonce 连续性 (nonce continuity)", replay.nonce_continuity],
      ["身份数量 (identity count)", replay.identity_count],
      ["无静默回退 (no_fallback)", replay.no_fallback ?? summary?.no_fallback],
    ]} />
  </div>;
}

function WorkloadGrid({ values }: { values: Row[] }) { return <dl className="stage-flow-kpis">{values.map(([label, item]) => <div key={label} data-testid={`v5-workload-metric-${slug(label)}`}><dt>{label}</dt><dd>{value(item)}</dd></div>)}</dl>; }
function sourceTypeText(value: unknown): string { return value === "dataset" ? "数据集" : value === "synthetic" ? "合成数据" : String(value ?? "—"); }
function truthLabelText(value: unknown): string {
  const key = String(value ?? "");
  return ({
    real_derived_controlled: "真实数据派生受控负载（real_derived_controlled）",
    real_trace: "真实链上轨迹（real_trace）",
    synthetic: "合成负载（synthetic）",
  } as Record<string, string>)[key] ?? (key || "—");
}
function artifactRoleText(value: string): string {
  return ({
    runtime_artifact: "运行时产物",
    raw_artifact: "原始产物",
    research_observability: "研究观测证据",
    node_mechanism_evidence: "节点机制证据",
    client_workload_evidence: "客户端工作负载证据",
    workload_evidence: "工作负载证据",
    aggregate_metric: "聚合指标",
    audit_log: "审计日志",
  } as Record<string, string>)[value] ?? value;
}
function artifactTruthText(value: string): string {
  return ({
    runtime_artifact: "运行时产物",
    runtime: "运行时",
    child_run: "子实验",
    node: "节点",
    per_node: "每节点",
    client: "客户端",
    aggregate: "聚合",
    research_observability: "研究观测",
    unspecified: "未指定",
  } as Record<string, string>)[value] ?? value;
}

function record(value: unknown): value is Record<string, unknown> { return typeof value === "object" && value !== null; }
