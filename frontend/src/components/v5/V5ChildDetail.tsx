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
    <h3>运行标识</h3><Grid values={[["子实验 ID", child.child_run_id], ["运行 ID", child.result?.run_id], ["实验类型", child.suite_type], ["方法", child.method.display_name], ["方法配置 ID", child.method_config_id, "v5-metric-method-config-id"], ["随机种子", child.seed], ["重复序号", child.repeat_index + 1], ["尝试次数", child.attempt], ["拓扑", `${child.topology_point.nodes}/${child.topology_point.shards}/${child.topology_point.validators_per_shard}`], ["交易数量", child.estimated_transactions], ["状态", child.status], ["执行状态", child.execution_status ?? child.result?.summary?.execution_status], ["产物状态", child.artifact_status ?? child.result?.summary?.artifact_status], ["正式结果", (child.formal_eligibility ?? child.result?.summary?.formal_eligibility) === true ? "可用" : (child.formal_eligibility ?? child.result?.summary?.formal_eligibility) === false ? "不可用" : "未提供"], ["论文候选", child.paper_candidate], ["对比组", child.comparison_group_id], ["扫描点", `${child.scan_variable || "—"}: ${child.scan_value || "—"}`]]} />
    <h3>性能指标</h3><Grid values={[["End-to-End TPS", metrics.end_to_end_tps ?? finality?.end_to_end_tps ?? metrics.throughput_tps ?? finality?.throughput_tps, "v5-metric-end-to-end-tps"], ["Logical Finality TPS", metrics.logical_finality_tps ?? finality?.logical_finality_tps, "v5-metric-logical-finality-tps"], ["Completion Duration ms", metrics.completion_duration_ms ?? finality?.completion_duration_ms, "v5-metric-completion-duration"], ["Tail Completion Overhead ms", metrics.tail_completion_overhead_ms ?? finality?.tail_completion_overhead_ms, "v5-metric-tail-completion-overhead"], ["P50 Finality", metrics.p50_finality_ms ?? metrics.p50_latency_ms ?? finality?.p50_finality_ms], ["P95 Finality", metrics.p95_finality_ms ?? metrics.p95_latency_ms ?? finality?.p95_finality_ms], ["P99 Finality", metrics.p99_finality_ms ?? metrics.p99_latency_ms ?? finality?.p99_finality_ms, "v5-metric-p99-finality"], ["已最终确认", metrics.finalized_tx_count], ["生命周期完整", metrics.lifecycle_complete], ["缺失指标", Array.isArray(metrics.missing) ? metrics.missing.join(", ") : metrics.missing]]} />
    <div data-testid="v5-artifact-contract"><h3>Artifact Contract</h3><Grid values={artifactContractRows(summary, metrics)} /></div>
    <WorkloadTruth summary={summary} child={child} />
    <div data-testid="v5-finality-summary"><h3>最终确认指标</h3><Grid values={finalityRows(finality)} /></div>
    <div data-testid="v5-runtime-evidence"><h3>运行真实性证据</h3><Grid values={runtimeRows(summary)} /><h4>Block Execution Engine</h4><Grid values={blockExecutorRows(summary)} /><p className="muted">运行阶段：{value(summary?.runtime_stage)}；运行真实性：{value(summary?.runtime_truth)}。production_blockchain=false；production_pbft=false。</p></div>
    <div data-testid="v5-mechanism-metrics"><h3>机制指标</h3><Grid values={mechanismRows(metrics)} /></div>
    {child.error && <p className="file-error">子实验错误：{child.error}</p>}
    <div data-testid="v5-child-artifact-catalog"><h3>子实验运行产物</h3><label><span>搜索产物</span><input aria-label="子实验产物搜索" value={query} onChange={(event) => setQuery(event.target.value)} /></label><ArtifactCatalog artifacts={artifacts} /></div>
  </section>;
}

function Grid({ values }: { values: Row[] }) { return <dl className="stage-flow-kpis">{values.map(([label, item, testId]) => <div key={label} data-testid={testId ?? `v5-metric-${slug(label)}`}><dt>{label}</dt><dd>{value(item)}</dd></div>)}</dl>; }
function value(item: unknown): string { return item === undefined || item === null || item === "" ? "—" : typeof item === "boolean" ? String(item) : typeof item === "number" ? item.toLocaleString(undefined, { maximumFractionDigits: 3 }) : String(item); }
function finalityRows(item: V5FinalityEvidence | undefined): Row[] { return [["已提交", item?.submitted_unique_tx_count, "v5-metric-submitted"], ["全局终态", item?.terminal_unique_tx_count, "v5-metric-terminal"], ["未完成", item?.incomplete_unique_tx_count, "v5-metric-incomplete"], ["已最终确认", item?.finalized_unique_logical_tx_count], ["片内已提交", item?.intra_shard_committed_unique_count, "v5-metric-intra-committed"], ["片内终态", item?.intra_shard_terminal_unique_count], ["跨片请求", item?.cross_shard_requested_unique_count, "v5-metric-cross-requested"], ["跨片目标提交", item?.cross_shard_target_committed_unique_count], ["跨片最终确认", item?.cross_shard_finalized_unique_count, "v5-metric-cross-finalized"], ["跨片退款", item?.cross_shard_refunded_unique_count], ["跨片失败", item?.cross_shard_failed_unique_count], ["指标真实性", item?.metric_truth], ["TCP 发送延迟已排除", item?.tcp_send_latency_excluded]]; }
function runtimeRows(source: V5RealClusterSummary | undefined): Row[] { return [["可提交", source?.ready_to_commit], ["每逻辑节点独立 OS 进程", source?.one_node_one_os_process], ["独立 TCP 端口", source?.independent_tcp_ports], ["全部分片活跃", source?.all_shards_active], ["每分片多个区块", source?.per_shard_multiple_blocks], ["真实客户端提交", source?.real_client_submission], ["真实跨片网络", source?.real_cross_shard_network], ["PBFT 风格消息", source?.real_pbft_style_messages], ["真实签名交易", source?.real_signed_tx], ["持久化状态", source?.persistent_state], ["插件驱动运行时", source?.plugin_driven_runtime], ["状态根一致", source?.state_root_consistent, "v5-metric-state-root-consistent"], ["无静默回退", source?.no_fallback, "v5-metric-no-fallback"], ["孤儿进程", source?.orphan_process_count, "v5-metric-orphan-processes"], ["实际 / 预期进程", source ? `${value(source.distinct_process_count)} / ${value(source.expected_process_count)}` : undefined], ["分片数量", source?.shard_count], ["分片区块", source?.shard_blocks ? JSON.stringify(source.shard_blocks) : undefined]]; }
function blockExecutorRows(source: V5RealClusterSummary | undefined): Row[] { return [["block_executor_id", source?.block_executor_id, "v5-metric-block-executor-id"], ["block_executor_consistent", source?.block_executor_consistent, "v5-metric-block-executor-consistent"], ["plan_digest_consistent", source?.plan_digest_consistent, "v5-metric-plan-digest-consistent"], ["state_root_consistent", source?.state_root_consistent, "v5-metric-state-root-consistent-block"]]; }
function artifactContractRows(summary: V5RealClusterSummary | undefined, metrics: Record<string, unknown>): Row[] {
  const missing = Array.isArray(metrics.missing_expected_artifacts) ? metrics.missing_expected_artifacts : Array.isArray(summary?.missing_expected_artifacts) ? summary?.missing_expected_artifacts : [];
  const contract = record(summary?.artifact_contract) ? summary?.artifact_contract as Record<string, unknown> : {};
  return [
    ["artifact_contract_status", metrics.artifact_contract_status ?? summary?.artifact_contract_status ?? contract.artifact_contract_status, "v5-artifact-contract-status"],
    ["expected_artifact_count", metrics.expected_artifact_count ?? contract.expected_artifact_count, "v5-artifact-contract-expected"],
    ["actual_artifact_count", metrics.actual_artifact_count ?? contract.actual_artifact_count, "v5-artifact-contract-actual"],
    ["missing_expected_artifacts", missing.length ? missing.join(", ") : "none", "v5-artifact-contract-missing"],
    ["unexpected_artifact_count", metrics.unexpected_artifact_count],
  ];
}
function mechanismRows(metrics: Record<string, unknown>): Row[] {
  return [
    ["worker_count", metrics.worker_count],
    ["maximum_parallel_width", metrics.maximum_parallel_width],
    ["abort_count", metrics.abort_count],
    ["reexecution_count", metrics.reexecution_count],
    ["dependency_wait_count", metrics.dependency_wait_count],
    ["validation_failure_count", metrics.validation_failure_count],
    ["planning_scheduler_event_count", metrics.planning_scheduler_event_count],
    ["runtime_scheduler_event_count", metrics.runtime_scheduler_event_count],
    ["leader_scheduler_event_count", metrics.leader_scheduler_event_count],
    ["replica_scheduler_event_count", metrics.replica_scheduler_event_count],
    ["unique_logical_scheduling_decision_count", metrics.unique_logical_scheduling_decision_count],
    ["blocked_logical_tx_count", metrics.blocked_logical_tx_count],
    ["wakeup_logical_tx_count", metrics.wakeup_logical_tx_count],
    ["dependency_wait_event_count", metrics.dependency_wait_event_count],
    ["work_steal_attempt_count", metrics.work_steal_attempt_count],
    ["work_steal_success_count", metrics.work_steal_success_count],
    ["physical_remote_operation_count", metrics.physical_remote_operation_count],
    ["physical_remote_fetch_count", metrics.physical_remote_fetch_count],
    ["physical_remote_writeback_count", metrics.physical_remote_writeback_count],
    ["physical_remote_failed_count", metrics.physical_remote_failed_count],
    ["replica_deduplicated_remote_operation_count", metrics.replica_deduplicated_remote_operation_count],
    ["replica_deduplicated_remote_fetch_count", metrics.replica_deduplicated_remote_fetch_count],
    ["replica_deduplicated_remote_writeback_count", metrics.replica_deduplicated_remote_writeback_count],
    ["remote_fetches_per_logical_tx", metrics.remote_fetches_per_logical_tx],
    ["remote_writebacks_per_logical_tx", metrics.remote_writebacks_per_logical_tx],
    ["remote_operations_per_logical_tx", metrics.remote_operations_per_logical_tx],
    ["replica_amplification_factor", metrics.replica_amplification_factor],
    ["remote_fetch_replica_amplification_factor", metrics.remote_fetch_replica_amplification_factor],
    ["remote_writeback_replica_amplification_factor", metrics.remote_writeback_replica_amplification_factor],
    ["aggregation_group_count", metrics.aggregation_group_count],
    ["executed_logical_transaction_count", metrics.executed_logical_transaction_count],
    ["executed_transaction_instance_count", metrics.executed_transaction_instance_count],
    ["pre_aggregation_physical_op_count", metrics.pre_aggregation_physical_op_count],
    ["post_aggregation_physical_op_count", metrics.post_aggregation_physical_op_count],
    ["aggregated_key_count", metrics.aggregated_key_count],
    ["aggregated_logical_delta_count", metrics.aggregated_logical_delta_count],
    ["physical_ops_saved_count", metrics.physical_ops_saved_count],
    ["aggregation_reduction_ratio", metrics.aggregation_reduction_ratio],
    ["logical_update_count_deprecated", metrics.logical_update_count_deprecated ?? metrics.logical_update_count],
    ["physical_update_count_deprecated", metrics.physical_update_count_deprecated ?? metrics.physical_update_count],
    ["scheduler_event_count_deprecated", metrics.scheduler_event_count],
    ["remote_state_access_count_deprecated", metrics.remote_state_access_count],
    ["remote_state_read_count_deprecated", metrics.remote_state_read_count],
    ["remote_state_write_apply_count_deprecated", metrics.remote_state_write_apply_count],
    ["serial_equivalent", metrics.serial_equivalent],
  ];
}
function ArtifactCatalog({ artifacts }: { artifacts: V5RuntimeArtifact[] }) {
  const ordered = [...artifacts].sort((a, b) => rank(a.name) - rank(b.name) || a.name.localeCompare(b.name));
  const key = ordered.filter((item) => rank(item.name) !== Number.MAX_SAFE_INTEGER);
  const other = ordered.filter((item) => rank(item.name) === Number.MAX_SAFE_INTEGER);
  return <>{key.length ? <ArtifactTable title="关键产物" artifacts={key} /> : null}<details><summary>其他高级产物（{other.length}）</summary>{grouped(other).map(([title, items]) => <ArtifactTable key={title} title={title} artifacts={items} />)}</details>{!ordered.length && <p className="muted">没有匹配的真实运行产物。</p>}</>;
}
function ArtifactTable({ title, artifacts }: { title: string; artifacts: V5RuntimeArtifact[] }) { return <section><h4>{title}</h4><div className="table-wrap"><table><thead><tr><th>产物</th><th>role</th><th>truth scope</th><th>producer / schema</th><th>字节数</th><th>下载</th></tr></thead><tbody>{artifacts.map((item) => <tr key={item.download_url}><td>{item.name}</td><td>{item.artifact_role ?? item.truth_category ?? "raw_artifact"}</td><td>{item.truth_scope ?? item.truth_category ?? "unspecified"}</td><td>{[item.producer, item.schema_version].filter(Boolean).join(" / ") || "—"}</td><td>{item.size_bytes}</td><td><a href={v5RealClusterArtifactURL(item.download_url)}>下载</a></td></tr>)}</tbody></table></div></section>; }
function rank(name: string): number { const index = preferred.indexOf(name); return index >= 0 ? index : Number.MAX_SAFE_INTEGER; }
function grouped(items: V5RuntimeArtifact[]): Array<[string, V5RuntimeArtifact[]]> { const labels = ["客户端", "节点", "日志 / Supervisor", "其他"]; return labels.map((label) => [label, items.filter((item) => category(item.name) === label)] as [string, V5RuntimeArtifact[]]).filter(([, current]) => current.length); }
function category(name: string): string { if (workloadArtifacts.includes(name) || workloadArtifacts.some((item) => name.endsWith(`/${item}`))) return "Workload Evidence"; if (blockSTMArtifacts.includes(name) || blockSTMArtifacts.some((item) => name.endsWith(`/${item}`))) return "Block-STM Evidence"; if (metatrackArtifacts.includes(name) || metatrackArtifacts.some((item) => name.endsWith(`/${item}`))) return "MetaTrack Evidence"; if (name.startsWith("client/")) return "客户端"; if (name.startsWith("nodes/")) return "节点"; if (name.startsWith("logs/") || name.startsWith("supervisor")) return "日志 / Supervisor"; return "其他"; }
function slug(value: string): string { return value.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/(^-|-$)/g, ""); }

function WorkloadTruth({ summary, child }: { summary: V5RealClusterSummary | undefined; child: V5FormalChildRun }) {
  const replay = record(summary?.workload_replay_summary) ? summary?.workload_replay_summary as Record<string, unknown> : {};
  const plan = record(child.result?.summary) ? child.result?.summary as Record<string, unknown> : {};
  const truth = replay.truth_label ?? child.result?.summary?.runtime_truth;
  return <div data-testid="v5-workload-truth"><h3>Workload Truth</h3>
    <p className="muted">{truthText(truth)}</p>
    <WorkloadGrid values={[
      ["workload plugin", replay.dataset_id ? "canonical_trace_replay" : "deterministic_signed_synthetic"],
      ["source type", replay.dataset_id ? "dataset" : "synthetic"],
      ["dataset_id", replay.dataset_id],
      ["variant", replay.variant_id],
      ["truth label", truth],
      ["requested / actual tx count", `${valueText(replay.expected_count ?? child.estimated_transactions)} / ${valueText(replay.read_count ?? child.estimated_transactions)}`],
      ["seed", child.seed],
      ["source hash", formatHash(replay.source_sha256)],
      ["canonical hash", formatHash(plan.canonical_sha256)],
      ["materialized hash", formatHash(replay.materialized_sha256)],
      ["target alpha", plan.target_alpha],
      ["realized skew", JSON.stringify(plan.realized_skew ?? {})],
      ["category distribution", JSON.stringify(plan.category_counts ?? {})],
      ["duplicate source-row ratio", plan.duplicate_source_row_ratio],
      ["expected cross-shard", `${valueText(replay.expected_cross_shard_count)} / ${valueText(replay.expected_cross_shard_ratio)}`],
      ["actual cross-shard", `${valueText(replay.actual_cross_shard_count)} / ${valueText(replay.actual_cross_shard_ratio)}`],
      ["signature verification", replay.signature_pass_count],
      ["nonce continuity", replay.nonce_continuity],
      ["identity count", replay.identity_count],
      ["no_fallback", replay.no_fallback ?? summary?.no_fallback],
    ]} />
  </div>;
}

function WorkloadGrid({ values }: { values: Row[] }) { return <dl className="stage-flow-kpis">{values.map(([label, item]) => <div key={label} data-testid={`v5-workload-metric-${slug(label)}`}><dt>{label}</dt><dd>{value(item)}</dd></div>)}</dl>; }
function record(value: unknown): value is Record<string, unknown> { return typeof value === "object" && value !== null; }
