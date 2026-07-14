import { useState } from "react";

import { v5RealClusterArtifactURL, type V5FinalityEvidence, type V5FormalChildRun, type V5RealClusterSummary, type V5RuntimeArtifact } from "../../api";

const preferred = ["real_cluster_summary.json", "finality_summary.json", "transaction_lifecycle.csv", "transaction_lifecycle.jsonl", "transaction_finality.csv", "latency_distribution.csv", "throughput_windows.csv", "drain_status.json", "client_receipt_log.csv", "compiled_run_plan.json", "supervisor_stdout.log", "supervisor_stderr.log"];
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
    <h3>运行标识</h3><Grid values={[["子实验 ID", child.child_run_id], ["运行 ID", child.result?.run_id], ["实验类型", child.suite_type], ["方法", child.method.display_name], ["方法配置 ID", child.method_config_id, "v5-metric-method-config-id"], ["随机种子", child.seed], ["重复序号", child.repeat_index + 1], ["尝试次数", child.attempt], ["拓扑", `${child.topology_point.nodes}/${child.topology_point.shards}/${child.topology_point.validators_per_shard}`], ["交易数量", child.estimated_transactions], ["状态", child.status], ["论文候选", child.paper_candidate], ["对比组", child.comparison_group_id], ["扫描点", `${child.scan_variable || "—"}: ${child.scan_value || "—"}`]]} />
    <h3>性能指标</h3><Grid values={[["吞吐量 TPS", metrics.throughput_tps ?? finality?.throughput_tps], ["P50", metrics.p50_latency_ms ?? finality?.p50_finality_ms], ["P95", metrics.p95_latency_ms ?? finality?.p95_finality_ms], ["P99", metrics.p99_latency_ms ?? finality?.p99_finality_ms], ["已最终确认", metrics.finalized_tx_count], ["生命周期完整", metrics.lifecycle_complete], ["缺失指标", Array.isArray(metrics.missing) ? metrics.missing.join(", ") : metrics.missing]]} />
    <div data-testid="v5-finality-summary"><h3>最终确认指标</h3><Grid values={finalityRows(finality)} /></div>
    <div data-testid="v5-runtime-evidence"><h3>运行真实性证据</h3><Grid values={runtimeRows(summary)} /><p className="muted">运行阶段：{value(summary?.runtime_stage)}；运行真实性：{value(summary?.runtime_truth)}。production_blockchain=false；production_pbft=false。</p></div>
    {child.error && <p className="file-error">子实验错误：{child.error}</p>}
    <div data-testid="v5-child-artifact-catalog"><h3>子实验运行产物</h3><label><span>搜索产物</span><input aria-label="子实验产物搜索" value={query} onChange={(event) => setQuery(event.target.value)} /></label><ArtifactCatalog artifacts={artifacts} /></div>
  </section>;
}

function Grid({ values }: { values: Row[] }) { return <dl className="stage-flow-kpis">{values.map(([label, item, testId]) => <div key={label} data-testid={testId ?? `v5-metric-${slug(label)}`}><dt>{label}</dt><dd>{value(item)}</dd></div>)}</dl>; }
function value(item: unknown): string { return item === undefined || item === null || item === "" ? "—" : typeof item === "boolean" ? String(item) : typeof item === "number" ? item.toLocaleString(undefined, { maximumFractionDigits: 3 }) : String(item); }
function finalityRows(item: V5FinalityEvidence | undefined): Row[] { return [["已提交", item?.submitted_unique_tx_count, "v5-metric-submitted"], ["全局终态", item?.terminal_unique_tx_count, "v5-metric-terminal"], ["未完成", item?.incomplete_unique_tx_count, "v5-metric-incomplete"], ["已最终确认", item?.finalized_unique_logical_tx_count], ["片内已提交", item?.intra_shard_committed_unique_count, "v5-metric-intra-committed"], ["片内终态", item?.intra_shard_terminal_unique_count], ["跨片请求", item?.cross_shard_requested_unique_count, "v5-metric-cross-requested"], ["跨片目标提交", item?.cross_shard_target_committed_unique_count], ["跨片最终确认", item?.cross_shard_finalized_unique_count, "v5-metric-cross-finalized"], ["跨片退款", item?.cross_shard_refunded_unique_count], ["跨片失败", item?.cross_shard_failed_unique_count], ["指标真实性", item?.metric_truth], ["TCP 发送延迟已排除", item?.tcp_send_latency_excluded]]; }
function runtimeRows(source: V5RealClusterSummary | undefined): Row[] { return [["可提交", source?.ready_to_commit], ["每逻辑节点独立 OS 进程", source?.one_node_one_os_process], ["独立 TCP 端口", source?.independent_tcp_ports], ["全部分片活跃", source?.all_shards_active], ["每分片多个区块", source?.per_shard_multiple_blocks], ["真实客户端提交", source?.real_client_submission], ["真实跨片网络", source?.real_cross_shard_network], ["PBFT 风格消息", source?.real_pbft_style_messages], ["真实签名交易", source?.real_signed_tx], ["持久化状态", source?.persistent_state], ["插件驱动运行时", source?.plugin_driven_runtime], ["状态根一致", source?.state_root_consistent, "v5-metric-state-root-consistent"], ["无静默回退", source?.no_fallback, "v5-metric-no-fallback"], ["孤儿进程", source?.orphan_process_count, "v5-metric-orphan-processes"], ["实际 / 预期进程", source ? `${value(source.distinct_process_count)} / ${value(source.expected_process_count)}` : undefined], ["分片数量", source?.shard_count], ["分片区块", source?.shard_blocks ? JSON.stringify(source.shard_blocks) : undefined]]; }
function ArtifactCatalog({ artifacts }: { artifacts: V5RuntimeArtifact[] }) {
  const ordered = [...artifacts].sort((a, b) => rank(a.name) - rank(b.name) || a.name.localeCompare(b.name));
  const key = ordered.filter((item) => rank(item.name) !== Number.MAX_SAFE_INTEGER);
  const other = ordered.filter((item) => rank(item.name) === Number.MAX_SAFE_INTEGER);
  return <>{key.length ? <ArtifactTable title="关键产物" artifacts={key} /> : null}<details><summary>其他高级产物（{other.length}）</summary>{grouped(other).map(([title, items]) => <ArtifactTable key={title} title={title} artifacts={items} />)}</details>{!ordered.length && <p className="muted">没有匹配的真实运行产物。</p>}</>;
}
function ArtifactTable({ title, artifacts }: { title: string; artifacts: V5RuntimeArtifact[] }) { return <section><h4>{title}</h4><div className="table-wrap"><table><thead><tr><th>产物</th><th>真实性类别</th><th>字节数</th><th>下载</th></tr></thead><tbody>{artifacts.map((item) => <tr key={item.download_url}><td>{item.name}</td><td>{item.truth_category}</td><td>{item.size_bytes}</td><td><a href={v5RealClusterArtifactURL(item.download_url)}>下载</a></td></tr>)}</tbody></table></div></section>; }
function rank(name: string): number { const index = preferred.indexOf(name); return index >= 0 ? index : Number.MAX_SAFE_INTEGER; }
function grouped(items: V5RuntimeArtifact[]): Array<[string, V5RuntimeArtifact[]]> { const labels = ["客户端", "节点", "日志 / Supervisor", "其他"]; return labels.map((label) => [label, items.filter((item) => category(item.name) === label)] as [string, V5RuntimeArtifact[]]).filter(([, current]) => current.length); }
function category(name: string): string { if (name.startsWith("client/")) return "客户端"; if (name.startsWith("nodes/")) return "节点"; if (name.startsWith("logs/") || name.startsWith("supervisor")) return "日志 / Supervisor"; return "其他"; }
function slug(value: string): string { return value.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/(^-|-$)/g, ""); }
