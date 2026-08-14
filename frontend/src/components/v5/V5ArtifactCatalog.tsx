// MBE_V5_RESULTS_UI_FINAL_CN_CLOSURE_20260814_V4
import { v5FormalBundleURL, v5RealClusterArtifactURL, type V5FormalArtifactCatalog } from "../../api";

const workloadArtifacts = [
  "workload_manifest_snapshot.json",
  "workload_source_spec.json",
  "workload_selection.json",
  "workload_skew_report.json",
  "workload_materialization_summary.json",
  "workload_identity_mapping_summary.json",
  "workload_replay_summary.json",
];

const coreArtifacts = [
  "raw_summary.csv",
  "aggregate_summary.csv",
  "confidence_interval.csv",
  "comparison_summary.csv",
  "ablation_summary.csv",
  "sensitivity_summary.csv",
  "scaling_summary.csv",
  "fault_recovery_summary.csv",
  "paper_figure_data.csv",
  "paper_table_data.csv",
  "run_group_report.md",
];

const coreRoles = new Set([
  "formal_run_group_summary",
  "paper_analysis",
]);

const artifactLabels: Record<string, string> = {
  workload_manifest_snapshot: "工作负载清单快照",
  workload_source_spec: "工作负载源规格",
  workload_selection: "工作负载选择",
  workload_skew_report: "工作负载偏斜报告",
  workload_materialization_summary: "工作负载物化摘要",
  workload_identity_mapping_summary: "身份映射摘要",
  workload_replay_summary: "工作负载回放摘要",
};

export default function V5ArtifactCatalog({ groupId, catalog, onRebuild, rebuilding = false }: { groupId: string; catalog: V5FormalArtifactCatalog | null; onRebuild?: () => void; rebuilding?: boolean }) {
  const files = catalog?.files ?? [];
  const groups = groupedArtifacts(files);
  return <section className="final-card wide" data-testid="v5-group-artifact-catalog">
    <h2>实验组产物</h2>
    <dl className="stage-flow-kpis">
      <Metric label="目录状态" value={catalogStatusText(catalog?.status)} />
      <Metric label="文件数量" value={catalog?.file_count} />
      <Metric label="证据包已就绪" value={catalog?.bundle_ready} testId="v5-bundle-ready" />
      <Metric label="证据包字节数" value={catalog?.bundle_size_bytes} />
    </dl>
    {catalog?.bundle_ready ? <a data-testid="v5-bundle-download" href={v5FormalBundleURL(groupId)} download>下载完整产物包</a> : <div className="button-row"><span className="muted">产物包尚未就绪。</span>{onRebuild && <button type="button" data-testid="v5-bundle-rebuild" onClick={onRebuild} disabled={rebuilding}>{rebuilding ? "正在生成…" : "重新生成一键下载包"}</button>}</div>}
    {groups.map((group, index) => group.files.length ? <details key={group.id} open={index === 0} data-testid={`v5-artifact-group-${group.id}`}>
      <summary>{group.label} ({group.files.length})</summary>
      <ArtifactTable files={group.files} />
    </details> : null)}
    {!files.length && <p className="muted">暂无真实运行清单产物。</p>}
  </section>;
}

function Metric({ label, value, testId }: { label: string; value: unknown; testId?: string }) {
  return <div data-testid={testId}><dt>{label}</dt><dd>{value === undefined || value === null ? "-" : typeof value === "boolean" ? (value ? "是" : "否") : String(value)}</dd></div>;
}

function ArtifactTable({ files }: { files: V5FormalArtifactCatalog["files"] }) {
  return <div className="table-wrap artifact-table"><table><thead><tr><th>产物角色</th><th>名称</th><th>真实性范围</th><th>生成器</th><th>模式版本</th><th>字节数</th><th>下载</th></tr></thead><tbody>{files.map((file) => <tr key={file.name}><td title={category(file)}>{roleText(category(file))}</td><td><span title={file.name}>{displayName(file.name)}</span></td><td title={file.truth_scope ?? ""}>{truthScopeText(file.truth_scope)}</td><td>{file.producer ?? "-"}</td><td>{file.schema_version ?? "-"}</td><td>{file.size_bytes}</td><td>{file.download_url ? <a href={v5RealClusterArtifactURL(file.download_url)}>下载</a> : "-"}</td></tr>)}</tbody></table></div>;
}

function catalogStatusText(value: string | undefined): string { return ({ ready: "已就绪", pending: "等待中" } as Record<string, string>)[String(value ?? "")] ?? (value ?? "—"); }
function roleText(value: string): string { return ({ formal_run_group_summary: "正式实验组摘要", paper_analysis: "论文分析", aggregate_metric: "聚合指标", child_experiment_record: "子实验记录", node_mechanism_evidence: "节点机制证据", client_workload_evidence: "客户端工作负载证据", workload_evidence: "工作负载证据", audit_log: "审计日志", failure_or_missing_metric: "失败或缺失指标", research_observability: "研究观测证据", raw_artifact: "原始产物" } as Record<string, string>)[value] ?? value; }
function truthScopeText(value: string | undefined): string { if (!value) return "—"; return ({ run_group: "实验组", formal_run_group: "正式实验组", child_run: "子实验", node: "节点", per_node: "每节点", client: "客户端", aggregate: "聚合", runtime: "运行时", research_observability: "研究观测" } as Record<string, string>)[value] ?? value; }

function displayName(name: string): string {
  const base = name.split("/").pop() ?? name;
  const key = base.replace(/\.json$/, "");
  return artifactLabels[key] ? `${artifactLabels[key]} (${base})` : base;
}

function groupedArtifacts(files: V5FormalArtifactCatalog["files"]) {
  const groups = [
    { id: "core-analysis", label: "核心分析结果", files: [] as V5FormalArtifactCatalog["files"] },
    { id: "aggregate-metrics", label: "聚合指标", files: [] as V5FormalArtifactCatalog["files"] },
    { id: "child-experiments", label: "子实验", files: [] as V5FormalArtifactCatalog["files"] },
    { id: "node-mechanism-evidence", label: "节点级机制证据", files: [] as V5FormalArtifactCatalog["files"] },
    { id: "client-workload-evidence", label: "客户端与工作负载证据", files: [] as V5FormalArtifactCatalog["files"] },
    { id: "logs-audit", label: "日志与审计", files: [] as V5FormalArtifactCatalog["files"] },
  ];
  const byId = new Map(groups.map((group) => [group.id, group]));
  for (const file of [...files].sort((a, b) => groupRank(a) - groupRank(b) || a.name.localeCompare(b.name))) {
    byId.get(groupId(file))?.files.push(file);
  }
  return groups;
}

function groupId(file: V5FormalArtifactCatalog["files"][number]): string {
  const role = category(file);
  if (coreArtifacts.some((name) => file.name.endsWith(name)) || coreRoles.has(role)) return "core-analysis";
  if (role === "aggregate_metric") return "aggregate-metrics";
  if (role === "child_experiment_record") return "child-experiments";
  if (role === "node_mechanism_evidence" || file.name.startsWith("nodes/")) return "node-mechanism-evidence";
  if (role === "client_workload_evidence" || role === "workload_evidence" || file.name.startsWith("client/")) return "client-workload-evidence";
  if (role === "audit_log" || file.name.startsWith("logs/") || file.name.startsWith("supervisor")) return "logs-audit";
  return "logs-audit";
}

function groupRank(file: V5FormalArtifactCatalog["files"][number]): number {
  return ["core-analysis", "aggregate-metrics", "child-experiments", "node-mechanism-evidence", "client-workload-evidence", "logs-audit"].indexOf(groupId(file));
}

function category(file: V5FormalArtifactCatalog["files"][number]): string {
  if (file.artifact_role) return file.artifact_role;
  const name = file.name;
  if (workloadArtifacts.some((artifact) => name.endsWith(artifact))) return "workload_evidence";
  if (/^(raw_summary|aggregate_summary|confidence_interval|comparison_summary|ablation_summary|sensitivity_summary|scaling_summary|fault_recovery_summary|paper_figure_data|paper_table_data|run_group_report)/.test(name)) return "paper_analysis";
  if (/^(formal_matrix|fairness_matrix|fairness_validation)/.test(name)) return "formal_run_group_summary";
  if (/^(failed_children|invalid_children|blocked_children|comparison_excluded_children|missing_metrics)/.test(name)) return "failure_or_missing_metric";
  if (name.startsWith("children/")) return "child_experiment_record";
  return "raw_artifact";
}
