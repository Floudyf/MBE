import { v5FormalBundleURL, type V5FormalArtifactCatalog } from "../../api";

const workloadArtifacts = [
  "workload_manifest_snapshot.json",
  "workload_source_spec.json",
  "workload_selection.json",
  "workload_skew_report.json",
  "workload_materialization_summary.json",
  "workload_identity_mapping_summary.json",
  "workload_replay_summary.json",
];

const preferredArtifacts = [
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
  ...workloadArtifacts,
];

const artifactLabels: Record<string, string> = {
  workload_manifest_snapshot: "负载 Manifest 快照",
  workload_source_spec: "负载来源规范",
  workload_selection: "负载选择窗口",
  workload_skew_report: "负载偏斜报告",
  workload_materialization_summary: "负载物化摘要",
  workload_identity_mapping_summary: "身份映射摘要",
  workload_replay_summary: "负载回放摘要",
};

export default function V5ArtifactCatalog({ groupId, catalog }: { groupId: string; catalog: V5FormalArtifactCatalog | null }) {
  const files = catalog?.files ?? [];
  const key = files.filter((item) => preferredArtifacts.some((name) => item.name.endsWith(name)));
  const other = files.filter((item) => !key.includes(item));
  return <section className="final-card wide" data-testid="v5-group-artifact-catalog">
    <h2>实验组产物</h2>
    <dl className="stage-flow-kpis">
      <Metric label="目录状态" value={catalog?.status} />
      <Metric label="文件数量" value={catalog?.file_count} />
      <Metric label="产物包已就绪" value={catalog?.bundle_ready} testId="v5-bundle-ready" />
      <Metric label="产物包字节数" value={catalog?.bundle_size_bytes} />
    </dl>
    {catalog?.bundle_ready ? <a data-testid="v5-bundle-download" href={v5FormalBundleURL(groupId)} download>下载全部产物</a> : <p className="muted">产物包尚未生成。</p>}
    {key.length ? <ArtifactTable files={key} /> : null}
    {other.length ? <details><summary>其他高级产物（{other.length}）</summary><ArtifactTable files={other} /></details> : null}
    {!files.length && <p className="muted">尚无真实 Manifest 产物。</p>}
  </section>;
}

function Metric({ label, value, testId }: { label: string; value: unknown; testId?: string }) {
  return <div data-testid={testId}><dt>{label}</dt><dd>{value === undefined || value === null ? "-" : String(value)}</dd></div>;
}

function ArtifactTable({ files }: { files: V5FormalArtifactCatalog["files"] }) {
  return <div className="table-wrap artifact-table"><table><thead><tr><th>类别</th><th>名称</th><th>字节数</th></tr></thead><tbody>{files.map((file) => <tr key={file.name}><td>{category(file.name)}</td><td><span title={file.name}>{displayName(file.name)}</span></td><td>{file.size_bytes}</td></tr>)}</tbody></table></div>;
}

function displayName(name: string): string {
  const base = name.split("/").pop() ?? name;
  const key = base.replace(/\.json$/, "");
  return artifactLabels[key] ? `${artifactLabels[key]} (${base})` : base;
}

function category(name: string): string {
  if (workloadArtifacts.some((artifact) => name.endsWith(artifact))) return "Workload Evidence";
  if (/^(raw_summary|aggregate_summary|confidence_interval|comparison_summary|ablation_summary|sensitivity_summary|scaling_summary|fault_recovery_summary|paper_figure_data|paper_table_data|run_group_report)/.test(name)) return "Summary / Paper Export";
  if (/^(formal_matrix|fairness_matrix|fairness_validation)/.test(name)) return "Matrix / Fairness";
  if (/^(failed_children|missing_metrics)/.test(name)) return "Failure / Missing";
  if (name.startsWith("children/")) return "Child Records";
  return "Other";
}
