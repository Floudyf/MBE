import { v5FormalBundleURL, type V5FormalArtifactCatalog } from "../../api";

export default function V5ArtifactCatalog({ groupId, catalog }: { groupId: string; catalog: V5FormalArtifactCatalog | null }) {
  const files = catalog?.files ?? [];
  const preferred = ["raw_summary.csv", "aggregate_summary.csv", "confidence_interval.csv", "comparison_summary.csv", "ablation_summary.csv", "sensitivity_summary.csv", "scaling_summary.csv", "fault_recovery_summary.csv", "paper_figure_data.csv", "paper_table_data.csv", "run_group_report.md"];
  const key = files.filter((item) => preferred.includes(item.name)); const other = files.filter((item) => !preferred.includes(item.name));
  return <section className="final-card wide" data-testid="v5-group-artifact-catalog"><h2>实验组产物</h2>
    <dl className="stage-flow-kpis"><Metric label="目录状态" value={catalog?.status} /><Metric label="文件数量" value={catalog?.file_count} /><Metric label="产物包已就绪" value={catalog?.bundle_ready} testId="v5-bundle-ready" /><Metric label="产物包字节数" value={catalog?.bundle_size_bytes} /></dl>
    {catalog?.bundle_ready ? <a data-testid="v5-bundle-download" href={v5FormalBundleURL(groupId)} download>下载全部产物</a> : <p className="muted">产物包尚未生成</p>}
    {key.length ? <ArtifactTable files={key} /> : null}{other.length ? <details><summary>其他高级产物（{other.length}）</summary><ArtifactTable files={other} /></details> : null}{!files.length && <p className="muted">尚无真实 Manifest 产物。</p>}
  </section>;
}
function Metric({ label, value, testId }: { label: string; value: unknown; testId?: string }) { return <div data-testid={testId}><dt>{label}</dt><dd>{value === undefined || value === null ? "-" : String(value)}</dd></div>; }
function ArtifactTable({ files }: { files: V5FormalArtifactCatalog["files"] }) { return <div className="table-wrap"><table><thead><tr><th>类别</th><th>名称</th><th>字节数</th></tr></thead><tbody>{files.map((file) => <tr key={file.name}><td>{category(file.name)}</td><td>{file.name}</td><td>{file.size_bytes}</td></tr>)}</tbody></table></div>; }
function category(name: string): string { if (/^(raw_summary|aggregate_summary|confidence_interval|comparison_summary|ablation_summary|sensitivity_summary|scaling_summary|fault_recovery_summary|paper_figure_data|paper_table_data|run_group_report)/.test(name)) return "Summary / Paper Export"; if (/^(formal_matrix|fairness_matrix|fairness_validation)/.test(name)) return "Matrix / Fairness"; if (/^(failed_children|missing_metrics)/.test(name)) return "Failure / Missing"; if (name.startsWith("children/")) return "Child Records"; return "Other"; }
