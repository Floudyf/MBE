import { v5FormalBundleURL, type V5FormalArtifactCatalog } from "../../api";

export default function V5ArtifactCatalog({ groupId, catalog }: { groupId: string; catalog: V5FormalArtifactCatalog | null }) {
  const files = catalog?.files ?? [];
  return <section className="final-card wide" data-testid="v5-group-artifact-catalog"><h2>Group Artifact Catalog</h2>
    <dl className="stage-flow-kpis"><Metric label="Catalog status" value={catalog?.status} /><Metric label="Files" value={catalog?.file_count} /><Metric label="Bundle ready" value={catalog?.bundle_ready} testId="v5-bundle-ready" /><Metric label="Bundle bytes" value={catalog?.bundle_size_bytes} /></dl>
    {catalog?.bundle_ready ? <a data-testid="v5-bundle-download" href={v5FormalBundleURL(groupId)} download>Download all</a> : <p className="muted">Bundle pending</p>}
    {files.length ? <div className="table-wrap"><table><thead><tr><th>Category</th><th>Name</th><th>Bytes</th></tr></thead><tbody>{files.map((file) => <tr key={file.name}><td>{category(file.name)}</td><td>{file.name}</td><td>{file.size_bytes}</td></tr>)}</tbody></table></div> : <p className="muted">No manifest is available yet.</p>}
  </section>;
}
function Metric({ label, value, testId }: { label: string; value: unknown; testId?: string }) { return <div data-testid={testId}><dt>{label}</dt><dd>{value === undefined || value === null ? "-" : String(value)}</dd></div>; }
function category(name: string): string { if (/^(raw_summary|aggregate_summary|confidence_interval|comparison_summary|ablation_summary|sensitivity_summary|scaling_summary|fault_recovery_summary|paper_figure_data|paper_table_data|run_group_report)/.test(name)) return "Summary / Paper Export"; if (/^(formal_matrix|fairness_matrix|fairness_validation)/.test(name)) return "Matrix / Fairness"; if (/^(failed_children|missing_metrics)/.test(name)) return "Failure / Missing"; if (name.startsWith("children/")) return "Child Records"; return "Other"; }
