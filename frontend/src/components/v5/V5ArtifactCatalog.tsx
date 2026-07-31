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
  workload_manifest_snapshot: "Workload manifest snapshot",
  workload_source_spec: "Workload source spec",
  workload_selection: "Workload selection",
  workload_skew_report: "Workload skew report",
  workload_materialization_summary: "Workload materialization summary",
  workload_identity_mapping_summary: "Identity mapping summary",
  workload_replay_summary: "Workload replay summary",
};

export default function V5ArtifactCatalog({ groupId, catalog }: { groupId: string; catalog: V5FormalArtifactCatalog | null }) {
  const files = catalog?.files ?? [];
  const groups = groupedArtifacts(files);
  return <section className="final-card wide" data-testid="v5-group-artifact-catalog">
    <h2>RunGroup Artifacts</h2>
    <dl className="stage-flow-kpis">
      <Metric label="Catalog status" value={catalog?.status} />
      <Metric label="File count" value={catalog?.file_count} />
      <Metric label="Bundle ready" value={catalog?.bundle_ready} testId="v5-bundle-ready" />
      <Metric label="Bundle bytes" value={catalog?.bundle_size_bytes} />
    </dl>
    {catalog?.bundle_ready ? <a data-testid="v5-bundle-download" href={v5FormalBundleURL(groupId)} download>Download full artifact bundle</a> : <p className="muted">Artifact bundle is not ready yet.</p>}
    {groups.map((group, index) => group.files.length ? <details key={group.id} open={index === 0} data-testid={`v5-artifact-group-${group.id}`}>
      <summary>{group.label} ({group.files.length})</summary>
      <ArtifactTable files={group.files} />
    </details> : null)}
    {!files.length && <p className="muted">No real manifest artifacts yet.</p>}
  </section>;
}

function Metric({ label, value, testId }: { label: string; value: unknown; testId?: string }) {
  return <div data-testid={testId}><dt>{label}</dt><dd>{value === undefined || value === null ? "-" : String(value)}</dd></div>;
}

function ArtifactTable({ files }: { files: V5FormalArtifactCatalog["files"] }) {
  return <div className="table-wrap artifact-table"><table><thead><tr><th>Role</th><th>Name</th><th>Truth scope</th><th>Producer</th><th>Schema</th><th>Bytes</th><th>Download</th></tr></thead><tbody>{files.map((file) => <tr key={file.name}><td>{category(file)}</td><td><span title={file.name}>{displayName(file.name)}</span></td><td>{file.truth_scope ?? "-"}</td><td>{file.producer ?? "-"}</td><td>{file.schema_version ?? "-"}</td><td>{file.size_bytes}</td><td>{file.download_url ? <a href={v5RealClusterArtifactURL(file.download_url)}>Download</a> : "-"}</td></tr>)}</tbody></table></div>;
}

function displayName(name: string): string {
  const base = name.split("/").pop() ?? name;
  const key = base.replace(/\.json$/, "");
  return artifactLabels[key] ? `${artifactLabels[key]} (${base})` : base;
}

function groupedArtifacts(files: V5FormalArtifactCatalog["files"]) {
  const groups = [
    { id: "core-analysis", label: "Core analysis results", files: [] as V5FormalArtifactCatalog["files"] },
    { id: "aggregate-metrics", label: "Aggregate metrics", files: [] as V5FormalArtifactCatalog["files"] },
    { id: "child-experiments", label: "Child experiments", files: [] as V5FormalArtifactCatalog["files"] },
    { id: "node-mechanism-evidence", label: "Node-level mechanism evidence", files: [] as V5FormalArtifactCatalog["files"] },
    { id: "client-workload-evidence", label: "Client and workload evidence", files: [] as V5FormalArtifactCatalog["files"] },
    { id: "logs-audit", label: "Logs and audit", files: [] as V5FormalArtifactCatalog["files"] },
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
  if (/^(failed_children|missing_metrics)/.test(name)) return "failure_or_missing_metric";
  if (name.startsWith("children/")) return "child_experiment_record";
  return "raw_artifact";
}
