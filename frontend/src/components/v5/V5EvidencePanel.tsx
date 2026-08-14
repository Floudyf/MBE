// MBE_V5_RESULTS_UI_TRUTH_CN_FINAL_20260814_V5
import { v5FormalBundleURL, v5RealClusterArtifactURL, type V5FormalArtifactCatalog, type V5FormalChildRun, type V5RuntimeArtifact } from "../../api";

type Props = {
  groupId: string;
  catalog: V5FormalArtifactCatalog | null;
  children: V5FormalChildRun[];
  onRebuild?: () => void;
  rebuilding?: boolean;
};

type EvidenceArtifact = V5RuntimeArtifact & {
  child_run_id?: string;
  method_id?: string;
  method_name?: string;
};

const paperPriority = [
  ["paper_table_data.csv", "论文结果总表"],
  ["paper_figure_data.csv", "论文图表数据"],
  ["confidence_interval.csv", "统计 / CI 数据"],
  ["aggregate/paper_result_analysis.csv", "论文结果分析"],
  ["run_group_report.md", "实验组报告"],
] as const;

const evidenceGroups = [
  ["工作负载证据", (name: string) => /workload|mapping/.test(name)],
  ["正确性证据", (name: string) => /finality|state_root|receipt|fairness|equivalence|drain|serial_equivalence|plan_digest/.test(name)],
  ["网络证据", (name: string) => /network_log|network_metrics|network_message_summary/.test(name)],
  ["资源证据", (name: string) => /resource_usage|resource_sampler/.test(name)],
  ["机制证据", (name: string) => /mechanism|block_execution|proposal_selection|scheduler_trace|dependency_graph|aggregation|execution_plan|aria|batch_si|groundhog|block_stm/.test(name)],
] as const;

export default function V5EvidencePanel({ groupId, catalog, children, onRebuild, rebuilding = false }: Props) {
  const files = catalog?.files ?? [];
  const childEvidence = children.flatMap((child) => (child.evidence_artifacts ?? []).map((artifact) => ({ ...artifact, child_run_id: artifact.child_run_id ?? child.child_run_id, method_id: artifact.method_id ?? child.method_config_id, method_name: artifact.method_name ?? child.method?.display_name }))) as EvidenceArtifact[];
  const evidenceFiles = dedupeEvidence([
    ...files.map((file) => ({ ...file } as EvidenceArtifact)),
    ...childEvidence,
  ]);
  const rawCount = children.reduce((total, child) => total + Number(child.runtime_artifact_count ?? 0), 0);
  const rawBytes = children.reduce((total, child) => total + Number(child.runtime_artifact_bytes ?? 0), 0);
  return <section className="v5-dashboard-section" data-testid="v5-evidence-panel">
    <div className="v5-dashboard-heading"><div><h3>证据与产物</h3><p className="muted">默认优先展示论文直接使用的文件；模式版本、生成器、真实性范围等工程元数据放到高级区。</p></div></div>
    <div className="v5-paper-downloads">
      {paperPriority.map(([suffix, label]) => {
        const file = files.find((item) => item.name.endsWith(suffix));
        return <div className="v5-paper-download-card" key={suffix}><span>{label}</span><strong>{file ? displayName(file.name) : "尚未生成"}</strong>{file?.download_url ? <a href={v5RealClusterArtifactURL(file.download_url)}>下载</a> : <span className="muted">—</span>}</div>;
      })}
      <div className="v5-paper-download-card"><span>实验组（RunGroup）汇总证据包</span><strong>{catalog?.bundle_ready ? formatBytes(catalog.bundle_size_bytes) : "尚未生成"}</strong>{catalog?.bundle_ready ? <a href={v5FormalBundleURL(groupId)} download>下载 ZIP</a> : onRebuild ? <button type="button" onClick={onRebuild} disabled={rebuilding}>{rebuilding ? "生成中…" : "重新生成"}</button> : <span>—</span>}</div>
      <div className="v5-paper-download-card"><span>子实验原始产物总量</span><strong>{rawCount ? `${rawCount.toLocaleString()} 个文件 · ${formatBytes(rawBytes)}` : "未提供"}</strong><span className="muted">按子实验索引统计</span></div>
    </div>
    <div className="v5-evidence-groups">{evidenceGroups.map(([label, predicate]) => {
      const matching = evidenceFiles.filter((file) => predicate(file.name.toLowerCase()));
      return <details key={label}><summary>{label} ({matching.length})</summary><ul>{matching.slice(0, 80).map((file) => <li key={evidenceKey(file)}><span title={file.name}>{file.method_name ? `${shortMethodName(file.method_id ?? "", file.method_name)} · ` : ""}{displayName(file.name)}</span>{file.download_url ? <a href={v5RealClusterArtifactURL(file.download_url)}>下载</a> : null}</li>)}</ul>{matching.length > 80 && <p className="muted">其余 {matching.length - 80} 项保留在子实验原始产物中。</p>}</details>;
    })}</div>
    <details className="v5-raw-artifacts"><summary>高级 / 原始产物 ({files.length})</summary>
      <div className="table-wrap"><table><thead><tr><th>文件名</th><th>产物角色</th><th>真实性范围</th><th>生成器</th><th>模式版本</th><th>大小</th><th>下载</th></tr></thead><tbody>{files.map((file) => <tr key={file.name}><td title={file.name}>{file.name}</td><td title={file.artifact_role ?? "raw_artifact"}>{roleText(file.artifact_role ?? "raw_artifact")}</td><td title={file.truth_scope ?? ""}>{truthScopeText(file.truth_scope)}</td><td>{file.producer ?? "—"}</td><td>{file.schema_version ?? "—"}</td><td>{formatBytes(file.size_bytes)}</td><td>{file.download_url ? <a href={v5RealClusterArtifactURL(file.download_url)}>下载</a> : "—"}</td></tr>)}</tbody></table></div>
    </details>
  </section>;
}

function dedupeEvidence(files: EvidenceArtifact[]): EvidenceArtifact[] {
  const seen = new Set<string>();
  return files.filter((file) => {
    const key = evidenceKey(file);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}
function evidenceKey(file: EvidenceArtifact): string { return `${file.child_run_id ?? "run-group"}:${file.name}:${file.download_url ?? ""}`; }
function shortMethodName(methodId: string, value: string): string {
  const id = methodId.toLowerCase();
  if (id === "hash_serial") return "Serial";
  if (id === "hash_block_stm") return "Block-STM";
  if (id === "hash_aria") return "Aria";
  if (id === "hash_groundhog") return "Groundhog";
  if (id === "hash_cg") return "CG";
  if (id === "hash_acg") return "ACG/Nezha";
  if (id === "hash_bsx") return "BSX";
  if (id === "hash_batch_si") return "Batch-SI";
  const lower = value.toLowerCase();
  if (lower.includes("address conflict graph")) return "ACG/Nezha";
  if (lower.includes("batch-schedule-execute")) return "BSX";
  if (lower.includes("conflict graph") && !lower.includes("address")) return "CG";
  if (lower.includes("block-stm")) return "Block-STM";
  if (lower.includes("batch-si")) return "Batch-SI";
  if (lower.includes("groundhog")) return "Groundhog";
  if (lower.includes("aria")) return "Aria";
  if (lower.includes("serial")) return "Serial";
  return value;
}

function roleText(value: string): string { return ({ formal_run_group_summary: "正式实验组摘要", paper_analysis: "论文分析", aggregate_metric: "聚合指标", child_experiment_record: "子实验记录", node_mechanism_evidence: "节点机制证据", client_workload_evidence: "客户端工作负载证据", workload_evidence: "工作负载证据", audit_log: "审计日志", failure_or_missing_metric: "失败或缺失指标", research_observability: "研究观测证据", raw_artifact: "原始产物" } as Record<string, string>)[value] ?? value; }
function truthScopeText(value: string | undefined): string { if (!value) return "—"; return ({ run_group: "实验组", formal_run_group: "正式实验组", child_run: "子实验", node: "节点", per_node: "每节点", client: "客户端", aggregate: "聚合", runtime: "运行时", research_observability: "研究观测" } as Record<string, string>)[value] ?? value; }

function displayName(name: string): string { return name.split("/").pop() ?? name; }
function formatBytes(value: unknown): string { const numeric = Number(value); if (!Number.isFinite(numeric)) return "—"; const units = ["B", "KiB", "MiB", "GiB"]; let current = numeric; let index = 0; while (current >= 1024 && index < units.length - 1) { current /= 1024; index += 1; } return `${current.toFixed(index === 0 ? 0 : 2)} ${units[index]}`; }
