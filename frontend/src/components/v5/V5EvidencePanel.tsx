// MBE_V5_ARTIFACT_STORAGE_COMPRESSION_CLOSURE_20260814_V1
import { v5FormalBundleURL, v5RealClusterArtifactURL, type V5ArtifactStorageGroup, type V5FormalArtifactCatalog, type V5FormalChildRun, type V5RuntimeArtifact } from "../../api";

type Props = {
  groupId: string;
  catalog: V5FormalArtifactCatalog | null;
  children: V5FormalChildRun[];
  storage: V5ArtifactStorageGroup | null;
  onRebuild?: () => void;
  rebuilding?: boolean;
  onCompactStorage?: () => void;
  onArchiveStorage?: () => void;
  onRestoreStorage?: () => void;
  storageBusyAction?: "compact" | "archive" | "restore" | "";
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

export default function V5EvidencePanel({
  groupId, catalog, children, storage, onRebuild, rebuilding = false,
  onCompactStorage, onArchiveStorage, onRestoreStorage, storageBusyAction = "",
}: Props) {
  const files = catalog?.files ?? [];
  const childEvidence = children.flatMap((child) => (child.evidence_artifacts ?? []).map((artifact) => ({ ...artifact, child_run_id: artifact.child_run_id ?? child.child_run_id, method_id: artifact.method_id ?? child.method_config_id, method_name: artifact.method_name ?? child.method?.display_name }))) as EvidenceArtifact[];
  const evidenceFiles = dedupeEvidence([...files.map((file) => ({ ...file } as EvidenceArtifact)), ...childEvidence]);
  const rawCount = children.reduce((total, child) => total + Number(child.runtime_artifact_count ?? 0), 0);
  const rawBytes = children.reduce((total, child) => total + Number(child.runtime_artifact_bytes ?? 0), 0);
  const storageErrors = storage?.operation_errors ?? [];
  const archivedChildren = storage?.children.filter((item) => Boolean(item.archive_download_url)) ?? [];
  const archiveJob = storage?.artifact_storage_job ?? null;
  const archiveJobRunning = archiveJob?.status === "queued" || archiveJob?.status === "running";
  const archiveJobInterrupted = archiveJob?.status === "interrupted";
  const canOperate = storage?.operation_ready === true && !storageBusyAction && !archiveJobRunning;

  return <section className="v5-dashboard-section" data-testid="v5-evidence-panel">
    <div className="v5-dashboard-heading"><div><h3>证据与产物</h3><p className="muted">论文结果与证据索引在实验完成时冻结；Raw 进入透明压缩或冷归档后，结果页统计、有效性与证据数量保持不变。</p></div></div>

    <section className="v5-dashboard-section" data-testid="v5-artifact-storage-panel">
      <div className="v5-dashboard-heading"><div><h4>产物存储管理</h4><p className="muted">Formal child 的结果、指标与证据索引冻结后自动冷归档为逐子实验 tar.zst。正式冷归档使用 exact SHA-256 文件级去重后再以 Zstandard level 10 多线程压缩；只有内容哈希和大小完全一致的文件才共享一个归档对象。底层通用归档仍保留 Windows tar.exe + Zstandard 多线程兼容路径。完整校验通过后才释放 Raw。</p></div></div>
      <div className="v5-status-kpis">
        <StorageKPI label="原始逻辑大小" value={formatBytes(storage?.original_logical_bytes)} />
        <StorageKPI label={storage?.current_effective_bytes == null ? "当前已知占用" : "当前有效占用"} value={storage?.current_effective_bytes == null ? `${formatBytes(storage?.current_known_effective_bytes)}${(storage?.unmeasured_effective_child_count ?? 0) > 0 ? ` · ${storage?.unmeasured_effective_child_count} child 未统计` : ""}` : formatBytes(storage.current_effective_bytes)} />
        <StorageKPI label={storage?.saved_bytes == null ? "已知节省" : "已节省"} value={storage?.saved_bytes == null ? `${formatBytes(storage?.known_saved_bytes)} · ${formatPercent(storage?.known_saving_ratio)}` : `${formatBytes(storage.saved_bytes)} · ${formatPercent(storage.saving_ratio)}`} />
        <StorageKPI label="NTFS 已压缩" value={`${storage?.ntfs_compressed_child_count ?? 0}/${storage?.child_count ?? children.length}`} />
        <StorageKPI label="冷归档文件" value={`${storage?.cold_archive_child_count ?? storage?.archived_child_count ?? 0}/${storage?.child_count ?? children.length}`} />
        <StorageKPI label="Raw 已释放" value={`${storage?.archived_child_count ?? 0}/${storage?.child_count ?? children.length}`} />
      </div>
      <div className="button-row">
        {onCompactStorage && <button type="button" onClick={onCompactStorage} disabled={!canOperate}>{storageBusyAction === "compact" ? "透明压缩中…" : "透明压缩现有 Raw"}</button>}
        {onArchiveStorage && <button type="button" onClick={onArchiveStorage} disabled={!canOperate}>{storageBusyAction === "archive" ? "正在启动归档…" : archiveJobRunning ? "后台归档进行中…" : archiveJobInterrupted ? "继续冷归档" : "冷归档并释放 Raw"}</button>}
        {onRestoreStorage && <button type="button" onClick={onRestoreStorage} disabled={!canOperate || (storage?.restorable_child_count ?? storage?.archived_child_count ?? 0) === 0}>{storageBusyAction === "restore" ? "恢复中…" : "恢复原始产物"}</button>}
      </div>
      {archiveJob && <div className="notice" data-testid="v5-artifact-storage-job">
        <strong>冷归档任务：{storageJobStatusText(archiveJob.status)}</strong>
        <p>处理进度：{Number(archiveJob.processed_children ?? 0)}/{Number(archiveJob.total_children ?? storage?.child_count ?? 0)}；本任务已确认归档 {Number(archiveJob.archived_children ?? 0)}；错误 {Number(archiveJob.error_count ?? 0)}；跳过 {Number(archiveJob.skipped_count ?? 0)}。</p>
        {(archiveJob.current_child_run_id || archiveJob.current_method_name) && <p>当前：<strong>{shortMethodName(String(archiveJob.current_method_id ?? ""), String(archiveJob.current_method_name ?? ""))}</strong>{archiveJob.current_theta != null ? ` · θ=${archiveJob.current_theta}` : archiveJob.current_alpha != null ? ` · α=${archiveJob.current_alpha}` : ""}{archiveJob.current_repeat_index != null ? ` · Repeat ${Number(archiveJob.current_repeat_index) + 1}` : ""}{archiveJob.current_child_run_id ? <> · <code>{archiveJob.current_child_run_id}</code></> : null}</p>}
        <p>阶段：{storagePhaseText(archiveJob.phase)}；引擎：{storageEngineText(archiveJob.archive_engine ?? archiveJob.archive_engine_preference)}{archiveJob.native_tar_executable ? `（${archiveJob.native_tar_executable}）` : ""}。</p>
        {(archiveJob.source_logical_bytes != null || archiveJob.archive_bytes_written != null || archiveJob.verified_file_count != null) && <p>当前 child：源数据 {formatBytes(archiveJob.source_logical_bytes)}；已写归档 {formatBytes(archiveJob.archive_bytes_written ?? archiveJob.archive_bytes)}{archiveJob.source_file_count != null ? `；文件 ${Number(archiveJob.verified_file_count ?? archiveJob.processed_file_count ?? 0)}/${Number(archiveJob.source_file_count)}` : ""}。</p>}
        <p className="muted">开始：{formatTimestamp(archiveJob.started_at ?? archiveJob.created_at)}；最后心跳：{formatTimestamp(archiveJob.heartbeat_at)}。切换页面或刷新后会从后端恢复该状态；后端重启会标记为 interrupted，可点击“继续冷归档”跳过已完成归档并接着处理。</p>
        {archiveJob.native_tar_error && <p className="muted">原生 tar 回退原因：{String(archiveJob.native_tar_error)}</p>}
        {archiveJob.fatal_error && <p className="file-error">归档任务错误：{String(archiveJob.fatal_error)}</p>}
      </div>}
      {storageErrors.length > 0 && <div className="notice"><strong>最近一次存储操作存在 {storageErrors.length} 个子实验错误</strong><ul>{storageErrors.slice(0, 10).map((item, index) => <li key={`${item.child_run_id ?? index}`}>{item.child_run_id ?? "子实验"}：{item.error}</li>)}</ul></div>}
      {(storage?.operation_skipped?.length ?? 0) > 0 && <p className="muted">有 {storage?.operation_skipped?.length} 个终态 child 没有 real-cluster run_id，因此没有 Raw 可归档。</p>}
      {archivedChildren.length > 0 && <details><summary>冷归档子实验 ({archivedChildren.length})</summary><div className="table-wrap"><table><thead><tr><th>方法</th><th>重复</th><th>原始大小</th><th>tar.zst</th><th>节省</th><th>SHA-256</th><th>下载</th></tr></thead><tbody>{archivedChildren.map((item) => <tr key={item.child_run_id}><td>{shortMethodName(item.method_id ?? "", item.method_name ?? "")}</td><td>{Number(item.repeat_index ?? 0) + 1}</td><td>{formatBytes(item.original_logical_bytes)}</td><td>{formatBytes(item.archive_bytes)}</td><td>{formatPercent(item.saving_ratio)}</td><td><code title={item.archive_sha256 ?? ""}>{shortHash(item.archive_sha256)}</code></td><td>{item.archive_download_url ? <a href={v5RealClusterArtifactURL(item.archive_download_url)}>下载 TAR.ZST</a> : "—"}</td></tr>)}</tbody></table></div></details>}
      <p className="muted">自动冷归档位于 metric extraction 与 child 结果冻结之后、下一个 child 启动之前；手动批量归档作为后端持久化任务运行。存储处理始终 <code>formal_eligibility_affected=false</code>，归档失败只记录存储错误，不改变实验执行结论。</p>
    </section>

    <div className="v5-paper-downloads">
      {paperPriority.map(([suffix, label]) => {
        const file = files.find((item) => item.name.endsWith(suffix));
        return <div className="v5-paper-download-card" key={suffix}><span>{label}</span><strong>{file ? displayName(file.name) : "尚未生成"}</strong>{file?.download_url ? <a href={v5RealClusterArtifactURL(file.download_url)}>下载</a> : <span className="muted">—</span>}</div>;
      })}
      <div className="v5-paper-download-card"><span>实验组（RunGroup）汇总证据包</span><strong>{catalog?.bundle_ready ? formatBytes(catalog.bundle_size_bytes) : "尚未生成"}</strong>{catalog?.bundle_ready ? <a href={v5FormalBundleURL(groupId)} download>下载 ZIP</a> : onRebuild ? <button type="button" onClick={onRebuild} disabled={rebuilding}>{rebuilding ? "生成中…" : "重新生成"}</button> : <span>—</span>}</div>
      <div className="v5-paper-download-card"><span>子实验原始产物总量</span><strong>{rawCount ? `${rawCount.toLocaleString()} 个文件 · ${formatBytes(rawBytes)}` : "未提供"}</strong><span className="muted">按实验完成时冻结的子实验索引统计</span></div>
    </div>
    <div className="v5-evidence-groups">{evidenceGroups.map(([label, predicate]) => {
      const matching = evidenceFiles.filter((file) => predicate(file.name.toLowerCase()));
      return <details key={label}><summary>{label} ({matching.length})</summary><ul>{matching.slice(0, 80).map((file) => <li key={evidenceKey(file)}><span title={file.name}>{file.method_name ? `${shortMethodName(file.method_id ?? "", file.method_name)} · ` : ""}{displayName(file.name)}</span>{file.download_url ? <a href={v5RealClusterArtifactURL(file.download_url)}>下载</a> : null}</li>)}</ul>{matching.length > 80 && <p className="muted">其余 {matching.length - 80} 项保留在子实验冻结产物索引中。</p>}</details>;
    })}</div>
    <details className="v5-raw-artifacts"><summary>高级 / 原始产物 ({files.length})</summary>
      <div className="table-wrap"><table><thead><tr><th>文件名</th><th>产物角色</th><th>真实性范围</th><th>生成器</th><th>模式版本</th><th>大小</th><th>下载</th></tr></thead><tbody>{files.map((file) => <tr key={file.name}><td title={file.name}>{file.name}</td><td title={file.artifact_role ?? "raw_artifact"}>{roleText(file.artifact_role ?? "raw_artifact")}</td><td title={file.truth_scope ?? ""}>{truthScopeText(file.truth_scope)}</td><td>{file.producer ?? "—"}</td><td>{file.schema_version ?? "—"}</td><td>{formatBytes(file.size_bytes)}</td><td>{file.download_url ? <a href={v5RealClusterArtifactURL(file.download_url)}>下载</a> : "—"}</td></tr>)}</tbody></table></div>
    </details>
  </section>;
}

function storageJobStatusText(value: unknown): string { return ({ queued: "等待启动", running: "运行中", interrupted: "已中断，可继续", completed: "已完成", completed_with_errors: "完成但存在错误", failed: "失败" } as Record<string, string>)[String(value ?? "")] ?? String(value ?? "未启动"); }
function storagePhaseText(value: unknown): string { return ({ queued: "排队", preparing: "准备", preparing_child: "准备当前 child", snapshotting: "冻结文件清单与哈希", packing: "构建冷归档", compressing: "Zstandard 多线程压缩", native_tar_fallback: "原生 tar 回退", checksumming_archive: "计算归档 SHA-256", verifying: "逐文件完整校验", releasing_raw: "释放 Raw", already_archived: "跳过已归档 child", child_finished: "当前 child 完成", completed: "全部完成", interrupted: "后端任务中断", failed: "任务失败" } as Record<string, string>)[String(value ?? "")] ?? String(value ?? "—"); }
function storageEngineText(value: unknown): string { return ({ windows_tar_zstd_multithread: "Windows tar.exe + Zstandard 多线程", python_tarfile_zstd_multithread: "Python tar + Zstandard 多线程", python_tarfile_zstd_exact_dedup: "Exact SHA-256 去重 + Zstandard 多线程", python_tarfile_zstd_exact_dedup_migration: "Exact SHA-256 迁移 + Zstandard 多线程" } as Record<string, string>)[String(value ?? "")] ?? String(value ?? "自动选择"); }
function formatTimestamp(value: unknown): string { const text = String(value ?? ""); if (!text) return "—"; const date = new Date(text); return Number.isNaN(date.getTime()) ? text : date.toLocaleString(); }
function StorageKPI({ label, value }: { label: string; value: string }) { return <div className="status-kpi"><span>{label}</span><strong>{value}</strong></div>; }
function dedupeEvidence(files: EvidenceArtifact[]): EvidenceArtifact[] { const seen = new Set<string>(); return files.filter((file) => { const key = evidenceKey(file); if (seen.has(key)) return false; seen.add(key); return true; }); }
function evidenceKey(file: EvidenceArtifact): string { return `${file.child_run_id ?? "run-group"}:${file.name}:${file.download_url ?? ""}`; }
function shortMethodName(methodId: string, value: string): string { const id = methodId.toLowerCase(); if (id === "hash_serial") return "Serial"; if (id === "hash_block_stm") return "Block-STM"; if (id === "hash_aria") return "Aria"; if (id === "hash_groundhog") return "Groundhog"; if (id === "hash_cg") return "CG"; if (id === "hash_acg") return "ACG/Nezha"; if (id === "hash_bsx") return "BSX"; if (id === "hash_batch_si") return "Batch-SI"; const lower = value.toLowerCase(); if (lower.includes("address conflict graph")) return "ACG/Nezha"; if (lower.includes("batch-schedule-execute")) return "BSX"; if (lower.includes("conflict graph") && !lower.includes("address")) return "CG"; if (lower.includes("block-stm")) return "Block-STM"; if (lower.includes("batch-si")) return "Batch-SI"; if (lower.includes("groundhog")) return "Groundhog"; if (lower.includes("aria")) return "Aria"; if (lower.includes("serial")) return "Serial"; return value || methodId || "—"; }
function roleText(value: string): string { return ({ formal_run_group_summary: "正式实验组摘要", paper_analysis: "论文分析", aggregate_metric: "聚合指标", child_experiment_record: "子实验记录", node_mechanism_evidence: "节点机制证据", client_workload_evidence: "客户端工作负载证据", workload_evidence: "工作负载证据", audit_log: "审计日志", failure_or_missing_metric: "失败或缺失指标", research_observability: "研究观测证据", storage_metadata: "存储元数据", raw_artifact: "原始产物" } as Record<string, string>)[value] ?? value; }
function truthScopeText(value: string | undefined): string { if (!value) return "—"; return ({ run_group: "实验组", formal_run_group: "正式实验组", child_run: "子实验", node: "节点", per_node: "每节点", client: "客户端", aggregate: "聚合", runtime: "运行时", research_observability: "研究观测", post_measurement_storage: "实验后存储" } as Record<string, string>)[value] ?? value; }
function displayName(name: string): string { return name.split("/").pop() ?? name; }
function shortHash(value: unknown): string { const text = String(value ?? ""); return text.length > 16 ? `${text.slice(0, 8)}…${text.slice(-8)}` : text || "—"; }
function formatPercent(value: unknown): string { const numeric = Number(value); return Number.isFinite(numeric) ? `${(numeric * 100).toFixed(1)}%` : "—"; }
function formatBytes(value: unknown): string { const numeric = Number(value); if (!Number.isFinite(numeric)) return "—"; const units = ["B", "KiB", "MiB", "GiB", "TiB"]; let current = numeric; let index = 0; while (current >= 1024 && index < units.length - 1) { current /= 1024; index += 1; } return `${current.toFixed(index === 0 ? 0 : 2)} ${units[index]}`; }
