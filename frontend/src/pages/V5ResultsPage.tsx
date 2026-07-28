import { useEffect, useRef, useState } from "react";

import {
  cleanupV5OrphanRealClusterDirs,
  deleteFailedV5FormalRunGroups,
  deleteSelectedV5FormalRunGroups,
  deleteV5FormalRunGroup,
  fetchV5FormalArtifactCatalog,
  fetchV5FormalChildRun,
  fetchV5FormalGroupAnalysis,
  fetchV5FormalGroupMetrics,
  fetchV5FormalRunGroup,
  listV5FormalRunGroupSummaries,
  scanV5OrphanRealClusterDirs,
  type V5FormalAggregate,
  type V5FormalAnalysis,
  type V5FormalArtifactCatalog,
  type V5CleanupReport,
  type V5FormalChildRun,
  type V5FormalRunGroupDetail,
  type V5FormalRunGroupSummary,
  type V5OrphanRealClusterScan,
} from "../api";
import V5AnalysisPanel from "../components/v5/V5AnalysisPanel";
import V5ArtifactCatalog from "../components/v5/V5ArtifactCatalog";
import V5ChildDetail from "../components/v5/V5ChildDetail";
import V5GroupSummary from "../components/v5/V5GroupSummary";
import { backendLabel, booleanLabel, statusLabel, suiteLabel } from "../v5Labels";

const recentGroupKey = "mbe.v5FormalRunGroupId";
const terminalStatuses = ["completed", "completed_with_failures", "failed", "cancelled"];

export default function V5ResultsPage({ preferredGroupId = "" }: { preferredGroupId?: string }) {
  const [groups, setGroups] = useState<V5FormalRunGroupSummary[]>([]);
  const [detail, setDetail] = useState<V5FormalRunGroupDetail | null>(null);
  const [aggregate, setAggregate] = useState<V5FormalAggregate | null>(null);
  const [catalog, setCatalog] = useState<V5FormalArtifactCatalog | null>(null);
  const [analysis, setAnalysis] = useState<V5FormalAnalysis | null>(null);
  const [selectedGroupId, setSelectedGroupId] = useState("");
  const [selectedChildId, setSelectedChildId] = useState("");
  const [selectedChild, setSelectedChild] = useState<V5FormalChildRun | null>(null);
  const [notice, setNotice] = useState("");
  const [error, setError] = useState("");
  const [childError, setChildError] = useState("");
  const [busy, setBusy] = useState(false);
  const [historyBusy, setHistoryBusy] = useState(false);
  const [historyError, setHistoryError] = useState("");
  const [historyOpen, setHistoryOpen] = useState(false);
  const [cleanupBusy, setCleanupBusy] = useState(false);
  const [cleanupReport, setCleanupReport] = useState<V5CleanupReport | null>(null);
  const [orphanScan, setOrphanScan] = useState<V5OrphanRealClusterScan | null>(null);
  const [selectedCleanupIds, setSelectedCleanupIds] = useState<string[]>([]);
  const [includeTests, setIncludeTests] = useState(false);
  const [search, setSearch] = useState("");
  const [statusFilter, setStatusFilter] = useState("");
  const [methodFilter, setMethodFilter] = useState("");
  const [suiteFilter, setSuiteFilter] = useState("");
  const [offset, setOffset] = useState(0);
  const [total, setTotal] = useState(0);
  const groupRevision = useRef(0);
  const childRevision = useRef(0);
  const historyRevision = useRef(0);
  const initializationRevision = useRef(0);
  const selectedGroupRef = useRef("");
  const selectedChildRef = useRef("");
  const detailRef = useRef<V5FormalRunGroupDetail | null>(null);
  const timer = useRef<number | null>(null);

  useEffect(() => {
    void initializeCurrentGroup();
    return () => {
      stopPolling();
      historyRevision.current += 1;
      groupRevision.current += 1;
      childRevision.current += 1;
    };
  }, []);

  useEffect(() => {
    void refreshHistory();
  }, [includeTests, offset, statusFilter, methodFilter, suiteFilter, search]);

  async function initializeCurrentGroup() {
    const revision = ++initializationRevision.current;
    const requested = preferredGroupId || window.localStorage.getItem(recentGroupKey) || "";
    if (requested) {
      setNotice("");
      const loaded = await loadGroup(requested);
      if (revision !== initializationRevision.current || selectedGroupRef.current !== requested) return;
      if (loaded) return;
      setNotice(`指定的实验组 ${requested} 不存在，已选择最新可用记录。`);
      clearSelection();
    }

    try {
      const page = await listV5FormalRunGroupSummaries({ limit: 1, offset: 0, include_tests: false });
      if (revision !== initializationRevision.current || selectedGroupRef.current) return;
      if (page.items[0]) await loadGroup(page.items[0].run_group_id);
      else clearSelection();
    } catch (caught) {
      setError(message(caught));
    }
  }

  async function refreshHistory() {
    const revision = ++historyRevision.current;
    setHistoryBusy(true);
    try {
      const page = await listV5FormalRunGroupSummaries({
        limit: 20,
        offset,
        include_tests: includeTests,
        search,
        status: statusFilter || undefined,
        method_id: methodFilter || undefined,
        suite: suiteFilter || undefined,
      });
      if (revision !== historyRevision.current) return;
      setGroups(page.items);
      setTotal(page.total);
      setHistoryError("");
    } catch (caught) {
      if (revision !== historyRevision.current) return;
      setHistoryError(message(caught));
    } finally {
      if (revision === historyRevision.current) setHistoryBusy(false);
    }
  }

  async function loadGroup(groupId: string, quiet = false): Promise<boolean> {
    const revision = ++groupRevision.current;
    stopPolling();
    const switched = selectedGroupRef.current !== groupId;
    selectedGroupRef.current = groupId;
    setSelectedGroupId(groupId);
    if (switched) clearChildSelection();
    setBusy(true);
    try {
      const [groupDetail, groupAggregate, artifactCatalog, groupAnalysis] = await Promise.all([
        fetchV5FormalRunGroup(groupId),
        fetchV5FormalGroupMetrics(groupId),
        fetchV5FormalArtifactCatalog(groupId),
        fetchV5FormalGroupAnalysis(groupId),
      ]);
      if (revision !== groupRevision.current) return false;
      detailRef.current = groupDetail;
      setDetail(groupDetail);
      setAggregate(groupAggregate);
      setCatalog(artifactCatalog);
      setAnalysis(groupAnalysis);
      setGroups((current) => current.map((item) => item.run_group_id === groupId ? {
        ...item,
        status: groupDetail.group.status,
        completed_child_runs: groupDetail.group.completed_child_runs,
        total_child_runs: groupDetail.group.total_child_runs,
        failed_child_runs: groupDetail.children.filter((child) => ["failed", "blocked"].includes(child.status)).length,
        aggregate: groupAggregate,
        updated_at: groupDetail.group.updated_at,
      } : item));
      const retainedChild = selectedChildRef.current;
      const childId = retainedChild && groupDetail.children.some((child) => child.child_run_id === retainedChild)
        ? retainedChild
        : groupDetail.children[0]?.child_run_id;
      if (childId) await loadChild(groupId, childId, revision);
      if (revision !== groupRevision.current) return false;
      setError("");
      if (terminal(groupDetail.group.status)) stopPolling();
      else schedulePolling(groupId);
      return true;
    } catch (caught) {
      if (revision !== groupRevision.current) return false;
      setError(message(caught));
      if (quiet && selectedGroupRef.current === groupId && detailRef.current && !terminal(detailRef.current.group.status)) {
        schedulePolling(groupId);
      }
      return false;
    } finally {
      if (revision === groupRevision.current) setBusy(false);
    }
  }

  async function loadChild(groupId: string, childId: string, parentRevision = groupRevision.current) {
    const revision = ++childRevision.current;
    selectedChildRef.current = childId;
    setSelectedChildId(childId);
    try {
      const child = await fetchV5FormalChildRun(groupId, childId);
      if (parentRevision === groupRevision.current && revision === childRevision.current && selectedGroupRef.current === groupId && selectedChildRef.current === childId) {
        setSelectedChild(child);
        setChildError("");
      }
    } catch (caught) {
      if (parentRevision === groupRevision.current && revision === childRevision.current) setChildError(message(caught));
    }
  }

  function schedulePolling(groupId: string) {
    stopPolling();
    timer.current = window.setTimeout(() => void loadGroup(groupId, true), 1800);
  }

  function stopPolling() {
    if (timer.current !== null) {
      window.clearTimeout(timer.current);
      timer.current = null;
    }
  }

  function clearChildSelection() {
    childRevision.current += 1;
    selectedChildRef.current = "";
    setSelectedChildId("");
    setSelectedChild(null);
    setChildError("");
  }

  function clearSelection() {
    groupRevision.current += 1;
    selectedGroupRef.current = "";
    setSelectedGroupId("");
    detailRef.current = null;
    setDetail(null);
    setAggregate(null);
    setCatalog(null);
    setAnalysis(null);
    clearChildSelection();
  }

  function invalidateHistoryRequest() {
    historyRevision.current += 1;
  }

  function toggleCleanupSelection(groupId: string, checked: boolean) {
    setSelectedCleanupIds((current) => checked ? Array.from(new Set([...current, groupId])) : current.filter((item) => item !== groupId));
  }

  async function cleanupCurrentGroup(dryRun: boolean) {
    if (!selectedGroupId) return;
    if (!dryRun && !window.confirm(`删除当前 RunGroup ${selectedGroupId}？其独占 real-cluster 输出目录也会被清理。`)) return;
    setCleanupBusy(true);
    try {
      const report = await deleteV5FormalRunGroup(selectedGroupId, dryRun);
      setCleanupReport(report);
      setOrphanScan(null);
      setNotice(dryRun ? "当前 RunGroup 清理 dry-run 已完成。" : "当前 RunGroup 清理已完成。");
      if (!dryRun) { clearSelection(); await refreshHistory(); }
    } catch (caught) {
      setHistoryError(message(caught));
    } finally {
      setCleanupBusy(false);
    }
  }

  async function cleanupSelectedGroups(dryRun: boolean) {
    if (!selectedCleanupIds.length) { setHistoryError("请先选择至少一个 RunGroup。"); return; }
    if (!dryRun && !window.confirm(`删除 ${selectedCleanupIds.length} 个已选择 RunGroup？其独占 real-cluster 输出目录也可能被清理。`)) return;
    setCleanupBusy(true);
    try {
      const report = await deleteSelectedV5FormalRunGroups(selectedCleanupIds, dryRun);
      setCleanupReport(report);
      setOrphanScan(null);
      setNotice(dryRun ? "批量清理 dry-run 已完成。" : "批量清理已完成。");
      if (!dryRun) { setSelectedCleanupIds([]); clearSelection(); await refreshHistory(); }
    } catch (caught) {
      setHistoryError(message(caught));
    } finally {
      setCleanupBusy(false);
    }
  }

  async function dryRunFailedGroups() {
    setCleanupBusy(true);
    try {
      setCleanupReport(await deleteFailedV5FormalRunGroups(true));
      setOrphanScan(null);
      setNotice("失败 RunGroup 清理 dry-run 已完成。");
    } catch (caught) {
      setHistoryError(message(caught));
    } finally {
      setCleanupBusy(false);
    }
  }

  async function scanOrphans() {
    setCleanupBusy(true);
    try {
      setOrphanScan(await scanV5OrphanRealClusterDirs(24));
      setCleanupReport(null);
      setNotice("孤儿 real-cluster 目录扫描已完成。");
    } catch (caught) {
      setHistoryError(message(caught));
    } finally {
      setCleanupBusy(false);
    }
  }

  async function cleanupOrphans(dryRun: boolean) {
    if (!dryRun && !window.confirm("删除超过 24 小时且无 RunGroup 引用的孤儿 real-cluster 目录？")) return;
    setCleanupBusy(true);
    try {
      const report = await cleanupV5OrphanRealClusterDirs(dryRun, 24);
      setCleanupReport(report);
      setOrphanScan(null);
      setNotice(dryRun ? "孤儿目录清理 dry-run 已完成。" : "孤儿目录清理已完成。");
      if (!dryRun) await refreshHistory();
    } catch (caught) {
      setHistoryError(message(caught));
    } finally {
      setCleanupBusy(false);
    }
  }

  const selectedGroup = detail?.group;
  return <section className="page-grid" data-testid="v5-results-page">
    <article className="final-card wide page-hero">
      <p className="eyebrow">V5 正式实验结果</p>
      <h2>结果与产物</h2>
      <p>结果来自已持久化的 V5 Formal RunGroup 与真实运行产物；浏览器不会获得本地输出路径。</p>
      {notice && <p className="notice">{notice}</p>}
      {error && <p className="file-error">{error}</p>}
      {childError && <p className="file-error">子实验详情错误：{childError}</p>}
      {selectedGroup && <div className="button-row">
        <button type="button" onClick={() => void cleanupCurrentGroup(true)} disabled={cleanupBusy}>当前组 dry-run</button>
        <button type="button" onClick={() => void cleanupCurrentGroup(false)} disabled={cleanupBusy}>删除当前组</button>
      </div>}
    </article>
    {selectedGroup && <V5GroupSummary group={selectedGroup} aggregate={aggregate} children={detail?.children ?? []} />}
    <V5AnalysisPanel analysis={analysis} />
    {detail && <article className="final-card wide">
      <h2>子实验</h2>
      <div className="table-wrap"><table data-testid="v5-child-table">
        <thead><tr><th>子实验</th><th>实验类型</th><th>方法</th><th>种子</th><th>重复</th><th>拓扑</th><th>交易</th><th>状态</th><th>TPS</th><th>P99</th><th>终态</th><th>未完成</th><th>论文候选</th></tr></thead>
        <tbody>{detail.children.map((child) => <ChildRow key={child.child_run_id} child={child} selected={child.child_run_id === selectedChildId} onSelect={() => void loadChild(detail.group.run_group_id, child.child_run_id)} />)}</tbody>
      </table></div>
    </article>}
    <V5ChildDetail child={selectedChild} />
    {selectedGroup && <V5ArtifactCatalog groupId={selectedGroup.run_group_id} catalog={catalog} />}
    <details className="final-card wide" data-testid="v5-run-group-list" open={historyOpen} onToggle={(event) => setHistoryOpen((event.currentTarget as HTMLDetailsElement).open)}>
      <summary>实验组历史</summary>
      <div className="section-heading">
        <label><span>搜索</span><input aria-label="搜索" value={search} onChange={(event) => { invalidateHistoryRequest(); setSearch(event.target.value); setOffset(0); }} /></label>
        <label><span>状态</span><select aria-label="状态" value={statusFilter} onChange={(event) => { invalidateHistoryRequest(); setStatusFilter(event.target.value); setOffset(0); }}><option value="">全部</option><option value="completed">已完成</option><option value="running">运行中</option><option value="failed">失败</option></select></label>
        <label><span>方法 ID</span><input aria-label="方法 ID" value={methodFilter} onChange={(event) => { invalidateHistoryRequest(); setMethodFilter(event.target.value); setOffset(0); }} /></label>
        <label><span>实验类型</span><select aria-label="实验类型" value={suiteFilter} onChange={(event) => { invalidateHistoryRequest(); setSuiteFilter(event.target.value); setOffset(0); }}><option value="">全部</option>{["main_experiment", "comparison_experiment", "ablation_experiment", "workload_sensitivity", "topology_scaling", "fault_recovery_experiment"].map((suite) => <option key={suite} value={suite}>{suiteLabel(suite)}</option>)}</select></label>
        <label><input type="checkbox" checked={includeTests} onChange={(event) => { invalidateHistoryRequest(); setIncludeTests(event.target.checked); setOffset(0); }} /> 显示测试记录</label>
        <button type="button" onClick={() => void refreshHistory()} disabled={historyBusy}>刷新实验组</button>
      </div>
      <div className="button-row">
        <button type="button" onClick={() => void cleanupSelectedGroups(true)} disabled={cleanupBusy || !selectedCleanupIds.length}>选中 dry-run</button>
        <button type="button" onClick={() => void cleanupSelectedGroups(false)} disabled={cleanupBusy || !selectedCleanupIds.length}>删除选中</button>
        <button type="button" onClick={() => void dryRunFailedGroups()} disabled={cleanupBusy}>失败实验 dry-run</button>
        <button type="button" onClick={() => void scanOrphans()} disabled={cleanupBusy}>扫描孤儿目录</button>
        <button type="button" onClick={() => void cleanupOrphans(true)} disabled={cleanupBusy}>孤儿目录 dry-run</button>
        <button type="button" onClick={() => void cleanupOrphans(false)} disabled={cleanupBusy}>删除孤儿目录</button>
      </div>
      <CleanupEvidence report={cleanupReport} scan={orphanScan} />
      {historyError && <p className="file-error">历史列表错误：{historyError}</p>}
      {groups.length ? <div className="table-wrap"><table><thead><tr><th>清理</th><th>ID</th><th>状态</th><th>计划</th><th>后端</th><th>更新时间</th><th>子实验</th><th>失败</th><th>实验类型</th><th>方法</th></tr></thead><tbody>
        {groups.map((group) => <tr key={group.run_group_id} className={group.run_group_id === selectedGroupId ? "selected-row" : ""}>
          <td><input aria-label={`选择 ${group.run_group_id} 清理`} type="checkbox" checked={selectedCleanupIds.includes(group.run_group_id)} onChange={(event) => toggleCleanupSelection(group.run_group_id, event.target.checked)} /></td>
          <td><button type="button" data-testid="v5-run-group-select" onClick={() => void loadGroup(group.run_group_id)}>{group.run_group_id}</button></td>
          <td><span>{statusLabel(group.status)}</span><small>{group.status}</small></td>
          <td>{group.plan_name || "—"}</td>
          <td><span>{backendLabel(group.execution_backend)}</span><small>{group.execution_backend}</small></td>
          <td>{group.updated_at || "—"}</td>
          <td>{group.completed_child_runs}/{group.total_child_runs}</td>
          <td>{metric(group.failed_child_runs)}</td>
          <td>{group.suite_names.map((suite) => `${suiteLabel(suite)} (${suite})`).join(", ") || "—"}</td>
          <td>{group.method_names.join(", ") || "—"}</td>
        </tr>)}
      </tbody></table></div> : <p className="muted">暂无符合条件的 V5 正式实验组。</p>}
      <div className="button-row"><button type="button" disabled={offset === 0} onClick={() => { invalidateHistoryRequest(); setOffset(Math.max(0, offset - 20)); }}>上一页</button><span>{total ? `${offset + 1}–${Math.min(offset + groups.length, total)} / ${total}` : "0 / 0"}</span><button type="button" disabled={offset + 20 >= total} onClick={() => { invalidateHistoryRequest(); setOffset(offset + 20); }}>下一页</button></div>
    </details>
  </section>;
}

function CleanupEvidence({ report, scan }: { report: V5CleanupReport | null; scan: V5OrphanRealClusterScan | null }) {
  if (!report && !scan) return null;
  if (scan) {
    return <div className="notice" data-testid="v5-cleanup-evidence">
      <strong>孤儿目录扫描</strong>
      <span>候选目录：{scan.orphan_dirs.length}</span>
      <span>预计释放：{formatBytes(scan.orphan_dirs.reduce((total, item) => total + item.size_bytes, 0))}</span>
    </div>;
  }
  const deletedGroups = report?.deleted_run_group_ids.length ?? 0;
  const deletedOutputs = (report?.deleted_output_dirs.length ?? 0) + (report?.deleted_orphan_dirs.length ?? 0);
  return <div className="notice" data-testid="v5-cleanup-evidence">
    <strong>{report?.dry_run ? "清理 dry-run" : "清理结果"}</strong>
    <span>RunGroup：{deletedGroups}</span>
    <span>输出目录：{deletedOutputs}</span>
    <span>预计释放：{formatBytes(report?.released_bytes ?? 0)}</span>
    {(report?.preserved_run_group_ids.length ?? 0) > 0 && <span>保留：{report?.preserved_run_group_ids.join(", ")}</span>}
    {(report?.skipped_active_runs.length ?? 0) > 0 && <span>运行中跳过：{report?.skipped_active_runs.join(", ")}</span>}
    {(report?.errors.length ?? 0) > 0 && <span>原因：{report?.errors.slice(0, 3).join("; ")}</span>}
  </div>;
}

function ChildRow({ child, selected, onSelect }: { child: V5FormalChildRun; selected: boolean; onSelect: () => void }) {
  const finality = child.result?.summary?.finality_evidence;
  return <tr className={selected ? "selected-row" : ""}>
    <td><button type="button" onClick={onSelect}>{child.child_run_id}</button></td>
    <td><span>{suiteLabel(child.suite_type)}</span><small>{child.suite_type}</small></td>
    <td>{child.method.display_name}</td><td>{child.seed}</td><td>{child.repeat_index + 1}</td>
    <td>{child.topology_point.nodes}/{child.topology_point.shards}/{child.topology_point.validators_per_shard}</td><td>{child.estimated_transactions}</td>
    <td><span>{statusLabel(child.status)}</span><small>{child.status}</small>{child.error ? <small>{child.error}</small> : null}</td>
    <td>{metric(child.metrics?.throughput_tps)}</td><td>{metric(child.metrics?.p99_latency_ms)}</td><td>{metric(finality?.terminal_unique_tx_count)}</td><td>{metric(finality?.incomplete_unique_tx_count)}</td><td>{booleanLabel(child.paper_candidate)}</td>
  </tr>;
}

function metric(value: unknown): string { return value === undefined || value === null ? "—" : String(value); }
function terminal(status: string): boolean { return terminalStatuses.includes(status); }
function message(value: unknown): string { return value instanceof Error ? value.message : String(value); }
function formatBytes(value: number): string {
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KiB`;
  return `${(value / 1024 / 1024).toFixed(1)} MiB`;
}
