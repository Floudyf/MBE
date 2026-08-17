// MBE_V5_RESULTS_UI_FINAL_CN_CLOSURE_20260814_V4
import { useEffect, useRef, useState } from "react";

import {
  cleanupV5LegacySavedConfigs,
  cleanupV5OrphanRealClusterDirs,
  archiveV5ArtifactStorage,
  compactV5ArtifactStorage,
  deleteFailedV5FormalRunGroups,
  deleteSelectedV5FormalRunGroups,
  deleteV5FormalRunGroup,
  fetchV5ArtifactStorage,
  fetchV5FormalArtifactCatalog,
  fetchV5FormalChildRun,
  fetchV5FormalGroupAnalysis,
  fetchV5FormalGroupMetrics,
  fetchV5FormalRunGroup,
  listV5FormalRunGroupSummaries,
  rebuildV5FormalBundle,
  restoreV5ArtifactStorage,
  scanV5LegacySavedConfigs,
  scanV5OrphanRealClusterDirs,
  type V5ArtifactStorageGroup,
  type V5FormalAggregate,
  type V5FormalAnalysis,
  type V5FormalArtifactCatalog,
  type V5CleanupReport,
  type V5FormalChildRun,
  type V5FormalRunGroupDetail,
  type V5FormalRunGroupSummary,
  type V5LegacySavedConfigScan,
  type V5OrphanRealClusterScan,
} from "../api";
import V5AnalysisPanel from "../components/v5/V5AnalysisPanel";
import V5SkewTpsChart from "../components/v5/V5SkewTpsChart"; // V5_SKEW_TPS_CHART_V1
import V5ArtifactCatalog from "../components/v5/V5ArtifactCatalog";
import V5ChildDetail from "../components/v5/V5ChildDetail";
import V5GroupSummary from "../components/v5/V5GroupSummary";
import V5ResultsDashboard from "../components/v5/V5ResultsDashboard";
import { backendLabel, booleanLabel, statusLabel, suiteLabel } from "../v5Labels";
import "../v5UiPolish.css";

const recentGroupKey = "mbe.v5FormalRunGroupId";
const terminalStatuses = ["completed", "completed_with_failures", "failed", "cancelled"];

export default function V5ResultsPage({ preferredGroupId = "" }: { preferredGroupId?: string }) {
  const [groups, setGroups] = useState<V5FormalRunGroupSummary[]>([]);
  const [selectorGroups, setSelectorGroups] = useState<V5FormalRunGroupSummary[]>([]);
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
  const [selectorBusy, setSelectorBusy] = useState(false);
  const [selectorError, setSelectorError] = useState("");
  const [historyError, setHistoryError] = useState("");
  const [historyOpen, setHistoryOpen] = useState(false);
  const [cleanupBusy, setCleanupBusy] = useState(false);
  const [bundleBusy, setBundleBusy] = useState(false);
  const [storage, setStorage] = useState<V5ArtifactStorageGroup | null>(null);
  const [storageBusyAction, setStorageBusyAction] = useState<"compact" | "archive" | "restore" | "">("");
  useEffect(() => {
    const job = storage?.artifact_storage_job;
    if (!selectedGroupId || !job || !["queued", "running"].includes(String(job.status ?? ""))) return;
    let disposed = false;
    const refreshStorageJob = async () => {
      try {
        const next = await fetchV5ArtifactStorage(selectedGroupId);
        if (!disposed) setStorage(next);
      } catch {
        // The normal page-level refresh/error path remains authoritative.
      }
    };
    const timer = window.setInterval(() => { void refreshStorageJob(); }, 2000);
    void refreshStorageJob();
    return () => { disposed = true; window.clearInterval(timer); };
  }, [selectedGroupId, storage?.artifact_storage_job?.job_id, storage?.artifact_storage_job?.status]);
  const [cleanupReport, setCleanupReport] = useState<V5CleanupReport | null>(null);
  const [orphanScan, setOrphanScan] = useState<V5OrphanRealClusterScan | null>(null);
  const [legacyScan, setLegacyScan] = useState<V5LegacySavedConfigScan | null>(null);
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
    void refreshSelectorGroups();
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
    const requested = window.localStorage.getItem(recentGroupKey) || preferredGroupId || "";
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

  async function refreshSelectorGroups() {
    setSelectorBusy(true);
    try {
      const page = await listV5FormalRunGroupSummaries({ limit: 100, offset: 0, include_tests: false });
      setSelectorGroups(page.items);
      setSelectorError("");
    } catch (caught) {
      setSelectorError(message(caught));
    } finally {
      setSelectorBusy(false);
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
    if (switched) {
      detailRef.current = null;
      setDetail(null);
      setAggregate(null);
      setCatalog(null);
      setAnalysis(null);
      setStorage(null);
      clearChildSelection();
    }
    setBusy(true);
    try {
      const [groupDetail, groupAggregate, artifactCatalog, groupAnalysis, storageStatus] = await Promise.all([
        fetchV5FormalRunGroup(groupId),
        fetchV5FormalGroupMetrics(groupId),
        fetchV5FormalArtifactCatalog(groupId),
        fetchV5FormalGroupAnalysis(groupId),
        fetchV5ArtifactStorage(groupId).catch(() => null),
      ]);
      if (revision !== groupRevision.current) return false;
      detailRef.current = groupDetail;
      setDetail(groupDetail);
      setAggregate(groupAggregate);
      setCatalog(artifactCatalog);
      setAnalysis(groupAnalysis);
      setStorage(storageStatus);
      window.localStorage.setItem(recentGroupKey, groupId);
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
    setStorage(null);
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
    if (!dryRun && !window.confirm(`删除当前 RunGroup ${selectedGroupId}？其独占真实集群输出目录也会被清理。`)) return;
    setCleanupBusy(true);
    try {
      const report = await deleteV5FormalRunGroup(selectedGroupId, dryRun);
      setCleanupReport(report);
      setOrphanScan(null);
      setLegacyScan(null);
      setNotice(dryRun ? "当前实验组清理预演已完成。" : "当前 RunGroup 清理已完成。");
      if (!dryRun) { clearSelection(); await Promise.all([refreshHistory(), refreshSelectorGroups()]); }
    } catch (caught) {
      setHistoryError(message(caught));
    } finally {
      setCleanupBusy(false);
    }
  }

  async function cleanupSelectedGroups(dryRun: boolean) {
    if (!selectedCleanupIds.length) { setHistoryError("请先选择至少一个 RunGroup。"); return; }
    if (!dryRun && !window.confirm(`删除 ${selectedCleanupIds.length} 个已选择 RunGroup？其独占真实集群输出目录也可能被清理。`)) return;
    setCleanupBusy(true);
    try {
      const report = await deleteSelectedV5FormalRunGroups(selectedCleanupIds, dryRun);
      setCleanupReport(report);
      setOrphanScan(null);
      setLegacyScan(null);
      setNotice(dryRun ? "批量清理预演已完成。" : "批量清理已完成。");
      if (!dryRun) { setSelectedCleanupIds([]); clearSelection(); await Promise.all([refreshHistory(), refreshSelectorGroups()]); }
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
      setLegacyScan(null);
      setNotice("失败实验组清理预演已完成。");
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
      setLegacyScan(null);
      setNotice("孤儿真实集群目录扫描已完成。");
    } catch (caught) {
      setHistoryError(message(caught));
    } finally {
      setCleanupBusy(false);
    }
  }

  async function cleanupOrphans(dryRun: boolean) {
    if (!dryRun && !window.confirm("删除超过 24 小时且无实验组引用的孤儿真实集群目录？")) return;
    setCleanupBusy(true);
    try {
      const report = await cleanupV5OrphanRealClusterDirs(dryRun, 24);
      setCleanupReport(report);
      setOrphanScan(null);
      setLegacyScan(null);
      setNotice(dryRun ? "孤儿目录清理预演已完成。" : "孤儿目录清理已完成。");
      if (!dryRun) await refreshHistory();
    } catch (caught) {
      setHistoryError(message(caught));
    } finally {
      setCleanupBusy(false);
    }
  }

  async function scanLegacySavedConfigs() {
    setCleanupBusy(true);
    try {
      setLegacyScan(await scanV5LegacySavedConfigs());
      setCleanupReport(null);
      setOrphanScan(null);
      setNotice("旧方案扫描已完成。");
    } catch (caught) {
      setHistoryError(message(caught));
    } finally {
      setCleanupBusy(false);
    }
  }

  async function cleanupLegacySavedConfigs(dryRun: boolean) {
    if (!dryRun && !window.confirm("删除旧 Formal Plan 和失效的 V5 method profile 记录？")) return;
    setCleanupBusy(true);
    try {
      const report = await cleanupV5LegacySavedConfigs(dryRun);
      setCleanupReport(report);
      setOrphanScan(null);
      setLegacyScan(null);
      setNotice(dryRun ? "旧方案清理预演已完成。" : "旧方案清理已完成。");
    } catch (caught) {
      setHistoryError(message(caught));
    } finally {
      setCleanupBusy(false);
    }
  }

  async function rebuildBundle() {
    if (!selectedGroupId) return;
    setBundleBusy(true);
    try {
      await rebuildV5FormalBundle(selectedGroupId);
      setNotice("一键下载包已重新生成。");
      await loadGroup(selectedGroupId, true);
    } catch (caught) {
      setError(`重新生成一键下载包失败：${message(caught)}`);
    } finally {
      setBundleBusy(false);
    }
  }

  async function compactStorage() {
    if (!selectedGroupId) return;
    setStorageBusyAction("compact");
    try {
      const result = await compactV5ArtifactStorage(selectedGroupId);
      setStorage(result);
      setNotice(result.operation_errors?.length ? `透明压缩完成，${result.operation_errors.length} 个子实验需要检查。` : "现有 Raw 的 NTFS 透明压缩处理完成。");
    } catch (caught) {
      setError(`透明压缩失败：${message(caught)}`);
    } finally {
      setStorageBusyAction("");
    }
  }

  async function archiveStorage() {
    if (!selectedGroupId) return;
    const interrupted = storage?.artifact_storage_job?.status === "interrupted";
    const prompt = interrupted
      ? "继续上次中断的冷归档任务？已经完整校验并释放 Raw 的 child 会自动跳过。"
      : "启动后台冷归档？Windows 将优先使用系统 tar.exe 流式打包，Zstandard level 3 使用全部逻辑 CPU 线程压缩；逐文件校验通过后才删除 Raw。页面切换或刷新不会丢失任务状态。";
    if (!window.confirm(prompt)) return;
    setStorageBusyAction("archive");
    try {
      const result = await archiveV5ArtifactStorage(selectedGroupId, true, 3);
      setStorage(result);
      const job = result.artifact_storage_job;
      setNotice(job?.status === "interrupted" ? "冷归档任务仍处于中断状态，请再次点击继续。" : `后台冷归档已启动${job?.total_children != null ? `：${job.total_children} 个终态 child` : ""}。可以切换页面，返回后会从后端恢复进度。`);
    } catch (caught) {
      setError(`启动冷归档失败：${message(caught)}`);
    } finally {
      setStorageBusyAction("");
    }
  }

  async function restoreStorage() {
    if (!selectedGroupId) return;
    if (!window.confirm("恢复当前 RunGroup 的冷归档 Raw？归档文件会继续保留，因此恢复后磁盘占用会上升。")) return;
    setStorageBusyAction("restore");
    try {
      const result = await restoreV5ArtifactStorage(selectedGroupId);
      setStorage(result);
      setNotice(result.operation_errors?.length ? `恢复完成，${result.operation_errors.length} 个子实验需要检查。` : "原始产物恢复完成，Windows 下已重新尝试 NTFS 透明压缩。");
    } catch (caught) {
      setError(`恢复原始产物失败：${message(caught)}`);
    } finally {
      setStorageBusyAction("");
    }
  }

  const selectedGroup = detail?.group;
  const selectedSelectorGroup = selectorGroups.find((group) => group.run_group_id === selectedGroupId);
  const selectedMethodNames = selectedSelectorGroup?.method_names
    ?? selectedGroup?.plan?.methods.map((method) => method.display_name)
    ?? [];
  return <section className="page-grid v5-results-page" data-testid="v5-results-page">
    <article className="final-card wide page-hero">
      <p className="eyebrow">V5 正式实验结果</p>
      <h2>结果与产物</h2>
      <p>结果来自已持久化的 V5 Formal RunGroup 与真实运行产物；浏览器不会获得本地输出路径。</p>
      {notice && <p className="notice">{notice}</p>}
      {error && <p className="file-error">{error}</p>}
      {childError && <p className="file-error">子实验详情错误：{childError}</p>}
      {selectedGroup && <div className="button-row">
        <button type="button" onClick={() => void cleanupCurrentGroup(true)} disabled={cleanupBusy}>当前组清理预演</button>
        <button type="button" onClick={() => void cleanupCurrentGroup(false)} disabled={cleanupBusy}>删除当前组</button>
      </div>}
    </article>
    <article className="final-card wide run-group-picker-card" data-testid="v5-run-group-picker">
      <div className="section-heading">
        <div>
          <h3>运行记录</h3>
          <p className="muted">选择一条 RunGroup 后，摘要、图表、子实验、分析和产物包都会切换到该记录。最近选择会保存在本机浏览器。</p>
        </div>
        <button type="button" onClick={() => void refreshSelectorGroups()} disabled={selectorBusy}>{selectorBusy ? "刷新中…" : "刷新记录"}</button>
      </div>
      <label className="run-group-picker-field">
        <span>选择运行记录</span>
        <select className="run-group-picker-select" data-testid="v5-run-group-picker-select" aria-label="运行记录" value={selectedGroupId} onChange={(event) => { const next = event.target.value; if (next) void loadGroup(next); }} disabled={selectorBusy && !selectorGroups.length}>
          {!selectedGroupId && <option value="">暂无已选择记录</option>}
          {selectedGroupId && !selectorGroups.some((group) => group.run_group_id === selectedGroupId) && <option value={selectedGroupId}>{selectedGroupId}（当前）</option>}
          {selectorGroups.map((group) => <option key={group.run_group_id} value={group.run_group_id} title={runGroupOptionTitle(group)}>{runGroupOptionLabel(group)}</option>)}
        </select>
      </label>
      {selectedMethodNames.length > 0 && <div className="run-group-method-preview" data-testid="v5-run-group-picker-methods">
        <span className="muted">本记录方法（{selectedMethodNames.length}）</span>
        <div className="run-group-method-chips">{selectedMethodNames.map((name) => <span key={name} title={name}>{compactMethodName(name)}</span>)}</div>
      </div>}
      {selectedGroupId && <p className="muted run-group-picker-current"><span>当前 RunGroup：</span><code>{selectedGroupId}</code></p>}
      {busy && !selectedGroup && <p className="muted">正在加载所选运行记录…</p>}
      {selectorError && <p className="file-error">运行记录列表错误：{selectorError}</p>}
    </article>
    {selectedGroup && <V5ResultsDashboard
      group={selectedGroup}
      aggregate={aggregate}
      children={detail?.children ?? []}
      analysis={analysis}
      catalog={catalog}
      selectedChild={selectedChild}
      selectedChildId={selectedChildId}
      onSelectChild={(childId) => { if (detail) void loadChild(detail.group.run_group_id, childId); }}
      onRebuild={() => void rebuildBundle()}
      rebuilding={bundleBusy}
      storage={storage}
      onCompactStorage={() => void compactStorage()}
      onArchiveStorage={() => void archiveStorage()}
      onRestoreStorage={() => void restoreStorage()}
      storageBusyAction={storageBusyAction}
    />}
    <details className="v5-legacy-results-details">
      <summary>高级 / 旧版完整详细视图</summary>
    {selectedGroup && <V5GroupSummary group={selectedGroup} aggregate={aggregate} children={detail?.children ?? []} />}
    <V5AnalysisPanel analysis={analysis} />
    <V5SkewTpsChart
      children={detail?.children ?? []}
      plannedThetaValues={(selectedGroup?.plan?.workload_points ?? [])
        .map((point) => Number((point as Record<string, unknown>).target_theta))
        .filter((value) => Number.isFinite(value))}
    />
    {detail && <article className="final-card wide">
      <h2>子实验</h2>
      <div className="table-wrap"><table className="v5-results-child-table" data-testid="v5-child-table">
        <thead><tr><th>子实验</th><th>实验类型</th><th>方法</th><th>种子</th><th>重复</th><th>拓扑</th><th>交易</th><th>执行状态</th><th>产物状态</th><th>正式结果</th><th>无回退</th><th>阻断原因</th><th>TPS</th><th>P99</th><th>终态</th><th>未完成</th><th>论文候选</th></tr></thead>
        <tbody>{detail.children.map((child) => <ChildRow key={child.child_run_id} child={child} selected={child.child_run_id === selectedChildId} onSelect={() => void loadChild(detail.group.run_group_id, child.child_run_id)} />)}</tbody>
      </table></div>
    </article>}
    <V5ChildDetail child={selectedChild} />
    {selectedGroup && <V5ArtifactCatalog groupId={selectedGroup.run_group_id} catalog={catalog} onRebuild={() => void rebuildBundle()} rebuilding={bundleBusy} />}
    </details>
    <details className="final-card wide" data-testid="v5-run-group-list" open={historyOpen} onToggle={(event) => setHistoryOpen((event.currentTarget as HTMLDetailsElement).open)}>
      <summary>实验组历史</summary>
      <div className="section-heading">
        <label><span>搜索</span><input aria-label="搜索" value={search} onChange={(event) => { invalidateHistoryRequest(); setSearch(event.target.value); setOffset(0); }} /></label>
        <label><span>状态</span><select aria-label="状态" value={statusFilter} onChange={(event) => { invalidateHistoryRequest(); setStatusFilter(event.target.value); setOffset(0); }}><option value="">全部</option>{["queued", "starting", "running", "completed", "completed_with_failures", "failed", "cancelled"].map((status) => <option key={status} value={status}>{statusLabel(status)}</option>)}</select></label>
        <label><span>方法 ID</span><input aria-label="方法 ID" value={methodFilter} onChange={(event) => { invalidateHistoryRequest(); setMethodFilter(event.target.value); setOffset(0); }} /></label>
        <label><span>实验类型</span><select aria-label="实验类型" value={suiteFilter} onChange={(event) => { invalidateHistoryRequest(); setSuiteFilter(event.target.value); setOffset(0); }}><option value="">全部</option>{["main_experiment", "comparison_experiment", "ablation_experiment", "workload_sensitivity", "topology_scaling", "fault_recovery_experiment"].map((suite) => <option key={suite} value={suite}>{suiteLabel(suite)}</option>)}</select></label>
        <label><input type="checkbox" checked={includeTests} onChange={(event) => { invalidateHistoryRequest(); setIncludeTests(event.target.checked); setOffset(0); }} /> 显示测试记录</label>
        <button type="button" onClick={() => void refreshHistory()} disabled={historyBusy}>刷新实验组</button>
      </div>
      <div className="button-row">
        <button type="button" onClick={() => void cleanupSelectedGroups(true)} disabled={cleanupBusy || !selectedCleanupIds.length}>选中项清理预演</button>
        <button type="button" onClick={() => void cleanupSelectedGroups(false)} disabled={cleanupBusy || !selectedCleanupIds.length}>删除选中</button>
        <button type="button" onClick={() => void dryRunFailedGroups()} disabled={cleanupBusy}>失败实验清理预演</button>
        <button type="button" onClick={() => void scanOrphans()} disabled={cleanupBusy}>扫描孤儿目录</button>
        <button type="button" onClick={() => void cleanupOrphans(true)} disabled={cleanupBusy}>孤儿目录清理预演</button>
        <button type="button" onClick={() => void cleanupOrphans(false)} disabled={cleanupBusy}>删除孤儿目录</button>
      </div>
        <button type="button" onClick={() => void scanLegacySavedConfigs()} disabled={cleanupBusy}>扫描旧方案</button>
        <button type="button" onClick={() => void cleanupLegacySavedConfigs(true)} disabled={cleanupBusy}>旧方案清理预演</button>
        <button type="button" onClick={() => void cleanupLegacySavedConfigs(false)} disabled={cleanupBusy}>删除旧方案</button>
      <CleanupEvidence report={cleanupReport} scan={orphanScan} legacyScan={legacyScan} />
      {historyError && <p className="file-error">历史列表错误：{historyError}</p>}
      {groups.length ? <div className="table-wrap"><table className="v5-results-history-table"><thead><tr><th>清理</th><th>ID</th><th>状态</th><th>计划</th><th>后端</th><th>更新时间</th><th>子实验</th><th>失败</th><th>实验类型</th><th>方法</th></tr></thead><tbody>
        {groups.map((group) => <tr key={group.run_group_id} className={group.run_group_id === selectedGroupId ? "selected-row" : ""}>
          <td><input aria-label={`选择 ${group.run_group_id} 清理`} type="checkbox" checked={selectedCleanupIds.includes(group.run_group_id)} onChange={(event) => toggleCleanupSelection(group.run_group_id, event.target.checked)} /></td>
          <td><button type="button" data-testid="v5-run-group-select" onClick={() => void loadGroup(group.run_group_id)}>{group.run_group_id}</button></td>
          <td><span>{statusLabel(group.status)}</span><small>{group.status}</small></td>
          <td>{group.plan_name || "—"}</td>
          <td><span>{backendLabel(group.execution_backend)}</span><small>{group.execution_backend}</small></td>
          <td>{formatLocalTimestamp(group.updated_at)}</td>
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

function CleanupEvidence({ report, scan, legacyScan }: { report: V5CleanupReport | null; scan: V5OrphanRealClusterScan | null; legacyScan: V5LegacySavedConfigScan | null }) {
  if (!report && !scan && !legacyScan) return null;
  if (scan) {
    return <div className="notice" data-testid="v5-cleanup-evidence">
      <strong>孤儿目录扫描</strong>
      <span>候选目录：{scan.orphan_dirs.length}</span>
      <span>预计释放：{formatBytes(scan.orphan_dirs.reduce((total, item) => total + item.size_bytes, 0))}</span>
    </div>;
  }
  if (legacyScan) {
    return <div className="notice" data-testid="v5-cleanup-evidence">
      <strong>旧方案扫描</strong>
      <span>候选方案：{legacyScan.candidate_count}</span>
      <span>保留：{legacyScan.preserved_configs.length}</span>
      {legacyScan.candidate_configs.length > 0 && <span>候选：{legacyScan.candidate_configs.slice(0, 3).map((item) => item.config_id).join(", ")}</span>}
    </div>;
  }
  const deletedGroups = report?.deleted_run_group_ids.length ?? 0;
  const deletedOutputs = (report?.deleted_output_dirs.length ?? 0) + (report?.deleted_orphan_dirs.length ?? 0);
  const deletedSavedConfigs = report?.deleted_saved_config_ids?.length ?? 0;
  return <div className="notice" data-testid="v5-cleanup-evidence">
    <strong>{report?.dry_run ? "清理预演" : "清理结果"}</strong>
    <span>RunGroup：{deletedGroups}</span>
    <span>输出目录：{deletedOutputs}</span>
    {deletedSavedConfigs > 0 && <span>旧方案：{deletedSavedConfigs}</span>}
    <span>预计释放：{formatBytes(report?.released_bytes ?? 0)}</span>
    {(report?.preserved_run_group_ids.length ?? 0) > 0 && <span>保留：{report?.preserved_run_group_ids.join(", ")}</span>}
    {(report?.skipped_active_runs.length ?? 0) > 0 && <span>运行中跳过：{report?.skipped_active_runs.join(", ")}</span>}
    {(report?.errors.length ?? 0) > 0 && <span>原因：{report?.errors.slice(0, 3).join("; ")}</span>}
    {report?.cleanup_report && <span>清理报告：{report.cleanup_report.json} / {report.cleanup_report.csv}</span>}
  </div>;
}

function ChildRow({ child, selected, onSelect }: { child: V5FormalChildRun; selected: boolean; onSelect: () => void }) {
  const finality = child.result?.summary?.finality_evidence;
  const execution = child.execution_status ?? child.result?.summary?.execution_status ?? child.status;
  const artifact = child.artifact_status ?? child.result?.summary?.artifact_status;
  const eligible = child.formal_eligibility ?? child.result?.summary?.formal_eligibility;
  const blockers = [...(child.execution_gate?.blockers ?? child.result?.summary?.execution_gate?.blockers ?? []), ...(child.artifact_gate?.blockers ?? child.result?.summary?.artifact_gate?.blockers ?? [])];
  const executionLabel = execution ? statusLabel(String(execution)) : "未提供";
  const artifactLabel = artifact === "complete" ? "完整" : artifact === "incomplete" ? "不完整" : String(artifact ?? "未提供");
  return <tr className={selected ? "selected-row" : ""}>
    <td><button type="button" onClick={onSelect}>{child.child_run_id}</button></td>
    <td>{suiteLabel(child.suite_type)}</td>
    <td>{child.method.display_name}</td><td>{child.seed}</td><td>{child.repeat_index + 1}</td>
    <td>{child.topology_point.nodes}/{child.topology_point.shards}/{child.topology_point.validators_per_shard}</td><td>{child.estimated_transactions}</td>
    <td>{executionLabel}</td><td>{artifactLabel}</td><td>{eligible === true ? "可用" : eligible === false ? "不可用" : "未提供"}</td><td>{child.result?.summary?.no_fallback === undefined ? "未提供" : String(child.result.summary.no_fallback)}</td><td>{blockers.length ? blockers.join("; ") : (child.error ?? "无")}</td>
    <td>{metric(child.metrics?.end_to_end_tps ?? finality?.end_to_end_tps ?? child.metrics?.throughput_tps)}</td><td>{metric(child.metrics?.p99_finality_ms ?? finality?.p99_finality_ms ?? child.metrics?.p99_latency_ms)}</td><td>{metric(finality?.terminal_unique_tx_count)}</td><td>{metric(finality?.incomplete_unique_tx_count)}</td><td>{booleanLabel(child.paper_candidate)}</td>
  </tr>;
}

function runGroupOptionLabel(group: V5FormalRunGroupSummary): string {
  // Anchor the selector to creation time. A resumed historical RunGroup may
  // update hours later, and using updated_at made it look like a newly-created
  // experiment. Dates are rendered in the browser's local timezone.
  const time = formatLocalTimestamp(group.created_at || group.updated_at, true);
  const suffix = group.run_group_id.split("_").pop() ?? group.run_group_id;
  const suite = group.suite_names.length ? suiteLabel(group.suite_names[0]) : (group.plan_name || "实验");
  const methods = group.method_names.length
    ? group.method_names.map(compactMethodName).join(" / ")
    : "方法未知";
  return `${time} · ${statusLabel(group.status)} · ${suite} · ${suffix} · ${methods}`;
}

function runGroupOptionTitle(group: V5FormalRunGroupSummary): string {
  const methods = group.method_names.length ? group.method_names.join(" / ") : "方法未知";
  return `${group.run_group_id}\n${statusLabel(group.status)} · ${group.suite_names.map(suiteLabel).join(" / ") || group.plan_name || "实验"}\n${methods}`;
}

function compactMethodName(name: string): string {
  const lower = name.toLowerCase();
  if (lower.includes("block-stm")) return "Block-STM";
  if (lower.includes("groundhog")) return "Groundhog";
  if (lower.includes("batch-si")) return "Batch-SI";
  if (lower.includes("aria")) return "Aria";
  if (lower.includes("block shredding") || lower.includes("bsx")) return "BSX";
  if ((lower.includes("address") && lower.includes("conflict graph")) || lower.includes("acg")) return "ACG";
  if (lower.includes("conflict graph") || /(^|\W)cg(\W|$)/i.test(name)) return "CG";
  if (lower.includes("serial")) return "Serial";
  return name;
}

function formatLocalTimestamp(value: string | undefined | null, compact = false): string {
  if (!value) return compact ? "时间未知" : "—";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  if (compact) {
    return parsed.toLocaleString(undefined, { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", hour12: false });
  }
  return parsed.toLocaleString();
}

function metric(value: unknown): string { return value === undefined || value === null ? "—" : String(value); }
function terminal(status: string): boolean { return terminalStatuses.includes(status); }
function message(value: unknown): string { return value instanceof Error ? value.message : String(value); }
function formatBytes(value: number): string {
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KiB`;
  return `${(value / 1024 / 1024).toFixed(1)} MiB`;
}
