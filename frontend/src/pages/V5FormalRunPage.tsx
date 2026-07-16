import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";

import {
  createV5FormalRunGroup,
  fetchV5FormalRunGroup,
  fetchV5PluginCatalog,
  fetchV5WorkloadDatasets,
  listV3SavedConfigs,
  previewV5FormalRun,
  previewV5Workload,
  validateV5ExperimentSpec,
  type V5CompatibilityResult,
  type V5ExperimentSpec,
  type V5FormalMethod,
  type V5FormalPreviewResponse,
  type V5FormalRunGroupDetail,
  type V5FormalRunRequest,
  type V5FormalSuite,
  type V5PluginManifest,
  type V5PluginSelection,
  type V5WorkloadDatasetSummary,
  type V5WorkloadPreview,
  type V5WorkloadSourceSpec,
} from "../api";
import WorkloadPreviewPanel from "../components/v5/WorkloadPreviewPanel";
import WorkloadSourceEditor, { type WorkloadEditorState } from "../components/v5/WorkloadSourceEditor";
import { backendLabel, blockerLabel, faultModeLabel, roleLabel, statusLabel, suiteLabel } from "../v5Labels";
import { applyV5MethodSelections, defaultV5PluginSelections, parseSavedV5Method } from "../v5MethodProfile";

const recentGroupKey = "mbe.v5FormalRunGroupId";
const suites: V5FormalSuite[] = ["main_experiment", "comparison_experiment", "ablation_experiment", "workload_sensitivity", "topology_scaling", "fault_recovery_experiment"];
const alphaValues = [0, 0.2, 0.4, 0.6, 0.8, 1, 1.2, 1.4];

type Topology = { nodes: number; shards: number; validators_per_shard: number };
type WorkloadPoint = { tx_count: number; cross_shard_ratio?: number; timeout_every?: number; target_alpha?: number };
type FaultMode = "disabled" | "delay_only" | "network_drop";
type FaultPoint = { mode: FaultMode; delay_ms?: number; drop_rate?: number; drop_message_types?: string[] };
type Props = { onOpenResults?: (groupId: string) => void; onPreferredMethodUnavailable?: (methodId: string) => void; preferredMethodId?: string };

const defaultWorkload: WorkloadEditorState = {
  mode: "synthetic",
  datasetId: "",
  txCount: 10_000,
  useFullDataset: false,
  seedText: "11",
  targetAlpha: 1,
  crossShardRatio: 0,
  timeoutEvery: 0,
  timeoutEnabled: false,
  skewAxis: "contract",
};

export default function V5FormalRunPage({ onOpenResults, onPreferredMethodUnavailable, preferredMethodId = "" }: Props) {
  const [catalog, setCatalog] = useState<V5PluginManifest[]>([]);
  const [datasets, setDatasets] = useState<V5WorkloadDatasetSummary[]>([]);
  const [savedMethods, setSavedMethods] = useState<V5FormalMethod[]>([]);
  const [selectedMethods, setSelectedMethods] = useState<string[]>(["v5_catalog_default"]);
  const [selectedSuites, setSelectedSuites] = useState<V5FormalSuite[]>(["main_experiment"]);
  const [topology, setTopology] = useState<Topology>({ nodes: 4, shards: 1, validators_per_shard: 4 });
  const [workload, setWorkload] = useState<WorkloadEditorState>(defaultWorkload);
  const [repeats, setRepeats] = useState(1);
  const [workloadPoints, setWorkloadPoints] = useState<WorkloadPoint[]>([]);
  const [topologyPoints, setTopologyPoints] = useState<Topology[]>([]);
  const [faultPoints, setFaultPoints] = useState<FaultPoint[]>([]);
  const [workloadPreview, setWorkloadPreview] = useState<V5WorkloadPreview | null>(null);
  const [workloadPreviewDirty, setWorkloadPreviewDirty] = useState(true);
  const [workloadPreviewError, setWorkloadPreviewError] = useState("");
  const [preview, setPreview] = useState<V5FormalPreviewResponse | null>(null);
  const [previewRequest, setPreviewRequest] = useState<V5FormalRunRequest | null>(null);
  const [methodCompatibility, setMethodCompatibility] = useState<Record<string, V5CompatibilityResult>>({});
  const [groupDetail, setGroupDetail] = useState<V5FormalRunGroupDetail | null>(null);
  const [groupId, setGroupId] = useState("");
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [catalogError, setCatalogError] = useState("");
  const [savedError, setSavedError] = useState("");
  const [busy, setBusy] = useState(false);
  const pollTimer = useRef<number | null>(null);
  const formRevision = useRef(0);
  const preferredConsumed = useRef(false);

  const catalogDefault = useMemo<V5FormalMethod>(() => ({ method_id: "v5_catalog_default", display_name: "目录默认基线", plugin_overrides: {}, role: "baseline" }), []);
  const methods = useMemo(() => [catalogDefault, ...savedMethods], [catalogDefault, savedMethods]);
  const seeds = useMemo(() => parseSeeds(workload.seedText), [workload.seedText]);
  const catalogReady = useMemo(() => {
    const categories = new Set(catalog.map((item) => item.category));
    return catalog.length > 0 && defaultV5PluginSelections(catalog).length === categories.size;
  }, [catalog]);
  const selected = methods.filter((method) => selectedMethods.includes(method.method_id));
  const previewRunnable = Boolean(preview?.rows.length && preview.rows.every((row) => row.runnable && !row.blockers.length));
  const workloadRunnable = Boolean(workloadPreview && !workloadPreviewDirty && !workloadPreview.blockers.length && !workloadPreviewError);
  const resources = estimateFormalResources(selectedSuites, selected.length, seeds.length, repeats, topology, workload.txCount, workloadPoints, topologyPoints, faultPoints);

  useEffect(() => { void loadCatalog(); const stored = window.localStorage.getItem(recentGroupKey); if (stored) void queryGroup(stored, true); return stopPolling; }, []);
  useEffect(() => { if (datasets.length && !workload.datasetId) setWorkload((current) => ({ ...current, datasetId: datasets[0].dataset_id })); }, [datasets.length, workload.datasetId]);
  useEffect(() => { if (groupId && groupDetail && !terminal(groupDetail.group.status)) schedulePolling(groupId); else stopPolling(); return stopPolling; }, [groupId, groupDetail?.group.status]);

  async function loadCatalog() {
    setBusy(true);
    try {
      const [pluginResponse, savedResponse, datasetResponse] = await Promise.all([fetchV5PluginCatalog("real_cluster"), listV3SavedConfigs("method"), fetchV5WorkloadDatasets()]);
      setCatalog(pluginResponse);
      setDatasets(datasetResponse);
      const categories = new Set(pluginResponse.map((item) => item.category));
      setCatalogError(pluginResponse.length > 0 && defaultV5PluginSelections(pluginResponse).length === categories.size ? "" : "真实集群插件目录不完整。");
      const parsed = savedResponse.flatMap((item) => { const method = parseSavedV5Method(item, pluginResponse); return method ? [method] : []; });
      setSavedMethods(parsed);
      setSavedError("");
      const available = ["v5_catalog_default", ...parsed.map((item) => item.method_id)];
      if (preferredMethodId && available.includes(preferredMethodId)) { setSelectedMethods([preferredMethodId]); preferredConsumed.current = true; }
      else { setSelectedMethods(["v5_catalog_default"]); if (preferredMethodId && !preferredConsumed.current) { preferredConsumed.current = true; onPreferredMethodUnavailable?.(preferredMethodId); } }
    } catch (caught) {
      setCatalogError(errorMessage(caught));
      setSavedMethods([]);
      setSelectedMethods(["v5_catalog_default"]);
    } finally {
      setBusy(false);
    }
  }

  function invalidateAll() {
    formRevision.current += 1;
    setWorkloadPreviewDirty(true);
    setPreview(null);
    setPreviewRequest(null);
    setMethodCompatibility({});
  }
  function update(fn: () => void) { fn(); invalidateAll(); }
  function updateWorkload(next: WorkloadEditorState) { setWorkload(next); invalidateAll(); }

  function currentWorkloadSource(): V5WorkloadSourceSpec | null {
    const seed = seeds[0];
    if (!globalThis.Number.isInteger(seed)) return null;
    if (workload.mode === "synthetic") {
      return { source_type: "synthetic", plugin_id: "deterministic_signed_synthetic", requested_tx_count: workload.txCount, seed, selection_mode: "contiguous_window", replay_mode: "max_throughput" };
    }
    const dataset = datasets.find((item) => item.dataset_id === workload.datasetId);
    if (!dataset?.selectable) return null;
    const datasetWithAxes = dataset as V5WorkloadDatasetSummary & { default_skew_axis?: string | null; supported_skew_axes?: string[] };
    const skewAxis = workload.skewAxis || datasetWithAxes.default_skew_axis || datasetWithAxes.supported_skew_axes?.[0] || "contract";
    return {
      source_type: "dataset",
      plugin_id: "canonical_trace_replay",
      dataset_id: dataset.dataset_id,
      variant_mode: workload.mode === "dataset_derived" ? "key_zipf" : "original_window",
      requested_tx_count: workload.useFullDataset ? dataset.row_count : workload.txCount,
      use_full_dataset: workload.useFullDataset,
      seed,
      selection_mode: "contiguous_window",
      replay_mode: "max_throughput",
      skew_axis: workload.mode === "dataset_derived" ? skewAxis : undefined,
      target_alpha: workload.mode === "dataset_derived" ? workload.targetAlpha : undefined,
      source_sha256: dataset.source_sha256,
    };
  }

  function methodSpec(method: V5FormalMethod, source: V5WorkloadSourceSpec): V5ExperimentSpec {
    const base: V5ExperimentSpec = { name: "v5_formal_real_cluster", execution_backend: "real_cluster", plugin_selections: defaultV5PluginSelections(catalog), topology, tx_count: source.requested_tx_count, seed: source.seed, workload_source: source, duration_ms: 6000, fault_policy: { mode: "disabled" }, requested_metrics: [] };
    const spec = applyV5MethodSelections(base, method);
    return { ...spec, plugin_selections: patchWorkloadSelections(spec.plugin_selections, source) };
  }

  function patchWorkloadSelections(selections: V5PluginSelection[], source: V5WorkloadSourceSpec): V5PluginSelection[] {
    return selections.map((selection) => {
      if (selection.category !== "workload") return selection;
      if (source?.source_type === "dataset") return { ...selection, plugin_id: "canonical_trace_replay", config: {} };
      return { ...selection, plugin_id: "deterministic_signed_synthetic", config: { ...selection.config, cross_shard_ratio: workload.crossShardRatio, timeout_every: workload.timeoutEnabled ? workload.timeoutEvery : 0 } };
    });
  }

  function buildRequest(): V5FormalRunRequest | null {
    const source = currentWorkloadSource();
    if (!source) return null;
    const base_spec = methodSpec(selected[0] ?? catalogDefault, source);
    const e2e = new URLSearchParams(window.location.search).get("e2e") === "1";
    return { execution_backend: "real_cluster", plan: { name: "v5_formal_real_cluster", base_spec, suites: selectedSuites, methods: selected, seeds, repeats, workload_points: cleanWorkloadPoints(workloadPoints), topology_points: topologyPoints, fault_points: faultPoints, source_label: e2e ? "e2e" : "user", tags: e2e ? ["e2e"] : [] } };
  }

  async function previewWorkload() {
    const source = currentWorkloadSource();
    if (!source) { setWorkloadPreviewError("workload_source 无法构造，请检查数据集可用性和 seed。"); return; }
    setBusy(true);
    try {
      const response = await previewV5Workload(source);
      setWorkloadPreview(response);
      setWorkloadPreviewError("");
      setWorkloadPreviewDirty(Boolean(response.blockers.length));
      setPreview(null);
      setPreviewRequest(null);
    } catch (caught) {
      setWorkloadPreview(null);
      setWorkloadPreviewError(errorMessage(caught));
      setWorkloadPreviewDirty(true);
    } finally {
      setBusy(false);
    }
  }

  async function previewMatrix() {
    const request = buildRequest();
    const source = currentWorkloadSource();
    let localWorkloadRunnable = workloadRunnable;
    if (source && (!workloadPreview || workloadPreviewDirty || workloadPreviewError)) {
      try {
        const nextPreview = await previewV5Workload(source);
        setWorkloadPreview(nextPreview);
        setWorkloadPreviewError("");
        setWorkloadPreviewDirty(Boolean(nextPreview.blockers.length));
        localWorkloadRunnable = !nextPreview.blockers.length;
      } catch (caught) {
        setWorkloadPreview(null);
        setWorkloadPreviewError(errorMessage(caught));
        setWorkloadPreviewDirty(true);
        localWorkloadRunnable = false;
      }
    }
    const form = formError({ catalogReady, selected, selectedSuites, topology, workload, source, seeds, repeats, workloadPoints, topologyPoints, faultPoints, estimatedChildren: resources.children, workloadRunnable: localWorkloadRunnable });
    if (!request || form) { setError(form ?? "无法构造 Formal RunGroup 请求。"); return; }
    setBusy(true);
    try {
      const compatibility = Object.fromEntries(await Promise.all(selected.map(async (method) => {
        const spec = methodSpec(method, request.plan.base_spec.workload_source ?? currentWorkloadSource()!);
        return [method.method_id, await validateV5ExperimentSpec(spec)] as const;
      })));
      setMethodCompatibility(compatibility);
      const response = await previewV5FormalRun(request);
      setPreview(response);
      setPreviewRequest(request);
      setError("");
    } catch (caught) {
      setPreview(null);
      setPreviewRequest(null);
      setError(errorMessage(caught));
    } finally {
      setBusy(false);
    }
  }

  async function startGroup() {
    if (!previewRequest || !previewRunnable || workloadPreviewDirty) { setError("配置已变化，请重新预览。"); return; }
    setBusy(true);
    try {
      const group = await createV5FormalRunGroup(previewRequest);
      const detail = await fetchV5FormalRunGroup(group.run_group_id);
      setGroupDetail(detail);
      setGroupId(detail.group.run_group_id);
      window.localStorage.setItem(recentGroupKey, detail.group.run_group_id);
      setMessage(`RunGroup 已启动：${detail.group.run_group_id}`);
      schedulePolling(detail.group.run_group_id);
      setError("");
    } catch (caught) { setError(errorMessage(caught)); } finally { setBusy(false); }
  }

  async function queryGroup(id = groupId, silent = false) {
    if (!id) return;
    try {
      const detail = await fetchV5FormalRunGroup(id);
      setGroupDetail(detail);
      setGroupId(id);
      if (!silent) setMessage(`已刷新 RunGroup：${id}`);
      if (!terminal(detail.group.status)) schedulePolling(id);
    } catch (caught) { if (!silent) setError(errorMessage(caught)); }
  }
  function schedulePolling(id: string) { stopPolling(); pollTimer.current = window.setTimeout(() => { void queryGroup(id, true); }, 1500); }
  function stopPolling() { if (pollTimer.current !== null) { window.clearTimeout(pollTimer.current); pollTimer.current = null; } }

  return <section className="page-grid" data-testid="v5-formal-run-page">
    <article className="overview-hero wide">
      <p className="eyebrow">V5 Formal RunGroup</p>
      <h2>运行正式实验</h2>
      <p>负载来源进入 immutable Child ExperimentSpec。数据集 preview 和 Formal Matrix preview 都必须随配置变化重新生成。</p>
      {catalogError && <p className="file-error">{catalogError}</p>}
      {savedError && <p className="file-error">{savedError}</p>}
      {message && <p className="notice">{message}</p>}
      {error && <p className="file-error">{error}</p>}
      <CurrentMethods methods={selected} preferredMethodId={preferredMethodId} />
    </article>

    <article className="final-card wide">
      <h3>实验类型</h3>
      <div className="selectable-card-grid">{suites.map((suite) => <label key={suite} data-testid={`v5-suite-${suite}`} className={`checkbox-card compact ${selectedSuites.includes(suite) ? "selected" : ""}`}><span><strong>{suiteLabel(suite)}</strong><small>{suite}</small></span><input type="checkbox" checked={selectedSuites.includes(suite)} onChange={() => update(() => setSelectedSuites((current) => toggle(suite, current)))} /></label>)}</div>
    </article>

    <article className="final-card wide">
      <h3>实验方法</h3>
      <div className="selectable-card-grid">{methods.map((method) => <label key={method.method_id} data-testid={`v5-run-method-${method.method_id}`} className={`checkbox-card compact ${selectedMethods.includes(method.method_id) ? "selected" : ""}`}><span><strong>{method.display_name}</strong><small>方法 ID：{method.method_id}</small><small>角色：{roleLabel(method.role ?? "custom")}</small></span><input type="checkbox" checked={selectedMethods.includes(method.method_id)} onChange={() => update(() => setSelectedMethods((current) => toggle(method.method_id, current)))} /></label>)}</div>
      {!selected.length && <p className="file-error">尚未选择执行方法。</p>}
    </article>

    <WorkloadSourceEditor state={workload} datasets={datasets} onChange={updateWorkload} />

    <article className="final-card wide">
      <h3>拓扑与重复</h3>
      <div className="experiment-condition-grid">
        <NumericInput label="节点数" aria="nodes" value={topology.nodes} onChange={(value) => update(() => setTopology({ ...topology, nodes: value }))} />
        <NumericInput label="分片数" aria="shards" value={topology.shards} onChange={(value) => update(() => setTopology({ ...topology, shards: value }))} />
        <NumericInput label="每片验证节点数" aria="validators per shard" value={topology.validators_per_shard} onChange={(value) => update(() => setTopology({ ...topology, validators_per_shard: value }))} />
        <NumericInput label="重复次数" aria="repeats" value={repeats} onChange={(value) => update(() => setRepeats(value))} />
      </div>
    </article>

    <WorkloadPreviewPanel preview={workloadPreview} dirty={workloadPreviewDirty} error={workloadPreviewError} onPreview={() => void previewWorkload()} disabled={busy} />

    {selectedSuites.includes("workload_sensitivity") && <PointEditor title="负载扫描点" onAdd={() => update(() => setWorkloadPoints((items) => [...items, defaultWorkloadPoint(workload)]))}>{workloadPoints.map((point, index) => <div key={index} className="experiment-condition-grid">
      <NumericInput label="交易数量" value={point.tx_count} onChange={(value) => update(() => setWorkloadPoints(replace(workloadPoints, index, { ...point, tx_count: value })))} />
      {workload.mode === "dataset_derived" && <NumericInput label="target_alpha" value={point.target_alpha ?? workload.targetAlpha} step={0.2} onChange={(value) => update(() => setWorkloadPoints(replace(workloadPoints, index, { ...point, target_alpha: snapAlpha(value) })))} />}
      {workload.mode === "synthetic" && <><NumericInput label="跨片交易比例" value={point.cross_shard_ratio ?? 0} step={0.01} onChange={(value) => update(() => setWorkloadPoints(replace(workloadPoints, index, { ...point, cross_shard_ratio: value })))} /><NumericInput label="timeout_every" value={point.timeout_every ?? 0} onChange={(value) => update(() => setWorkloadPoints(replace(workloadPoints, index, { ...point, timeout_every: value })))} /></>}
      <button type="button" onClick={() => update(() => setWorkloadPoints(workloadPoints.filter((_, item) => item !== index)))}>删除点</button>
    </div>)}</PointEditor>}

    {selectedSuites.includes("topology_scaling") && <PointEditor title="拓扑扫描点" onAdd={() => update(() => setTopologyPoints((items) => [...items, { ...topology }]))}>{topologyPoints.map((point, index) => <div key={index} className="experiment-condition-grid">
      <NumericInput label="节点数" value={point.nodes} onChange={(value) => update(() => setTopologyPoints(replace(topologyPoints, index, { ...point, nodes: value })))} />
      <NumericInput label="分片数" value={point.shards} onChange={(value) => update(() => setTopologyPoints(replace(topologyPoints, index, { ...point, shards: value })))} />
      <NumericInput label="每片验证节点数" value={point.validators_per_shard} onChange={(value) => update(() => setTopologyPoints(replace(topologyPoints, index, { ...point, validators_per_shard: value })))} />
      <button type="button" onClick={() => update(() => setTopologyPoints(topologyPoints.filter((_, item) => item !== index)))}>删除点</button>
    </div>)}</PointEditor>}

    {selectedSuites.includes("fault_recovery_experiment") && <PointEditor title="故障扫描点" onAdd={() => update(() => setFaultPoints((items) => [...items, { mode: "disabled" }]))}>{faultPoints.map((point, index) => <div key={index} className="experiment-condition-grid">
      <label><span>故障模式</span><select value={point.mode} onChange={(event) => update(() => setFaultPoints(replace(faultPoints, index, defaultFaultPoint(event.target.value as FaultMode))))}>{(["disabled", "delay_only", "network_drop"] as FaultMode[]).map((mode) => <option key={mode} value={mode}>{faultModeLabel(mode)} ({mode})</option>)}</select></label>
      {point.mode !== "disabled" && <NumericInput label="delay_ms" value={point.delay_ms ?? 5} min={0} max={1000} onChange={(value) => update(() => setFaultPoints(replace(faultPoints, index, { ...point, delay_ms: value })))} />}
      {point.mode === "network_drop" && <NumericInput label="drop_rate" value={point.drop_rate ?? 0.1} min={0.01} max={1} step={0.01} onChange={(value) => update(() => setFaultPoints(replace(faultPoints, index, { ...point, drop_rate: value })))} />}
      <button type="button" onClick={() => update(() => setFaultPoints(faultPoints.filter((_, item) => item !== index)))}>删除点</button>
    </div>)}</PointEditor>}

    <article className="final-card wide">
      <p>预计子实验：<strong data-testid="v5-estimated-children">{resources.children}</strong>；预计节点进程启动次数：<strong data-testid="v5-estimated-process-starts">{resources.processStarts}</strong>；预计交易总量：<strong data-testid="v5-estimated-transactions">{resources.transactions}</strong></p>
      <div className="button-row"><button type="button" data-testid="v5-formal-preview-button" onClick={() => void previewMatrix()} disabled={busy || !catalogReady || !selected.length}>预览正式实验矩阵</button><button type="button" data-testid="v5-start-run-group-button" className="v3-secondary-button" onClick={() => void startGroup()} disabled={busy || !previewRunnable}>启动真实集群实验组</button></div>
      {!workloadRunnable && <p className="file-error">请先完成可通过的 workload preview。</p>}
      {Object.entries(methodCompatibility).map(([id, result]) => <p key={id} className={result.valid ? "muted" : "file-error"}>方法 {id}：{result.valid ? "兼容" : result.blockers.map(blockerLabel).join("；")}</p>)}
      {preview && <PreviewTable preview={preview} source={currentWorkloadSource()} />}
    </article>

    <article className="final-card wide">
      <div className="section-heading"><div><h3>实验组状态</h3><p className="muted">最近的实验组 ID 保存在浏览器，只用于刷新后的查询。</p></div><button type="button" onClick={() => void queryGroup()} disabled={!groupId || busy}>重新查询</button></div>
      {groupId && <p><strong>run_group_id：</strong><code>{groupId}</code> {onOpenResults && <button type="button" onClick={() => onOpenResults(groupId)}>查看结果与产物</button>}</p>}
      {groupDetail && <GroupStatus detail={groupDetail} />}
    </article>
  </section>;
}

function PreviewTable({ preview, source }: { preview: V5FormalPreviewResponse; source: V5WorkloadSourceSpec | null }) {
    return <div className="table-wrap"><p data-testid="v5-formal-preview-summary"><strong>执行后端：</strong>{preview.execution_backend}；<strong>矩阵行数：</strong>{preview.rows.length}</p><table><thead><tr><th>实验类型</th><th>方法</th><th>source_type</th><th>dataset_id</th><th>variant</th><th>count</th><th>seed</th><th>axis</th><th>alpha</th><th>truth</th><th>materialization</th><th>兼容性</th></tr></thead><tbody>{preview.rows.map((row) => <tr key={row.child_run_id} data-method-config-id={row.method_config_id}><td>{suiteLabel(row.suite_type)}</td><td>{row.method.display_name}</td><td>{source?.source_type ?? "synthetic"}</td><td>{source?.dataset_id ?? "synthetic"}</td><td>{source?.variant_mode ?? "synthetic"}</td><td>{row.estimated_transactions}</td><td>{row.seed}</td><td>{stringValue(source?.skew_axis)}</td><td>{stringValue(row.workload_point.target_alpha ?? source?.target_alpha)}</td><td>{source?.source_type === "dataset" ? (source.variant_mode === "key_zipf" || source.variant_mode === "contract_zipf" ? "real_derived_resampled" : "real_observed") : "synthetic_generated"}</td><td>{source?.source_type === "dataset" ? "child_start_before_materialization" : "not_required"}</td><td>{row.runnable ? "可运行" : row.blockers.map(blockerLabel).join("；") || "已阻止"}</td></tr>)}</tbody></table></div>;
}

function CurrentMethods({ methods, preferredMethodId }: { methods: V5FormalMethod[]; preferredMethodId: string }) {
  return <div data-testid="v5-run-preferred-method"><strong>当前执行方法：</strong>{methods.length ? methods.map((method) => <span key={method.method_id}> {method.display_name}（{method.method_id}，{method.method_id === preferredMethodId ? "来源：实验设计" : method.method_id === "v5_catalog_default" ? "来源：目录默认基线" : "来源：已保存方法"}，{roleLabel(method.role ?? "custom")}）</span>) : "未选择"}</div>;
}

function NumericInput({ label, aria, value, onChange, step = 1, min = 0, max }: { label: string; aria?: string; value: number; onChange: (value: number) => void; step?: number; min?: number; max?: number }) {
  return <label><span>{label}</span><input aria-label={aria ?? label} type="number" min={min} max={max} step={step} value={value} onChange={(event) => onChange(globalThis.Number(event.target.value))} /></label>;
}

function PointEditor({ title, onAdd, children }: { title: string; onAdd: () => void; children: ReactNode }) {
  return <article className="final-card wide" data-testid={`v5-point-editor-${title}`}><div className="section-heading"><h3>{title}</h3><button type="button" onClick={onAdd}>添加扫描点</button></div>{children}</article>;
}

function GroupStatus({ detail }: { detail: V5FormalRunGroupDetail }) {
  const failed = detail.children.filter((child) => ["failed", "blocked"].includes(child.status)).length;
  return <><p data-testid="v5-formal-group-summary"><strong>状态：</strong>{statusLabel(detail.group.status)}；<strong>执行后端：</strong>{backendLabel(detail.group.execution_backend)}；<strong>子实验：</strong>{detail.group.completed_child_runs}/{detail.group.total_child_runs}；<strong>失败：</strong>{failed}</p><div className="table-wrap"><table data-testid="v5-formal-child-table"><thead><tr><th>子实验</th><th>实验类型</th><th>方法</th><th>种子</th><th>交易</th><th>状态</th><th>无回退</th></tr></thead><tbody>{detail.children.map((child) => <tr key={child.child_run_id}><td>{child.child_run_id}</td><td>{suiteLabel(child.suite_type)}</td><td>{child.method.display_name}</td><td>{child.seed}</td><td>{child.estimated_transactions}</td><td>{child.status}</td><td>{child.result?.summary?.no_fallback === undefined ? "未提供" : String(child.result.summary.no_fallback)}</td></tr>)}</tbody></table></div></>;
}

function formError(input: { catalogReady: boolean; selected: V5FormalMethod[]; selectedSuites: V5FormalSuite[]; topology: Topology; workload: WorkloadEditorState; source: V5WorkloadSourceSpec | null; seeds: number[]; repeats: number; workloadPoints: WorkloadPoint[]; topologyPoints: Topology[]; faultPoints: FaultPoint[]; estimatedChildren: number; workloadRunnable: boolean }): string | null {
  if (!input.catalogReady) return "真实集群插件目录不完整，无法预览。";
  if (!input.selected.length) return "请至少选择一个执行方法。";
  if (!input.selectedSuites.length) return "请至少选择一种实验类型。";
  if (!input.seeds.length) return "随机种子必须是一到十个不重复整数。";
  if (!input.source) return "workload_source 无法构造，请检查数据集和 seed。";
  if (!input.workloadRunnable) return "配置已变化，请先重新运行 workload preview。";
  if (input.topology.nodes < 1 || input.topology.shards < 1 || input.topology.validators_per_shard < 1 || input.topology.nodes !== input.topology.shards * input.topology.validators_per_shard) return "节点数必须等于分片数乘以每片验证节点数。";
  if (input.selectedSuites.includes("comparison_experiment") && input.selected.length < 2) return "方法对比实验至少需要两个方法。";
  if (input.selectedSuites.includes("workload_sensitivity") && input.workloadPoints.length < 2) return "负载敏感性实验至少需要两个负载扫描点。";
  if (input.selectedSuites.includes("topology_scaling") && input.topologyPoints.length < 2) return "拓扑扩展实验至少需要两个拓扑扫描点。";
  if (input.selectedSuites.includes("fault_recovery_experiment") && (input.faultPoints.length < 2 || !input.faultPoints.some((item) => item.mode === "disabled") || !input.faultPoints.some((item) => item.mode !== "disabled"))) return "故障实验需要无故障基准点和至少一个故障点。";
  if (input.estimatedChildren > 100) return "正式矩阵超过 100 个子实验硬上限。";
  return null;
}

function defaultWorkloadPoint(workload: WorkloadEditorState): WorkloadPoint {
  if (workload.mode === "dataset_derived") return { tx_count: workload.txCount, target_alpha: workload.targetAlpha };
  if (workload.mode === "dataset_original") return { tx_count: workload.txCount };
  return { tx_count: workload.txCount, cross_shard_ratio: workload.crossShardRatio, timeout_every: workload.timeoutEnabled ? workload.timeoutEvery : 0 };
}
function defaultFaultPoint(mode: FaultMode): FaultPoint { if (mode === "delay_only") return { mode, delay_ms: 5 }; if (mode === "network_drop") return { mode, drop_rate: 0.1, delay_ms: 0 }; return { mode: "disabled" }; }
function estimateFormalResources(selectedSuites: V5FormalSuite[], methods: number, seeds: number, repeats: number, topology: Topology, txCount: number, workloadPoints: WorkloadPoint[], topologyPoints: Topology[], faultPoints: FaultPoint[]): { children: number; processStarts: number; transactions: number } {
  const factor = methods * seeds * repeats;
  return selectedSuites.reduce((total, suite) => {
    const points = suite === "workload_sensitivity" ? workloadPoints.map((point) => ({ nodes: topology.nodes, txCount: point.tx_count })) : suite === "topology_scaling" ? topologyPoints.map((point) => ({ nodes: point.nodes, txCount })) : suite === "fault_recovery_experiment" ? faultPoints.map(() => ({ nodes: topology.nodes, txCount })) : [{ nodes: topology.nodes, txCount }];
    return { children: total.children + points.length * factor, processStarts: total.processStarts + points.reduce((sum, point) => sum + point.nodes * factor, 0), transactions: total.transactions + points.reduce((sum, point) => sum + point.txCount * factor, 0) };
  }, { children: 0, processStarts: 0, transactions: 0 });
}
function parseSeeds(value: string): number[] { const values = value.split(",").map((item) => item.trim()).filter(Boolean).map((item) => globalThis.Number(item)); return !values.length || values.length > 10 || values.some((item) => !globalThis.Number.isInteger(item)) ? [] : [...new Set(values)]; }
function snapAlpha(value: number): number { return alphaValues.reduce((best, item) => Math.abs(item - value) < Math.abs(best - value) ? item : best, 0); }
function toggle<T>(item: T, values: T[]): T[] { return values.includes(item) ? values.filter((value) => value !== item) : [...values, item]; }
function replace<T>(items: T[], index: number, value: T): T[] { return items.map((item, current) => current === index ? value : item); }
function terminal(status: string): boolean { return ["completed", "completed_with_failures", "failed", "cancelled"].includes(status); }
function errorMessage(error: unknown): string { return error instanceof Error ? error.message : String(error); }
function stringValue(value: unknown): string { return value === undefined || value === null || value === "" ? "未提供" : String(value); }
function cleanWorkloadPoints(points: WorkloadPoint[]): Array<Record<string, number>> { return points.map((point) => Object.fromEntries(Object.entries(point).filter(([, value]) => typeof value === "number" && globalThis.Number.isFinite(value))) as Record<string, number>); }
