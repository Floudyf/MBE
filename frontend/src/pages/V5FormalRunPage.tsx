import { useEffect, useMemo, useRef, useState } from "react";

import {
  createV5FormalRunGroup,
  fetchV5FormalRunGroup,
  fetchV5PluginCatalog,
  listV3SavedConfigs,
  previewV5FormalRun,
  validateV5ExperimentSpec,
  type V5CompatibilityResult,
  type V5ExperimentSpec,
  type V5FormalChildRun,
  type V5FormalMethod,
  type V5FormalPreviewResponse,
  type V5FormalRunRequest,
  type V5FormalRunGroupDetail,
  type V5FormalSuite,
  type V5PluginManifest,
  type V5PluginSelection,
} from "../api";
import { applyV5MethodSelections, defaultV5PluginSelections, parseSavedV5Method } from "../v5MethodProfile";

const recentGroupKey = "mbe.v5FormalRunGroupId";
const suites: Array<{ id: V5FormalSuite; label: string }> = [
  { id: "main_experiment", label: "Main experiment" },
  { id: "comparison_experiment", label: "Comparison" },
  { id: "ablation_experiment", label: "Ablation" },
  { id: "workload_sensitivity", label: "Workload sensitivity" },
  { id: "topology_scaling", label: "Topology scaling" },
  { id: "fault_recovery_experiment", label: "Fault / recovery" },
];

type Topology = { nodes: number; shards: number; validators_per_shard: number };
type Props = { onOpenResults?: (groupId: string) => void; onPreferredMethodUnavailable?: (methodId: string) => void; preferredMethodId?: string };

export default function V5FormalRunPage({ onOpenResults, onPreferredMethodUnavailable, preferredMethodId = "" }: Props) {
  const [catalog, setCatalog] = useState<V5PluginManifest[]>([]);
  const [savedMethods, setSavedMethods] = useState<V5FormalMethod[]>([]);
  const [selectedMethods, setSelectedMethods] = useState<string[]>(["v5_catalog_default"]);
  const [selectedSuites, setSelectedSuites] = useState<V5FormalSuite[]>(["main_experiment"]);
  const [topology, setTopology] = useState<Topology>({ nodes: 4, shards: 1, validators_per_shard: 4 });
  const [txCount, setTxCount] = useState(20);
  const [crossShardRatio, setCrossShardRatio] = useState(0);
  const [timeoutEvery, setTimeoutEvery] = useState(17);
  const [seedText, setSeedText] = useState("11");
  const [repeats, setRepeats] = useState(1);
  const [preview, setPreview] = useState<V5FormalPreviewResponse | null>(null);
  const [previewRequest, setPreviewRequest] = useState<V5FormalRunRequest | null>(null);
  const [methodCompatibility, setMethodCompatibility] = useState<Record<string, V5CompatibilityResult>>({});
  const [groupDetail, setGroupDetail] = useState<V5FormalRunGroupDetail | null>(null);
  const [groupId, setGroupId] = useState("");
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const pollTimer = useRef<number | null>(null);
  const formRevision = useRef(0);
  const preferredConsumed = useRef(false);

  const catalogDefault = useMemo<V5FormalMethod>(() => ({ method_id: "v5_catalog_default", display_name: "V5 Catalog Default", plugin_overrides: {} }), []);
  const methods = useMemo(() => [catalogDefault, ...savedMethods], [catalogDefault, savedMethods]);
  const seeds = useMemo(() => parseSeeds(seedText), [seedText]);
  const catalogReady = useMemo(() => defaultV5PluginSelections(catalog).length === 17, [catalog]);
  const previewRunnable = preview?.rows.length && preview.rows.every((row) => row.runnable && row.blockers.length === 0);

  useEffect(() => {
    void loadCatalog();
    const stored = window.localStorage.getItem(recentGroupKey);
    if (stored) void queryGroup(stored, true);
    return () => stopPolling();
  }, []);

  useEffect(() => {
    if (!groupId || !groupDetail || isTerminalGroup(groupDetail.group.status)) return;
    stopPolling();
    pollTimer.current = window.setTimeout(() => void queryGroup(groupId, true), 1500);
    return () => stopPolling();
  }, [groupId, groupDetail]);

  async function loadCatalog() {
    try {
      const [items, saved] = await Promise.all([fetchV5PluginCatalog("real_cluster"), listV3SavedConfigs("method")]);
      setCatalog(items);
      const parsed = saved.map((item) => parseSavedV5Method(item, items)).filter((item): item is V5FormalMethod => item !== null);
      setSavedMethods(parsed);
      if (!preferredConsumed.current && preferredMethodId) {
        preferredConsumed.current = true;
        const preferred = parsed.find((item) => item.method_id === preferredMethodId);
        if (preferred) { setSelectedMethods([preferred.method_id]); invalidatePreview(); setMessage(`Method received from Design: ${preferred.display_name}`); }
        else { setSelectedMethods(["v5_catalog_default"]); onPreferredMethodUnavailable?.(preferredMethodId); setError(`Preferred Method ${preferredMethodId} is unavailable or incompatible; V5 Catalog Default was selected instead.`); }
      }
    } catch (caught) {
      setError(errorMessage(caught));
    }
  }

  function invalidatePreview() {
    formRevision.current += 1;
    setPreview(null);
    setPreviewRequest(null);
    setMethodCompatibility({});
    setError("");
  }

  function buildRequest(): ReturnType<typeof requestFor> | null {
    const problem = formError({ catalog, selectedMethods, selectedSuites, topology, txCount, crossShardRatio, timeoutEvery, seeds, repeats });
    if (problem) {
      setError(problem);
      return null;
    }
    const byId = new Map(methods.map((method) => [method.method_id, method]));
    const selected = selectedMethods.map((id) => byId.get(id)).filter((method): method is V5FormalMethod => Boolean(method));
    return requestFor(catalog, selected, selectedSuites, topology, txCount, crossShardRatio, timeoutEvery, seeds, repeats);
  }

  async function previewMatrix() {
    const request = buildRequest();
    if (!request) return;
    const revision = formRevision.current;
    setBusy(true);
    try {
      const compatibilityEntries = await Promise.all(request.plan.methods.map(async (method) => [method.method_id, await validateV5ExperimentSpec(effectiveSpecFor(request.plan.base_spec, method))] as const));
      const compatibility = Object.fromEntries(compatibilityEntries);
      const response = await previewV5FormalRun(request);
      if (revision !== formRevision.current) return;
      const merged = mergeCompatibility(response, compatibility);
      setMethodCompatibility(compatibility);
      setPreview(merged);
      setPreviewRequest(request);
      setMessage(`Formal matrix contains ${merged.rows.length} real_cluster child run(s).`);
      setError("");
    } catch (caught) {
      if (revision === formRevision.current) setError(errorMessage(caught));
    } finally {
      setBusy(false);
    }
  }

  async function startGroup() {
    if (!previewRequest || !previewRunnable) {
      setError("Preview the current form before starting a RunGroup.");
      return;
    }
    setBusy(true);
    try {
      const group = await createV5FormalRunGroup(previewRequest);
      window.localStorage.setItem(recentGroupKey, group.run_group_id);
      setGroupId(group.run_group_id);
      setGroupDetail({ group, children: [] });
      setMessage(`Started V5 Formal RunGroup ${group.run_group_id}.`);
      setError("");
      await queryGroup(group.run_group_id, true);
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setBusy(false);
    }
  }

  async function queryGroup(id = groupId, quiet = false) {
    if (!id) return;
    try {
      const detail = await fetchV5FormalRunGroup(id);
      setGroupId(id);
      setGroupDetail(detail);
      if (!quiet) setMessage(`Refreshed RunGroup ${id}.`);
      setError("");
    } catch (caught) {
      setError(errorMessage(caught));
    }
  }

  function stopPolling() {
    if (pollTimer.current !== null) {
      window.clearTimeout(pollTimer.current);
      pollTimer.current = null;
    }
  }

  return <section className="page-grid" data-testid="v5-formal-run-page">
    <article className="final-card wide page-hero">
      <p className="eyebrow">V5 Formal Experiment / real_cluster</p>
      <h2>运行实验</h2>
      <p>正式执行使用独立 OS 进程和 localhost TCP。当前数据源为 Deterministic Signed Synthetic；Real Cluster 启动失败会直接失败，不会回退到 Simulation 或 V4 smoke。</p>
      {error && <p className="file-error">{error}</p>}
      {message && <p className="notice">{message}</p>}
      {preferredMethodId && <p data-testid="v5-run-preferred-method">{preferredMethodId} {methods.find((item) => item.method_id === preferredMethodId)?.display_name ?? "unavailable"}</p>}
    </article>

    <article className="final-card wide">
      <h3>Experiment Suite</h3>
      <div className="selectable-card-grid">{suites.map((suite) => <label key={suite.id} className={`checkbox-card compact ${selectedSuites.includes(suite.id) ? "selected" : ""}`}><span><strong>{suite.label}</strong><small>{suite.id}</small></span><input type="checkbox" checked={selectedSuites.includes(suite.id)} onChange={() => { setSelectedSuites((current) => toggle(suite.id, current)); invalidatePreview(); }} /></label>)}</div>
    </article>

    <article className="final-card wide">
      <h3>Methods</h3>
      <p className="muted">Catalog Default uses the real-cluster catalog selection. Saved methods are shown only when their V5 plugin profile is parseable; formal methods currently override plugin IDs while the base ExperimentSpec supplies shared plugin parameters.</p>
      <div className="selectable-card-grid">{methods.map((method) => <label key={method.method_id} data-testid={`v5-run-method-${method.method_id}`} className={`checkbox-card compact ${selectedMethods.includes(method.method_id) ? "selected" : ""}`}><span><strong>{method.display_name}</strong><small>{method.method_id}</small><small>{Object.keys(method.plugin_overrides).length ? `${Object.keys(method.plugin_overrides).length} plugin override(s)` : "catalog defaults"}</small></span><input type="checkbox" checked={selectedMethods.includes(method.method_id)} onChange={() => { setSelectedMethods((current) => toggle(method.method_id, current)); invalidatePreview(); }} /></label>)}</div>
      {!savedMethods.length && <p className="muted">No compatible saved V5 method profiles are currently available.</p>}
    </article>

    <article className="final-card wide">
      <h3>Workload and topology</h3>
      <p className="muted">Data source: Deterministic Signed Synthetic</p>
      <div className="experiment-condition-grid">
        <label><span>nodes</span><input aria-label="nodes" type="number" min={1} max={16} value={topology.nodes} onChange={(event) => { setTopology({ ...topology, nodes: Number(event.target.value) }); invalidatePreview(); }} /></label>
        <label><span>shards</span><input aria-label="shards" type="number" min={1} max={4} value={topology.shards} onChange={(event) => { setTopology({ ...topology, shards: Number(event.target.value) }); invalidatePreview(); }} /></label>
        <label><span>validators per shard</span><input aria-label="validators per shard" type="number" min={1} max={16} value={topology.validators_per_shard} onChange={(event) => { setTopology({ ...topology, validators_per_shard: Number(event.target.value) }); invalidatePreview(); }} /></label>
        <label><span>tx_count</span><input aria-label="tx_count" type="number" min={1} max={10000} value={txCount} onChange={(event) => { setTxCount(Number(event.target.value)); invalidatePreview(); }} /></label>
        <label><span>cross_shard_ratio</span><input aria-label="cross_shard_ratio" type="number" min={0} max={1} step={0.01} value={crossShardRatio} onChange={(event) => { setCrossShardRatio(Number(event.target.value)); invalidatePreview(); }} /></label>
        <label><span>timeout_every</span><input aria-label="timeout_every" type="number" min={0} max={1000} value={timeoutEvery} onChange={(event) => { setTimeoutEvery(Number(event.target.value)); invalidatePreview(); }} /></label>
        <label><span>seeds</span><input aria-label="seeds" value={seedText} onChange={(event) => { setSeedText(event.target.value); invalidatePreview(); }} /><small>Comma-separated integers, up to 10.</small></label>
        <label><span>repeats</span><input aria-label="repeats" type="number" min={1} max={20} value={repeats} onChange={(event) => { setRepeats(Number(event.target.value)); invalidatePreview(); }} /></label>
      </div>
    </article>

    <article className="final-card wide">
      <div className="button-row"><button type="button" onClick={() => void previewMatrix()} disabled={busy || !catalogReady}>Preview Formal Matrix</button><button type="button" className="v3-secondary-button" onClick={() => void startGroup()} disabled={busy || !previewRunnable}>Start Real-Cluster RunGroup</button></div>
      {Object.entries(methodCompatibility).map(([methodId, result]) => <p key={methodId} className={result.valid ? "muted" : "file-error"}>Method {methodId}: {result.valid ? "compatible" : result.blockers.join("; ") || "blocked"}{result.warnings.length ? ` (${result.warnings.join("; ")})` : ""}</p>)}
      {preview && <PreviewTable preview={preview} />}
    </article>

    <article className="final-card wide">
      <div className="section-heading"><div><h3>RunGroup status</h3><p className="muted">The latest RunGroup ID is persisted locally for query-after-refresh; it is never re-created automatically.</p></div><button type="button" onClick={() => void queryGroup()} disabled={!groupId || busy}>重新查询</button></div>
      {groupId && <p><strong>run_group_id:</strong> <code>{groupId}</code> {onOpenResults && <button type="button" onClick={() => onOpenResults(groupId)}>Open Results</button>}</p>}
      {groupDetail && <GroupStatus detail={groupDetail} />}
    </article>
  </section>;
}

function requestFor(catalog: V5PluginManifest[], methods: V5FormalMethod[], suites: V5FormalSuite[], topology: Topology, txCount: number, crossShardRatio: number, timeoutEvery: number, seeds: number[], repeats: number): V5FormalRunRequest {
  const plugin_selections: V5PluginSelection[] = defaultV5PluginSelections(catalog).map((selection) => selection.category === "workload" ? { ...selection, config: { ...selection.config, cross_shard_ratio: crossShardRatio, timeout_every: timeoutEvery } } : selection);
  const base_spec: V5ExperimentSpec = { name: "v5_formal_real_cluster", execution_backend: "real_cluster", plugin_selections, topology, tx_count: txCount, seed: seeds[0], duration_ms: 6000, fault_policy: { mode: "disabled" }, requested_metrics: [] };
  return { execution_backend: "real_cluster" as const, plan: { name: "v5_formal_real_cluster", base_spec, suites, methods, seeds, repeats, topology_points: [], workload_points: [], fault_points: [] } };
}


function formError(input: { catalog: V5PluginManifest[]; selectedMethods: string[]; selectedSuites: V5FormalSuite[]; topology: Topology; txCount: number; crossShardRatio: number; timeoutEvery: number; seeds: number[]; repeats: number }): string | null {
  if (!input.catalog.length || defaultV5PluginSelections(input.catalog).length !== 17) return "The real_cluster plugin catalog is incomplete; wait for the catalog to load before previewing.";
  if (!input.selectedMethods.length) return "Select at least one method before previewing.";
  if (!input.selectedSuites.length) return "Select at least one experiment suite before previewing.";
  if (!input.seeds.length) return "Seeds must be one to ten unique integers, for example 11,12,13.";
  if (input.topology.nodes < 1 || input.topology.shards < 1 || input.topology.validators_per_shard < 1 || input.topology.nodes !== input.topology.shards * input.topology.validators_per_shard) return "Topology requires nodes = shards × validators per shard.";
  if (!Number.isInteger(input.txCount) || input.txCount < 1 || input.txCount > 10000) return "tx_count must be an integer between 1 and 10000.";
  if (!Number.isFinite(input.crossShardRatio) || input.crossShardRatio < 0 || input.crossShardRatio > 1) return "cross_shard_ratio must be between 0 and 1.";
  if (!Number.isInteger(input.timeoutEvery) || input.timeoutEvery < 0 || input.timeoutEvery > 1000) return "timeout_every must be an integer between 0 and 1000.";
  if (!Number.isInteger(input.repeats) || input.repeats < 1 || input.repeats > 20) return "repeats must be an integer between 1 and 20.";
  return null;
}

function effectiveSpecFor(base: V5ExperimentSpec, method: V5FormalMethod): V5ExperimentSpec {
  return applyV5MethodSelections(base, method);
}

function mergeCompatibility(response: V5FormalPreviewResponse, compatibility: Record<string, V5CompatibilityResult>): V5FormalPreviewResponse {
  return {
    ...response,
    rows: response.rows.map((row) => {
      const result = compatibility[row.method_config_id];
      if (!result) return row;
      return {
        ...row,
        runnable: row.runnable && result.valid,
        blockers: [...new Set([...row.blockers, ...result.blockers])],
        warnings: [...new Set([...row.warnings, ...result.warnings])],
      };
    }),
  };
}

function parseSeeds(value: string): number[] {
  const raw = value.split(",").map((item) => item.trim()).filter(Boolean);
  if (!raw.length || raw.length > 10) return [];
  const values = raw.map(Number);
  if (values.some((item) => !Number.isInteger(item))) return [];
  return [...new Set(values)];
}

function toggle<T>(value: T, values: T[]): T[] { return values.includes(value) ? values.filter((item) => item !== value) : [...values, value]; }
function errorMessage(caught: unknown): string { return caught instanceof Error ? caught.message : String(caught); }
function isRecord(value: unknown): value is Record<string, unknown> { return typeof value === "object" && value !== null; }
function isTerminalGroup(status: string): boolean { return ["completed", "failed", "cancelled", "completed_with_failures"].includes(status); }

function PreviewTable({ preview }: { preview: V5FormalPreviewResponse }) {
  return <div className="table-wrap"><p data-testid="v5-formal-preview-summary"><strong>execution backend:</strong> {preview.execution_backend}; <strong>matrix rows:</strong> {preview.rows.length}</p><table><thead><tr><th>Suite</th><th>Method</th><th>Method config ID</th><th>Seed</th><th>Repeat</th><th>Topology</th><th>tx_count</th><th>Runtime</th><th>Compatibility</th></tr></thead><tbody>{preview.rows.map((row) => <tr key={row.child_run_id} data-method-config-id={row.method_config_id}><td>{row.suite_type}</td><td>{row.method.display_name}</td><td>{row.method_config_id}</td><td>{row.seed}</td><td>{row.repeat_index + 1}</td><td>{row.topology_point.nodes}/{row.topology_point.shards}/{row.topology_point.validators_per_shard}</td><td>{row.estimated_transactions}</td><td>{row.execution_backend}</td><td>{row.runnable ? "runnable" : row.blockers.join("; ") || "blocked"}{row.warnings.length ? ` ${row.warnings.join("; ")}` : ""}</td></tr>)}</tbody></table></div>;
}

function GroupStatus({ detail }: { detail: V5FormalRunGroupDetail }) {
  const failed = detail.children.filter((child) => ["failed", "blocked"].includes(child.status)).length;
  return <><p data-testid="v5-formal-group-summary"><strong>status:</strong> {detail.group.status}; <strong>execution backend:</strong> {detail.group.execution_backend}; <strong>runtime truth:</strong> {detail.group.runtime_truth}; <strong>children:</strong> {detail.group.completed_child_runs}/{detail.group.total_child_runs}; <strong>failed:</strong> {failed}</p><div className="table-wrap"><table data-testid="v5-formal-child-table"><thead><tr><th>Child</th><th>Suite</th><th>Method</th><th>Seed</th><th>Repeat</th><th>Topology</th><th>tx_count</th><th>Status</th><th>Terminal</th><th>Incomplete</th><th>Orphans</th><th>No fallback</th></tr></thead><tbody>{detail.children.map((child) => <ChildRow key={child.child_run_id} child={child} />)}</tbody></table></div></>;
}

function ChildRow({ child }: { child: V5FormalChildRun }) {
  const summary = child.result?.summary ?? {};
  const finality = isRecord(summary.finality_evidence) ? summary.finality_evidence : {};
  const metric = (name: string) => typeof finality[name] === "number" || typeof summary[name] === "number" || typeof summary[name] === "boolean" ? String(finality[name] ?? summary[name]) : "—";
  return <tr><td>{child.child_run_id}</td><td>{child.suite_type}</td><td>{child.method.display_name}</td><td>{child.seed}</td><td>{child.repeat_index + 1}</td><td>{child.topology_point.nodes}/{child.topology_point.shards}/{child.topology_point.validators_per_shard}</td><td>{child.estimated_transactions}</td><td>{child.status}{child.error ? `: ${child.error}` : ""}</td><td>{metric("terminal_unique_tx_count")}</td><td>{metric("incomplete_unique_tx_count")}</td><td>{metric("orphan_process_count")}</td><td>{metric("no_fallback")}</td></tr>;
}
