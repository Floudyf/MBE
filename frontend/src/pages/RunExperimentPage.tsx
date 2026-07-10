import { useEffect, useMemo, useState } from "react";

import {
  deriveV4RealismRequest,
  executeSelectedRunMatrix,
  fetchExperimentMethods,
  fetchExperimentTopologies,
  fetchExperimentWorkloads,
  previewExperimentRunMatrix,
  type ExperimentMatrixRow,
  type ExperimentMethod,
  type ExperimentRunMatrixPreview,
  type ExperimentSuiteRequest,
  type ExperimentTopology,
  type ExperimentWorkload,
  type RunSuiteExecutionResponse,
  type V4DerivedRequestPreview,
} from "../api";
import RunStageFlow from "../components/experiment/RunStageFlow";

const suiteTypes = [
  ["quick_validation", "快速验证"],
  ["main_experiment", "主实验"],
  ["comparison_experiment", "对比实验"],
  ["ablation_experiment", "消融实验"],
  ["workload_sensitivity", "负载敏感性"],
  ["topology_scaling", "拓扑扩展"],
  ["v4_realism_validation", "V4 真实性验证"],
] as const;

const formalPreviewOnlySuites = new Set([
  "main_experiment",
  "comparison_experiment",
  "ablation_experiment",
  "workload_sensitivity",
  "topology_scaling",
]);
const derivedV4StorageKey = "mbe.derivedV4RealismRequest";

type Props = { onOpenV4Details?: () => void };

export default function RunExperimentPage({ onOpenV4Details }: Props) {
  const [methods, setMethods] = useState<ExperimentMethod[]>([]);
  const [workloads, setWorkloads] = useState<ExperimentWorkload[]>([]);
  const [topologies, setTopologies] = useState<ExperimentTopology[]>([]);
  const [selectedSuiteTypes, setSelectedSuiteTypes] = useState<string[]>(["quick_validation"]);
  const [selectedMethodIds, setSelectedMethodIds] = useState<string[]>([]);
  const [selectedWorkloadIds, setSelectedWorkloadIds] = useState<string[]>(["small_test"]);
  const [topologyMode, setTopologyMode] = useState<"preset" | "custom">("preset");
  const [selectedTopologyId, setSelectedTopologyId] = useState("local_8_nodes_2_shards");
  const [seedText, setSeedText] = useState("1");
  const [nodes, setNodes] = useState(8);
  const [shards, setShards] = useState(2);
  const [validatorsPerShard, setValidatorsPerShard] = useState(4);
  const [txCount, setTxCount] = useState(20);
  const [repeatCount, setRepeatCount] = useState(1);
  const [matrix, setMatrix] = useState<ExperimentRunMatrixPreview | null>(null);
  const [selectedRowIds, setSelectedRowIds] = useState<string[]>([]);
  const [runMode, setRunMode] = useState<"dry_run" | "execute">("dry_run");
  const [maxExecuteRows, setMaxExecuteRows] = useState(3);
  const [executionResult, setExecutionResult] = useState<RunSuiteExecutionResponse | null>(null);
  const [derived, setDerived] = useState<V4DerivedRequestPreview | null>(null);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  useEffect(() => { void loadCatalog(); }, []);

  const methodsByRole = useMemo(() => ({
    main: sortMethods(methods.filter((item) => item.role === "main")),
    baseline: sortMethods(methods.filter((item) => item.role === "baseline")),
    ablation: sortMethods(methods.filter((item) => item.role === "ablation")),
    custom: sortMethods(methods.filter((item) => item.role === "custom")),
  }), [methods]);
  const selectedPreset = topologies.find((item) => item.topology_id === selectedTopologyId);
  const seeds = parseSeeds(seedText);
  const estimatedRows = selectedSuiteTypes.length * selectedMethodIds.length * selectedWorkloadIds.length * Math.max(seeds.length, 0) * repeatCount;

  async function loadCatalog() {
    try {
      const [methodItems, workloadItems, topologyItems] = await Promise.all([
        fetchExperimentMethods(true),
        fetchExperimentWorkloads(),
        fetchExperimentTopologies(),
      ]);
      setMethods(methodItems);
      setWorkloads(workloadItems);
      setTopologies(topologyItems);
      const defaultMethod = sortMethods(methodItems.filter((item) => item.role === "main" && item.runnable))[0];
      setSelectedMethodIds((current) => current.length ? current : defaultMethod ? [defaultMethod.method_id] : []);
      const topology = topologyItems.find((item) => item.topology_id === selectedTopologyId) || topologyItems[0];
      if (topology) applyPresetTopology(topology);
      setError("");
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    }
  }

  function applyPresetTopology(topology: ExperimentTopology) {
    setSelectedTopologyId(topology.topology_id);
    setNodes(topology.nodes);
    setShards(topology.shards);
    setValidatorsPerShard(topology.validators_per_shard);
  }

  function buildRequest(): ExperimentSuiteRequest | null {
    const parsedSeeds = parseSeeds(seedText);
    if (!parsedSeeds.length) return failRequest("Seeds 必须是 1 到 10 个整数，例如 1,2,3。");
    if (!selectedMethodIds.length) return failRequest("请至少选择一个方法模板。");
    if (!selectedWorkloadIds.length) return failRequest("请至少选择一个 workload。");
    if (!selectedSuiteTypes.length) return failRequest("请至少选择一个实验类型。");
    if (topologyMode === "custom" && (nodes < 1 || shards < 1 || validatorsPerShard < 1 || shards > nodes)) {
      return failRequest("Custom topology 要求所有数值为正数，且 shards 不能大于 nodes。");
    }
    if (txCount < 1 || repeatCount < 1 || repeatCount > 10) return failRequest("tx_count 必须大于 0，repeat_count 必须在 1 到 10 之间。");
    setError("");
    return {
      plan_name: "current_experiment_plan",
      selected_method_ids: selectedMethodIds,
      selected_suite_types: selectedSuiteTypes,
      workload_ids: selectedWorkloadIds,
      topology_ids: topologyMode === "preset" ? [selectedTopologyId] : [],
      seeds: parsedSeeds,
      include_v4_realism: selectedSuiteTypes.includes("v4_realism_validation"),
      conditions: {
        topology_mode: topologyMode,
        topology_id: topologyMode === "preset" ? selectedTopologyId : null,
        nodes,
        shards,
        validators_per_shard: validatorsPerShard,
        tx_count: txCount,
        repeat_count: repeatCount,
      },
    };
  }

  function failRequest(message: string): null {
    setError(message);
    return null;
  }

  async function previewMatrix() {
    const request = buildRequest();
    if (!request) return;
    try {
      setBusy(true);
      const response = await previewExperimentRunMatrix(request);
      setMatrix(response);
      setSelectedRowIds(response.rows.filter((row) => row.runnable).map((row) => row.row_id));
      setExecutionResult(null);
      setMessage(`矩阵预览已生成 ${response.rows.length} 行；正式 suite 本轮仍为 preview-only。`);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setBusy(false);
    }
  }

  async function deriveV4Request() {
    const request = buildRequest();
    if (!request) return;
    try {
      setBusy(true);
      const response = await deriveV4RealismRequest(request);
      setDerived(response);
      setMessage(response.runnable ? "已从当前条件派生 V4 真实性验证请求。" : "V4 请求已派生，但当前组合被 warnings 阻塞。");
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setBusy(false);
    }
  }

  function saveDerivedV4Request() {
    if (!derived) return;
    window.localStorage.setItem(derivedV4StorageKey, JSON.stringify(derived.v4_request));
    setMessage("V4 request 已保存到真实性验证详情页。");
    onOpenV4Details?.();
  }

  async function executeRows(nextRunMode: "dry_run" | "execute") {
    if (!matrix) return failRequest("请先预览实验矩阵。");
    const selectedRows = matrix.rows.filter((row) => selectedRowIds.includes(row.row_id));
    if (!selectedRows.length) return failRequest("请至少选择一个 runnable row。");
    try {
      setBusy(true);
      setRunMode(nextRunMode);
      const response = await executeSelectedRunMatrix({
        run_mode: nextRunMode,
        selected_rows: selectedRows.map(toSelectedRowRequest),
        include_v4_realism: selectedRows.some((row) => row.suite_type === "v4_realism_validation"),
        v4_request_override: derived?.v4_request ?? null,
        max_execute_rows: maxExecuteRows,
      });
      setExecutionResult(response);
      setMessage(`${nextRunMode === "execute" ? "Execute" : "Dry-run"} completed for selected rows.`);
      setError("");
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setBusy(false);
    }
  }

  return (
    <section className="page-grid">
      <article className="final-card wide page-hero">
        <p className="eyebrow">Experiment Plan to Run Matrix</p>
        <h2>运行实验</h2>
        <p>选择方法模板、负载、单一 preset/custom topology、seed 和重复次数，生成真实反映这些条件的矩阵。</p>
        {error && <p className="file-error">{error}</p>}
        {message && <p className="notice">{message}</p>}
      </article>

      <article className="final-card wide">
        <h3>执行边界</h3>
        <p className="muted">真实执行仍仅支持 quick_validation 和 v4_realism_validation。main/comparison/ablation/workload_sensitivity/topology_scaling 只预览，正式运行继续使用 Formal benchmark 兼容入口。</p>
      </article>

      <article className="final-card wide">
        <h3>实验类型</h3>
        <div className="selectable-card-grid">
          {suiteTypes.map(([id, label]) => (
            <button key={id} type="button" className={`selectable-card ${selectedSuiteTypes.includes(id) ? "selected" : ""}`} onClick={() => toggleValue(id, selectedSuiteTypes, setSelectedSuiteTypes)}>
              <strong>{label}</strong><small>{id}</small>
              <span className={`status-badge ${formalPreviewOnlySuites.has(id) ? "badge-preview" : "badge-runnable"}`}>{formalPreviewOnlySuites.has(id) ? "preview-only" : "supported execute"}</span>
            </button>
          ))}
        </div>
      </article>

      <article className="final-card wide">
        <h3>方法模板</h3>
        <p className="muted">数据来自 experiment-flow：已验证用户模板与系统默认模板使用同一后端方法解析链路。valid/unknown 可进入预览但 row 会被阻塞；blocked 模板不可选择。</p>
        <MethodGroup title="主方法" methods={methodsByRole.main} selected={selectedMethodIds} onToggle={(id) => toggleValue(id, selectedMethodIds, setSelectedMethodIds)} open />
        <MethodGroup title="对比方法" methods={methodsByRole.baseline} selected={selectedMethodIds} onToggle={(id) => toggleValue(id, selectedMethodIds, setSelectedMethodIds)} />
        <MethodGroup title="消融方法" methods={methodsByRole.ablation} selected={selectedMethodIds} onToggle={(id) => toggleValue(id, selectedMethodIds, setSelectedMethodIds)} />
        <MethodGroup title="自定义方法" methods={methodsByRole.custom} selected={selectedMethodIds} onToggle={(id) => toggleValue(id, selectedMethodIds, setSelectedMethodIds)} />
      </article>

      <article className="final-card wide">
        <h3>实验条件</h3>
        <div className="topology-field-grid run-condition-layout">
          <div className="field-card">
            <span>Workloads</span>
            {workloads.map((item) => <WorkloadChoice key={item.workload_id} item={item} selected={selectedWorkloadIds.includes(item.workload_id)} onToggle={() => toggleValue(item.workload_id, selectedWorkloadIds, setSelectedWorkloadIds)} />)}
          </div>

          <div className="field-card">
            <span>Topology mode</span>
            <div className="segmented-control">
              <button type="button" className={topologyMode === "preset" ? "active" : ""} onClick={() => {
                setTopologyMode("preset");
                if (selectedPreset) applyPresetTopology(selectedPreset);
              }}>preset</button>
              <button type="button" className={topologyMode === "custom" ? "active" : ""} onClick={() => setTopologyMode("custom")}>custom</button>
            </div>
            {topologyMode === "preset" ? (
              <>
                <label><span>Topology preset</span><select value={selectedTopologyId} onChange={(event) => {
                  const topology = topologies.find((item) => item.topology_id === event.target.value);
                  if (topology) applyPresetTopology(topology);
                }}>{topologies.map((item) => <option key={item.topology_id} value={item.topology_id}>{item.label}</option>)}</select></label>
                <TopologySummary nodes={nodes} shards={shards} validators={validatorsPerShard} />
                <small>Preset 数值只读；custom 输入不会覆盖 preset catalog。</small>
              </>
            ) : (
              <div className="experiment-condition-grid compact-grid">
                <label><span>nodes</span><input type="number" min={1} value={nodes} onChange={(event) => setNodes(Number(event.target.value))} /></label>
                <label><span>shards</span><input type="number" min={1} value={shards} onChange={(event) => setShards(Number(event.target.value))} /></label>
                <label><span>validators_per_shard</span><input type="number" min={1} value={validatorsPerShard} onChange={(event) => setValidatorsPerShard(Number(event.target.value))} /></label>
              </div>
            )}
          </div>

          <div className="field-card">
            <span>Scale and repetition</span>
            <div className="experiment-condition-grid compact-grid">
              <label><span>tx_count</span><input type="number" min={1} value={txCount} onChange={(event) => setTxCount(Number(event.target.value))} /></label>
              <label><span>repeat_count</span><input type="number" min={1} max={10} value={repeatCount} onChange={(event) => setRepeatCount(Number(event.target.value))} /></label>
              <label><span>seeds</span><input value={seedText} onChange={(event) => setSeedText(event.target.value)} placeholder="1,2,3" /></label>
            </div>
            <p className="matrix-size-note">Matrix: {selectedMethodIds.length} methods x {selectedWorkloadIds.length} workloads x 1 topology x {seeds.length} seeds x {repeatCount} repeats{selectedSuiteTypes.length > 1 ? ` x ${selectedSuiteTypes.length} suites` : ""} = {estimatedRows} rows</p>
          </div>
        </div>
        <div className="button-row">
          <button type="button" onClick={previewMatrix} disabled={busy}>预览实验矩阵</button>
          <button type="button" className="v3-secondary-button" onClick={deriveV4Request} disabled={busy}>派生 V4 真实性验证请求</button>
        </div>
      </article>

      {matrix && <MatrixPreview matrix={matrix} selectedRowIds={selectedRowIds} setSelectedRowIds={setSelectedRowIds} runMode={runMode} setRunMode={setRunMode} maxExecuteRows={maxExecuteRows} setMaxExecuteRows={setMaxExecuteRows} busy={busy} onExecute={executeRows} />}

      {executionResult && (
        <article className="final-card wide">
          <h3>执行结果</h3>
          <RunStageFlow childRuns={executionResult.child_runs} rows={matrix?.rows || []} mode={runMode} />
          <dl className="v3-result-grid compact">
            <div><dt>run_group_id</dt><dd>{executionResult.run_group_id}</dd></div>
            <div><dt>selected</dt><dd>{executionResult.selected_row_count}</dd></div>
            <div><dt>started</dt><dd>{executionResult.started_row_count}</dd></div>
            <div><dt>blocked</dt><dd>{executionResult.blocked_row_count}</dd></div>
          </dl>
          <div className="table-scroll"><table><thead><tr><th>suite</th><th>method</th><th>runner</th><th>status</th><th>run_id</th><th>warnings</th></tr></thead><tbody>{executionResult.child_runs.map((child) => <tr key={child.row_id}><td>{child.suite_type}</td><td>{child.method_id}</td><td>{child.runner}</td><td>{child.status}</td><td>{child.run_id || "-"}</td><td>{child.warnings.join("; ") || child.blocked_reason || "-"}</td></tr>)}</tbody></table></div>
        </article>
      )}

      {derived && (
        <article className="final-card wide">
          <h3>V4 realism 派生请求</h3>
          <p className="muted">Runnable: {String(derived.runnable)}</p>
          {derived.warnings.length > 0 && <ul className="boundary-list">{derived.warnings.map((warning) => <li key={warning}>{warning}</li>)}</ul>}
          <pre>{JSON.stringify(derived.v4_request, null, 2)}</pre>
          <button type="button" onClick={saveDerivedV4Request}>应用到 V4 真实性验证详情</button>
        </article>
      )}
    </section>
  );
}

function MethodGroup({ title, methods, selected, onToggle, open = false }: { title: string; methods: ExperimentMethod[]; selected: string[]; onToggle: (id: string) => void; open?: boolean }) {
  return (
    <details className="v3-foldout" open={open}>
      <summary className="v3-foldout-summary">{title} ({methods.length})</summary>
      <div className="v3-checkbox-grid">
        {methods.map((method) => {
          const disabled = !method.previewable || method.validation_status === "blocked";
          return (
            <label key={method.method_id} className={`checkbox-card field-card method-choice ${disabled ? "disabled" : ""}`}>
              <span>{method.label}<small>{method.role} / {method.config_source === "saved_config" ? "saved template" : "builtin"}</small></span>
              <span className={`status-badge badge-${method.validation_status === "runnable" ? "runnable" : method.validation_status === "blocked" ? "blocked" : "preview"}`}>{method.validation_status}</span>
              <input type="checkbox" checked={selected.includes(method.method_id)} disabled={disabled} onChange={() => onToggle(method.method_id)} />
            </label>
          );
        })}
      </div>
    </details>
  );
}

function WorkloadChoice({ item, selected, onToggle }: { item: ExperimentWorkload; selected: boolean; onToggle: () => void }) {
  const status = item.planned ? "planned / dataset not attached" : item.csv_required ? "requires CSV" : "runnable";
  return (
    <label className={`checkbox-card compact workload-choice ${item.planned ? "planned" : ""}`}>
      <span>{item.label}<small>{item.workload_id} / {status}</small></span>
      <input type="checkbox" checked={selected} onChange={onToggle} />
    </label>
  );
}

function TopologySummary({ nodes, shards, validators }: { nodes: number; shards: number; validators: number }) {
  return <div className="topology-readonly-summary"><span>{nodes}<small>nodes</small></span><span>{shards}<small>shards</small></span><span>{validators}<small>validators/shard</small></span></div>;
}

function MatrixPreview({ matrix, selectedRowIds, setSelectedRowIds, runMode, setRunMode, maxExecuteRows, setMaxExecuteRows, busy, onExecute }: {
  matrix: ExperimentRunMatrixPreview;
  selectedRowIds: string[];
  setSelectedRowIds: (ids: string[]) => void;
  runMode: "dry_run" | "execute";
  setRunMode: (mode: "dry_run" | "execute") => void;
  maxExecuteRows: number;
  setMaxExecuteRows: (value: number) => void;
  busy: boolean;
  onExecute: (mode: "dry_run" | "execute") => void;
}) {
  return (
    <article className="final-card wide">
      <h3>矩阵预览</h3>
      <p className="muted">runnable={matrix.runnable_row_count} / blocked={matrix.blocked_row_count} / rows={matrix.rows.length}</p>
      <RunStageFlow rows={matrix.rows} mode="preview" />
      <div className="topology-field-grid compact-grid">
        <label className="field-card"><span>run_mode</span><select value={runMode} onChange={(event) => setRunMode(event.target.value as "dry_run" | "execute")}><option value="dry_run">dry_run</option><option value="execute">execute</option></select></label>
        <label className="field-card"><span>max_execute_rows</span><input type="number" min={1} max={3} value={maxExecuteRows} onChange={(event) => setMaxExecuteRows(Number(event.target.value))} /></label>
      </div>
      <div className="button-row"><button type="button" onClick={() => onExecute("dry_run")} disabled={busy}>Dry-run selected rows</button><button type="button" className="v3-secondary-button" onClick={() => onExecute("execute")} disabled={busy}>Execute selected supported rows</button></div>
      <div className="table-scroll"><table className="run-matrix-table"><thead><tr><th>select</th><th>method</th><th>source</th><th>validation</th><th>workload</th><th>topology</th><th>nodes</th><th>shards</th><th>validators</th><th>tx_count</th><th>seed</th><th>repeat</th><th>status</th><th>warnings</th></tr></thead><tbody>{matrix.rows.map((row) => <tr key={row.row_id} title={row.row_id}><td><input type="checkbox" checked={selectedRowIds.includes(row.row_id)} disabled={!row.runnable} onChange={() => toggleValue(row.row_id, selectedRowIds, setSelectedRowIds)} /></td><td>{row.resolved_method_name}<small>{row.method_role}</small></td><td>{row.config_source}</td><td>{row.validation_status}</td><td>{row.workload_id}</td><td>{row.topology_mode}<small>{row.topology_id}</small></td><td>{row.nodes}</td><td>{row.shards}</td><td>{row.validators_per_shard}</td><td>{row.tx_count}</td><td>{row.seed}</td><td>{row.repeat_index}</td><td><span className={`status-badge ${row.runnable ? "badge-runnable" : "badge-blocked"}`}>{row.runnable ? "runnable" : "blocked"}</span></td><td>{row.warnings.join("; ") || "-"}</td></tr>)}</tbody></table></div>
    </article>
  );
}

function sortMethods(methods: ExperimentMethod[]) {
  const statusRank: Record<string, number> = { runnable: 0, valid: 1, unknown: 2, blocked: 3 };
  return [...methods].sort((a, b) => {
    const sourceDifference = (a.config_source === "saved_config" ? 0 : 1) - (b.config_source === "saved_config" ? 0 : 1);
    if (sourceDifference) return sourceDifference;
    return (statusRank[a.validation_status] ?? 4) - (statusRank[b.validation_status] ?? 4) || a.label.localeCompare(b.label);
  });
}

function toggleValue(value: string, values: string[], setValues: (next: string[]) => void) {
  setValues(values.includes(value) ? values.filter((item) => item !== value) : [...values, value]);
}

function toSelectedRowRequest(row: ExperimentMatrixRow) {
  return {
    row_id: row.row_id,
    suite_type: row.suite_type,
    method_id: row.method_id,
    method_role: row.method_role,
    config_source: row.config_source,
    method_config_id: row.method_config_id,
    resolved_method_name: row.resolved_method_name,
    validation_status: row.validation_status,
    workload_id: row.workload_id,
    topology_id: row.topology_id,
    topology_mode: row.topology_mode,
    nodes: row.nodes,
    shards: row.shards,
    validators_per_shard: row.validators_per_shard,
    tx_count: row.tx_count,
    seed: row.seed,
    repeat_index: row.repeat_index,
    runtime_target: row.runtime_target,
    runnable: row.runnable,
    warnings: row.warnings,
  };
}

function parseSeeds(value: string): number[] {
  const parsed = value.split(",").map((item) => Number(item.trim())).filter((item) => Number.isInteger(item));
  return parsed.length <= 10 ? Array.from(new Set(parsed)) : [];
}
