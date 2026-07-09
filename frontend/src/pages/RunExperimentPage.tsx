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

const derivedV4StorageKey = "mbe.derivedV4RealismRequest";

type Props = {
  onOpenV4Details?: () => void;
};

export default function RunExperimentPage({ onOpenV4Details }: Props) {
  const [methods, setMethods] = useState<ExperimentMethod[]>([]);
  const [workloads, setWorkloads] = useState<ExperimentWorkload[]>([]);
  const [topologies, setTopologies] = useState<ExperimentTopology[]>([]);
  const [selectedSuiteTypes, setSelectedSuiteTypes] = useState<string[]>(["quick_validation"]);
  const [selectedMethodIds, setSelectedMethodIds] = useState<string[]>(["metatrack_full"]);
  const [selectedWorkloadIds, setSelectedWorkloadIds] = useState<string[]>(["small_test"]);
  const [selectedTopologyIds, setSelectedTopologyIds] = useState<string[]>(["local_8_nodes_2_shards"]);
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
    main: methods.filter((item) => item.role === "main"),
    baseline: methods.filter((item) => item.role === "baseline"),
    ablation: methods.filter((item) => item.role === "ablation"),
  }), [methods]);

  async function loadCatalog() {
    try {
      const [methodItems, workloadItems, topologyItems] = await Promise.all([
        fetchExperimentMethods(),
        fetchExperimentWorkloads(),
        fetchExperimentTopologies(),
      ]);
      setMethods(methodItems);
      setWorkloads(workloadItems);
      setTopologies(topologyItems);
      setError("");
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    }
  }

  function buildRequest(): ExperimentSuiteRequest | null {
    const seeds = parseSeeds(seedText);
    if (!seeds.length) {
      setError("Seed 输入需要是 1 到 10 个数字，例如 1,2,3。");
      return null;
    }
    setError("");
    return {
      plan_name: "current_experiment_plan",
      selected_method_ids: selectedMethodIds,
      selected_suite_types: selectedSuiteTypes,
      workload_ids: selectedWorkloadIds,
      topology_ids: selectedTopologyIds,
      seeds,
      include_v4_realism: selectedSuiteTypes.includes("v4_realism_validation"),
    };
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
      setMessage("实验矩阵已预览；本页不会启动批量运行。");
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
      setDerived({
        ...response,
        v4_request: {
          ...response.v4_request,
          nodes,
          shards,
          tx_count: txCount,
        },
      });
      setMessage(response.runnable ? "已派生 V4 真实性验证请求。" : "已派生请求，但当前组合不可运行，请查看 warnings。");
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setBusy(false);
    }
  }

  function saveDerivedV4Request() {
    if (!derived) return;
    window.localStorage.setItem(derivedV4StorageKey, JSON.stringify(derived.v4_request));
    setMessage("已保存到 V4 真实性验证详情。");
    onOpenV4Details?.();
  }

  async function executeRows(nextRunMode: "dry_run" | "execute") {
    if (!matrix) {
      setError("请先预览实验矩阵。");
      return;
    }
    const selectedRows = matrix.rows.filter((row) => selectedRowIds.includes(row.row_id));
    if (!selectedRows.length) {
      setError("请至少选择一个 runnable row。");
      return;
    }
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
        <p className="eyebrow">Experiment Plan to RunSuite to RunMatrix</p>
        <h2>运行实验</h2>
        <p>选择已保存方法模板、负载、拓扑和 seed，生成并执行实验矩阵。</p>
        <p className="muted">当前实验计划来自 11 模块 Composer；本页不重新配置模块，只选择实验条件、运行类型和运行矩阵。</p>
        {error && <p className="file-error">{error}</p>}
        {message && <p className="notice">{message}</p>}
      </article>

      <article className="final-card wide">
        <h3>执行桥能力</h3>
        <p className="muted">当前支持真实执行：quick_validation、v4_realism_validation。main/comparison/ablation/workload_sensitivity/topology_scaling 仅 preview；正式运行仍使用实验设计页中的 Formal benchmark 入口。</p>
      </article>

      <article className="final-card wide">
        <h3>运行类型</h3>
        <div className="selectable-card-grid">
          {suiteTypes.map(([id, label]) => (
            <button key={id} type="button" className={`selectable-card ${selectedSuiteTypes.includes(id) ? "selected" : ""}`} onClick={() => toggleValue(id, selectedSuiteTypes, setSelectedSuiteTypes)}>
              <strong>{label}</strong>
              <small>{id}</small>
              <span className={`status-badge ${id === "quick_validation" || id === "v4_realism_validation" ? "badge-runnable" : "badge-preview"}`}>{id === "quick_validation" || id === "v4_realism_validation" ? "supported execute" : "preview-only"}</span>
            </button>
          ))}
        </div>
      </article>

      <article className="final-card wide">
        <h3>方法选择</h3>
        <MethodGroup title="主方法" methods={methodsByRole.main} selected={selectedMethodIds} onToggle={(id) => toggleValue(id, selectedMethodIds, setSelectedMethodIds)} />
        <MethodGroup title="对比方法" methods={methodsByRole.baseline} selected={selectedMethodIds} onToggle={(id) => toggleValue(id, selectedMethodIds, setSelectedMethodIds)} />
        <MethodGroup title="消融方法" methods={methodsByRole.ablation} selected={selectedMethodIds} onToggle={(id) => toggleValue(id, selectedMethodIds, setSelectedMethodIds)} />
      </article>

      <article className="final-card wide">
        <h3>实验条件</h3>
        <p className="muted">Workload、nodes、shards、validators_per_shard、tx_count、seed 和 repeat_count 在运行页选择，不属于方法模板主配置。</p>
        <div className="topology-field-grid">
          <div className="field-card">
            <span>Workloads</span>
            {workloads.map((item) => (
              <label key={item.workload_id} className="checkbox-card compact">
                <span>{item.label}<small>{item.workload_id}{item.planned ? " / 规划中 / 数据集未接入" : ""}</small></span>
                <input type="checkbox" checked={selectedWorkloadIds.includes(item.workload_id)} onChange={() => toggleValue(item.workload_id, selectedWorkloadIds, setSelectedWorkloadIds)} />
              </label>
            ))}
          </div>
          <div className="field-card">
            <span>Topologies</span>
            {topologies.map((item) => (
              <label key={item.topology_id} className="checkbox-card compact">
                <span>{item.label}<small>{item.topology_id} / nodes={item.nodes} shards={item.shards} validators={item.validators_per_shard}</small></span>
                <input type="checkbox" checked={selectedTopologyIds.includes(item.topology_id)} onChange={() => {
                  toggleValue(item.topology_id, selectedTopologyIds, setSelectedTopologyIds);
                  setNodes(item.nodes);
                  setShards(item.shards);
                  setValidatorsPerShard(item.validators_per_shard);
                }} />
              </label>
            ))}
          </div>
          <div className="field-card">
            <span>Topology numbers</span>
            <div className="experiment-condition-grid compact-grid">
              <label><span>nodes</span><input type="number" min={1} value={nodes} onChange={(event) => setNodes(Number(event.target.value))} /></label>
              <label><span>shards</span><input type="number" min={1} value={shards} onChange={(event) => setShards(Number(event.target.value))} /></label>
              <label><span>validators_per_shard</span><input type="number" min={1} value={validatorsPerShard} onChange={(event) => setValidatorsPerShard(Number(event.target.value))} /></label>
            </div>
          </div>
          <div className="field-card">
            <span>Scale</span>
            <div className="experiment-condition-grid compact-grid">
              <label><span>tx_count</span><input type="number" min={1} value={txCount} onChange={(event) => setTxCount(Number(event.target.value))} /></label>
              <label><span>repeat_count</span><input type="number" min={1} max={10} value={repeatCount} onChange={(event) => setRepeatCount(Number(event.target.value))} /></label>
            </div>
          </div>
          <label className="field-card">
            <span>Seeds</span>
            <input value={seedText} onChange={(event) => setSeedText(event.target.value)} placeholder="1,2,3" />
            <small>最多 10 个 seed。</small>
          </label>
        </div>
        <div className="button-row">
          <button type="button" onClick={previewMatrix} disabled={busy}>预览实验矩阵</button>
          <button type="button" className="v3-secondary-button" onClick={deriveV4Request} disabled={busy}>派生 V4 真实性验证请求</button>
        </div>
      </article>

      {matrix && (
        <article className="final-card wide">
          <h3>矩阵预览</h3>
          <p className="muted">runnable={matrix.runnable_row_count} / blocked={matrix.blocked_row_count} / repeat_count={repeatCount}</p>
          <RunStageFlow rows={matrix.rows} mode="preview" />
          <div className="topology-field-grid">
            <label className="field-card">
              <span>执行模式</span>
              <select value={runMode} onChange={(event) => setRunMode(event.target.value as "dry_run" | "execute")}>
                <option value="dry_run">dry_run</option>
                <option value="execute">execute</option>
              </select>
              <small>dry_run 不调用 runner；execute 只启动受支持的少量 selected rows。</small>
            </label>
            <label className="field-card">
              <span>max_execute_rows</span>
              <input type="number" min={1} max={3} value={maxExecuteRows} onChange={(event) => setMaxExecuteRows(Number(event.target.value))} />
              <small>本轮只做轻量执行桥，默认最多执行 3 行。</small>
            </label>
          </div>
          <div className="button-row">
            <button type="button" onClick={() => executeRows("dry_run")} disabled={busy}>Dry-run selected rows</button>
            <button type="button" className="v3-secondary-button" onClick={() => executeRows("execute")} disabled={busy}>Execute selected supported rows</button>
          </div>
          <div className="table-scroll">
            <table>
              <thead>
                <tr><th>select</th><th>suite_type</th><th>method_id</th><th>role</th><th>workload_id</th><th>topology_id</th><th>seed</th><th>runtime_target</th><th>runnable</th><th>warnings</th></tr>
              </thead>
              <tbody>
                {matrix.rows.map((row) => (
                  <tr key={row.row_id}>
                    <td><input type="checkbox" checked={selectedRowIds.includes(row.row_id)} disabled={!row.runnable} onChange={() => toggleValue(row.row_id, selectedRowIds, setSelectedRowIds)} /></td>
                    <td>{row.suite_type}</td>
                    <td>{row.method_id}</td>
                    <td>{row.method_role}</td>
                    <td>{row.workload_id}</td>
                    <td>{row.topology_id}</td>
                    <td>{row.seed}</td>
                    <td>{row.runtime_target}</td>
                    <td><span className={`status-badge ${row.runnable ? "badge-runnable" : "badge-blocked"}`}>{row.runnable ? "runnable" : "blocked"}</span></td>
                    <td>{row.warnings.length ? row.warnings.map((warning) => <span key={warning} className="status-badge badge-preview">{warning}</span>) : "-"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </article>
      )}

      {executionResult && (
        <article className="final-card wide">
          <h3>执行结果</h3>
          <RunStageFlow childRuns={executionResult.child_runs} rows={matrix?.rows || []} mode={runMode} />
          <dl className="v3-result-grid compact">
            <div><dt>run_group_id</dt><dd>{executionResult.run_group_id}</dd></div>
            <div><dt>selected_row_count</dt><dd>{executionResult.selected_row_count}</dd></div>
            <div><dt>started_row_count</dt><dd>{executionResult.started_row_count}</dd></div>
            <div><dt>blocked_row_count</dt><dd>{executionResult.blocked_row_count}</dd></div>
          </dl>
          {executionResult.warnings.length > 0 && <ul className="boundary-list">{executionResult.warnings.map((warning) => <li key={warning}>{warning}</li>)}</ul>}
          <div className="table-scroll">
            <table>
              <thead>
                <tr><th>row_id</th><th>suite_type</th><th>method_id</th><th>runner</th><th>status</th><th>run_id</th><th>warnings</th><th>blocked_reason</th></tr>
              </thead>
              <tbody>
                {executionResult.child_runs.map((child) => (
                  <tr key={child.row_id}>
                    <td>{child.row_id}</td>
                    <td>{child.suite_type}</td>
                    <td>{child.method_id}</td>
                    <td>{child.runner}</td>
                    <td><span className={`status-badge badge-${child.status}`}>{child.status}</span></td>
                    <td>{child.run_id || "-"}</td>
                    <td>{child.warnings.join("; ") || "-"}</td>
                    <td>{child.blocked_reason || (child.suite_type === "v4_realism_validation" && child.run_id ? "可进入 V4 真实性验证详情查看 summary/artifacts。" : "-")}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
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

function MethodGroup({ title, methods, selected, onToggle }: { title: string; methods: ExperimentMethod[]; selected: string[]; onToggle: (id: string) => void }) {
  return (
    <details className="v3-foldout" open={title === "主方法"}>
      <summary className="v3-foldout-summary">{title}</summary>
      <div className="v3-checkbox-grid">
        {methods.map((method) => (
          <label key={method.method_id} className="checkbox-card field-card">
            <span>{method.label}<small>{method.method_id}</small></span>
            <input type="checkbox" checked={selected.includes(method.method_id)} onChange={() => onToggle(method.method_id)} />
          </label>
        ))}
      </div>
    </details>
  );
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
    workload_id: row.workload_id,
    topology_id: row.topology_id,
    seed: row.seed,
    runtime_target: row.runtime_target,
    runnable: row.runnable,
    warnings: row.warnings,
  };
}

function parseSeeds(value: string): number[] {
  const seeds = value.split(",").map((item) => Number(item.trim())).filter((item) => Number.isInteger(item));
  return seeds.slice(0, 10);
}
