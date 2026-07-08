import { useEffect, useMemo, useState } from "react";

import {
  deriveV4RealismRequest,
  fetchExperimentMethods,
  fetchExperimentTopologies,
  fetchExperimentWorkloads,
  previewExperimentRunMatrix,
  type ExperimentMethod,
  type ExperimentRunMatrixPreview,
  type ExperimentSuiteRequest,
  type ExperimentTopology,
  type ExperimentWorkload,
  type V4DerivedRequestPreview,
} from "../api";

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
  const [matrix, setMatrix] = useState<ExperimentRunMatrixPreview | null>(null);
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
      setMatrix(await previewExperimentRunMatrix(request));
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
      setDerived(response);
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

  return (
    <section className="page-grid">
      <article className="final-card wide">
        <p className="eyebrow">Experiment Plan to RunSuite to RunMatrix</p>
        <h2>运行实验</h2>
        <p>当前实验计划来自 11 模块 Composer；本页不重新配置模块，只选择运行类型和运行矩阵。</p>
        <p className="muted">正式性能实验仍使用实验设计页中的 Formal benchmark 入口运行；本页只做 preview / derive。</p>
        {error && <p className="file-error">{error}</p>}
        {message && <p className="notice">{message}</p>}
      </article>

      <article className="final-card wide">
        <h3>运行类型</h3>
        <div className="v3-checkbox-grid">
          {suiteTypes.map(([id, label]) => (
            <label key={id} className="checkbox-card field-card">
              <span>{label}<small>{id}</small></span>
              <input type="checkbox" checked={selectedSuiteTypes.includes(id)} onChange={() => toggleValue(id, selectedSuiteTypes, setSelectedSuiteTypes)} />
            </label>
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
        <h3>负载与拓扑</h3>
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
                <span>{item.label}<small>{item.topology_id}</small></span>
                <input type="checkbox" checked={selectedTopologyIds.includes(item.topology_id)} onChange={() => toggleValue(item.topology_id, selectedTopologyIds, setSelectedTopologyIds)} />
              </label>
            ))}
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
          <p className="muted">runnable={matrix.runnable_row_count} / blocked={matrix.blocked_row_count}</p>
          <div className="table-scroll">
            <table>
              <thead>
                <tr><th>suite_type</th><th>method_id</th><th>role</th><th>workload_id</th><th>topology_id</th><th>seed</th><th>runtime_target</th><th>runnable</th><th>warnings</th></tr>
              </thead>
              <tbody>
                {matrix.rows.map((row) => (
                  <tr key={row.row_id}>
                    <td>{row.suite_type}</td>
                    <td>{row.method_id}</td>
                    <td>{row.method_role}</td>
                    <td>{row.workload_id}</td>
                    <td>{row.topology_id}</td>
                    <td>{row.seed}</td>
                    <td>{row.runtime_target}</td>
                    <td>{String(row.runnable)}</td>
                    <td>{row.warnings.join("; ") || "-"}</td>
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

function parseSeeds(value: string): number[] {
  const seeds = value.split(",").map((item) => Number(item.trim())).filter((item) => Number.isInteger(item));
  return seeds.slice(0, 10);
}
