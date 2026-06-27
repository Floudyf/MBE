import { useEffect, useMemo, useState } from "react";

import {
  API_BASE_URL,
  fetchV1AblationPresets,
  fetchV1CustomRunFiles,
  fetchV1CustomRunSummary,
  fetchV1Experiments,
  fetchV1FabricTraceStatus,
  fetchV1Status,
  fetchV1SweepFiles,
  fetchV1SweepReport,
  fetchV1SweepSummary,
  fetchV1Workloads,
  fetchV2CalibrationConfigs,
  fetchV2ChainBackends,
  fetchV2FabricSmokeStatus,
  fetchV2Protocols,
  fetchV2RunArtifacts,
  fetchV2Runs,
  fetchV2Sweeps,
  fetchV2TraceSources,
  runV1CustomExperiment,
  runV1Sweep,
  runV2Calibration,
  runV2DualChainReplay,
  runV2ProtocolReplay,
  runV2Sweep,
  v1CustomRunFileDownloadURL,
  v1SweepFileDownloadURL,
  v2ArtifactDownloadURL,
  type V1AblationPreset,
  type V1CustomRunRequest,
  type V1FabricTraceStatus,
  type V1StageStatus,
  type V1SweepFile,
  type V1SweepRow,
  type V1WorkloadOption,
  type V2Artifact,
  type V2CalibrationInfo,
  type V2CalibrationRunResponse,
  type V2ChainBackend,
  type V2FabricSmokeStatus,
  type V2ProtocolInfo,
  type V2RunSummary,
  type V2SweepInfo,
  type V2SweepRunResponse,
  type V2TraceSource,
} from "./api";
import V2Dashboard from "./components/V2Dashboard";
import V3ComposerPage from "./pages/V3ComposerPage";

type PageId =
  | "overview"
  | "single"
  | "ablation"
  | "dual"
  | "protocol"
  | "sweep"
  | "calibration"
  | "v3composer"
  | "runs"
  | "boundaries"
  | "developer";

const defaultCustomForm: V1CustomRunRequest = {
  workload: "asset_hotspot_v1",
  source_type: "synthetic",
  tx_count: 100,
  seed: 42,
  hot_tx_ratio: 0.6,
  conflict_injection_ratio: 0.3,
  commutative_update_ratio: 0.35,
  access_set_size: 4,
  multi_hotspot_count: 3,
  arrival_rate: 100,
  burst_rate: 500,
  routing_policy: "co_access",
  dual_track_enabled: true,
  hot_update_aggregation_enabled: true,
  preset: "full_v1",
  trace_path: "tests/golden/trace_small.jsonl.gz",
};

const navGroups: { title: string; items: { id: PageId; label: string }[] }[] = [
  { title: "V3", items: [{ id: "v3composer", label: "V3 单链 Composer" }] },
  { title: "平台", items: [{ id: "overview", label: "平台总览" }] },
  {
    title: "实验中心",
    items: [
      { id: "single", label: "单链机制实验" },
      { id: "ablation", label: "单链消融对比" },
      { id: "dual", label: "双链回放实验" },
      { id: "protocol", label: "跨链协议基线" },
      { id: "sweep", label: "批量对比与报告" },
      { id: "calibration", label: "真实链轨迹校准" },
    ],
  },
  { title: "结果", items: [{ id: "runs", label: "运行记录与产物" }] },
  { title: "说明", items: [{ id: "boundaries", label: "系统边界" }, { id: "developer", label: "开发者模式" }] },
];

function App() {
  const [activePage, setActivePage] = useState<PageId>("overview");
  const [v1Stages, setV1Stages] = useState<V1StageStatus[]>([]);
  const [workloads, setWorkloads] = useState<V1WorkloadOption[]>([]);
  const [presets, setPresets] = useState<V1AblationPreset[]>([]);
  const [fabricTrace, setFabricTrace] = useState<V1FabricTraceStatus | null>(null);
  const [customForm, setCustomForm] = useState<V1CustomRunRequest>(defaultCustomForm);
  const [customSummary, setCustomSummary] = useState<V1SweepRow | null>(null);
  const [customFiles, setCustomFiles] = useState<V1SweepFile[]>([]);
  const [customMessage, setCustomMessage] = useState("尚未运行单链实验。");
  const [sweepRows, setSweepRows] = useState<V1SweepRow[]>([]);
  const [sweepFiles, setSweepFiles] = useState<V1SweepFile[]>([]);
  const [v1Report, setV1Report] = useState("");
  const [v2Runs, setV2Runs] = useState<V2RunSummary[]>([]);
  const [selectedRunId, setSelectedRunId] = useState("");
  const [selectedArtifacts, setSelectedArtifacts] = useState<V2Artifact[]>([]);
  const [traceSources, setTraceSources] = useState<V2TraceSource[]>([]);
  const [backends, setBackends] = useState<V2ChainBackend[]>([]);
  const [protocols, setProtocols] = useState<V2ProtocolInfo[]>([]);
  const [sweeps, setSweeps] = useState<V2SweepInfo[]>([]);
  const [calibrations, setCalibrations] = useState<V2CalibrationInfo[]>([]);
  const [fabricSmokeStatus, setFabricSmokeStatus] = useState<V2FabricSmokeStatus | null>(null);
  const [v2Result, setV2Result] = useState<Record<string, unknown> | null>(null);
  const [v2Artifacts, setV2Artifacts] = useState<V2Artifact[]>([]);
  const [sweepId, setSweepId] = useState("v2_baseline_sweep");
  const [calibrationId, setCalibrationId] = useState("v2_synthetic_calibration_sample");
  const [busy, setBusy] = useState("");
  const [error, setError] = useState("");

  useEffect(() => { void loadAll(); }, []);

  const runnableV2 = useMemo(() => ({
    dual: backends.some((item) => item.backend_type === "local_virtual" && item.status === "runnable"),
    protocol: protocols.some((item) => item.name === "lock_mint_serial" && item.status === "runnable"),
    sweep: sweeps.some((item) => item.id === "v2_baseline_sweep"),
    calibration: calibrations.some((item) => item.id === "v2_synthetic_calibration_sample"),
  }), [backends, protocols, sweeps, calibrations]);

  async function loadAll() {
    try {
      setBusy("正在加载平台状态...");
      const [status, workloadItems, presetItems, fabricStatus, latestCustom, customFileList, sweepSummary, sweepReport, sweepFileList, runs, sources, backendItems, protocolItems, sweepItems, calibrationItems, fabricSmoke] = await Promise.all([
        fetchV1Status(),
        fetchV1Workloads(),
        fetchV1AblationPresets(),
        fetchV1FabricTraceStatus(),
        fetchV1CustomRunSummary(),
        fetchV1CustomRunFiles(),
        fetchV1SweepSummary(),
        fetchV1SweepReport(),
        fetchV1SweepFiles(),
        fetchV2Runs(50),
        fetchV2TraceSources(),
        fetchV2ChainBackends(),
        fetchV2Protocols(),
        fetchV2Sweeps(),
        fetchV2CalibrationConfigs(),
        fetchV2FabricSmokeStatus(),
      ]);
      setV1Stages(status.stages);
      setWorkloads(workloadItems);
      setPresets(presetItems);
      setFabricTrace(fabricStatus);
      setCustomSummary(latestCustom.summary && Object.keys(latestCustom.summary).length ? latestCustom.summary : null);
      setCustomFiles(customFileList.files);
      setSweepRows(sweepSummary.rows);
      setV1Report(sweepReport.content || "");
      setSweepFiles(sweepFileList.files);
      setV2Runs(runs);
      setTraceSources(sources);
      setBackends(backendItems);
      setProtocols(protocolItems);
      setSweeps(sweepItems);
      setCalibrations(calibrationItems);
      setFabricSmokeStatus(fabricSmoke);
      setError("");
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setBusy("");
    }
  }

  async function runSingleChain() {
    const validation = validateCustomForm(customForm);
    if (validation) {
      setCustomMessage(validation);
      return;
    }
    try {
      setBusy("正在运行单链机制实验...");
      const result = await runV1CustomExperiment(customForm);
      setCustomSummary(result.summary);
      setCustomFiles(result.files);
      setCustomMessage(`运行完成：${result.run_id || result.output_dir}`);
      await refreshRuns(result.run_id);
      setError("");
    } catch (caught) {
      setCustomMessage(`运行失败：${errorMessage(caught)}`);
    } finally {
      setBusy("");
    }
  }

  async function runAblationSweep() {
    try {
      setBusy("正在运行 V1 sweep/report...");
      await runV1Sweep();
      const [summary, report, files] = await Promise.all([fetchV1SweepSummary(), fetchV1SweepReport(), fetchV1SweepFiles()]);
      setSweepRows(summary.rows);
      setV1Report(report.content || "");
      setSweepFiles(files.files);
      setError("");
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setBusy("");
    }
  }

  async function runDualReplay() {
    await runV2Action("正在运行双链回放...", () => runV2DualChainReplay());
  }

  async function runProtocolReplay() {
    await runV2Action("正在运行跨链协议基线...", () => runV2ProtocolReplay());
  }

  async function runSweepExperiment() {
    await runV2Action("正在运行批量对比与报告...", () => runV2Sweep(sweepId));
  }

  async function runCalibrationExperiment(id = calibrationId) {
    await runV2Action("正在运行真实链轨迹校准...", () => runV2Calibration(id));
  }

  async function runV2Action(message: string, action: () => Promise<Record<string, unknown>>) {
    try {
      setBusy(message);
      const result = await action();
      setV2Result(result);
      const artifacts = Array.isArray(result.artifacts) ? result.artifacts as V2Artifact[] : [];
      setV2Artifacts(artifacts);
      const runId = typeof result.run_id === "string" ? result.run_id : "";
      if (runId) await refreshRuns(runId);
      setError("");
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setBusy("");
    }
  }

  async function refreshFabricSmoke() {
    try {
      setFabricSmokeStatus(await fetchV2FabricSmokeStatus());
      setError("");
    } catch (caught) {
      setError(errorMessage(caught));
    }
  }

  async function refreshRuns(runId = selectedRunId) {
    const runs = await fetchV2Runs(50);
    setV2Runs(runs);
    const next = runId || runs[0]?.run_id || "";
    setSelectedRunId(next);
    if (next) {
      const response = await fetchV2RunArtifacts(next);
      setSelectedArtifacts(response.artifacts);
    }
  }

  async function selectRun(runId: string) {
    setSelectedRunId(runId);
    if (!runId) {
      setSelectedArtifacts([]);
      return;
    }
    try {
      setSelectedArtifacts((await fetchV2RunArtifacts(runId)).artifacts);
      setError("");
    } catch (caught) {
      setError(errorMessage(caught));
    }
  }

  function applyPreset(id: string) {
    const preset = presets.find((item) => item.id === id);
    setCustomForm((form) => ({
      ...form,
      preset: id,
      routing_policy: preset?.routing_policy ?? form.routing_policy,
      dual_track_enabled: preset?.dual_track_enabled ?? form.dual_track_enabled,
      hot_update_aggregation_enabled: preset?.hot_update_aggregation_enabled ?? form.hot_update_aggregation_enabled,
    }));
  }

  return <div className="final-shell">
    <aside className="final-sidebar">
      <div className="brand-block"><span>MBE</span><strong>V2 实验平台</strong><small>V3-ready local replay</small></div>
      {navGroups.map((group) => <nav key={group.title} aria-label={group.title}>
        <p>{group.title}</p>
        {group.items.map((item) => <button key={item.id} type="button" className={activePage === item.id ? "nav-active" : ""} onClick={() => setActivePage(item.id)}>{item.label}</button>)}
      </nav>)}
    </aside>
    <main className="final-main">
      <header className="final-topbar">
        <div><p className="eyebrow">当前后端：{API_BASE_URL}</p><h1>{pageTitle(activePage)}</h1></div>
        <button type="button" onClick={loadAll}>刷新平台状态</button>
      </header>
      {busy && <p className="notice">{busy}</p>}
      {error && <p className="file-error">{error}</p>}
      {activePage === "overview" && <OverviewPage runnableV2={runnableV2} setActivePage={setActivePage} fabricSmokeStatus={fabricSmokeStatus} />}
      {activePage === "single" && <SingleChainPage form={customForm} setForm={setCustomForm} presets={presets} workloads={workloads} fabricTrace={fabricTrace} customSummary={customSummary} customFiles={customFiles} customMessage={customMessage} applyPreset={applyPreset} runSingleChain={runSingleChain} />}
      {activePage === "ablation" && <AblationPage rows={sweepRows} files={sweepFiles} report={v1Report} runAblationSweep={runAblationSweep} />}
      {activePage === "dual" && <DualPage backends={backends} result={v2Result} artifacts={v2Artifacts} runDualReplay={runDualReplay} />}
      {activePage === "protocol" && <ProtocolPage protocols={protocols} result={v2Result} artifacts={v2Artifacts} runProtocolReplay={runProtocolReplay} />}
      {activePage === "sweep" && <SweepPage sweeps={sweeps} sweepId={sweepId} setSweepId={setSweepId} result={v2Result as V2SweepRunResponse | null} artifacts={v2Artifacts} runSweepExperiment={runSweepExperiment} />}
      {activePage === "calibration" && <CalibrationPage calibrations={calibrations} calibrationId={calibrationId} setCalibrationId={setCalibrationId} fabricSmokeStatus={fabricSmokeStatus} refreshFabricSmoke={refreshFabricSmoke} result={v2Result as V2CalibrationRunResponse | null} artifacts={v2Artifacts} runCalibrationExperiment={runCalibrationExperiment} />}
      {activePage === "v3composer" && <V3ComposerPage onRunCompleted={(runId) => { void refreshRuns(runId); }} />}
      {activePage === "runs" && <RunHistoryPage runs={v2Runs} selectedRunId={selectedRunId} artifacts={selectedArtifacts} selectRun={selectRun} refreshRuns={() => refreshRuns()} />}
      {activePage === "boundaries" && <BoundariesPage />}
      {activePage === "developer" && <DeveloperPage traceSources={traceSources} backends={backends} protocols={protocols} sweeps={sweeps} calibrations={calibrations} v1Stages={v1Stages} />}
    </main>
  </div>;
}

function OverviewPage({ runnableV2, setActivePage, fabricSmokeStatus }: { runnableV2: Record<string, boolean>; setActivePage: (page: PageId) => void; fabricSmokeStatus: V2FabricSmokeStatus | null }) {
  return <section className="page-grid">
    <article className="overview-hero"><p className="eyebrow">平台总览</p><h2>V3-ready 本地模块化跨链实验平台</h2><p>V2 已把 registry、trace source、job/artifact、dual-chain replay、protocol baseline、sweep/report、chain-backed calibration 串成可演示的本地实验平台。</p><BoundaryNote /></article>
    <CapabilityGrid items={[
      ["V1 单链 MetaTrack 实验", "可运行", "single"],
      ["V2.5 双链回放实验", runnableV2.dual ? "可运行" : "不可用", "dual"],
      ["V2.6 跨链协议基线", runnableV2.protocol ? "可运行" : "不可用", "protocol"],
      ["V2.8 批量对比与报告", runnableV2.sweep ? "可运行" : "不可用", "sweep"],
      ["V2.9 真实链轨迹校准", fabricSmokeStatus?.status === "ready" ? "Fabric trace ready" : "synthetic 可运行 / Fabric 视 trace 状态", "calibration"],
      ["V3 live backend", "规划中", "boundaries"],
    ]} setActivePage={setActivePage} />
  </section>;
}

function CapabilityGrid({ items, setActivePage }: { items: [string, string, PageId][]; setActivePage: (page: PageId) => void }) {
  return <div className="final-card-grid">{items.map(([title, status, page]) => <article key={title} className="final-card"><StatusBadge status={status.includes("规划") ? "planned" : status.includes("不可") ? "blocked" : "runnable"} /><h3>{title}</h3><p>{status}</p><button type="button" onClick={() => setActivePage(page)}>进入</button></article>)}</div>;
}

function SingleChainPage(props: {
  form: V1CustomRunRequest;
  setForm: (form: V1CustomRunRequest) => void;
  presets: V1AblationPreset[];
  workloads: V1WorkloadOption[];
  fabricTrace: V1FabricTraceStatus | null;
  customSummary: V1SweepRow | null;
  customFiles: V1SweepFile[];
  customMessage: string;
  applyPreset: (id: string) => void;
  runSingleChain: () => void;
}) {
  const { form, setForm } = props;
  return <section className="page-grid">
    <InfoPanel title="单链机制实验（MetaTrack）" note="只展示 V1 single-chain 机制：co-access routing、dual-track execution、hot update aggregation。Fabric 轨迹只回放已有 trace，不启动 Fabric。" />
    <article className="final-card wide">
      <h3>实验配置</h3>
      <div className="form-grid">
        <label><span>数据来源</span><select value={form.source_type} onChange={(e) => setForm({ ...form, source_type: e.target.value })}><option value="synthetic">合成负载回放（synthetic_replay）</option><option value="existing_trace">已有轨迹回放（existing_trace_replay）</option><option value="chain_backed">Fabric 轨迹回放（网页只 replay）</option></select></label>
        <label><span>workload</span><select value={form.workload} onChange={(e) => setForm({ ...form, workload: e.target.value })}>{props.workloads.filter((item) => item.source_type === "synthetic").map((item) => <option key={item.id} value={item.id}>{item.label}（{item.id}）</option>)}</select></label>
        <label><span>机制组合</span><select value={form.preset} onChange={(e) => props.applyPreset(e.target.value)}>{props.presets.map((preset) => <option key={preset.id} value={preset.id}>{preset.id}</option>)}</select></label>
        {form.source_type === "existing_trace" && <label><span>trace_path</span><input value={form.trace_path ?? ""} onChange={(e) => setForm({ ...form, trace_path: e.target.value })} /></label>}
        {form.source_type === "chain_backed" && <label><span>trace_path</span><input value=".cache/fabric_smoke/latest/trace.jsonl.gz" disabled /></label>}
      </div>
      <div className="toggle-row">
        <label><input type="checkbox" checked={form.routing_policy === "co_access"} onChange={(e) => setForm({ ...form, routing_policy: e.target.checked ? "co_access" : "hash", preset: "custom" })} /> 共访问路由（co_access）</label>
        <label><input type="checkbox" checked={form.dual_track_enabled} onChange={(e) => setForm({ ...form, dual_track_enabled: e.target.checked, preset: "custom" })} /> 双轨执行（dual_track）</label>
        <label><input type="checkbox" checked={form.hot_update_aggregation_enabled} onChange={(e) => setForm({ ...form, hot_update_aggregation_enabled: e.target.checked, preset: "custom" })} /> 热更新聚合（hot_update_aggregation）</label>
      </div>
      <button type="button" onClick={props.runSingleChain}>运行单链实验</button>
      <p className="muted">{props.customMessage}</p>
      {form.source_type === "synthetic" && <p className="acceptance-note">当前为小规模 synthetic/smoke 输入，用于验证流程；完整性能对比请使用 sweep/report 或更大 workload。</p>}
      {(form.source_type === "existing_trace" || form.source_type === "chain_backed") && <p className="acceptance-note">轨迹模式会复用已有 trace；网页不启动链，也不会自动生成真实链数据。</p>}
      <p className="muted">Fabric trace 状态：{props.fabricTrace?.message ?? "未检查"}。网页不会启动 Docker/Fabric/network.sh。</p>
    </article>
    <MetricsCard title="最新单链结果" row={props.customSummary} keys={["tx_count", "success_count", "failed_count", "throughput_tps", "avg_latency_ms", "p95_latency_ms", "p99_latency_ms"]} />
    <article className="final-card wide"><p className="acceptance-note">当前样例 trace 的虚拟延迟分布可能较窄，avg/p95/p99 可能相同。若 `latency.csv` 中每笔 latency 本身相同，这不是前端字段绑定错误；请使用更大 workload 或 sweep/report 观察分布差异。</p></article>
    <LegacyArtifactList files={props.customFiles} urlFor={v1CustomRunFileDownloadURL} preferred={["summary.csv", "latency.csv", "runtime.log", "report.md"]} />
  </section>;
}

function AblationPage({ rows, files, report, runAblationSweep }: { rows: V1SweepRow[]; files: V1SweepFile[]; report: string; runAblationSweep: () => void }) {
  return <section className="page-grid">
    <InfoPanel title="单链消融对比" note="对比 baseline_hash_only、co_access_only、co_access_dual_track、full_v1。当前默认使用小规模 smoke trace，结果用于验证配置链路和产物生成。" />
    <article className="final-card wide"><p className="acceptance-note">当前消融结果为 smoke-level validation，仅验证配置链路、机制开关传递和产物生成，不代表最终机制效果。若四组 TPS/P99 相同，请同时查看 routing/dual-track/aggregation 机制指标。</p><button type="button" onClick={runAblationSweep}>运行 V1 sweep / report</button><div className="table-scroll"><table><thead><tr><th>name</th><th>TPS</th><th>P99 latency</th><th>aggregation saved</th><th>fast track</th><th>routing policy</th></tr></thead><tbody>{rows.map((row) => <tr key={String(row.name)}><td>{String(row.name)}</td><td>{formatMetricValue(row.throughput_tps, "tps")}</td><td>{formatMetricValue(row.p99_latency_ms, "latency")}</td><td>{formatMetricValue(row.aggregation_saved_commit_count, "count")}</td><td>{formatMetricValue(row.fast_track_tx_count, "count")}</td><td>{String(row.routing_policy ?? "-")}</td></tr>)}</tbody></table></div></article>
    <LegacyArtifactList files={files} urlFor={v1SweepFileDownloadURL} preferred={["sweep_summary.csv", "sweep_summary.json", "report.md"]} />
    <article className="final-card wide"><h3>报告摘要</h3><pre>{report || "尚未生成 report.md"}</pre></article>
  </section>;
}

function DualPage({ backends, result, artifacts, runDualReplay }: { backends: V2ChainBackend[]; result: Record<string, unknown> | null; artifacts: V2Artifact[]; runDualReplay: () => void }) {
  return <section className="page-grid">
    <InfoPanel title="双链回放实验（V2.5）" note="本地虚拟时间回放，不是真实链执行。planned live backend 不显示运行按钮。" />
    <div className="final-card-grid">{backends.map((backend) => <article key={backend.backend_type} className="final-card"><BackendBadge backendType={backend.backend_type} /><StatusBadge status={backend.status} /><p>virtual={String(backend.supports_virtual_time)} replay={String(backend.supports_replay)} real={String(backend.supports_real_time)}</p></article>)}</div>
    <ActionResultCard title="运行双链回放" button="运行双链回放" onRun={runDualReplay} result={result} keys={["run_id", "data_truth_label", "backend_type", "source_backend_type", "target_backend_type", "cross_tx_count", "stage_record_count", "finality_wait_time_ms", "source_wait_time_ms", "target_wait_time_ms", "chain_speed_imbalance"]} artifacts={artifacts} />
  </section>;
}

function ProtocolPage({ protocols, result, artifacts, runProtocolReplay }: { protocols: V2ProtocolInfo[]; result: Record<string, unknown> | null; artifacts: V2Artifact[]; runProtocolReplay: () => void }) {
  return <section className="page-grid">
    <InfoPanel title="跨链协议基线（V2.6）" note="本地协议基线回放，不是生产级跨链桥。这不是 MetaFlow；committee_bridge_basic 不包含真实签名或证明。" />
    <div className="final-card-grid">{protocols.map((protocol) => <article key={protocol.name} className="final-card"><StatusBadge status={protocol.status} /><h3>{protocol.name === "disabled" ? "无跨链协议对照项（disabled）" : protocol.name}</h3><p>{protocol.reason}</p></article>)}</div>
    <ActionResultCard title="运行协议基线" button="运行协议基线" onRun={runProtocolReplay} result={result} keys={["run_id", "protocol_truth", "data_truth_label", "backend_type"]} artifacts={artifacts} />
  </section>;
}

function SweepPage({ sweeps, sweepId, setSweepId, result, artifacts, runSweepExperiment }: { sweeps: V2SweepInfo[]; sweepId: string; setSweepId: (id: string) => void; result: V2SweepRunResponse | null; artifacts: V2Artifact[]; runSweepExperiment: () => void }) {
  return <section className="page-grid">
    <InfoPanel title="批量对比与报告（V2.8）" note="本地 sweep/report，不是真实链实验。报告可下载，普通页面不展开 raw case artifacts。" />
    <article className="final-card wide"><label><span>sweep 类型</span><select value={sweepId} onChange={(e) => setSweepId(e.target.value)}>{sweeps.map((sweep) => <option key={sweep.id} value={sweep.id}>{sweep.name}（{sweep.id}）</option>)}</select></label><button type="button" onClick={runSweepExperiment}>运行批量实验</button></article>
    <MetricsCard title="批量实验结果" row={result?.summary as V1SweepRow | undefined ?? null} keys={["sweep_id", "case_count", "completed_count", "failed_count", "data_truth_label", "backend_type"]} />
    <ArtifactList artifacts={artifacts} />
  </section>;
}

function CalibrationPage({ calibrations, calibrationId, setCalibrationId, fabricSmokeStatus, refreshFabricSmoke, result, artifacts, runCalibrationExperiment }: { calibrations: V2CalibrationInfo[]; calibrationId: string; setCalibrationId: (id: string) => void; fabricSmokeStatus: V2FabricSmokeStatus | null; refreshFabricSmoke: () => void; result: V2CalibrationRunResponse | null; artifacts: V2Artifact[]; runCalibrationExperiment: (id?: string) => void }) {
  return <section className="page-grid">
    <InfoPanel title="真实链轨迹校准（V2.9）" note="这是 chain-backed trace calibration，不是 V3 live backend。网页不控制 Fabric。" />
    <div className="final-card-grid">
      <article className="final-card"><h3>合成样例校准</h3><p>用于验证 calibration 流程，不是真实链数据。</p></article>
      <article className="final-card"><h3>Fabric 轨迹校准</h3><p>使用已有 Fabric smoke trace。网页不启动 Docker/Fabric/network.sh，不是 FabricLiveBackend。</p></article>
    </div>
    <article className="final-card wide"><label><span>calibration config</span><select value={calibrationId} onChange={(e) => setCalibrationId(e.target.value)}>{calibrations.map((item) => <option key={item.id} value={item.id}>{item.name}（{item.id}）</option>)}</select></label><div className="button-row"><button type="button" onClick={refreshFabricSmoke}>检查 Fabric 轨迹状态</button><button type="button" onClick={() => runCalibrationExperiment("v2_synthetic_calibration_sample")}>运行合成样例校准</button><button type="button" onClick={() => runCalibrationExperiment("v2_fabric_smoke_calibration")}>运行 Fabric 轨迹校准</button></div></article>
    <article className="final-card wide"><h3>Fabric smoke status</h3><StatusBadge status={fabricSmokeStatus?.status ?? "unknown"} /><p>{fabricSmokeStatus?.status === "missing" ? "Fabric smoke trace missing." : "Fabric smoke trace ready 状态取决于本机已有文件。"}</p><pre>{fabricSmokeStatus?.cli_command ?? "python scripts/v1_fabric_smoke.py --strict --channel mbechannel --out .cache/fabric_smoke/latest"}</pre><p className="muted">网页不会启动 Fabric / Docker / network.sh。</p></article>
    <MetricsCard title="校准结果" row={result?.summary as V1SweepRow | undefined ?? null} keys={["calibration_id", "matched_record_count", "unmatched_observed_count", "avg_abs_latency_error_ms", "data_truth_label", "calibration_truth"]} />
    <ArtifactList artifacts={artifacts} />
  </section>;
}

function RunHistoryPage({ runs, selectedRunId, artifacts, selectRun, refreshRuns }: { runs: V2RunSummary[]; selectedRunId: string; artifacts: V2Artifact[]; selectRun: (id: string) => void; refreshRuns: () => void }) {
  return <section className="page-grid">
    <InfoPanel title="运行记录与产物" note="运行记录与首页分离；下载链接统一走 artifact API，不直接暴露 .cache 绝对路径。" />
    <article className="final-card wide"><div className="button-row"><button type="button" onClick={refreshRuns}>刷新运行记录</button></div><label><span>选择 run</span><select value={selectedRunId} onChange={(e) => selectRun(e.target.value)}><option value="">请选择</option>{runs.map((run) => <option key={run.run_id} value={run.run_id}>{run.run_id} / {run.stage} / {run.status}</option>)}</select></label></article>
    <div className="final-card-grid">{runs.map((run) => <article key={run.run_id} className="final-card"><StatusBadge status={run.status} /><h3>{run.run_id}</h3><p>{run.stage} / {run.source}</p><TruthBadge label={run.data_truth_label} /><p>artifacts={run.artifact_count} report={String(run.report_available)}</p></article>)}</div>
    <ArtifactList artifacts={artifacts} />
  </section>;
}

function BoundariesPage() {
  return <section className="page-grid"><InfoPanel title="系统边界" note="V2 是 V3-ready 本地模块化实验平台。" /><article className="final-card wide"><ul className="boundary-list"><li>V2 不从网页启动 Docker / Fabric / network.sh。</li><li>V2 不连接公网链实时节点。</li><li>local_virtual backend 不是真实链。</li><li>protocol baseline 不是生产级跨链桥。</li><li>Fabric smoke trace replay 不是网页实时控制 Fabric。</li><li>MetaFlow 当前未实现。</li><li>FabricLiveBackend / EVMLiveBackend 属于 V3。</li></ul></article></section>;
}

function DeveloperPage(props: { traceSources: V2TraceSource[]; backends: V2ChainBackend[]; protocols: V2ProtocolInfo[]; sweeps: V2SweepInfo[]; calibrations: V2CalibrationInfo[]; v1Stages: V1StageStatus[] }) {
  return <section className="page-grid">
    <InfoPanel title="开发者模式" note="仅用于调试。raw JSON、capabilities、limitations、blocked_by 等信息默认不干扰普通实验流程。" />
    <article className="final-card wide"><details><summary>V2 调试控制台（旧 Dashboard）</summary><V2Dashboard /></details></article>
    <article className="final-card wide"><details><summary>Raw API / JSON</summary><pre>{JSON.stringify(props, null, 2)}</pre></details></article>
  </section>;
}

function InfoPanel({ title, note }: { title: string; note: string }) {
  return <article className="final-card wide"><h2>{title}</h2><p>{note}</p></article>;
}

function BoundaryNote() {
  return <p className="boundary-note">当前 V2 是本地 replay / calibration 实验平台，不是真实链实时部署平台。local_virtual backend 不是真实链；protocol baseline 不是生产级跨链桥；FabricLiveBackend / EVMLiveBackend 属于 V3 规划。</p>;
}

function ActionResultCard({ title, button, onRun, result, keys, artifacts }: { title: string; button: string; onRun: () => void; result: Record<string, unknown> | null; keys: string[]; artifacts: V2Artifact[] }) {
  const summary = (result?.summary && typeof result.summary === "object" ? result.summary : {}) as Record<string, unknown>;
  const merged = result ? { ...summary, ...result } as V1SweepRow : null;
  return <article className="final-card wide"><h3>{title}</h3><button type="button" onClick={onRun}>{button}</button>{result ? <><MetricsCard title="运行结果" row={merged} keys={keys} /><ArtifactList artifacts={artifacts} /></> : <p className="muted">尚未运行；不会显示旧 artifact。</p>}</article>;
}

function MetricsCard({ title, row, keys }: { title: string; row: V1SweepRow | null; keys: string[] }) {
  return <article className="final-card wide"><h3>{title}</h3>{row ? <dl className="metrics-grid compact">{keys.map((key) => <div key={key}><dt>{key}</dt><dd>{formatMetricByKey(key, row[key])}</dd></div>)}</dl> : <p className="muted">暂无结果。</p>}</article>;
}

function LegacyArtifactList({ files, urlFor, preferred }: { files: V1SweepFile[]; urlFor: (name: string) => string; preferred: string[] }) {
  return <article className="final-card wide"><h3>产物下载</h3><ul className="file-list">{preferred.map((name) => {
    const file = files.find((item) => item.name === name);
    return <li key={name}><span>{name}<small>{artifactDescription(name)}</small></span><span className={file ? "file-present" : "file-missing"}>{file ? `${file.size_bytes} bytes` : "缺失"}</span>{file ? <a href={urlFor(name)}>下载</a> : <span>-</span>}</li>;
  })}</ul></article>;
}

function ArtifactList({ artifacts }: { artifacts: V2Artifact[] }) {
  return <article className="final-card wide"><h3>产物下载</h3>{artifacts.length ? <ul className="file-list">{artifacts.map((artifact) => <li key={artifact.name}><span>{artifact.name}<small>{artifactDescription(artifact.name)}</small></span><span className="file-present">{artifact.size_bytes} bytes</span><a href={v2ArtifactDownloadURL(artifact.download_url)}>下载</a></li>)}</ul> : <p className="muted">暂无可下载产物。</p>}</article>;
}

function StatusBadge({ status }: { status: string }) {
  const cls = status === "runnable" || status === "completed" || status === "可运行" ? "badge-success" : status === "planned" || status === "blocked" || status === "missing" ? "badge-planned" : status === "failed" || status === "invalid" ? "badge-danger" : "badge-cli";
  const text: Record<string, string> = { runnable: "可运行", planned: "规划中", experimental: "实验性", invalid: "不可用", completed: "完成", failed: "失败", running: "运行中", created: "已创建", blocked: "阻塞", missing: "缺失" };
  return <span className={`badge ${cls}`}>{text[status] ?? status}</span>;
}

function TruthBadge({ label }: { label: string }) {
  const text: Record<string, string> = {
    synthetic_replay: "合成回放 / 非真实链",
    existing_trace_replay: "轨迹回放 / 不启动链",
    fabric_chain_backed_trace_replay: "Fabric 轨迹 / 网页回放",
    public_chain_imported_trace_semantic_unknown: "公链轨迹 / 语义未知",
    planned_cross_chain_replay: "规划回放 / 不可运行",
  };
  return <span className="badge badge-cli">{text[label] ?? label}</span>;
}

function BackendBadge({ backendType }: { backendType: string }) {
  const normalized = backendType === "fabric_live" ? "fabric_live_planned" : backendType === "evm_live" ? "evm_live_planned" : backendType;
  const text: Record<string, string> = {
    local_virtual: "本地虚拟 / 非真实链",
    trace_replay: "轨迹回放",
    fabric_live_planned: "Fabric Live / V3 规划",
    evm_live_planned: "EVM Live / V3 规划",
  };
  return <span className={`badge ${normalized.includes("planned") ? "badge-planned" : "badge-success"}`}>{text[normalized] ?? backendType}</span>;
}

function pageTitle(page: PageId): string {
  return navGroups.flatMap((group) => group.items).find((item) => item.id === page)?.label ?? "平台总览";
}

function validateCustomForm(form: V1CustomRunRequest): string {
  if (!Number.isFinite(form.tx_count) || form.tx_count < 1 || form.tx_count > 100000) return "tx_count 必须在 1 到 100000 之间。";
  if (form.source_type === "existing_trace" && !form.trace_path) return "已有轨迹回放需要 trace_path。";
  return "";
}

function formatMetricByKey(key: string, value: V1SweepRow[string]): string {
  if (key.includes("throughput") || key.endsWith("_tps")) return formatMetricValue(value, "tps");
  if (key.includes("latency") || key.includes("wait_time") || key.includes("_time_ms")) return formatMetricValue(value, "latency");
  if (key.includes("ratio")) return formatMetricValue(value, "ratio");
  if (key.includes("count") || key.endsWith("_depth")) return formatMetricValue(value, "count");
  return value === undefined || value === null || value === "" ? "-" : String(value);
}

function formatMetricValue(value: V1SweepRow[string], kind: "tps" | "latency" | "ratio" | "count"): string {
  if (value === undefined || value === null || value === "") return "-";
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return String(value);
  if (kind === "tps") return `${numeric.toFixed(2)} TPS`;
  if (kind === "latency") return `${numeric.toFixed(2)} ms`;
  if (kind === "ratio") return numeric <= 1 ? `${(numeric * 100).toFixed(2)}%` : numeric.toFixed(2);
  return String(Math.round(numeric));
}

function artifactDescription(name: string): string {
  const descriptions: Record<string, string> = {
    "summary.csv": "单次实验汇总",
    "latency.csv": "交易延迟明细",
    "runtime.log": "运行日志",
    "report.md": "实验报告",
    "sweep_summary.csv": "批量实验汇总表",
    "sweep_summary.json": "批量实验汇总数据",
    "sweep_report.md": "批量实验报告",
    "dual_chain_summary.json": "双链回放汇总",
    "stage_metrics.csv": "阶段级指标",
    "protocol_summary.json": "协议基线汇总",
    "protocol_results.csv": "协议交易结果",
    "protocol_events.csv": "协议事件明细",
    "calibration_summary.json": "校准汇总",
    "replay_vs_observed.csv": "回放与观测对比表",
    "calibration_report.md": "校准报告",
    "used_config.yaml": "本次运行配置",
  };
  return descriptions[name] ?? "运行产物";
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export default App;
