import { useEffect, useState } from "react";

import {
  API_BASE_URL,
  experimentFileDownloadURL,
  fetchExperimentFiles,
  fetchRuntimeLog,
  fetchSummary,
  fetchV1Experiments,
  fetchV1Status,
  fetchV1SweepFiles,
  fetchV1SweepReport,
  fetchV1SweepSummary,
  runDefaultExperiment,
  runV1Sweep,
  v1SweepFileDownloadURL,
  type Summary,
  type V1Experiment,
  type V1StageStatus,
  type V1SweepFile,
  type V1SweepRow,
} from "./api";

const plugins = [
  ["chain_backend", "mockchain"], ["workload", "asset_hotspot"], ["trace", "jsonl_gzip"],
  ["consensus_protocol", "simple_ordering"], ["consensus_sharding", "single_group"],
  ["state_sharding", "hash_state_sharding"], ["execution_sharding", "hash_execution_sharding"],
  ["routing", "hash_routing"], ["cross_shard_protocol", "local_only"],
  ["cross_chain_protocol", "disabled"], ["execution", "serial_execution"], ["commit", "normal_commit"],
  ["clock", "virtual_clock"], ["network_model", "fixed_latency_model"], ["metrics", "basic_metrics"],
  ["composer", "default_composer"],
] as const;

const chainComponents = plugins.filter(([type]) => [
  "consensus_protocol", "consensus_sharding", "state_sharding", "execution_sharding", "routing",
  "cross_shard_protocol", "execution", "commit", "clock", "network_model", "metrics", "composer",
].includes(type));

const metricKeys = ["tx_count", "success_count", "failed_count", "throughput_tps", "avg_latency_ms", "p95_latency_ms", "p99_latency_ms", "cross_shard_ratio", "remote_fetch_count", "wall_clock_runtime_ms"] as const;
const resultFiles = ["config.yaml", "trace_meta.json", "summary.csv", "latency.csv", "runtime.log"] as const;

const scopes = [
  ["单链实验", "single-chain", "当前可用", "用于 V0 默认实验和 V1 单链分片论文实验。"],
  ["双链实验", "dual-chain", "V2/V3 规划", "用于两条异构链之间的跨链操作，V2 正式实现。"],
  ["多链实验", "multi-chain", "V2/V3 规划", "用于 AssetChain、SceneChain、RewardChain 等多链元宇宙场景，V2 正式实现。"],
  ["跨链协议实验", "cross-chain protocol", "V2/V3 规划", "用于 committee bridge、Pending Pool、MetaFlow 等跨链协议，V2 正式实现。"],
] as const;

const suites = [
  ["执行策略对比", "v1_execution_comparison", "已接入 V1.8 sweep", ["baseline_hash_only：hash routing + serial/normal commit", "co_access_dual_track：co-access + dual-track", "full_v1：co-access + dual-track + hot aggregation"]],
  ["MetaTrack 主实验", "v1_metatrack_main", "V1 机制已完成", ["V1.5 co-access routing：已完成", "V1.6 dual-track execution：已完成", "V1.7 hot update aggregation：已完成"]],
  ["消融实验", "v1_ablation", "已接入 V1.8 sweep", ["baseline_hash_only", "co_access_only", "co_access_dual_track", "full_v1"]],
  ["Fabric 链上 trace 校验", "v1_fabric_chain_backed", "已完成，CLI/WSL smoke runner", ["网页不会自动启动 Docker/Fabric", "真实 smoke 请在 WSL2 + Docker Desktop + fabric-samples 中运行"]],
] as const;

const baselines = [
  ["baseline_hash_only", "hash", "disabled", "disabled"],
  ["co_access_only", "co_access", "disabled", "disabled"],
  ["co_access_dual_track", "co_access", "enabled", "disabled"],
  ["full_v1", "co_access", "enabled", "enabled"],
] as const;

const baseMetricKeys = ["tx_count", "success_count", "failed_count", "throughput_tps", "avg_latency_ms", "p95_latency_ms", "p99_latency_ms", "virtual_time_ms"] as const;
const routingMetricKeys = ["routing_policy", "routing_cross_shard_tx_count", "routing_cross_shard_tx_ratio", "routing_remote_key_count", "co_access_group_count", "routing_time_ms"] as const;
const dualTrackMetricKeys = ["dual_track_enabled", "fast_track_tx_count", "conservative_track_tx_count", "fast_track_tx_ratio", "conservative_track_tx_ratio", "scheduler_idle_count"] as const;
const aggregationMetricKeys = ["hot_update_aggregation_enabled", "aggregation_policy", "aggregation_candidate_tx_count", "aggregated_tx_count", "aggregated_commit_count", "conservative_commit_count", "aggregation_saved_commit_count", "aggregation_group_count", "aggregation_hot_key_count"] as const;

function App() {
  const [runStatus, setRunStatus] = useState("就绪");
  const [runResponse, setRunResponse] = useState("尚未发送运行请求。");
  const [runtimeLog, setRuntimeLog] = useState("尚未加载运行日志。");
  const [summary, setSummary] = useState<Summary | null>(null);
  const [availableFiles, setAvailableFiles] = useState<string[]>([]);
  const [fileError, setFileError] = useState("");
  const [busy, setBusy] = useState(false);
  const [v1Experiments, setV1Experiments] = useState<V1Experiment[]>([]);
  const [v1Stages, setV1Stages] = useState<V1StageStatus[]>([]);
  const [v1Error, setV1Error] = useState("");
  const [sweepBusy, setSweepBusy] = useState(false);
  const [sweepStatus, setSweepStatus] = useState("尚未运行 V1.8 sweep。");
  const [sweepRows, setSweepRows] = useState<V1SweepRow[]>([]);
  const [sweepFiles, setSweepFiles] = useState<V1SweepFile[]>([]);
  const [report, setReport] = useState("");

  useEffect(() => { void loadV1Acceptance(); }, []);

  async function loadV1Acceptance() {
    try {
      const [experiments, status, sweepSummary, sweepReport, files] = await Promise.all([
        fetchV1Experiments(),
        fetchV1Status(),
        fetchV1SweepSummary(),
        fetchV1SweepReport(),
        fetchV1SweepFiles(),
      ]);
      setV1Experiments(experiments);
      setV1Stages(status.stages);
      setSweepRows(sweepSummary.rows);
      setSweepFiles(files.files);
      setReport(sweepReport.content || sweepReport.message || "");
      setSweepStatus(sweepSummary.status === "ready" ? "V1.8 sweep 结果已加载。" : sweepSummary.message ?? "尚未运行 V1.8 sweep。");
      setV1Error("");
    } catch (error) { setV1Error(errorMessage(error)); }
  }

  async function runExperiment() {
    setBusy(true); setRunStatus("正在运行默认实验…");
    try {
      const response = await runDefaultExperiment();
      setRunStatus("运行完成"); setRunResponse(JSON.stringify(response, null, 2));
      await Promise.all([refreshSummary(), refreshLog(), refreshFiles()]);
    } catch (error) { setRunStatus("运行失败"); setRunResponse(errorMessage(error)); }
    finally { setBusy(false); }
  }

  async function runSweep() {
    setSweepBusy(true); setSweepStatus("正在运行 V1.8 baseline sweep…");
    try {
      const response = await runV1Sweep();
      setSweepStatus(`运行完成：${response.output_dir}`);
      await loadV1Acceptance();
    } catch (error) { setSweepStatus(`运行失败：${errorMessage(error)}`); }
    finally { setSweepBusy(false); }
  }

  async function refreshLog() {
    try { setRuntimeLog(await fetchRuntimeLog()); }
    catch (error) { setRuntimeLog(`无法加载 runtime.log：${errorMessage(error)}`); }
  }

  async function refreshSummary() {
    try { setSummary(await fetchSummary()); }
    catch (error) { setSummary(null); setRunResponse(`无法加载 summary：${errorMessage(error)}`); }
  }

  async function refreshFiles() {
    try { setAvailableFiles(await fetchExperimentFiles()); setFileError(""); }
    catch (error) { setAvailableFiles([]); setFileError(`无法加载结果文件：${errorMessage(error)}`); }
  }

  const runnableExperiment = v1Experiments.find((experiment) => experiment.runnable && experiment.implemented);
  const plannedExperiments = v1Experiments.filter((experiment) => !experiment.runnable || !experiment.implemented);

  return <main className="app-shell">
    <header className="hero"><p className="eyebrow">元宇宙区块链实验平台</p><h1>V1-final 验收集成</h1><p className="muted">后端地址：{API_BASE_URL} · 保留 V0 默认实验，同时接入 V1.5/V1.6/V1.7/V1.8 验收入口。</p></header>
    <section className="panel experiments" aria-labelledby="experiments-title"><div><p className="eyebrow">V0 保留入口</p><h2 id="experiments-title">v0_default_asset_hotspot</h2><p>使用虚拟时钟串行回放的默认 MockChain asset_hotspot 工作负载。</p><a href={`${API_BASE_URL}/api/v0/config/default`} target="_blank" rel="noreferrer">查看默认配置</a></div><button type="button" onClick={runExperiment} disabled={busy}>{busy ? "运行中…" : "运行默认实验"}</button></section>
    <section className="panel" aria-labelledby="composer-title"><p className="eyebrow">组件编排预览</p><h2 id="composer-title">默认 V0 插件包</h2><dl className="plugin-grid">{plugins.map(([kind, plugin]) => <div key={kind}><dt>{kind}</dt><dd>{plugin}</dd></div>)}</dl></section>
    <section className="panel" aria-labelledby="v1-status-title">
      <div className="section-heading"><div><p className="eyebrow">V1-final</p><h2 id="v1-status-title">V1.1–V1.8 完成状态</h2></div><button type="button" onClick={loadV1Acceptance}>刷新 V1 状态</button></div>
      <p className="muted">这里展示的是 V1 core implementation 的验收状态；dual-chain / multi-chain / cross-chain / MetaFlow 仍保持 V2/V3 planned。</p>
      {v1Error && <p className="file-error">无法加载 V1 验收数据：{v1Error}</p>}
      <div className="status-grid">{v1Stages.map((stage) => <article key={stage.id} className="status-card"><strong>{stage.label}</strong><span>{stage.id}</span><b className={`badge ${statusClass(stage.status)}`}>{statusLabel(stage.status)}</b></article>)}</div>
    </section>
    <section className="panel v1-wizard" aria-labelledby="v1-title">
      <div className="section-heading"><div><p className="eyebrow">V1 拓扑与策略</p><h2 id="v1-title">单链 V1 paper-experiment 验收视图</h2></div></div>
      <p className="muted">V1 已完成单链可运行论文实验链路：V1.4 Fabric smoke 为 CLI/WSL 真实性入口；V1.5 routing、V1.6 dual-track、V1.7 aggregation 和 V1.8 sweep/report 已接入网页验收。</p>
      <section className="wizard-step"><h3>Step 1：实验范围</h3><div className="wizard-grid">{scopes.map(([label, key, status, description]) => <article key={key} className={status === "当前可用" ? "wizard-card selected" : "wizard-card"}><strong>{label}</strong><span>{key}</span><b className={`badge ${status === "当前可用" ? "badge-success" : "badge-planned"}`}>{status}</b><small>{description}</small></article>)}</div></section>
      <section className="wizard-step"><h3>Step 2：链拓扑</h3><div className="wizard-grid"><article className="wizard-card selected"><strong>当前单链拓扑</strong><span>链数量：1 · Chain ID：chain_0 / mockchain</span><span>chain_backend：mockchain · shard_count：4</span><span>consensus_sharding：single_group · cross_chain_protocol：disabled</span></article><article className="wizard-card"><strong>仍为规划能力</strong><span>双链拓扑、异构链配置、链间连接关系、跨链边配置</span><b className="badge badge-planned">V2/V3 planned</b></article></div></section>
      <section className="wizard-step"><h3>Step 3：链内组件</h3><dl className="plugin-grid">{chainComponents.map(([kind, plugin]) => <div key={kind}><dt>{kind}</dt><dd>{plugin}</dd></div>)}</dl></section>
      <section className="wizard-step"><h3>Step 4：负载来源</h3><div className="wizard-grid"><article className="wizard-card selected"><strong>Synthetic workload</strong><span>当前插件：asset_hotspot / V1 workload trace enhancement</span><b className="badge badge-success">当前可用</b></article><article className="wizard-card selected"><strong>Existing trace replay</strong><span>复用已有 trace 进行 replay 与 V1.8 sweep。</span><b className="badge badge-success">当前可用</b></article><article className="wizard-card selected"><strong>Fabric chain-backed trace</strong><span>小规模真实链上 trace smoke runner。</span><b className="badge badge-cli">已完成，CLI/WSL</b></article></div></section>
      <section className="wizard-step"><h3>Step 5：实验套件 / 策略组</h3><div className="suite-list">{suites.map(([title, id, status, strategies]) => <article key={id} className="suite-card"><div><strong>{title}</strong><span>{id}</span></div><b>{status}</b><ul>{strategies.map((strategy) => <li key={strategy}>{strategy}</li>)}</ul></article>)}</div></section>
      <section className="wizard-step"><h3>Step 6：Composer Preview</h3><div className="wizard-grid"><article className="wizard-card selected"><strong>兼容保留的 V1.1 可运行配置</strong><span>{runnableExperiment?.id ?? "v1_baseline_hash_serial"}</span><span>组件：hash_routing + serial_execution + normal_commit</span><small>早期 composer 语义保持不破坏。</small></article><article className="wizard-card"><strong>早期声明仍不可直接运行</strong><ul className="planned-list">{plannedExperiments.map((experiment) => <li key={experiment.id}>{experiment.id}</li>)}</ul><small>V1-final 的运行入口使用 V1.8 baseline sweep。</small></article></div></section>
    </section>
    <section className="panel" aria-labelledby="sweep-title">
      <div className="section-heading"><div><p className="eyebrow">V1.8 baseline / sweep / report</p><h2 id="sweep-title">一键运行与结果验收</h2></div><div className="button-row"><button type="button" onClick={runSweep} disabled={sweepBusy}>{sweepBusy ? "运行中…" : "运行 V1.8 baseline sweep"}</button><button type="button" onClick={loadV1Acceptance}>刷新 V1.8 结果</button></div></div>
      <p className="muted">{sweepStatus}</p>
      <div className="baseline-grid">{baselines.map(([name, routing, dualTrack, aggregation]) => <article key={name} className={`baseline-card ${baselineClass(name)}`}><b className="badge baseline-badge">{baselineLabel(name)}</b><strong>{name}</strong><span>routing.policy = {routing}</span><span>dual_track_enabled = {dualTrack}</span><span>hot_update_aggregation_enabled = {aggregation}</span></article>)}</div>
      {sweepRows.length > 0 ? <SweepTable rows={sweepRows} /> : <p className="file-error">尚无 sweep summary；点击运行按钮后会生成四组 baseline/ablation 结果。</p>}
      <div className="section-heading files-heading"><div><h3>V1.8 产物下载</h3><p className="muted">输出目录为 .cache/v1_8_sweeps/latest，不进入 Git。</p></div></div>
      <ul className="file-list">{["report.md", "sweep_summary.csv", "sweep_summary.json"].map((filename) => { const file = sweepFiles.find((item) => item.name === filename); return <li key={filename}><span><b className="file-type">{fileType(filename)}</b>{filename}</span><span className={file ? "file-present" : "file-missing"}>{file ? `${file.size_bytes} bytes` : "未生成"}</span>{file ? <a href={v1SweepFileDownloadURL(filename)}>下载</a> : <span>—</span>}</li>; })}</ul>
    </section>
    <section className="panel" aria-labelledby="report-title">
      <div className="section-heading"><div><p className="eyebrow">V1.8 report.md</p><h2 id="report-title">报告查看</h2></div><button type="button" onClick={loadV1Acceptance}>刷新 report</button></div>
      <pre>{report || "尚未生成 report.md。"}</pre>
    </section>
    <section className="panel fabric-panel" aria-labelledby="fabric-title">
      <p className="eyebrow">V1.4 Fabric chain-backed trace</p><h2 id="fabric-title">Fabric smoke 是 CLI/WSL 入口</h2>
      <p className="muted">网页不会自动启动 Docker、Fabric、network.sh、deployCC 或 peer invoke。真实 smoke 请在 WSL2 Ubuntu + Docker Desktop + fabric-samples 环境中运行：</p>
      <pre>python scripts/v1_fabric_smoke.py --strict --channel mbechannel --out .cache/fabric_smoke/latest</pre>
    </section>
    <section className="panel" aria-labelledby="console-title"><div className="section-heading"><div><p className="eyebrow">V0 运行控制台</p><h2 id="console-title">{runStatus}</h2></div><button type="button" onClick={refreshLog}>刷新 runtime.log</button></div><h3>运行 API 返回内容</h3><pre>{runResponse}</pre><h3>runtime.log</h3><pre>{runtimeLog}</pre></section>
    <section className="panel" aria-labelledby="results-title"><div className="section-heading"><div><p className="eyebrow">V0 结果</p><h2 id="results-title">基础指标</h2></div><button type="button" onClick={refreshSummary}>刷新 summary</button></div><dl className="metrics-grid">{metricKeys.map((key) => <div key={key}><dt>{key}</dt><dd>{summary?.[key] ?? "—"}</dd></div>)}</dl><div className="section-heading files-heading"><div><h3>结果文件</h3><p className="muted">运行完成后可下载当前实验产物。</p></div><button type="button" onClick={refreshFiles}>刷新文件列表</button></div>{fileError && <p className="file-error">{fileError}</p>}<ul className="file-list">{resultFiles.map((filename) => { const exists = availableFiles.includes(filename); return <li key={filename}><span><b className="file-type">{fileType(filename)}</b>{filename}</span><span className={exists ? "file-present" : "file-missing"}>{exists ? "已生成" : "未生成"}</span>{exists ? <a href={experimentFileDownloadURL(filename)}>下载</a> : <span>—</span>}</li>; })}</ul></section>
  </main>;
}

function SweepTable({ rows }: { rows: V1SweepRow[] }) {
  return <div className="sweep-results">{rows.map((row) => <article key={String(row.name)} className="sweep-row">
    <h3>{row.name}</h3>
    <MetricSection title="基础指标" row={row} keys={baseMetricKeys} />
    <MetricSection title="Routing 指标" row={row} keys={routingMetricKeys} />
    <MetricSection title="Dual-track 指标" row={row} keys={dualTrackMetricKeys} />
    <MetricSection title="Aggregation 指标" row={row} keys={aggregationMetricKeys} />
  </article>)}</div>;
}

function MetricSection({ title, row, keys }: { title: string; row: V1SweepRow; keys: readonly string[] }) {
  return <section className={`metric-section ${metricSectionClass(title)}`}><h4>{title}</h4><dl className="metrics-grid compact">{keys.map((key) => <div key={key}><dt>{key}</dt><dd>{formatMetric(row[key])}</dd></div>)}</dl></section>;
}

function statusLabel(status: string): string {
  if (status === "completed_cli_only") return "已完成 · CLI/WSL";
  if (status === "completed") return "已完成";
  return status;
}

function statusClass(status: string): string {
  if (status === "completed_cli_only") return "badge-cli";
  if (status === "completed") return "badge-success";
  return "badge-planned";
}

function baselineLabel(name: string): string {
  if (name === "baseline_hash_only") return "Baseline";
  if (name === "co_access_only") return "Routing";
  if (name === "co_access_dual_track") return "Dual-track";
  return "Full V1";
}

function baselineClass(name: string): string {
  return `baseline-${name.replace(/_/g, "-")}`;
}

function metricSectionClass(title: string): string {
  if (title.startsWith("Routing")) return "metric-routing";
  if (title.startsWith("Dual")) return "metric-dual";
  if (title.startsWith("Aggregation")) return "metric-aggregation";
  return "metric-base";
}

function fileType(filename: string): string {
  return filename.split(".").pop()?.toUpperCase() ?? "FILE";
}

function formatMetric(value: V1SweepRow[string]): string {
  if (value === undefined || value === null || value === "") return "—";
  return String(value);
}

function errorMessage(error: unknown): string { return error instanceof Error ? error.message : String(error); }

export default App;
