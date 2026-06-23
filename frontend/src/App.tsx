import { useEffect, useState } from "react";

import {
  API_BASE_URL,
  experimentFileDownloadURL,
  fetchExperimentFiles,
  fetchRuntimeLog,
  fetchSummary,
  fetchV1Experiments,
  runDefaultExperiment,
  type Summary,
  type V1Experiment,
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
  ["双链实验", "dual-chain", "规划中", "用于两条异构链之间的跨链操作，V2 正式实现。"],
  ["多链实验", "multi-chain", "规划中", "用于 AssetChain、SceneChain、RewardChain 等多链元宇宙场景，V2 正式实现。"],
  ["跨链协议实验", "cross-chain protocol", "规划中", "用于 committee bridge、Pending Pool、MetaFlow 等跨链协议，V2 正式实现。"],
] as const;

const suites = [
  ["执行策略对比", "v1_execution_comparison", "可预览", ["hash_serial：可运行", "blockstm_like：规划中", "calvin_like：规划中", "porygon_like：规划中"]],
  ["MetaTrack 主实验", "v1_metatrack_main", "规划中", ["hash_serial：可运行基础线", "blockstm_like：规划中", "calvin_like：规划中", "porygon_like：规划中", "ours_metatrack：规划中"]],
  ["消融实验", "v1_ablation", "规划中", ["ours_no_routing：规划中", "ours_no_dual_track：规划中", "ours_no_hot_aggregation：规划中"]],
  ["Fabric 链上 trace 校验", "v1_fabric_chain_backed", "规划中", ["fabric_chain_backed_asset：规划中"]],
] as const;

function App() {
  const [runStatus, setRunStatus] = useState("就绪");
  const [runResponse, setRunResponse] = useState("尚未发送运行请求。");
  const [runtimeLog, setRuntimeLog] = useState("尚未加载运行日志。");
  const [summary, setSummary] = useState<Summary | null>(null);
  const [availableFiles, setAvailableFiles] = useState<string[]>([]);
  const [fileError, setFileError] = useState("");
  const [busy, setBusy] = useState(false);
  const [v1Experiments, setV1Experiments] = useState<V1Experiment[]>([]);
  const [v1Error, setV1Error] = useState("");

  useEffect(() => { void loadV1Composer(); }, []);

  async function loadV1Composer() {
    try {
      setV1Experiments(await fetchV1Experiments());
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
    <header><p className="eyebrow">元宇宙区块链实验平台</p><h1>V0 默认单链实验</h1><p className="muted">后端地址：{API_BASE_URL}</p></header>
    <section className="panel experiments" aria-labelledby="experiments-title"><div><p className="eyebrow">实验</p><h2 id="experiments-title">v0_default_asset_hotspot</h2><p>使用虚拟时钟串行回放的默认 MockChain asset_hotspot 工作负载。</p><a href={`${API_BASE_URL}/api/v0/config/default`} target="_blank" rel="noreferrer">查看默认配置</a></div><button type="button" onClick={runExperiment} disabled={busy}>{busy ? "运行中…" : "运行默认实验"}</button></section>
    <section className="panel" aria-labelledby="composer-title"><p className="eyebrow">组件编排预览</p><h2 id="composer-title">默认 V0 插件包</h2><dl className="plugin-grid">{plugins.map(([kind, plugin]) => <div key={kind}><dt>{kind}</dt><dd>{plugin}</dd></div>)}</dl></section>
    <section className="panel v1-wizard" aria-labelledby="v1-title">
      <div className="section-heading"><div><p className="eyebrow">V1.1</p><h2 id="v1-title">拓扑优先实验向导</h2></div><button type="button" onClick={loadV1Composer}>刷新 V1 向导</button></div>
      <p className="muted">当前阶段只完成实验范围、链拓扑、链内组件、负载来源、策略组和 Composer Preview 的声明式展示。当前仅 single-chain / hash_serial 可运行；双链、多链、跨链、Fabric 和论文机制均处于规划中。</p>
      {v1Error && <p className="file-error">无法加载 V1 Composer：{v1Error}</p>}
      <section className="wizard-step"><h3>Step 1：实验范围</h3><div className="wizard-grid">{scopes.map(([label, key, status, description]) => <article key={key} className={status === "当前可用" ? "wizard-card selected" : "wizard-card"}><strong>{label}</strong><span>{key}</span><b>{status}</b><small>{description}</small></article>)}</div></section>
      <section className="wizard-step"><h3>Step 2：链拓扑</h3><div className="wizard-grid"><article className="wizard-card selected"><strong>当前单链拓扑</strong><span>链数量：1 · Chain ID：chain_0 / mockchain</span><span>chain_backend：mockchain · shard_count：4</span><span>consensus_sharding：single_group · cross_chain_protocol：disabled</span></article><article className="wizard-card"><strong>规划能力</strong><span>双链拓扑、异构链配置、链间连接关系、跨链边配置</span><b>规划中</b></article></div></section>
      <section className="wizard-step"><h3>Step 3：链内组件</h3><dl className="plugin-grid">{chainComponents.map(([kind, plugin]) => <div key={kind}><dt>{kind}</dt><dd>{plugin}</dd></div>)}</dl></section>
      <section className="wizard-step"><h3>Step 4：负载来源</h3><div className="wizard-grid"><article className="wizard-card selected"><strong>Synthetic workload</strong><span>当前插件：asset_hotspot</span><b>当前可用</b></article><article className="wizard-card"><strong>Existing trace replay</strong><span>复用已有 trace 进行 replay，后续完善。</span><b>可预览 / 可复用</b></article><article className="wizard-card"><strong>Fabric chain-backed trace</strong><span>V1 后续用于小规模单链真实性校验，不在 V1.1 实现。</span><b>规划中</b></article></div></section>
      <section className="wizard-step"><h3>Step 5：实验套件 / 策略组</h3><div className="suite-list">{suites.map(([title, id, status, strategies]) => <article key={id} className="suite-card"><div><strong>{title}</strong><span>{id}</span></div><b>{status}</b><ul>{strategies.map((strategy) => <li key={strategy}>{strategy}</li>)}</ul></article>)}</div></section>
      <section className="wizard-step"><h3>Step 6：Composer Preview</h3><div className="wizard-grid"><article className="wizard-card selected"><strong>当前唯一可运行配置</strong><span>{runnableExperiment?.id ?? "v1_baseline_hash_serial"}</span><span>组件：hash_routing + serial_execution + normal_commit</span><small>来源：复用 V0 默认可运行链路。</small></article><article className="wizard-card"><strong>规划中配置</strong><ul className="planned-list">{plannedExperiments.map((experiment) => <li key={experiment.id}>{experiment.id}</li>)}</ul><small>仅显示 Composer 状态；这些配置没有运行按钮。</small></article></div></section>
    </section>
    <section className="panel" aria-labelledby="console-title"><div className="section-heading"><div><p className="eyebrow">运行控制台</p><h2 id="console-title">{runStatus}</h2></div><button type="button" onClick={refreshLog}>刷新 runtime.log</button></div><h3>运行 API 返回内容</h3><pre>{runResponse}</pre><h3>runtime.log</h3><pre>{runtimeLog}</pre></section>
    <section className="panel" aria-labelledby="results-title"><div className="section-heading"><div><p className="eyebrow">结果</p><h2 id="results-title">基础指标</h2></div><button type="button" onClick={refreshSummary}>刷新 summary</button></div><dl className="metrics-grid">{metricKeys.map((key) => <div key={key}><dt>{key}</dt><dd>{summary?.[key] ?? "—"}</dd></div>)}</dl><div className="section-heading files-heading"><div><h3>结果文件</h3><p className="muted">运行完成后可下载当前实验产物。</p></div><button type="button" onClick={refreshFiles}>刷新文件列表</button></div>{fileError && <p className="file-error">{fileError}</p>}<ul className="file-list">{resultFiles.map((filename) => { const exists = availableFiles.includes(filename); return <li key={filename}><span>{filename}</span><span className={exists ? "file-present" : "file-missing"}>{exists ? "已生成" : "未生成"}</span>{exists ? <a href={experimentFileDownloadURL(filename)}>下载</a> : <span>—</span>}</li>; })}</ul></section>
  </main>;
}

function errorMessage(error: unknown): string { return error instanceof Error ? error.message : String(error); }

export default App;
