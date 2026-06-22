import { useState } from "react";

import {
  API_BASE_URL,
  experimentFileDownloadURL,
  fetchExperimentFiles,
  fetchRuntimeLog,
  fetchSummary,
  runDefaultExperiment,
  type Summary,
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

const metricKeys = ["tx_count", "success_count", "failed_count", "throughput_tps", "avg_latency_ms", "p95_latency_ms", "p99_latency_ms", "cross_shard_ratio", "remote_fetch_count", "wall_clock_runtime_ms"] as const;
const resultFiles = ["config.yaml", "trace_meta.json", "summary.csv", "latency.csv", "runtime.log"] as const;

function App() {
  const [runStatus, setRunStatus] = useState("就绪");
  const [runResponse, setRunResponse] = useState("尚未发送运行请求。");
  const [runtimeLog, setRuntimeLog] = useState("尚未加载运行日志。");
  const [summary, setSummary] = useState<Summary | null>(null);
  const [availableFiles, setAvailableFiles] = useState<string[]>([]);
  const [fileError, setFileError] = useState("");
  const [busy, setBusy] = useState(false);

  async function runExperiment() {
    setBusy(true); setRunStatus("正在运行默认实验……");
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

  return <main className="app-shell">
    <header><p className="eyebrow">元宇宙区块链实验平台</p><h1>V0 默认单链实验</h1><p className="muted">后端地址：{API_BASE_URL}</p></header>
    <section className="panel experiments" aria-labelledby="experiments-title"><div><p className="eyebrow">实验</p><h2 id="experiments-title">v0_default_asset_hotspot</h2><p>使用虚拟时钟串行回放的默认 MockChain asset_hotspot 工作负载。</p><a href={`${API_BASE_URL}/api/v0/config/default`} target="_blank" rel="noreferrer">查看默认配置</a></div><button type="button" onClick={runExperiment} disabled={busy}>{busy ? "运行中……" : "运行默认实验"}</button></section>
    <section className="panel" aria-labelledby="composer-title"><p className="eyebrow">组件编排预览</p><h2 id="composer-title">默认 V0 插件包</h2><dl className="plugin-grid">{plugins.map(([kind, plugin]) => <div key={kind}><dt>{kind}</dt><dd>{plugin}</dd></div>)}</dl></section>
    <section className="panel" aria-labelledby="console-title"><div className="section-heading"><div><p className="eyebrow">运行控制台</p><h2 id="console-title">{runStatus}</h2></div><button type="button" onClick={refreshLog}>刷新 runtime.log</button></div><h3>运行 API 返回内容</h3><pre>{runResponse}</pre><h3>runtime.log</h3><pre>{runtimeLog}</pre></section>
    <section className="panel" aria-labelledby="results-title"><div className="section-heading"><div><p className="eyebrow">结果</p><h2 id="results-title">基础指标</h2></div><button type="button" onClick={refreshSummary}>刷新 summary</button></div><dl className="metrics-grid">{metricKeys.map((key) => <div key={key}><dt>{key}</dt><dd>{summary?.[key] ?? "—"}</dd></div>)}</dl><div className="section-heading files-heading"><div><h3>结果文件</h3><p className="muted">运行完成后可下载当前实验产物。</p></div><button type="button" onClick={refreshFiles}>刷新文件列表</button></div>{fileError && <p className="file-error">{fileError}</p>}<ul className="file-list">{resultFiles.map((filename) => { const exists = availableFiles.includes(filename); return <li key={filename}><span>{filename}</span><span className={exists ? "file-present" : "file-missing"}>{exists ? "已生成" : "未生成"}</span>{exists ? <a href={experimentFileDownloadURL(filename)}>下载</a> : <span>—</span>}</li>; })}</ul></section>
  </main>;
}

function errorMessage(error: unknown): string { return error instanceof Error ? error.message : String(error); }

export default App;
