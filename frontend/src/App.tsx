import { useState } from "react";

import { API_BASE_URL, fetchRuntimeLog, fetchSummary, runDefaultExperiment, type Summary } from "./api";

const plugins = [
  ["chain_backend", "mockchain"],
  ["workload", "asset_hotspot"],
  ["trace", "jsonl_gzip"],
  ["consensus_protocol", "simple_ordering"],
  ["consensus_sharding", "single_group"],
  ["state_sharding", "hash_state_sharding"],
  ["execution_sharding", "hash_execution_sharding"],
  ["routing", "hash_routing"],
  ["cross_shard_protocol", "local_only"],
  ["cross_chain_protocol", "disabled"],
  ["execution", "serial_execution"],
  ["commit", "normal_commit"],
  ["clock", "virtual_clock"],
  ["network_model", "fixed_latency_model"],
  ["metrics", "basic_metrics"],
  ["composer", "default_composer"],
] as const;

const metricKeys = [
  "tx_count",
  "success_count",
  "failed_count",
  "throughput_tps",
  "avg_latency_ms",
  "p95_latency_ms",
  "p99_latency_ms",
  "cross_shard_ratio",
  "remote_fetch_count",
  "wall_clock_runtime_ms",
] as const;

function App() {
  const [runStatus, setRunStatus] = useState("Ready");
  const [runResponse, setRunResponse] = useState("No run request has been sent.");
  const [runtimeLog, setRuntimeLog] = useState("Runtime log has not been loaded.");
  const [summary, setSummary] = useState<Summary | null>(null);
  const [busy, setBusy] = useState(false);

  async function runExperiment() {
    setBusy(true);
    setRunStatus("Running default experiment...");
    try {
      const response = await runDefaultExperiment();
      setRunStatus("Run completed");
      setRunResponse(JSON.stringify(response, null, 2));
      await refreshSummary();
      await refreshLog();
    } catch (error) {
      setRunStatus("Run failed");
      setRunResponse(errorMessage(error));
    } finally {
      setBusy(false);
    }
  }

  async function refreshLog() {
    try {
      setRuntimeLog(await fetchRuntimeLog());
    } catch (error) {
      setRuntimeLog(`Unable to load runtime.log: ${errorMessage(error)}`);
    }
  }

  async function refreshSummary() {
    try {
      setSummary(await fetchSummary());
    } catch (error) {
      setSummary(null);
      setRunResponse(`Unable to load summary: ${errorMessage(error)}`);
    }
  }

  return (
    <main className="app-shell">
      <header>
        <p className="eyebrow">Metaverse Blockchain Experiment Platform</p>
        <h1>V0 Default Single-Chain Experiment</h1>
        <p className="muted">Backend: {API_BASE_URL}</p>
      </header>

      <section className="panel experiments" aria-labelledby="experiments-title">
        <div>
          <p className="eyebrow">Experiments</p>
          <h2 id="experiments-title">v0_default_asset_hotspot</h2>
          <p>Default MockChain asset-hotspot workload with virtual-clock serial replay.</p>
          <a href={`${API_BASE_URL}/api/v0/config/default`} target="_blank" rel="noreferrer">View default configuration</a>
        </div>
        <button type="button" onClick={runExperiment} disabled={busy}>
          {busy ? "Running..." : "Run Default Experiment"}
        </button>
      </section>

      <section className="panel" aria-labelledby="composer-title">
        <p className="eyebrow">Composer Preview</p>
        <h2 id="composer-title">Default V0 plugin package</h2>
        <dl className="plugin-grid">
          {plugins.map(([kind, plugin]) => (
            <div key={kind}>
              <dt>{kind}</dt>
              <dd>{plugin}</dd>
            </div>
          ))}
        </dl>
      </section>

      <section className="panel" aria-labelledby="console-title">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Run Console</p>
            <h2 id="console-title">{runStatus}</h2>
          </div>
          <button type="button" onClick={refreshLog}>Refresh runtime.log</button>
        </div>
        <h3>Run API response</h3>
        <pre>{runResponse}</pre>
        <h3>runtime.log</h3>
        <pre>{runtimeLog}</pre>
      </section>

      <section className="panel" aria-labelledby="results-title">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Results</p>
            <h2 id="results-title">Basic metrics</h2>
          </div>
          <button type="button" onClick={refreshSummary}>Refresh summary</button>
        </div>
        <dl className="metrics-grid">
          {metricKeys.map((key) => (
            <div key={key}>
              <dt>{key}</dt>
              <dd>{summary?.[key] ?? "—"}</dd>
            </div>
          ))}
        </dl>
      </section>
    </main>
  );
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export default App;
