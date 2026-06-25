import { useEffect, useMemo, useState } from "react";

import {
  fetchV2ChainBackends,
  fetchV2DualChainSampleConfig,
  fetchV2ProtocolSampleConfig,
  fetchV2Protocols,
  fetchV2RunArtifacts,
  fetchV2Runs,
  fetchV2TraceSource,
  fetchV2TraceSources,
  previewV2Composer,
  runV2DualChainReplay,
  runV2ProtocolReplay,
  validateV2TraceSource,
  v2ArtifactDownloadURL,
  type V2Artifact,
  type V2ChainBackend,
  type V2ComposerPreviewResult,
  type V2DualChainReplayResponse,
  type V2ProtocolInfo,
  type V2ProtocolReplayResponse,
  type V2RunSummary,
  type V2SampleConfig,
  type V2TraceSource,
  type V2TraceSourceValidationResult,
} from "../api";

const composerPresets = [
  {
    id: "single_chain_full_v1",
    label: "single_chain + synthetic + full_v1",
    payload: { topology: "single_chain", trace_source: "synthetic", workload: "asset_hotspot", routing: "co_access", execution: "dual_track", commit: "hot_update_aggregation", cross_chain_protocol: "disabled" },
  },
  {
    id: "fabric_chain_backed",
    label: "single_chain + Fabric chain-backed trace",
    payload: { topology: "single_chain", trace_source: "fabric_chain_backed_trace", workload: "asset_hotspot", routing: "hash", execution: "serial", commit: "normal_commit", cross_chain_protocol: "disabled" },
  },
  {
    id: "public_chain_semantic_unknown",
    label: "public_chain_imported_trace + co_access",
    payload: { topology: "single_chain", trace_source: "public_chain_imported_trace", workload: "asset_hotspot", routing: "co_access", execution: "serial", commit: "normal_commit", cross_chain_protocol: "disabled" },
  },
  {
    id: "public_chain_hot_update_gap",
    label: "public_chain_imported_trace + hot_update_aggregation",
    payload: { topology: "single_chain", trace_source: "public_chain_imported_trace", workload: "asset_hotspot", routing: "hash", execution: "serial", commit: "hot_update_aggregation", cross_chain_protocol: "disabled" },
  },
  {
    id: "dual_chain_planned",
    label: "dual_chain planned topology",
    payload: { topology: "dual_chain", trace_source: "synthetic", cross_chain_protocol: "disabled" },
  },
  {
    id: "protocol_baseline_planned_preview",
    label: "cross_chain_replay + local protocol baseline preview",
    payload: { topology: "cross_chain_replay", trace_source: "synthetic", cross_chain_protocol: "lock_mint_serial" },
  },
];

const traceValidationPayloads: Record<string, Record<string, unknown>> = {
  synthetic: { source_id: "synthetic", workload: "asset_hotspot_v1", tx_count: 100 },
  existing_trace: { source_id: "existing_trace", trace_path: "tests/golden/trace_small.jsonl.gz" },
  fabric_chain_backed_trace: { source_id: "fabric_chain_backed_trace" },
  public_chain_imported_trace: { source_id: "public_chain_imported_trace", trace_path: "data/public_chain/sample.jsonl.gz" },
};

export default function V2Dashboard() {
  const [traceSources, setTraceSources] = useState<V2TraceSource[]>([]);
  const [selectedTrace, setSelectedTrace] = useState<V2TraceSource | null>(null);
  const [traceValidation, setTraceValidation] = useState<V2TraceSourceValidationResult | null>(null);
  const [backends, setBackends] = useState<V2ChainBackend[]>([]);
  const [protocols, setProtocols] = useState<V2ProtocolInfo[]>([]);
  const [dualConfig, setDualConfig] = useState<V2SampleConfig | null>(null);
  const [protocolConfig, setProtocolConfig] = useState<V2SampleConfig | null>(null);
  const [composerPresetId, setComposerPresetId] = useState(composerPresets[0].id);
  const [composerPreview, setComposerPreview] = useState<V2ComposerPreviewResult | null>(null);
  const [dualResult, setDualResult] = useState<V2DualChainReplayResponse | null>(null);
  const [protocolResult, setProtocolResult] = useState<V2ProtocolReplayResponse | null>(null);
  const [runs, setRuns] = useState<V2RunSummary[]>([]);
  const [selectedRunId, setSelectedRunId] = useState("");
  const [runArtifacts, setRunArtifacts] = useState<V2Artifact[]>([]);
  const [loading, setLoading] = useState("");
  const [error, setError] = useState("");

  const selectedPreset = useMemo(() => composerPresets.find((item) => item.id === composerPresetId) ?? composerPresets[0], [composerPresetId]);

  useEffect(() => { void loadDashboard(); }, []);
  useEffect(() => { void runComposerPreview(); }, [composerPresetId]);

  async function loadDashboard() {
    setLoading("Loading V2 dashboard data");
    try {
      const [sources, backendItems, protocolItems, dualSample, protocolSample, runItems] = await Promise.all([
        fetchV2TraceSources(),
        fetchV2ChainBackends(),
        fetchV2Protocols(),
        fetchV2DualChainSampleConfig(),
        fetchV2ProtocolSampleConfig(),
        fetchV2Runs(),
      ]);
      setTraceSources(sources);
      setBackends(backendItems);
      setProtocols(protocolItems);
      setDualConfig(dualSample);
      setProtocolConfig(protocolSample);
      setRuns(runItems);
      setError("");
      if (sources[0]) {
        setSelectedTrace(sources[0]);
      }
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setLoading("");
    }
  }

  async function selectTraceSource(id: string) {
    try {
      setSelectedTrace(await fetchV2TraceSource(id));
      setTraceValidation(null);
      setError("");
    } catch (caught) {
      setError(errorMessage(caught));
    }
  }

  async function validateTraceSource(id: string) {
    try {
      setTraceValidation(await validateV2TraceSource(traceValidationPayloads[id] ?? { source_id: id }));
      setError("");
    } catch (caught) {
      setTraceValidation(null);
      setError(errorMessage(caught));
    }
  }

  async function runComposerPreview() {
    try {
      setComposerPreview(await previewV2Composer(selectedPreset.payload));
    } catch (caught) {
      setComposerPreview(null);
      setError(errorMessage(caught));
    }
  }

  async function runDualReplay() {
    setLoading("Running V2.5 dual-chain sample replay");
    try {
      const result = await runV2DualChainReplay();
      setDualResult(result);
      await refreshRuns(result.run_id);
      setError("");
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setLoading("");
    }
  }

  async function runProtocolReplay() {
    setLoading("Running V2.6 protocol baseline sample replay");
    try {
      const result = await runV2ProtocolReplay();
      setProtocolResult(result);
      await refreshRuns(result.run_id);
      setError("");
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setLoading("");
    }
  }

  async function refreshRuns(runId = selectedRunId) {
    const runItems = await fetchV2Runs();
    setRuns(runItems);
    const nextRunId = runId || runItems[0]?.run_id || "";
    setSelectedRunId(nextRunId);
    if (nextRunId) {
      const response = await fetchV2RunArtifacts(nextRunId);
      setRunArtifacts(response.artifacts);
    }
  }

  async function loadArtifactsForRun(runId: string) {
    setSelectedRunId(runId);
    try {
      const response = await fetchV2RunArtifacts(runId);
      setRunArtifacts(response.artifacts);
      setError("");
    } catch (caught) {
      setRunArtifacts([]);
      setError(errorMessage(caught));
    }
  }

  return <section className="panel v2-dashboard" aria-labelledby="v2-title">
    <div className="section-heading">
      <div><p className="eyebrow">V2 Platform Console</p><h2 id="v2-title">V3-ready local replay platform</h2></div>
      <button type="button" onClick={loadDashboard}>Refresh V2 data</button>
    </div>
    <p className="muted">V2 connects registry, composer, trace sources, job artifacts, ChainBackend, dual-chain replay, and cross-chain protocol baselines. Local replay is not real chain execution.</p>
    {loading && <p className="muted">{loading}</p>}
    {error && <p className="file-error">{error}</p>}

    <div className="v2-card-grid">
      <article className="v2-card"><V2StatusBadge status="runnable" /><strong>Local replay samples</strong><span>V2.5 and V2.6 sample runs are local virtual-time replay only.</span></article>
      <article className="v2-card"><V2BackendBadge backendType="local_virtual" /><strong>Default backend</strong><span>Local virtual-time backend. Not real chain execution.</span></article>
      <article className="v2-card"><V2TruthLabelBadge label="synthetic_replay" /><strong>Data truth</strong><span>Sample traces are synthetic replay inputs, not real on-chain data.</span></article>
    </div>

    <PanelTitle title="Data Truth Labels" />
    <div className="v2-badge-grid">
      {["synthetic_replay", "existing_trace_replay", "fabric_chain_backed_trace_replay", "public_chain_imported_trace_semantic_unknown", "planned_cross_chain_replay"].map((label) => <V2TruthLabelBadge key={label} label={label} />)}
    </div>

    <PanelTitle title="Trace Sources" />
    <div className="v2-list-grid">
      {traceSources.map((source) => <article key={source.id} className="v2-card">
        <div className="v2-card-head"><strong>{source.label}</strong><V2StatusBadge status={source.status} /></div>
        <V2TruthLabelBadge label={source.data_truth_label} />
        <span>{source.description}</span>
        <small>Entry: {source.entry_mode.join(", ")}</small>
        <small>Limitations: {source.limitations.join(" ")}</small>
        <div className="button-row"><button type="button" onClick={() => selectTraceSource(source.id)}>Details</button><button type="button" onClick={() => validateTraceSource(source.id)}>Validate</button></div>
      </article>)}
    </div>
    {selectedTrace && <InfoBlock title={`Trace source detail: ${selectedTrace.id}`} value={selectedTrace} />}
    {traceValidation && <InfoBlock title={`Validation: ${traceValidation.source_id}`} value={traceValidation} />}

    <PanelTitle title="Chain Backends" />
    <div className="v2-list-grid">
      {backends.map((backend) => <article key={backend.backend_type} className="v2-card">
        <div className="v2-card-head"><V2BackendBadge backendType={backend.backend_type} /><V2StatusBadge status={backend.status} /></div>
        <span>virtual_time={String(backend.supports_virtual_time)} replay={String(backend.supports_replay)} real_time={String(backend.supports_real_time)} listener={String(backend.supports_event_listener)}</span>
        <V2TruthLabelBadge label={backend.data_truth_label} />
        <small>{backend.limitations.join(" ")}</small>
      </article>)}
    </div>

    <PanelTitle title="Composer Preview" />
    <div className="v2-control-row">
      <label><span>Preset</span><select value={composerPresetId} onChange={(event) => setComposerPresetId(event.target.value)}>{composerPresets.map((preset) => <option key={preset.id} value={preset.id}>{preset.label}</option>)}</select></label>
      <button type="button" onClick={runComposerPreview}>Preview only</button>
    </div>
    {composerPreview && <article className="v2-card v2-wide">
      <div className="v2-card-head"><V2StatusBadge status={composerPreview.status} /><V2TruthLabelBadge label={composerPreview.data_truth_label} /></div>
      <span>runnable={String(composerPreview.runnable)} stage={composerPreview.stage} topology={composerPreview.topology}</span>
      <ReasonList title="Reasons" items={composerPreview.reasons} />
      <ReasonList title="Warnings" items={composerPreview.warnings} />
      <ReasonList title="Blocked by" items={composerPreview.blocked_by} />
    </article>}

    <PanelTitle title="Dual-chain Replay" />
    <article className="v2-card v2-wide">
      <p className="muted">Local virtual-time replay only. Not real chain execution.</p>
      <span>Sample config: {dualConfig?.path ?? "not loaded"}</span>
      <ChainProfiles config={dualConfig?.config} />
      <button type="button" onClick={runDualReplay} disabled={Boolean(loading)}>Run V2.5 sample replay</button>
      {dualResult && <RunResultSummary result={dualResult} summaryKeys={["cross_tx_count", "stage_record_count", "finality_wait_time_ms", "source_wait_time_ms", "target_wait_time_ms", "chain_speed_imbalance", "data_truth_label", "source_backend_type", "target_backend_type"]} />}
    </article>

    <PanelTitle title="Cross-chain Protocol Baselines" />
    <div className="v2-list-grid">
      {protocols.map((protocol) => <article key={protocol.name} className="v2-card"><div className="v2-card-head"><strong>{protocol.name}</strong><V2StatusBadge status={protocol.status} /></div><span>maturity={protocol.maturity}</span><small>{protocol.reason}</small></article>)}
    </div>
    <article className="v2-card v2-wide">
      <p className="muted">Local protocol baseline replay only. Not production bridge. Not MetaFlow.</p>
      <span>Sample config: {protocolConfig?.path ?? "not loaded"}</span>
      <button type="button" onClick={runProtocolReplay} disabled={Boolean(loading)}>Run V2.6 protocol baseline replay</button>
      {protocolResult && <ProtocolSummary result={protocolResult} />}
    </article>

    <PanelTitle title="Run History & Artifacts" />
    <div className="v2-control-row">
      <button type="button" onClick={() => refreshRuns()}>Refresh runs</button>
      <label><span>Run</span><select value={selectedRunId} onChange={(event) => loadArtifactsForRun(event.target.value)}><option value="">Select run</option>{runs.map((run) => <option key={run.run_id} value={run.run_id}>{run.run_id} / {run.stage} / {run.status}</option>)}</select></label>
    </div>
    <div className="v2-list-grid">{runs.slice(0, 6).map((run) => <article key={run.run_id} className="v2-card">
      <div className="v2-card-head"><strong>{run.run_id}</strong><V2StatusBadge status={run.status} /></div>
      <span>{run.stage} / {run.source}</span>
      <V2TruthLabelBadge label={run.data_truth_label} />
      <small>created={run.created_at} updated={run.updated_at}</small>
      <small>artifacts={run.artifact_count} summary={String(run.summary_available)} report={String(run.report_available)}</small>
    </article>)}</div>
    <ArtifactList artifacts={runArtifacts} />

    <PanelTitle title="Boundaries / Non-goals" />
    <ul className="v2-boundaries">
      <li>V2 is a V3-ready local replay platform.</li>
      <li>V2 does not start Docker/Fabric/network.sh from the web UI.</li>
      <li>V2 does not connect to live public-chain nodes.</li>
      <li>local_virtual backend is not real chain execution.</li>
      <li>Protocol baselines are not production bridges.</li>
      <li>MetaFlow is planned, not implemented in V2.7.</li>
      <li>FabricLiveBackend and EVMLiveBackend are planned for V3.</li>
      <li>V2.8 will add sweep/report; V2.9 may add realism bridge; V3 replaces backend/deployment/monitoring layers.</li>
    </ul>
  </section>;
}

function PanelTitle({ title }: { title: string }) {
  return <div className="v2-panel-title"><h3>{title}</h3></div>;
}

function V2TruthLabelBadge({ label }: { label: string }) {
  const text: Record<string, string> = {
    synthetic_replay: "Synthetic replay / not real chain execution",
    existing_trace_replay: "Existing trace replay / no chain launched",
    fabric_chain_backed_trace_replay: "Fabric chain-backed trace replay / web only replays existing trace",
    public_chain_imported_trace_semantic_unknown: "Public-chain imported trace / semantic unknown",
    planned_cross_chain_replay: "Planned cross-chain replay / not runnable",
    production_deployment_planned: "Production deployment planned / V3 only",
  };
  return <span className="badge badge-cli">{text[label] ?? label}</span>;
}

function V2BackendBadge({ backendType }: { backendType: string }) {
  const normalized = backendType === "fabric_live" ? "fabric_live_planned" : backendType === "evm_live" ? "evm_live_planned" : backendType;
  const text: Record<string, string> = {
    local_virtual: "Local virtual-time backend",
    trace_replay: "Trace replay backend",
    fabric_live_planned: "Fabric live backend planned / not implemented",
    evm_live_planned: "EVM live backend planned / not implemented",
  };
  return <span className={`badge ${normalized.includes("planned") ? "badge-planned" : "badge-success"}`}>{text[normalized] ?? backendType}</span>;
}

function V2StatusBadge({ status }: { status: string }) {
  const cls = status === "runnable" || status === "completed" ? "badge-success" : status === "planned" || status === "blocked" ? "badge-planned" : status === "failed" || status === "invalid" ? "badge-danger" : "badge-cli";
  return <span className={`badge ${cls}`}>{status}</span>;
}

function InfoBlock({ title, value }: { title: string; value: unknown }) {
  return <article className="v2-card v2-wide"><strong>{title}</strong><pre>{JSON.stringify(value, null, 2)}</pre></article>;
}

function ReasonList({ title, items }: { title: string; items: string[] }) {
  return <div><strong>{title}</strong>{items.length ? <ul>{items.map((item) => <li key={item}>{item}</li>)}</ul> : <span> none</span>}</div>;
}

function ChainProfiles({ config }: { config?: Record<string, unknown> }) {
  const chains = (config?.chains ?? {}) as Record<string, Record<string, unknown>>;
  return <div className="v2-list-grid">{Object.values(chains).map((chain) => <article key={String(chain.chain_id)} className="v2-card"><strong>{String(chain.chain_id)}</strong><span>role={String(chain.role)} block_interval_ms={String(chain.block_interval_ms)} finality_depth={String(chain.finality_depth)}</span><V2BackendBadge backendType={String(chain.backend_type)} /></article>)}</div>;
}

function RunResultSummary({ result, summaryKeys }: { result: V2DualChainReplayResponse; summaryKeys: string[] }) {
  return <div className="v2-result"><strong>run_id: {result.run_id}</strong><V2StatusBadge status={result.status} /><dl className="metrics-grid compact">{summaryKeys.map((key) => <div key={key}><dt>{key}</dt><dd>{String(result.summary[key] ?? "-")}</dd></div>)}</dl><ArtifactList artifacts={result.artifacts} /></div>;
}

function ProtocolSummary({ result }: { result: V2ProtocolReplayResponse }) {
  return <div className="v2-result"><strong>run_id: {result.run_id}</strong><V2StatusBadge status={result.status} /><V2TruthLabelBadge label={result.data_truth_label} /><span>protocol_truth={result.protocol_truth ?? "local_baseline_model"}</span><div className="v2-list-grid">{result.summary.items.map((item) => <article key={String(item.protocol_name)} className="v2-card"><strong>{String(item.protocol_name)}</strong><span>success={String(item.success_count)} timeout={String(item.timeout_count)} refund={String(item.refund_count)}</span><span>avg_e2e={String(item.avg_e2e_latency_ms)} p99={String(item.p99_e2e_latency_ms)} pending={String(item.max_pending_count)}</span><span>backend={String(item.source_backend_type)} / {String(item.target_backend_type)}</span><V2TruthLabelBadge label={String(item.data_truth_label)} /></article>)}</div><ArtifactList artifacts={result.artifacts} /></div>;
}

function ArtifactList({ artifacts }: { artifacts: V2Artifact[] }) {
  if (!artifacts.length) return <p className="muted">No artifacts selected or available.</p>;
  return <ul className="file-list">{artifacts.map((artifact) => <li key={artifact.name}><span><b className="file-type">{artifact.name.split(".").pop()?.toUpperCase() ?? "FILE"}</b>{artifact.name}</span><span className="file-present">{artifact.size_bytes} bytes</span><a href={v2ArtifactDownloadURL(artifact.download_url)}>Download</a></li>)}</ul>;
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
