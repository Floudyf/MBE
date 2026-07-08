import { useEffect, useState } from "react";

import { fetchV4RealismArtifacts, fetchV4RealismStatus, fetchV4RealismSummary, runV4RealismSmoke, v4RealismArtifactURL, type V4RealismArtifact, type V4RealismSmokeRequest, type V4RealismStatus, type V4RealismSmokeResponse } from "../../api";

const truthKeys = [
  "real_signed_tx",
  "sender_public_key_binding",
  "signed_tx_authenticity",
  "per_node_mempool",
  "real_p2p",
  "pbft_style_consensus",
  "real_pbft_messages",
  "persistent_state_db",
  "state_root_from_real_state_updates",
  "real_cross_shard_state_machine",
  "real_cross_shard_network_commit",
  "recovery_supported",
  "fault_injection_supported",
  "real_fault_injection",
  "blockemulator_trace_to_signed_tx",
  "frontend_realism_mode",
];

const nonClaims = ["production_pbft", "full_byzantine_security", "fabric_evm_backend", "production_blockchain", "production_atomic_commit", "full_blockemulator_compatibility"];
const defaultRequest: V4RealismSmokeRequest = { nodes: 4, shards: 1, tx_count: 10, enable_cross_shard: true, enable_faults: true, fault_profile: "network_delay", blockemulator_tx_limit: 10, run_duration_ms: 1000 };
const recommendedValidationRequest: V4RealismSmokeRequest = { nodes: 8, shards: 2, tx_count: 20, enable_cross_shard: true, enable_faults: true, fault_profile: "mixed_light", blockemulator_tx_limit: 20, run_duration_ms: 1000, blockemulator_csv: undefined };

export default function RealismModePanel() {
  const [status, setStatus] = useState<V4RealismStatus | null>(null);
  const [result, setResult] = useState<V4RealismSmokeResponse | null>(null);
  const [form, setForm] = useState<V4RealismSmokeRequest>(defaultRequest);
  const [runIdQuery, setRunIdQuery] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => { void loadStatus(); }, []);

  async function loadStatus() {
    try {
      setStatus(await fetchV4RealismStatus());
      setError("");
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    }
  }

  async function runSmoke() {
    try {
      const validation = validateRequest(form);
      if (validation) {
        setError(validation);
        return;
      }
      setBusy(true);
      const response = await runV4RealismSmoke(normalizeRequest(form));
      setResult(response);
      setRunIdQuery(response.run_id);
      setError(response.status === "failed" ? String(response.stderr ?? response.stdout ?? "V4.3 smoke failed.") : "");
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setBusy(false);
    }
  }

  async function loadRun() {
    if (!runIdQuery.trim()) return;
    try {
      setBusy(true);
      const [summary, artifactResponse] = await Promise.all([fetchV4RealismSummary(runIdQuery.trim()), fetchV4RealismArtifacts(runIdQuery.trim())]);
      setResult({ run_id: runIdQuery.trim(), status: "loaded", output_dir: "", summary, artifacts: artifactResponse.artifacts });
      setError("");
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setBusy(false);
    }
  }

  async function refreshArtifacts() {
    const runId = result?.run_id || runIdQuery.trim();
    if (!runId) return;
    try {
      const artifactResponse = await fetchV4RealismArtifacts(runId);
      setResult((current) => current ? { ...current, artifacts: artifactResponse.artifacts } : { run_id: runId, status: "loaded", output_dir: "", summary: {}, artifacts: artifactResponse.artifacts });
      setError("");
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    }
  }

  const summary: Record<string, unknown> = result?.summary ?? status ?? {};
  const artifacts: V4RealismArtifact[] = result?.artifacts ?? [];

  return <section className="page-grid">
    <article className="final-card wide">
      <p className="eyebrow">当前步骤：② 验证 / 真实运行</p>
      <h2>② 验证 / 真实运行</h2>
      <p>使用 V4.3 真实运行时执行 signed tx、localhost TCP P2P、PBFT-style commit、状态执行、跨片证据、故障注入和 BlockEmulator bridge。</p>
      <p>Runtime truth: {String(summary.runtime_truth ?? "v4_real_state_cross_shard_recovery")}</p>
      <div className="button-row">
        <button type="button" onClick={runSmoke} disabled={busy}>{busy ? "Running..." : "Run V4.3 Smoke"}</button>
        <button type="button" onClick={loadStatus}>Refresh Status</button>
      </div>
      {error && <p className="file-error">{error}</p>}
    </article>

    <div className="final-card-grid">
      <article className="final-card">
        <h3>小规模真实节点验证</h3>
        <p className="muted">推荐配置：nodes=8, shards=2, tx_count=20, blockemulator_tx_limit=20, fault_profile=mixed_light, enable_cross_shard=true, enable_faults=true, BlockEmulator CSV path=空。</p>
        <button type="button" onClick={() => setForm(recommendedValidationRequest)}>使用推荐配置</button>
      </article>
      <article className="final-card">
        <h3>真实负载运行</h3>
        <p className="muted">用于后续接入已筛选的真实负载、小规模测试负载、不同偏斜度负载或 BlockEmulator CSV 子集。初期建议 small workload，确认 imported_tx_count 和 signed_tx_verify_pass_count 后再扩大规模。</p>
      </article>
    </div>

    <article className="final-card wide">
      <h3>Smoke Controls</h3>
      <div className="form-grid">
        <NumberInput label="nodes" value={form.nodes} min={4} max={8} onChange={(value) => setForm({ ...form, nodes: value })} />
        <NumberInput label="shards" value={form.shards} min={1} max={4} onChange={(value) => setForm({ ...form, shards: value })} />
        <NumberInput label="tx_count" value={form.tx_count} min={1} max={100} onChange={(value) => setForm({ ...form, tx_count: value })} />
        <NumberInput label="blockemulator_tx_limit" value={form.blockemulator_tx_limit} min={1} max={1000} onChange={(value) => setForm({ ...form, blockemulator_tx_limit: value })} />
        <label><span>fault_profile</span><select value={form.fault_profile} onChange={(event) => setForm({ ...form, fault_profile: event.target.value })}><option value="none">none</option><option value="network_delay">network_delay</option><option value="message_drop">message_drop</option><option value="node_restart">node_restart</option><option value="mixed_light">mixed_light</option></select></label>
        <label><span>BlockEmulator CSV path</span><input value={form.blockemulator_csv ?? ""} onChange={(event) => setForm({ ...form, blockemulator_csv: event.target.value || undefined })} /></label>
      </div>
      <div className="toggle-row">
        <label><input type="checkbox" checked={form.enable_cross_shard} onChange={(event) => setForm({ ...form, enable_cross_shard: event.target.checked })} /> enable_cross_shard</label>
        <label><input type="checkbox" checked={form.enable_faults} onChange={(event) => setForm({ ...form, enable_faults: event.target.checked })} /> enable_faults</label>
      </div>
      <div className="button-row">
        <button type="button" onClick={() => setForm({ ...form, blockemulator_csv: undefined, blockemulator_tx_limit: 10 })}>Use sample CSV</button>
        <button type="button" onClick={runSmoke} disabled={busy}>Run bridge + V4</button>
      </div>
    </article>

    <TruthGrid title="Implemented Evidence" description="这些字段表示 V4.3 runtime 已实现或已保持的真实性证据；Non-Claims 表示当前仍不宣称生产级能力。" values={summary} keys={truthKeys} />
    <TruthGrid title="Non-Claims" values={summary} keys={nonClaims} invert />

    <article className="final-card wide">
      <h3>Latest Smoke Summary</h3>
      <div className="metric-grid">
        <Metric label="Ready to commit" value={summary["ready_to_commit"]} />
        <Metric label="State root mismatches" value={summary["state_root_mismatch_count"]} />
        <Metric label="Committed height" value={summary["committed_height"]} />
        <Metric label="Cross-shard tx" value={summary["cross_shard_tx_count"]} />
        <Metric label="Fault events" value={summary["fault_event_count"]} />
      </div>
    </article>

    <article className="final-card wide">
      <h3>Run Lookup</h3>
      <div className="button-row">
        <input value={runIdQuery} onChange={(event) => setRunIdQuery(event.target.value)} placeholder="v4_..." />
        <button type="button" onClick={loadRun} disabled={busy}>Refresh Summary</button>
        <button type="button" onClick={refreshArtifacts}>Refresh Artifacts</button>
      </div>
    </article>

    <article className="final-card wide">
      <h3>Artifacts</h3>
      <p className="muted">当前 V4.3 运行产物在本页查看；左侧“实验产物”用于通用/历史运行记录。后续会统一 run registry。</p>
      {artifacts.length === 0 ? <p>No smoke artifacts loaded yet.</p> : <div className="artifact-list">
        {artifacts.map((item) => <a key={item.name} href={v4RealismArtifactURL(item.download_url)}>{item.name} ({item.size_bytes} bytes)</a>)}
      </div>}
    </article>
  </section>;
}

function NumberInput({ label, value, min, max, onChange }: { label: string; value: number; min: number; max: number; onChange: (value: number) => void }) {
  return <label><span>{label}</span><input type="number" min={min} max={max} value={value} onChange={(event) => onChange(Number(event.target.value))} /></label>;
}

function normalizeRequest(request: V4RealismSmokeRequest): V4RealismSmokeRequest {
  return {
    ...request,
    fault_profile: request.enable_faults ? request.fault_profile : "none",
  };
}

function validateRequest(request: V4RealismSmokeRequest): string {
  if (!Number.isFinite(request.nodes) || request.nodes < 4 || request.nodes > 8) return "nodes must be between 4 and 8 for V4.3 PBFT-style smoke.";
  if (!Number.isFinite(request.shards) || request.shards < 1 || request.shards > 4) return "shards must be between 1 and 4.";
  if (!Number.isFinite(request.tx_count) || request.tx_count < 1 || request.tx_count > 100) return "tx_count must be between 1 and 100.";
  if (!Number.isFinite(request.blockemulator_tx_limit) || request.blockemulator_tx_limit < 1 || request.blockemulator_tx_limit > 1000) return "blockemulator_tx_limit must be between 1 and 1000.";
  return "";
}

function TruthGrid({ title, description, values, keys, invert = false }: { title: string; description?: string; values: Record<string, unknown>; keys: string[]; invert?: boolean }) {
  return <article className="final-card wide">
    <h3>{title}</h3>
    {description && <p className="muted">{description}</p>}
    <div className="final-card-grid compact-grid">
      {keys.map((key) => {
        const value = Boolean(values[key]);
        const good = invert ? !value : value;
        return <div key={key} className="mini-status">
          <span className={good ? "status-dot ok" : "status-dot blocked"} />
          <strong>{key}</strong>
          <small>{String(value)}</small>
        </div>;
      })}
    </div>
  </article>;
}

function Metric({ label, value }: { label: string; value: unknown }) {
  return <div className="metric-card"><span>{label}</span><strong>{String(value ?? "-")}</strong></div>;
}
