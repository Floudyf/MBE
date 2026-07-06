import { useEffect, useState } from "react";

import { fetchV4RealismStatus, runV4RealismSmoke, v4RealismArtifactURL, type V4RealismArtifact, type V4RealismStatus, type V4RealismSmokeResponse } from "../../api";

const truthKeys = [
  "real_signed_tx",
  "per_node_mempool",
  "real_p2p",
  "pbft_style_consensus",
  "persistent_state_db",
  "state_root_from_real_state_updates",
  "real_cross_shard_state_machine",
  "recovery_supported",
  "fault_injection_supported",
  "frontend_realism_mode",
];

const nonClaims = ["production_pbft", "full_byzantine_security", "fabric_evm_backend", "production_blockchain"];

export default function RealismModePanel() {
  const [status, setStatus] = useState<V4RealismStatus | null>(null);
  const [result, setResult] = useState<V4RealismSmokeResponse | null>(null);
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
      setBusy(true);
      setResult(await runV4RealismSmoke());
      setError("");
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setBusy(false);
    }
  }

  const summary: Record<string, unknown> = result?.summary ?? status ?? {};
  const artifacts: V4RealismArtifact[] = result?.artifacts ?? [];

  return <section className="page-grid">
    <article className="final-card wide">
      <p className="eyebrow">V4 Realism Mode</p>
      <h2>Research-grade real multi-node sharded emulator</h2>
      <p>Runtime truth: {String(summary.runtime_truth ?? "v4_real_state_cross_shard_recovery")}</p>
      <div className="button-row">
        <button type="button" onClick={runSmoke} disabled={busy}>{busy ? "Running..." : "Run V4.2 Smoke"}</button>
        <button type="button" onClick={loadStatus}>Refresh Status</button>
      </div>
      {error && <p className="file-error">{error}</p>}
    </article>

    <TruthGrid title="Implemented Evidence" values={summary} keys={truthKeys} />
    <TruthGrid title="Non-Claims" values={summary} keys={nonClaims} invert />

    <article className="final-card wide">
      <h3>Latest Smoke Summary</h3>
      <div className="metric-grid">
        <Metric label="Committed height" value={summary["committed_height"]} />
        <Metric label="State root mismatches" value={summary["state_root_mismatch_count"]} />
        <Metric label="Cross-shard tx" value={summary["cross_shard_tx_count"]} />
        <Metric label="Ready to commit" value={summary["ready_to_commit"]} />
      </div>
    </article>

    <article className="final-card wide">
      <h3>Artifacts</h3>
      {artifacts.length === 0 ? <p>No smoke artifacts loaded yet.</p> : <div className="artifact-list">
        {artifacts.map((item) => <a key={item.name} href={v4RealismArtifactURL(item.download_url)}>{item.name} ({item.size_bytes} bytes)</a>)}
      </div>}
    </article>
  </section>;
}

function TruthGrid({ title, values, keys, invert = false }: { title: string; values: Record<string, unknown>; keys: string[]; invert?: boolean }) {
  return <article className="final-card wide">
    <h3>{title}</h3>
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
