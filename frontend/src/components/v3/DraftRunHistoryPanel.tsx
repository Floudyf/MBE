import { useState } from "react";

import { fetchV3DraftRunDetail, fetchV3DraftRuns, type V3DraftRunDetail, type V3DraftRunSummary } from "../../api";
import ArtifactGroups from "./ArtifactGroups";

const summaryKeys = ["tx_count", "success_count", "failure_count", "failed_count", "avg_latency_ms", "p95_latency_ms", "p99_latency_ms", "queue_wait_ms", "txpool_avg_wait_ms", "txpool_p95_wait_ms", "txpool_peak_size", "txpool_admitted_count", "txpool_rejected_count", "block_count", "avg_block_size", "empty_block_count", "avg_block_interval_ms", "blockproducer_count_cut_count", "blockproducer_time_cut_count", "blockproducer_drain_cut_count", "avg_consensus_latency_ms", "p95_consensus_latency_ms", "consensus_message_count", "avg_consensus_message_count", "consensus_round_count", "finalized_block_count", "failed_block_count", "view_change_count", "experiment_template", "preset_id", "preset_name", "variable_module", "fairness_validated"];

export default function DraftRunHistoryPanel() {
  const [runs, setRuns] = useState<V3DraftRunSummary[]>([]);
  const [selected, setSelected] = useState<V3DraftRunDetail | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  async function refresh() {
    try {
      setLoading(true);
      const nextRuns = await fetchV3DraftRuns(20);
      setRuns(nextRuns);
      setError("");
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setLoading(false);
    }
  }

  async function openRun(runId: string) {
    try {
      setLoading(true);
      const detail = await fetchV3DraftRunDetail(runId);
      setSelected(detail);
      setError("");
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setLoading(false);
    }
  }

  return (
    <details className="final-card wide v3-foldout v3-draft-history">
      <summary className="v3-foldout-summary">
        <span>Recent Draft Smoke runs</span>
        <small>Local .cache history only, not a formal result database</small>
      </summary>
      <div className="v3-history-toolbar">
        <p className="muted">Draft Smoke history is for local debugging, demos, and configuration tracing. It does not represent paper experiment evidence.</p>
        <button type="button" className="v3-secondary-button" disabled={loading} onClick={refresh}>
          {loading ? "Loading..." : "Refresh history"}
        </button>
      </div>
      {error && <p className="file-error">{error}</p>}
      {runs.length === 0 && <p className="muted">No Draft Smoke runs loaded yet. Click refresh after running a Draft Smoke.</p>}
      {runs.length > 0 && (
        <div className="v3-history-list">
          {runs.map((run) => (
            <button key={run.run_id} type="button" className="v3-history-row" onClick={() => openRun(run.run_id)}>
              <span>
                <strong>{run.run_id}</strong>
                <small>{run.created_at}</small>
              </span>
              <span>{run.template_id}</span>
              <span>{run.preset_id || String(run.summary_preview?.preset_id || "legacy/default smoke")}</span>
              <span>{run.variable_module || String(run.summary_preview?.variable_module || "template legacy")}</span>
              <span>{pluginSummary(run.selected_plugins)}</span>
              <b>{run.artifact_count} artifacts</b>
            </button>
          ))}
        </div>
      )}
      {selected && (
        <div className="v3-history-detail">
          <div className="v3-section-head">
            <div>
              <p className="eyebrow">Draft run detail</p>
              <h3>{selected.run_id}</h3>
            </div>
            <span className={`v3-status-badge status-${selected.validation?.is_runnable ? "variable" : "planned"}`}>
              {selected.validation?.is_runnable ? "runnable" : "not runnable"}
            </span>
          </div>
          <div className="v3-summary-preview">
            {summaryKeys.filter((key) => key in selected.summary_preview).map((key) => (
              <div key={key}><dt>{key}</dt><dd>{String(selected.summary_preview[key])}</dd></div>
            ))}
          </div>
          {selected.missing_files.length > 0 && (
            <p className="file-error">Missing files: {selected.missing_files.join(", ")}</p>
          )}
          <ArtifactGroups
            artifacts={selected.artifacts}
            title="Selected Draft run artifacts"
            emptyMessage="This historical run has no downloadable artifacts."
            expectedArtifacts={readStringArray(selected.summary_preview.expected_artifacts)}
            defaultOpen
            embedded
          />
        </div>
      )}
    </details>
  );
}

function readStringArray(value: unknown): string[] {
  if (Array.isArray(value)) return value.map(String);
  if (typeof value === "string" && value.trim()) return value.split(",").map((item) => item.trim()).filter(Boolean);
  return [];
}

function pluginSummary(plugins: Record<string, string>): string {
  const keys = ["Consensus", "Routing", "Execution", "StateAccess", "Commit"];
  const pairs = keys.filter((key) => plugins[key]).map((key) => `${key}:${plugins[key]}`);
  return pairs.length ? pairs.join(" / ") : "no plugin summary";
}
