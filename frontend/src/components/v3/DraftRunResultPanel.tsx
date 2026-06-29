import type { V3DraftSmokeRunResponse } from "../../api";

type Props = {
  result?: V3DraftSmokeRunResponse | null;
};

const summaryKeys = ["tx_count", "success_count", "failure_count", "failed_count", "avg_latency_ms", "p95_latency_ms", "p99_latency_ms"];
const txPoolSummaryKeys = ["queue_wait_ms", "txpool_avg_wait_ms", "txpool_p95_wait_ms", "txpool_peak_size", "txpool_admitted_count", "txpool_rejected_count"];
const blockProducerSummaryKeys = ["block_count", "avg_block_size", "max_block_size", "empty_block_count", "block_interval_ms", "avg_block_interval_ms", "blockproducer_count_cut_count", "blockproducer_time_cut_count", "blockproducer_drain_cut_count"];
const consensusSummaryKeys = ["avg_consensus_latency_ms", "p95_consensus_latency_ms", "consensus_message_count", "avg_consensus_message_count", "consensus_round_count", "finalized_block_count", "failed_block_count", "view_change_count"];

export default function DraftRunResultPanel({ result }: Props) {
  if (!result) return null;
  const normalized = result.validation?.normalized_draft || {};
  const selectedPlugins = readPluginSelection(normalized);
  const summary = result.summary || {};
  const artifactNames = new Set((result.artifacts || []).map((artifact) => artifact.name));

  return (
    <section className="final-card wide v3-draft-run-result">
      <div className="v3-section-head">
        <div>
          <p className="eyebrow">Draft Smoke result</p>
          <h3>Current Draft Smoke result</h3>
        </div>
        <span className={`v3-status-badge status-${result.validation?.is_runnable ? "variable" : "planned"}`}>
          {result.validation?.is_runnable ? "runnable" : "not runnable"}
        </span>
      </div>
      <dl className="v3-result-grid">
        <div><dt>run_id</dt><dd>{result.run_id}</dd></div>
        <div><dt>run_mode</dt><dd>{result.run_mode || "draft_smoke"}</dd></div>
        <div><dt>template</dt><dd>{String(normalized.template_id || "-")}</dd></div>
        <div><dt>validation</dt><dd>{result.validation?.is_valid ? "valid" : "invalid"}</dd></div>
      </dl>
      <div className="v3-plugin-summary">
        <strong>Actual plugin selection</strong>
        <ul>
          {Object.entries(selectedPlugins).map(([moduleId, plugin]) => (
            <li key={moduleId}><span>{moduleId}</span><code>{plugin}</code></li>
          ))}
        </ul>
      </div>
      <div className="v3-summary-preview">
        {summaryKeys.filter((key) => key in summary).map((key) => (
          <div key={key}><dt>{key}</dt><dd>{String(summary[key])}</dd></div>
        ))}
      </div>
      <div className="v3-summary-preview">
        {txPoolSummaryKeys.filter((key) => key in summary).map((key) => (
          <div key={key}><dt>{key}</dt><dd>{String(summary[key])}</dd></div>
        ))}
        <div><dt>txpool_log.csv</dt><dd>{artifactNames.has("txpool_log.csv") ? "available" : "missing"}</dd></div>
      </div>
      <div className="v3-summary-preview">
        {blockProducerSummaryKeys.filter((key) => key in summary).map((key) => (
          <div key={key}><dt>{key}</dt><dd>{String(summary[key])}</dd></div>
        ))}
        <div><dt>block_log.csv</dt><dd>{artifactNames.has("block_log.csv") ? "available" : "missing"}</dd></div>
      </div>
      <div className="v3-summary-preview">
        <div><dt>consensus_plugin</dt><dd>{selectedPlugins.Consensus || "N/A"}</dd></div>
        {consensusSummaryKeys.filter((key) => key in summary).map((key) => (
          <div key={key}><dt>{key}</dt><dd>{String(summary[key])}</dd></div>
        ))}
        <div><dt>consensus_log.csv</dt><dd>{artifactNames.has("consensus_log.csv") ? "available" : "missing"}</dd></div>
      </div>
      <p className="muted">Draft Smoke history is a local debugging, demo, and configuration tracing record. It is not a formal paper experiment result.</p>
    </section>
  );
}

function readPluginSelection(normalized: Record<string, unknown>): Record<string, string> {
  const selection = normalized.plugin_selection;
  if (!selection || typeof selection !== "object" || Array.isArray(selection)) return {};
  return Object.fromEntries(Object.entries(selection).map(([key, value]) => [key, String(value)]));
}
