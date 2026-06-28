import type { V3DraftSmokeRunResponse } from "../../api";

type Props = {
  result?: V3DraftSmokeRunResponse | null;
};

const summaryKeys = ["tx_count", "success_count", "failure_count", "failed_count", "avg_latency_ms", "p95_latency_ms", "p99_latency_ms"];

export default function DraftRunResultPanel({ result }: Props) {
  if (!result) return null;
  const normalized = result.validation?.normalized_draft || {};
  const selectedPlugins = readPluginSelection(normalized);
  const summary = result.summary || {};

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
      <p className="muted">Draft Smoke history is a local debugging, demo, and configuration tracing record. It is not a formal paper experiment result.</p>
    </section>
  );
}

function readPluginSelection(normalized: Record<string, unknown>): Record<string, string> {
  const selection = normalized.plugin_selection;
  if (!selection || typeof selection !== "object" || Array.isArray(selection)) return {};
  return Object.fromEntries(Object.entries(selection).map(([key, value]) => [key, String(value)]));
}
