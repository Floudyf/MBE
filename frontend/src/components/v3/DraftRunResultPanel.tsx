import type { V3DraftSmokeRunResponse } from "../../api";

type Props = {
  result?: V3DraftSmokeRunResponse | null;
};

const summaryKeys = ["tx_count", "success_count", "failure_count", "failed_count", "avg_latency_ms", "p95_latency_ms", "p99_latency_ms"];
const txPoolSummaryKeys = ["queue_wait_ms", "txpool_avg_wait_ms", "txpool_p95_wait_ms", "txpool_peak_size", "txpool_admitted_count", "txpool_rejected_count"];
const blockProducerSummaryKeys = ["block_count", "avg_block_size", "max_block_size", "empty_block_count", "block_interval_ms", "avg_block_interval_ms", "blockproducer_count_cut_count", "blockproducer_time_cut_count", "blockproducer_drain_cut_count"];
const consensusSummaryKeys = ["avg_consensus_latency_ms", "p95_consensus_latency_ms", "consensus_message_count", "avg_consensus_message_count", "consensus_round_count", "finalized_block_count", "failed_block_count", "view_change_count"];
const routingSummaryKeys = ["routing_plugin", "routing_decision_count", "cross_shard_tx_count", "cross_shard_ratio", "local_tx_count", "remote_state_access_count", "avg_touched_shards", "max_touched_shards", "hotspot_key_count", "coaccess_group_count", "avg_routing_overhead_ms"];
const executionSummaryKeys = ["execution_plugin", "execution_tx_count", "fast_track_count", "conservative_track_count", "blocked_tx_count", "dependency_edge_count", "avg_dependency_edges_per_tx", "avg_execution_latency_ms", "p95_execution_latency_ms", "max_execution_latency_ms", "logical_worker_count", "parallelizable_tx_count", "serial_tx_count"];
const stateAccessSummaryKeys = ["state_access_plugin", "state_access_count", "local_state_access_count", "remote_state_access_count", "remote_state_access_ratio", "cache_hit_count", "cache_miss_count", "cache_hit_rate", "prefetch_hit_count", "prefetch_miss_count", "prefetch_hit_rate", "avg_state_access_latency_ms", "p95_state_access_latency_ms", "max_state_access_latency_ms", "remote_state_access_latency_ms", "witness_estimated_count", "proof_estimated_count", "estimated_witness_bytes", "estimated_proof_bytes"];
const commitSummaryKeys = ["commit_plugin", "commit_tx_count", "commit_update_count", "normal_commit_count", "conservative_commit_count", "hotspot_update_count", "aggregated_update_count", "raw_update_count", "aggregation_group_count", "aggregation_ratio", "constraint_check_count", "constraint_passed_count", "constraint_failed_count", "avg_commit_latency_ms", "p95_commit_latency_ms", "max_commit_latency_ms"];

export default function DraftRunResultPanel({ result }: Props) {
  if (!result) return null;
  const normalized = result.validation?.normalized_draft || {};
  const selectedPlugins = readPluginSelection(normalized);
  const summary = result.summary || {};
  const artifactNames = new Set((result.artifacts || []).map((artifact) => artifact.name));
  const lockedModules = readStringMap(summary.locked_modules || normalized.locked_modules);
  const primaryMetrics = readStringArray(summary.primary_metrics || normalized.primary_metrics);
  const expectedArtifacts = readStringArray(summary.expected_artifacts || normalized.expected_artifacts);
  const enabledComponents = readStringArray(summary.enabled_metatrack_components || normalized.enabled_metatrack_components);
  const resultGuide = String(summary.result_guide || normalized.result_guide || "");
  const truthfulnessNote = String(summary.truthfulness_note || normalized.truthfulness_note || "");

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
        <div><dt>template</dt><dd>{String(summary.experiment_template || normalized.experiment_template || normalized.template_id || "-")}</dd></div>
        <div><dt>preset_id</dt><dd>{String(summary.preset_id || normalized.preset_id || "legacy/default smoke")}</dd></div>
        <div><dt>preset_name</dt><dd>{String(summary.preset_name || normalized.preset_name || "-")}</dd></div>
        <div><dt>ablation_stage</dt><dd>{String(summary.ablation_stage || normalized.ablation_stage || "-")}</dd></div>
        <div><dt>enabled_components</dt><dd>{enabledComponents.length ? enabledComponents.join(", ") : "baseline / none"}</dd></div>
        <div><dt>variable_module</dt><dd>{String(summary.variable_module || normalized.variable_module || "-")}</dd></div>
        <div><dt>fairness_validated</dt><dd>{String(summary.fairness_validated ?? normalized.fairness_validated ?? "false")}</dd></div>
        <div><dt>validation</dt><dd>{result.validation?.is_valid ? "valid" : "invalid"}</dd></div>
      </dl>
      {(resultGuide || primaryMetrics.length > 0 || expectedArtifacts.length > 0 || truthfulnessNote) && (
        <div className="v3-plugin-summary">
          <strong>Template Result Guide</strong>
          {resultGuide && <p className="muted">{resultGuide}</p>}
          {primaryMetrics.length > 0 && (
            <ul>
              {primaryMetrics.map((metric) => <li key={metric}><span>metric</span><code>{metric}</code></li>)}
            </ul>
          )}
          {expectedArtifacts.length > 0 && (
            <ul>
              {expectedArtifacts.map((artifact) => (
                <li key={artifact}><span>{artifactNames.has(artifact) ? "available" : "legacy missing"}</span><code>{artifact}</code></li>
              ))}
            </ul>
          )}
          {truthfulnessNote && <p className="muted">{truthfulnessNote}</p>}
        </div>
      )}
      {Object.keys(lockedModules).length > 0 && (
        <div className="v3-plugin-summary">
          <strong>Locked modules</strong>
          <ul>
            {Object.entries(lockedModules).map(([moduleId, plugin]) => (
              <li key={moduleId}><span>{moduleId}</span><code>{plugin}</code></li>
            ))}
          </ul>
        </div>
      )}
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
      <div className="v3-summary-preview">
        {routingSummaryKeys.filter((key) => key in summary).map((key) => (
          <div key={key}><dt>{key}</dt><dd>{String(summary[key])}</dd></div>
        ))}
        <div><dt>routing_log.csv</dt><dd>{artifactNames.has("routing_log.csv") ? "available" : "missing"}</dd></div>
      </div>
      <div className="v3-summary-preview">
        {executionSummaryKeys.filter((key) => key in summary).map((key) => (
          <div key={key}><dt>{key}</dt><dd>{String(summary[key])}</dd></div>
        ))}
        <div><dt>execution_log.csv</dt><dd>{artifactNames.has("execution_log.csv") ? "available" : "missing"}</dd></div>
      </div>
      <div className="v3-summary-preview">
        {stateAccessSummaryKeys.filter((key) => key in summary).map((key) => (
          <div key={key}><dt>{key}</dt><dd>{String(summary[key])}</dd></div>
        ))}
        <div><dt>state_access_log.csv</dt><dd>{artifactNames.has("state_access_log.csv") ? "available" : "missing"}</dd></div>
      </div>
      <div className="v3-summary-preview">
        {commitSummaryKeys.filter((key) => key in summary).map((key) => (
          <div key={key}><dt>{key}</dt><dd>{String(summary[key])}</dd></div>
        ))}
        <div><dt>state_commit_log.csv</dt><dd>{artifactNames.has("state_commit_log.csv") ? "available" : "missing"}</dd></div>
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

function readStringMap(value: unknown): Record<string, string> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, String(item)]));
}

function readStringArray(value: unknown): string[] {
  if (Array.isArray(value)) return value.map(String);
  if (typeof value === "string" && value.trim()) return value.split(",").map((item) => item.trim()).filter(Boolean);
  return [];
}
