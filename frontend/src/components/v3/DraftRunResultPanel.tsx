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
const nodeTopologySummaryKeys = ["shard_count", "validators_per_shard", "logical_node_count", "validator_node_count", "executor_node_count", "storage_node_count", "supervisor_node_count", "message_count", "network_message_count", "consensus_message_count", "node_event_count"];
const launcherPreviewSummaryKeys = ["launcher_mode", "launchable_node_count", "node_address_count", "launcher_script_count", "windows_launcher_available", "linux_launcher_available", "launcher_preview_only"];
const nodeProcessSummaryKeys = ["node_process_entrypoint_available", "node_process_preview_available", "node_process_status_available", "node_process_manifest_available", "node_process_preview_only"];
const networkAdapterSummaryKeys = ["network_adapter_selected", "tcp_preview_enabled", "tcp_listen_node_count", "tcp_send_count", "tcp_receive_count", "typed_message_count", "network_error_count"];
const consensusNetworkSummaryKeys = ["consensus_over_network_enabled", "consensus_runtime_selected", "proposal_preview_count", "vote_preview_count", "light_quorum_reached_count", "consensus_network_error_count", "consensus_network_path"];
const pbftPreviewSummaryKeys = ["consensus_runtime_selected", "pbft_preview_enabled", "pbft_view", "pbft_sequence", "pbft_preprepare_count", "pbft_prepare_count", "pbft_commit_count", "pbft_quorum_reached_count", "pbft_finalized_block_count", "pbft_consensus_latency_ms", "pbft_quorum_threshold"];
const pbftNetworkSummaryKeys = ["consensus_runtime_selected", "network_adapter_selected", "pbft_over_network_enabled", "pbft_network_path", "pbft_network_message_count", "pbft_network_error_count", "pbft_preprepare_network_count", "pbft_prepare_network_count", "pbft_commit_network_count", "pbft_finalized_network_count", "pbft_network_quorum_reached_count"];
const crossShardSummaryKeys = ["cross_shard_protocol_selected", "cross_shard_tx_count", "cross_shard_ratio", "cross_shard_message_count", "relay_preview_count", "cross_shard_completed_count", "cross_shard_failed_count", "cross_shard_avg_latency_ms"];
const stateAuthenticitySummaryKeys = ["state_backend_selected", "persistent_state_enabled", "state_root_enabled", "state_root_count", "state_key_count", "state_update_count", "state_proof_generated_count", "state_proof_verified_count", "state_proof_failed_count", "witness_generated_count", "witness_verified_count", "witness_failed_count", "state_authenticity_error_count"];

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
        <div><dt>Cross-shard summary</dt><dd>relay_preview skeleton only - not atomic commit - not full Relay/Broker/2PC</dd></div>
        {crossShardSummaryKeys.filter((key) => key in summary).map((key) => (
          <div key={key}><dt>{key}</dt><dd>{String(summary[key])}</dd></div>
        ))}
        <div><dt>cross_shard_tx_log.csv</dt><dd>{artifactNames.has("cross_shard_tx_log.csv") ? "available" : "legacy missing"}</dd></div>
        <div><dt>cross_shard_message_log.csv</dt><dd>{artifactNames.has("cross_shard_message_log.csv") ? "available" : "legacy missing"}</dd></div>
        <div><dt>relay_preview_log.csv</dt><dd>{artifactNames.has("relay_preview_log.csv") ? "available" : "legacy missing"}</dd></div>
        <div><dt>cross_shard_status.csv</dt><dd>{artifactNames.has("cross_shard_status.csv") ? "available" : "legacy missing"}</dd></div>
        <div><dt>cross_shard_summary.json</dt><dd>{artifactNames.has("cross_shard_summary.json") ? "available" : "legacy missing"}</dd></div>
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
        <div><dt>State Authenticity</dt><dd>MVP proof/witness artifacts only - not Ethereum-compatible MPT - not full stateless execution</dd></div>
        {stateAuthenticitySummaryKeys.filter((key) => key in summary).map((key) => (
          <div key={key}><dt>{key}</dt><dd>{String(summary[key])}</dd></div>
        ))}
        <div><dt>state_storage_log.csv</dt><dd>{artifactNames.has("state_storage_log.csv") ? "available" : "legacy missing"}</dd></div>
        <div><dt>state_version_log.csv</dt><dd>{artifactNames.has("state_version_log.csv") ? "available" : "legacy missing"}</dd></div>
        <div><dt>state_root_log.csv</dt><dd>{artifactNames.has("state_root_log.csv") ? "available" : "legacy missing"}</dd></div>
        <div><dt>state_proof_log.csv</dt><dd>{artifactNames.has("state_proof_log.csv") ? "available" : "legacy missing"}</dd></div>
        <div><dt>state_proof_verification_log.csv</dt><dd>{artifactNames.has("state_proof_verification_log.csv") ? "available" : "legacy missing"}</dd></div>
        <div><dt>witness_log.csv</dt><dd>{artifactNames.has("witness_log.csv") ? "available" : "legacy missing"}</dd></div>
        <div><dt>witness_verification_log.csv</dt><dd>{artifactNames.has("witness_verification_log.csv") ? "available" : "legacy missing"}</dd></div>
        <div><dt>state_authenticity_summary.json</dt><dd>{artifactNames.has("state_authenticity_summary.json") ? "available" : "legacy missing"}</dd></div>
      </div>
      <div className="v3-summary-preview">
        {commitSummaryKeys.filter((key) => key in summary).map((key) => (
          <div key={key}><dt>{key}</dt><dd>{String(summary[key])}</dd></div>
        ))}
        <div><dt>state_commit_log.csv</dt><dd>{artifactNames.has("state_commit_log.csv") ? "available" : "missing"}</dd></div>
      </div>
      <div className="v3-summary-preview">
        <div><dt>Node Topology / Logical Runtime</dt><dd>single-process logical nodes</dd></div>
        {nodeTopologySummaryKeys.filter((key) => key in summary).map((key) => (
          <div key={key}><dt>{key}</dt><dd>{String(summary[key])}</dd></div>
        ))}
        <div><dt>node_topology.csv</dt><dd>{artifactNames.has("node_topology.csv") ? "available" : "missing"}</dd></div>
        <div><dt>node_log.csv</dt><dd>{artifactNames.has("node_log.csv") ? "available" : "missing"}</dd></div>
        <div><dt>network_log.csv</dt><dd>{artifactNames.has("network_log.csv") ? "available" : "missing"}</dd></div>
        <div><dt>consensus_message_log.csv</dt><dd>{artifactNames.has("consensus_message_log.csv") ? "available" : "missing"}</dd></div>
      </div>
      <div className="v3-summary-preview">
        <div><dt>Launcher Preview</dt><dd>preview only · not real TCP · not real PBFT · not BlockEmulator backend</dd></div>
        {launcherPreviewSummaryKeys.filter((key) => key in summary).map((key) => (
          <div key={key}><dt>{key}</dt><dd>{String(summary[key])}</dd></div>
        ))}
        <div><dt>node_address_table.csv</dt><dd>{artifactNames.has("node_address_table.csv") ? "available" : "missing"}</dd></div>
        <div><dt>topology.json</dt><dd>{artifactNames.has("topology.json") ? "available" : "missing"}</dd></div>
        <div><dt>launch_nodes_windows.bat</dt><dd>{artifactNames.has("launch_nodes_windows.bat") ? "available" : "missing"}</dd></div>
        <div><dt>launch_nodes_linux.sh</dt><dd>{artifactNames.has("launch_nodes_linux.sh") ? "available" : "missing"}</dd></div>
        <div><dt>launcher_readme.md</dt><dd>{artifactNames.has("launcher_readme.md") ? "available" : "missing"}</dd></div>
      </div>
      <div className="v3-summary-preview">
        <div><dt>Node Process Preview</dt><dd>local preview only - not production networking - not real PBFT</dd></div>
        {nodeProcessSummaryKeys.filter((key) => key in summary).map((key) => (
          <div key={key}><dt>{key}</dt><dd>{String(summary[key])}</dd></div>
        ))}
        <div><dt>node_process_status.csv</dt><dd>{artifactNames.has("node_process_status.csv") ? "available" : "missing"}</dd></div>
        <div><dt>node_process_manifest.json</dt><dd>{artifactNames.has("node_process_manifest.json") ? "available" : "missing"}</dd></div>
        <div><dt>node_process_log_sample.log</dt><dd>{artifactNames.has("node_process_log_sample.log") ? "available" : "missing"}</dd></div>
      </div>
      <div className="v3-summary-preview">
        <div><dt>NetworkAdapter</dt><dd>localhost TCP typed message preview only - not real PBFT - not production network</dd></div>
        {networkAdapterSummaryKeys.filter((key) => key in summary).map((key) => (
          <div key={key}><dt>{key}</dt><dd>{String(summary[key])}</dd></div>
        ))}
        <div><dt>tcp_adapter_status.csv</dt><dd>{artifactNames.has("tcp_adapter_status.csv") ? "available" : "legacy missing"}</dd></div>
        <div><dt>network_send_log.csv</dt><dd>{artifactNames.has("network_send_log.csv") ? "available" : "legacy missing"}</dd></div>
        <div><dt>network_receive_log.csv</dt><dd>{artifactNames.has("network_receive_log.csv") ? "available" : "legacy missing"}</dd></div>
        <div><dt>typed_message_log.csv</dt><dd>{artifactNames.has("typed_message_log.csv") ? "available" : "legacy missing"}</dd></div>
      </div>
      <div className="v3-summary-preview">
        <div><dt>Consensus-light over NetworkAdapter</dt><dd>proposal/vote preview only - not PBFT - not BlockEmulator-aligned PBFT</dd></div>
        {consensusNetworkSummaryKeys.filter((key) => key in summary).map((key) => (
          <div key={key}><dt>{key}</dt><dd>{String(summary[key])}</dd></div>
        ))}
        <div><dt>consensus_network_light_log.csv</dt><dd>{artifactNames.has("consensus_network_light_log.csv") ? "available" : "legacy missing"}</dd></div>
        <div><dt>network_consensus_summary.json</dt><dd>{artifactNames.has("network_consensus_summary.json") ? "available" : "legacy missing"}</dd></div>
      </div>
      <div className="v3-summary-preview">
        <div><dt>PBFT state machine preview</dt><dd>optional ConsensusRuntime preview only - not production PBFT - network path shown below</dd></div>
        {pbftPreviewSummaryKeys.filter((key) => key in summary).map((key) => (
          <div key={key}><dt>{key}</dt><dd>{String(summary[key])}</dd></div>
        ))}
        <div><dt>pbft_state_log.csv</dt><dd>{artifactNames.has("pbft_state_log.csv") ? "available" : "legacy missing"}</dd></div>
        <div><dt>pbft_message_log.csv</dt><dd>{artifactNames.has("pbft_message_log.csv") ? "available" : "legacy missing"}</dd></div>
        <div><dt>quorum_log.csv</dt><dd>{artifactNames.has("quorum_log.csv") ? "available" : "legacy missing"}</dd></div>
        <div><dt>finalized_block_log.csv</dt><dd>{artifactNames.has("finalized_block_log.csv") ? "available" : "legacy missing"}</dd></div>
      </div>
      <div className="v3-summary-preview">
        <div><dt>PBFT over NetworkAdapter</dt><dd>BlockEmulator-aligned preview only - not production PBFT - not BlockEmulator backend</dd></div>
        {pbftNetworkSummaryKeys.filter((key) => key in summary).map((key) => (
          <div key={key}><dt>{key}</dt><dd>{String(summary[key])}</dd></div>
        ))}
        <div><dt>consensus_network_log.csv</dt><dd>{artifactNames.has("consensus_network_log.csv") ? "available" : "legacy missing"}</dd></div>
        <div><dt>pbft_network_summary.json</dt><dd>{artifactNames.has("pbft_network_summary.json") ? "available" : "legacy missing"}</dd></div>
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
