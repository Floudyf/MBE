const configuredBaseURL = import.meta.env.VITE_API_BASE_URL;

// In development, Vite proxies relative requests to this default local backend.
export const API_BASE_URL = configuredBaseURL ?? "http://127.0.0.1:8000";
const requestBaseURL = configuredBaseURL ?? "";

const experimentPath = "/api/v0/experiments/v0_default_asset_hotspot";

export type Summary = Record<string, string>;
export type V1Template = { name: string; stage: string; runnable: boolean; description: string };
export type V1Experiment = { id: string; stage: string; runnable: boolean; implemented: boolean; description: string; template: string };
export type V1StageStatus = { id: string; label: string; status: string };
export type V1Status = { phase: string; scope: string; stages: V1StageStatus[]; boundaries: Record<string, string> };
export type V1SweepRow = Record<string, string | number | boolean | null | undefined> & { name: string };
export type V1SweepSummary = { status: string; message?: string; output_dir?: string; rows: V1SweepRow[] };
export type V1SweepReport = { status: string; message?: string; path?: string; content: string };
export type V1SweepFile = { name: string; download_url: string; size_bytes: number };
export type V1SweepFiles = { status: string; output_dir: string; files: V1SweepFile[] };
export type V1SweepRunResult = { status: string; output_dir: string; stdout: string; stderr: string; files: V1SweepFile[] };
export type V1WorkloadOption = { id: string; label: string; description: string; source_type: string; supported_params: string[]; limitations: string[] };
export type V1AblationPreset = { id: string; routing_policy: string; dual_track_enabled: boolean; hot_update_aggregation_enabled: boolean; description: string };
export type V1FabricTraceStatus = { status: string; ready: boolean; output_dir: string; files: Record<string, { path: string; exists: boolean }>; message: string; cli_command: string; limitations: string[] };
export type V1CustomRunRequest = {
  workload: string;
  source_type: string;
  tx_count: number;
  seed: number;
  hot_tx_ratio: number;
  conflict_injection_ratio: number;
  commutative_update_ratio: number;
  access_set_size: number;
  multi_hotspot_count: number;
  arrival_rate: number;
  burst_rate: number;
  routing_policy: string;
  dual_track_enabled: boolean;
  hot_update_aggregation_enabled: boolean;
  preset: string;
  trace_path?: string;
};
export type V1CustomRunResult = { run_id: string; status: string; output_dir: string; source_type: string; truth_label: string; summary: V1SweepRow; files: V1SweepFile[]; stdout?: string };
export type V1CustomRunSummary = { status: string; message?: string; summary: V1SweepRow; source_type: string; truth_label: string; output_dir?: string };
export type V2Status = "runnable" | "planned" | "experimental" | "invalid" | "completed" | "failed" | "running" | "created" | "blocked" | string;
export type V2Artifact = { name: string; download_url: string; size_bytes: number };
export type V2TraceSource = {
  id: string;
  label: string;
  status: V2Status;
  maturity?: string;
  data_truth_label: string;
  description: string;
  entry_mode: string[];
  capabilities: Record<string, unknown>;
  limitations: string[];
  validation?: Record<string, unknown>;
  compatible_topologies?: string[];
  notes?: string[];
};
export type V2TraceSourceValidationResult = {
  source_id: string;
  status: V2Status;
  runnable: boolean;
  data_truth_label: string;
  trace_path?: string;
  capabilities?: Record<string, unknown>;
  limitations?: string[];
  warnings: string[];
  blocked_by: string[];
  ready?: boolean;
  meta_detected?: boolean;
  cli_command?: string;
};
export type V2ChainBackend = {
  backend_type: string;
  status: V2Status;
  supports_submit: boolean;
  supports_finality_observation: boolean;
  supports_event_listener: boolean;
  supports_real_time: boolean;
  supports_replay: boolean;
  supports_virtual_time: boolean;
  data_truth_label: string;
  limitations: string[];
};
export type V2ComposerPreviewResult = {
  status: V2Status;
  runnable: boolean;
  stage: string;
  topology: string;
  selected_plugins: unknown[];
  resolved_components: unknown[];
  data_truth_label: string;
  reasons: string[];
  warnings: string[];
  blocked_by: string[];
};
export type V2RunSummary = Record<string, unknown> & {
  run_id: string;
  stage: string;
  source: string;
  status: V2Status;
  created_at: string;
  updated_at: string;
  data_truth_label: string;
  artifact_count: number;
  summary_available: boolean;
  report_available: boolean;
};
export type V2ArtifactsResponse = { run_id: string; status: string; artifacts: V2Artifact[]; run: V2RunSummary };
export type V2DualChainReplayResponse = { run_id: string; status: V2Status; stage: string; output_dir: string; data_truth_label: string; summary: Record<string, unknown>; artifacts: V2Artifact[] };
export type V2ProtocolInfo = { name: string; status: V2Status; maturity: string; reason: string };
export type V2ProtocolReplayResponse = { run_id: string; status: V2Status; stage: string; output_dir: string; data_truth_label: string; protocol_truth?: string; summary: { items: Record<string, unknown>[] }; artifacts: V2Artifact[] };
export type V2SampleConfig = { path: string; config: Record<string, unknown> };
export type V2SweepInfo = { id: string; name: string; status: V2Status; stage: string; data_truth_label: string; backend_type: string; protocol_truth: string; description: string; parameters: Record<string, unknown>; protocols: string[]; limitations: string[] };
export type V2SweepRunResponse = { run_id: string; status: V2Status; stage: string; output_dir: string; data_truth_label: string; backend_type: string; protocol_truth: string; summary: Record<string, unknown>; rows: Record<string, unknown>[]; artifacts: V2Artifact[] };
export type V2CalibrationInfo = { id: string; name: string; status: V2Status; stage: string; data_truth_label: string; backend_type: string; calibration_truth: string; description: string; source_type: string; limitations: string[] };
export type V2FabricSmokeStatus = { status: string; ready?: boolean; trace_path: string; meta_path: string; data_truth_label: string; web_starts_fabric: boolean; cli_command: string; warnings: string[]; reason?: string };
export type V2CalibrationRunResponse = { run_id?: string; status: V2Status; stage?: string; output_dir?: string; data_truth_label?: string; backend_type?: string; calibration_truth?: string; summary?: Record<string, unknown>; artifacts?: V2Artifact[]; reason?: string; cli_command?: string; warnings?: string[] };
export type V3ModuleStatus = "default" | "fixed" | "variable" | "disabled" | "planned" | "output" | string;
export type V3ComposerModule = {
  module_id: string;
  display_name: string;
  plugin?: string;
  status: V3ModuleStatus;
  role?: string;
  tags?: string[];
  position: number;
  allowed_plugins?: string[];
  metrics?: string[];
  artifacts?: string[];
};
export type V3ComposerEdge = { source: string; target: string };
export type V3PluginMatrixRow = { method_id: string; label?: string; role?: string; module_plugins: Record<string, string>; tags?: string[] };
export type V3FairnessScope = {
  template_id?: string;
  variable_modules?: string[];
  fixed_modules?: string[];
  disabled_modules?: string[];
  planned_modules?: string[];
  output_modules?: string[];
  only_variable_modules_may_differ?: boolean;
  fixed_modules_must_match?: boolean;
  planned_modules_not_runnable?: boolean;
  [key: string]: unknown;
};
export type V3ComposerPreview = {
  view: string;
  template_id: string;
  chain_mode: string;
  modules: V3ComposerModule[];
  edges: V3ComposerEdge[];
  plugin_matrix: V3PluginMatrixRow[];
  fairness_scope: V3FairnessScope;
  truth_labels?: Record<string, string>;
  runnable: boolean;
};
export type V3ComposerPreviewResponse = {
  experiment_profile_id: string;
  stage: string;
  current_stage?: string;
  latest_runtime_stage?: string;
  latest_completed_runtime_stage?: string;
  current_capability?: string;
  closure_stage?: string;
  runtime_truth?: string;
  next_stage?: string;
  profile_preview: Record<string, unknown>;
  composer_preview: V3ComposerPreview;
  experiment_template: string;
  module_graph: { modules: V3ComposerModule[]; edges: V3ComposerEdge[] };
  plugin_matrix: V3PluginMatrixRow[];
  fairness_scope: V3FairnessScope;
  runnable: boolean;
};
export type V3TemplateSummary = {
  template_id: string;
  template_name?: string;
  stage: string;
  chain_mode: string;
  runnable: boolean;
  preview_only: boolean;
  status?: string;
  description: string;
  variable_module?: string;
  allowed_variable_plugins?: string[];
  locked_modules?: Record<string, string>;
  fairness_rule?: string;
  truthfulness_note?: string;
  default_preset_id?: string;
  presets?: V3TemplatePreset[];
  variable_modules: string[];
  fixed_modules: string[];
  disabled_modules: string[];
  planned_modules: string[];
  output_modules: string[];
};
export type V3TemplatePreset = {
  preset_id: string;
  preset_name?: string;
  description?: string;
  default_chain_profile?: string;
  default_plugin_selection?: Record<string, string>;
  ablation_stage?: string;
  enabled_metatrack_components?: string[];
  controlled_modules?: string[];
  variable_module?: string;
  locked_modules?: Record<string, string>;
  primary_metrics?: string[];
  secondary_metrics?: string[];
  expected_artifacts?: string[];
  result_guide?: string;
  truthfulness_note?: string;
};
export type V3RuntimeSummary = Record<string, unknown> & {
  experiment_template?: string;
  preset_id?: string;
  preset_name?: string;
  ablation_stage?: string;
  enabled_metatrack_components?: string[] | string;
  controlled_modules?: string[] | string;
  variable_module?: string;
  locked_modules?: Record<string, string> | string;
  fairness_scope?: Record<string, unknown>;
  fairness_validated?: boolean | string;
  primary_metrics?: string[] | string;
  expected_artifacts?: string[] | string;
  result_guide?: string;
  truthfulness_note?: string;
  consensus_latency_ms?: number | string;
  avg_consensus_latency_ms?: number | string;
  p95_consensus_latency_ms?: number | string;
  consensus_message_count?: number | string;
  avg_consensus_message_count?: number | string;
  consensus_round_count?: number | string;
  view_change_count?: number | string;
  finalized_block_count?: number | string;
  failed_block_count?: number | string;
  routing_plugin?: string;
  routing_decision_count?: number | string;
  cross_shard_tx_count?: number | string;
  local_tx_count?: number | string;
  remote_state_access_count?: number | string;
  avg_touched_shards?: number | string;
  max_touched_shards?: number | string;
  hotspot_key_count?: number | string;
  coaccess_group_count?: number | string;
  avg_routing_overhead_ms?: number | string;
  execution_plugin?: string;
  execution_tx_count?: number | string;
  fast_track_count?: number | string;
  conservative_track_count?: number | string;
  blocked_tx_count?: number | string;
  dependency_edge_count?: number | string;
  avg_dependency_edges_per_tx?: number | string;
  avg_execution_latency_ms?: number | string;
  p95_execution_latency_ms?: number | string;
  max_execution_latency_ms?: number | string;
  logical_worker_count?: number | string;
  parallelizable_tx_count?: number | string;
  serial_tx_count?: number | string;
  state_access_plugin?: string;
  state_access_count?: number | string;
  local_state_access_count?: number | string;
  remote_state_access_ratio?: number | string;
  cache_hit_count?: number | string;
  cache_miss_count?: number | string;
  cache_hit_rate?: number | string;
  prefetch_hit_count?: number | string;
  prefetch_miss_count?: number | string;
  prefetch_hit_rate?: number | string;
  avg_state_access_latency_ms?: number | string;
  p95_state_access_latency_ms?: number | string;
  max_state_access_latency_ms?: number | string;
  remote_state_access_latency_ms?: number | string;
  witness_estimated_count?: number | string;
  proof_estimated_count?: number | string;
  estimated_witness_bytes?: number | string;
  estimated_proof_bytes?: number | string;
  commit_plugin?: string;
  commit_tx_count?: number | string;
  commit_update_count?: number | string;
  normal_commit_count?: number | string;
  conservative_commit_count?: number | string;
  hotspot_update_count?: number | string;
  aggregated_update_count?: number | string;
  raw_update_count?: number | string;
  aggregation_group_count?: number | string;
  aggregation_ratio?: number | string;
  constraint_check_count?: number | string;
  constraint_passed_count?: number | string;
  constraint_failed_count?: number | string;
  avg_commit_latency_ms?: number | string;
  p95_commit_latency_ms?: number | string;
  max_commit_latency_ms?: number | string;
  shard_count?: number | string;
  validators_per_shard?: number | string;
  logical_node_count?: number | string;
  validator_node_count?: number | string;
  executor_node_count?: number | string;
  storage_node_count?: number | string;
  supervisor_node_count?: number | string;
  message_count?: number | string;
  network_message_count?: number | string;
  node_event_count?: number | string;
  launcher_mode?: string;
  launcher_script_count?: number | string;
  launchable_node_count?: number | string;
  node_address_count?: number | string;
  windows_launcher_available?: boolean | string;
  linux_launcher_available?: boolean | string;
  launcher_preview_only?: boolean | string;
  node_process_entrypoint_available?: boolean | string;
  node_process_preview_available?: boolean | string;
  node_process_status_available?: boolean | string;
  node_process_manifest_available?: boolean | string;
  node_process_preview_only?: boolean | string;
  network_adapter_selected?: string;
  tcp_preview_enabled?: boolean | string;
  tcp_listen_node_count?: number | string;
  tcp_send_count?: number | string;
  tcp_receive_count?: number | string;
  typed_message_count?: number | string;
  network_error_count?: number | string;
  consensus_over_network_enabled?: boolean | string;
  consensus_runtime_selected?: string;
  proposal_preview_count?: number | string;
  vote_preview_count?: number | string;
  light_quorum_reached_count?: number | string;
  consensus_network_error_count?: number | string;
  consensus_network_path?: string;
  pbft_view?: number | string;
  pbft_sequence?: number | string;
  pbft_preprepare_count?: number | string;
  pbft_prepare_count?: number | string;
  pbft_commit_count?: number | string;
  pbft_quorum_reached_count?: number | string;
  pbft_finalized_block_count?: number | string;
  pbft_consensus_latency_ms?: number | string;
  pbft_preview_enabled?: boolean | string;
  pbft_quorum_threshold?: number | string;
  pbft_over_network_enabled?: boolean | string;
  pbft_network_path?: string;
  pbft_network_message_count?: number | string;
  pbft_network_error_count?: number | string;
  pbft_preprepare_network_count?: number | string;
  pbft_prepare_network_count?: number | string;
  pbft_commit_network_count?: number | string;
  pbft_finalized_network_count?: number | string;
  pbft_network_quorum_reached_count?: number | string;
  cross_shard_protocol_selected?: string;
  cross_shard_message_count?: number | string;
  relay_preview_count?: number | string;
  relay_mvp_enabled?: boolean | string;
  relay_mvp_tx_count?: number | string;
  relay_source_lock_count?: number | string;
  relay_certificate_count?: number | string;
  relay_proof_verified_count?: number | string;
  relay_proof_failed_count?: number | string;
  relay_target_verified_count?: number | string;
  relay_target_commit_count?: number | string;
  relay_source_finalized_count?: number | string;
  relay_timeout_count?: number | string;
  relay_refund_count?: number | string;
  relay_abort_count?: number | string;
  relay_success_count?: number | string;
  relay_failed_count?: number | string;
  relay_avg_latency_ms?: number | string;
  relay_mvp_truth?: string;
  cross_shard_completed_count?: number | string;
  cross_shard_failed_count?: number | string;
  cross_shard_avg_latency_ms?: number | string;
  state_backend_selected?: string;
  persistent_state_enabled?: boolean | string;
  state_root_enabled?: boolean | string;
  state_root_count?: number | string;
  state_key_count?: number | string;
  state_update_count?: number | string;
  state_proof_generated_count?: number | string;
  state_proof_verified_count?: number | string;
  state_proof_failed_count?: number | string;
  witness_generated_count?: number | string;
  witness_verified_count?: number | string;
  witness_failed_count?: number | string;
  state_authenticity_error_count?: number | string;
  benchmark_template_selected?: string;
  baseline_profile_selected?: string;
  benchmark_run_count?: number | string;
  sweep_parameter_count?: number | string;
  repeat_count?: number | string;
  benchmark_artifact_count?: number | string;
  baseline_comparison_count?: number | string;
  reproducibility_manifest_available?: boolean | string;
  benchmark_report_available?: boolean | string;
  paper_grade_benchmark?: boolean | string;
  node_runtime_mode?: string;
  process_runtime_mode?: string;
  local_multi_process_enabled?: boolean | string;
  planned_process_count?: number | string;
  started_process_count?: number | string;
  stopped_process_count?: number | string;
  failed_process_count?: number | string;
  capped_process_count?: number | string;
  max_local_processes?: number | string;
  network_path_truth?: string;
  committee_count?: number | string;
  epoch_count?: number | string;
  reconfiguration_event_count?: number | string;
  committee_epoch_enabled?: boolean | string;
  committee_epoch_truth?: string;
  runtime_realism_truth?: string;
  metaverse_suite_enabled?: boolean | string;
  metaverse_scenario_selected?: string;
  metaverse_tx_count?: number | string;
  metaverse_user_count?: number | string;
  metaverse_asset_count?: number | string;
  metaverse_item_count?: number | string;
  metaverse_avatar_count?: number | string;
  metaverse_scene_count?: number | string;
  metaverse_count?: number | string;
  metaverse_hotspot_ratio?: number | string;
  metaverse_cross_scene_ratio?: number | string;
  metaverse_cross_shard_ratio?: number | string;
  metaverse_cross_scene_count?: number | string;
  metaverse_cross_shard_count?: number | string;
  metaverse_burst_count?: number | string;
  metaverse_offchain_confirmation_count?: number | string;
  metaverse_offchain_failure_count?: number | string;
  metaverse_cross_metaverse_count?: number | string;
  baseline_matrix_enabled?: boolean | string;
  baseline_count?: number | string;
  multi_seed_enabled?: boolean | string;
  seed_count?: number | string;
  paper_export_enabled?: boolean | string;
  paper_table_available?: boolean | string;
  paper_figure_data_available?: boolean | string;
  metaverse_experiment_truth?: string;
};
export type V3SmokeRunResponse = Omit<V2SweepRunResponse, "summary"> & { runtime_mode?: string; summary: V3RuntimeSummary };
export type V3DraftModuleStatus = "default" | "fixed" | "variable" | "disabled" | "planned" | "output";
export type V3ComposerDraftModuleRequest = {
  module_id: string;
  status: V3DraftModuleStatus;
  plugin: string;
  params?: Record<string, string | number | boolean>;
};
export type V3ComposerDraftRequest = {
  template_id: string;
  preset_id?: string;
  modules: Record<string, V3ComposerDraftModuleRequest>;
  topology?: V3RuntimeTopology;
};
export type V3RuntimeTopology = {
  shard_count: number;
  validators_per_shard: number;
  executors_per_shard: number;
  storage_nodes_per_shard: number;
  supervisor_enabled: boolean;
  node_runtime_mode: "logical_single_process" | "local_multi_process" | string;
  process_runtime_mode?: "dry_run" | "smoke" | string;
  max_local_processes?: number;
  enable_committee_epoch?: boolean;
  epoch_count?: number;
  network_mode: "in_memory_message_bus" | string;
  network_adapter?: "in_memory_message_bus" | "localhost_tcp_preview" | string;
  cross_shard_protocol?: "none" | "relay_preview" | "relay_mvp" | "broker_preview" | "two_phase_commit_preview" | string;
  relay_failure_mode?: "none" | "proof_fail" | "timeout" | "target_reject" | string;
  relay_force_proof_fail_every_n?: number;
  relay_force_timeout_every_n?: number;
  relay_timeout_ms?: number;
  state_backend?: "memory_kv" | "persistent_kv" | "merkle_trie_mvp" | "ethereum_mpt_compatible" | string;
  benchmark_template?: "metatrack_hotspot_template" | "pbft_network_template" | "cross_shard_relay_preview_template" | "state_authenticity_template" | "full_stack_v3_template" | string;
  baseline_profile?: "baseline_simple_chain" | "baseline_hash_sharding" | "baseline_no_prefetch" | "baseline_no_cross_shard_protocol" | "baseline_memory_kv" | "baseline_no_state_authenticity" | string;
  repeat_count?: number;
  metaverse_suite_enabled?: boolean;
  metaverse_scenario?: "asset_transfer" | "avatar_update" | "scene_hotspot" | "item_transfer" | "cross_scene_migration" | "onchain_offchain_confirmation" | "cross_metaverse_transfer" | "mixed_metaverse" | string;
  user_count?: number;
  asset_count?: number;
  item_count?: number;
  avatar_count?: number;
  scene_count?: number;
  metaverse_count?: number;
  tx_count?: number;
  seed?: number;
  hotspot_ratio?: number;
  cross_scene_ratio?: number;
  cross_shard_ratio?: number;
  burst_rate?: number;
  read_write_ratio?: number;
  asset_skew?: number;
  scene_skew?: number;
  offchain_confirmation_enabled?: boolean;
  offchain_confirm_delay_ms?: number;
  offchain_failure_ratio?: number;
  cross_metaverse_enabled?: boolean;
  benchmark_suite_enabled?: boolean;
  baseline_matrix_enabled?: boolean;
  multi_seed_enabled?: boolean;
  paper_export_enabled?: boolean;
  sweep_seed_count?: number;
  sweep_shard_counts?: number[];
  sweep_cross_shard_ratios?: number[];
  sweep_hotspot_ratios?: number[];
};
export type V3DraftValidationResponse = {
  is_valid: boolean;
  is_runnable: boolean;
  run_mode: string;
  normalized_draft?: Record<string, unknown> | null;
  variable_modules: string[];
  fixed_modules: string[];
  disabled_modules: string[];
  planned_modules: string[];
  output_modules: string[];
  errors: string[];
  warnings: string[];
};
export type V3DraftSmokeRunResponse = V3SmokeRunResponse & {
  job_id?: string;
  run_mode?: string;
  validation: V3DraftValidationResponse;
};
export type V3DraftRunSummary = {
  run_id: string;
  created_at: string;
  template_id: string;
  experiment_template?: string;
  preset_id?: string;
  preset_name?: string;
  variable_module?: string;
  locked_modules?: Record<string, string>;
  fairness_validated?: boolean;
  run_mode: string;
  is_valid: boolean;
  is_runnable: boolean;
  selected_plugins: Record<string, string>;
  variable_modules: string[];
  fixed_modules: string[];
  disabled_modules: string[];
  output_modules: string[];
  artifact_count: number;
  artifact_groups?: { title: string; files: V2Artifact[] }[];
  summary_preview?: Record<string, unknown>;
  missing_files?: string[];
};
export type V3DraftRunDetail = {
  run_id: string;
  created_at: string;
  composer_draft: Record<string, unknown>;
  normalized_draft: Record<string, unknown>;
  validation: V3DraftValidationResponse;
  generated_experiment_profile: Record<string, unknown>;
  generated_plugin_profile: Record<string, unknown>;
  artifact_groups: { title: string; files: V2Artifact[] }[];
  artifacts: V2Artifact[];
  summary_preview: Record<string, unknown>;
  missing_files: string[];
};
export type V3ControlledSmokeRunResponse = {
  run_id: string;
  status: string;
  stage: string;
  current_stage?: string;
  latest_runtime_stage?: string;
  latest_completed_runtime_stage?: string;
  current_capability?: string;
  closure_stage?: string;
  runtime_truth?: string;
  next_stage?: string;
  output_dir: string;
  data_truth_label: string;
  backend_type: string;
  runtime_mode: string;
  run_mode: string;
  preset_order: string[];
  run_index: Record<string, unknown>[];
  aggregate_summary: Record<string, unknown>[];
  realism_readiness: {
    stage?: string;
    backend_truth?: string;
    not_real_chain_claims?: string[];
    modules?: Record<string, unknown>[];
  };
  artifacts: V2Artifact[];
};

export async function runDefaultExperiment(): Promise<unknown> {
  return request(`${experimentPath}/run`, { method: "POST" });
}

export async function fetchRuntimeLog(): Promise<string> {
  const response = await request<{ log: string }>(`${experimentPath}/logs`);
  return response.log;
}

export async function fetchSummary(): Promise<Summary> {
  return request<Summary>(`${experimentPath}/summary`);
}

export async function fetchExperimentFiles(): Promise<string[]> {
  return request<string[]>(`${experimentPath}/files`);
}

export function experimentFileDownloadURL(filename: string): string {
  return `${requestBaseURL}${experimentPath}/files/${encodeURIComponent(filename)}`;
}

export async function fetchV1Templates(): Promise<V1Template[]> {
  return request<V1Template[]>("/api/v1/composer/templates");
}

export async function fetchV1Experiments(): Promise<V1Experiment[]> {
  return request<V1Experiment[]>("/api/v1/composer/experiments");
}

export async function fetchV1Status(): Promise<V1Status> {
  return request<V1Status>("/api/v1/status");
}

export async function runV1Sweep(): Promise<V1SweepRunResult> {
  return request<V1SweepRunResult>("/api/v1/sweep/run", { method: "POST" });
}

export async function fetchV1SweepSummary(): Promise<V1SweepSummary> {
  return request<V1SweepSummary>("/api/v1/sweep/summary");
}

export async function fetchV1SweepReport(): Promise<V1SweepReport> {
  return request<V1SweepReport>("/api/v1/sweep/report");
}

export async function fetchV1SweepFiles(): Promise<V1SweepFiles> {
  return request<V1SweepFiles>("/api/v1/sweep/files");
}

export function v1SweepFileDownloadURL(filename: string): string {
  return `${requestBaseURL}/api/v1/sweep/files/${encodeURIComponent(filename)}`;
}

export async function fetchV1Workloads(): Promise<V1WorkloadOption[]> {
  const response = await request<{ items: V1WorkloadOption[] }>("/api/v1/workloads");
  return response.items;
}

export async function fetchV1AblationPresets(): Promise<V1AblationPreset[]> {
  const response = await request<{ items: V1AblationPreset[] }>("/api/v1/ablation-presets");
  return response.items;
}

export async function fetchV1FabricTraceStatus(): Promise<V1FabricTraceStatus> {
  return request<V1FabricTraceStatus>("/api/v1/fabric/trace-status");
}

export async function runV1CustomExperiment(payload: V1CustomRunRequest): Promise<V1CustomRunResult> {
  return request<V1CustomRunResult>("/api/v1/custom-run", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) });
}

export async function fetchV1CustomRunSummary(): Promise<V1CustomRunSummary> {
  return request<V1CustomRunSummary>("/api/v1/custom-run/latest/summary");
}

export async function fetchV1CustomRunFiles(): Promise<V1SweepFiles> {
  return request<V1SweepFiles>("/api/v1/custom-run/latest/files");
}

export function v1CustomRunFileDownloadURL(filename: string): string {
  return `${requestBaseURL}/api/v1/custom-run/latest/files/${encodeURIComponent(filename)}`;
}

export async function fetchV2TraceSources(): Promise<V2TraceSource[]> {
  const response = await request<{ items: V2TraceSource[] }>("/api/v2/trace-sources");
  return response.items;
}

export async function fetchV2TraceSource(id: string): Promise<V2TraceSource> {
  return request<V2TraceSource>(`/api/v2/trace-sources/${encodeURIComponent(id)}`);
}

export async function validateV2TraceSource(payload: Record<string, unknown>): Promise<V2TraceSourceValidationResult> {
  return request<V2TraceSourceValidationResult>("/api/v2/trace-sources/validate", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) });
}

export async function fetchV2ChainBackends(): Promise<V2ChainBackend[]> {
  const response = await request<{ items: V2ChainBackend[] }>("/api/v2/chain-backends");
  return response.items;
}

export async function previewV2Composer(payload: Record<string, unknown>): Promise<V2ComposerPreviewResult> {
  return request<V2ComposerPreviewResult>("/api/v2/composer/preview", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) });
}

export async function fetchV2DualChainSampleConfig(): Promise<V2SampleConfig> {
  return request<V2SampleConfig>("/api/v2/dual-chain/sample-config");
}

export async function runV2DualChainReplay(config_path = "configs/experiments/v2_dual_chain_sample.yaml"): Promise<V2DualChainReplayResponse> {
  return request<V2DualChainReplayResponse>("/api/v2/dual-chain/replay", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ config_path }) });
}

export async function fetchV2Protocols(): Promise<V2ProtocolInfo[]> {
  const response = await request<{ items: V2ProtocolInfo[] }>("/api/v2/cross-chain/protocols");
  return response.items;
}

export async function fetchV2ProtocolSampleConfig(): Promise<V2SampleConfig> {
  return request<V2SampleConfig>("/api/v2/cross-chain/sample-config");
}

export async function runV2ProtocolReplay(config_path = "configs/experiments/v2_cross_chain_protocol_sample.yaml"): Promise<V2ProtocolReplayResponse> {
  return request<V2ProtocolReplayResponse>("/api/v2/cross-chain/protocol-replay", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ config_path }) });
}

export async function fetchV2Sweeps(): Promise<V2SweepInfo[]> {
  const response = await request<{ items: V2SweepInfo[] }>("/api/v2/sweeps");
  return response.items;
}

export async function runV2Sweep(sweep_id = "v2_baseline_sweep"): Promise<V2SweepRunResponse> {
  return request<V2SweepRunResponse>("/api/v2/sweeps/run", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ sweep_id }) });
}

export async function fetchV2CalibrationConfigs(): Promise<V2CalibrationInfo[]> {
  const response = await request<{ items: V2CalibrationInfo[] }>("/api/v2/calibration/configs");
  return response.items;
}

export async function fetchV2FabricSmokeStatus(): Promise<V2FabricSmokeStatus> {
  return request<V2FabricSmokeStatus>("/api/v2/calibration/fabric-smoke/status");
}

export async function runV2Calibration(config_id = "v2_synthetic_calibration_sample"): Promise<V2CalibrationRunResponse> {
  return request<V2CalibrationRunResponse>("/api/v2/calibration/run", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ config_id }) });
}

export async function fetchV2Runs(limit = 20): Promise<V2RunSummary[]> {
  const response = await request<{ items: V2RunSummary[] }>(`/api/v2/runs?limit=${encodeURIComponent(String(limit))}`);
  return response.items;
}

export async function fetchV2RunArtifacts(runId: string): Promise<V2ArtifactsResponse> {
  return request<V2ArtifactsResponse>(`/api/v2/runs/${encodeURIComponent(runId)}/artifacts`);
}

export function v2ArtifactDownloadURL(downloadURL: string): string {
  return `${requestBaseURL}${downloadURL}`;
}

export async function fetchV3ComposerTemplates(): Promise<V3TemplateSummary[]> {
  const response = await request<{ items: V3TemplateSummary[] }>("/api/v3/composer/templates");
  return response.items;
}

export async function fetchV3ComposerPreview(experimentProfileId = "metatrack_go_backed_ablation_smoke"): Promise<V3ComposerPreviewResponse> {
  return request<V3ComposerPreviewResponse>(`/api/v3/composer/preview?experiment_profile_id=${encodeURIComponent(experimentProfileId)}`);
}

export async function runV3ComposerSmoke(): Promise<V3SmokeRunResponse> {
  return request<V3SmokeRunResponse>("/api/v3/composer/run-smoke", { method: "POST" });
}

export async function validateV3ComposerDraft(draft: V3ComposerDraftRequest): Promise<V3DraftValidationResponse> {
  return request<V3DraftValidationResponse>("/api/v3/composer/validate-draft", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(draft) });
}

export async function runV3ComposerDraftSmoke(draft: V3ComposerDraftRequest): Promise<V3DraftSmokeRunResponse> {
  return request<V3DraftSmokeRunResponse>("/api/v3/composer/run-draft-smoke", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(draft) });
}

export async function runV3ControlledSmoke(): Promise<V3ControlledSmokeRunResponse> {
  return request<V3ControlledSmokeRunResponse>("/api/v3/composer/run-controlled-smoke", { method: "POST" });
}

export async function fetchV3DraftRuns(limit = 20): Promise<V3DraftRunSummary[]> {
  const response = await request<{ runs: V3DraftRunSummary[] }>(`/api/v3/composer/draft-runs?limit=${encodeURIComponent(String(limit))}`);
  return response.runs;
}

export async function fetchV3DraftRunDetail(runId: string): Promise<V3DraftRunDetail> {
  return request<V3DraftRunDetail>(`/api/v3/composer/draft-runs/${encodeURIComponent(runId)}`);
}

async function request<T = unknown>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${requestBaseURL}${path}`, init);
  if (!response.ok) {
    const body = await response.text();
    throw new Error(`${response.status} ${response.statusText}${body ? `: ${body}` : ""}`);
  }
  return response.json() as Promise<T>;
}
