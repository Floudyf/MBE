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
  controlled_experiment_enabled?: boolean | string;
  plugin_selection_mode?: string;
  failure_summary?: {
    failed_run_count?: number;
    top_errors?: { count: number; message: string }[];
    failed_runs_file?: string;
    child_artifact_index_file?: string;
  };
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
  v3_final_enabled?: boolean | string;
  stage_alignment_ok?: boolean | string;
  frontend_backend_alignment_truth?: string;
  v3_final_truth?: string;
  fault_injection_enabled?: boolean | string;
  fault_profile?: string;
  fault_event_count?: number | string;
  node_failure_count?: number | string;
  node_recovery_count?: number | string;
  network_delay_event_count?: number | string;
  network_drop_event_count?: number | string;
  target_congestion_event_count?: number | string;
  relay_fault_event_count?: number | string;
  fault_injection_truth?: string;
  observability_enabled?: boolean | string;
  observability_level?: string;
  component_health_count?: number | string;
  component_warning_count?: number | string;
  component_error_count?: number | string;
  observability_truth?: string;
  reproducibility_bundle_enabled?: boolean | string;
  final_artifact_catalog_available?: boolean | string;
  reproducibility_guide_available?: boolean | string;
  experiment_manual_available?: boolean | string;
  paper_mapping_available?: boolean | string;
  reproducibility_truth?: string;
};

export type V4RealismStatus = {
  runtime_stage: string;
  runtime_truth: string;
  real_signed_tx: boolean;
  sender_public_key_binding: boolean;
  signed_tx_authenticity: boolean;
  per_node_mempool: boolean;
  real_p2p: boolean;
  pbft_style_consensus: boolean;
  real_pbft_messages: boolean;
  production_pbft: boolean;
  full_byzantine_security: boolean;
  persistent_state_db: boolean;
  state_root_from_real_state_updates: boolean;
  real_cross_shard_state_machine: boolean;
  real_cross_shard_network_commit: boolean;
  recovery_supported: boolean;
  fault_injection_supported: boolean;
  real_fault_injection: boolean;
  blockemulator_trace_to_signed_tx: boolean;
  blockemulator_bridge_upgraded: boolean;
  frontend_realism_mode: boolean;
  fabric_evm_backend: boolean;
  production_blockchain: boolean;
  production_atomic_commit: boolean;
  full_blockemulator_compatibility: boolean;
};

export type V4RealismArtifact = { name: string; download_url: string; size_bytes: number };
export type V4RealismSmokeRequest = {
  nodes: number;
  shards: number;
  tx_count: number;
  enable_cross_shard: boolean;
  enable_faults: boolean;
  fault_profile: string;
  blockemulator_csv?: string | null;
  blockemulator_tx_limit: number;
  run_duration_ms: number;
};
export type V4RealismSmokeResponse = {
  run_id: string;
  status: string;
  output_dir: string;
  summary?: Record<string, unknown>;
  artifacts?: V4RealismArtifact[];
  stdout?: string;
  stderr?: string;
};
export type ExperimentProfile = {
  profile_id: string;
  label: string;
  description: string;
  runtime_target: string;
  mechanism_tags: string[];
  default_topology_id: string;
  default_workload_id: string;
  runnable: boolean;
};
export type ExperimentTopology = {
  topology_id: string;
  label: string;
  nodes: number;
  shards: number;
  validators_per_shard: number;
  runtime_mode: string;
  description: string;
  runnable: boolean;
};
export type ExperimentWorkload = {
  workload_id: string;
  label: string;
  source_type: string;
  scale_label: string;
  skew_label: string;
  description: string;
  planned: boolean;
  default_tx_count: number;
  default_blockemulator_tx_limit: number;
  csv_required: boolean;
};
export type ExperimentRunPlanRequest = {
  profile_id: string;
  topology_id: string;
  workload_id: string;
  blockemulator_csv?: string | null;
  tx_count_override?: number | null;
  fault_profile_override?: string | null;
};
export type ExperimentRunPlanPreview = {
  profile: ExperimentProfile;
  topology: ExperimentTopology;
  workload: ExperimentWorkload;
  runtime: string;
  recommended_v4_request: V4RealismSmokeRequest;
  runnable: boolean;
  warnings: string[];
  next_step: string;
};
export type ExperimentMethod = {
  method_id: string;
  label: string;
  role: string;
  description: string;
  module_overrides: Record<string, string>;
  runnable: boolean;
  config_source: "builtin" | "saved_config" | string;
  config_id?: string | null;
  validation_status: "unknown" | "valid" | "runnable" | "blocked" | string;
  tags: string[];
  previewable: boolean;
};
export type ExperimentConditions = {
  topology_mode: "preset" | "custom";
  topology_id?: string | null;
  nodes?: number | null;
  shards?: number | null;
  validators_per_shard?: number | null;
  tx_count: number;
  repeat_count: number;
};
export type ExperimentSuiteRequest = {
  plan_name?: string | null;
  selected_method_ids?: string[];
  selected_suite_types?: string[];
  workload_ids?: string[];
  topology_ids?: string[];
  seeds?: number[];
  include_v4_realism?: boolean;
  composer_draft?: Record<string, unknown> | null;
  formal_config?: Record<string, unknown> | null;
  blockemulator_csv?: string | null;
  conditions?: ExperimentConditions | null;
};
export type ExperimentMatrixRow = {
  row_id: string;
  suite_type: string;
  method_id: string;
  method_role: string;
  config_source: string;
  method_config_id?: string | null;
  resolved_method_name: string;
  validation_status: string;
  workload_id: string;
  topology_id: string;
  topology_mode: string;
  nodes: number;
  shards: number;
  validators_per_shard: number;
  tx_count: number;
  seed: number;
  repeat_index: number;
  runtime_target: string;
  runnable: boolean;
  warnings: string[];
};
export type ExperimentRunMatrixPreview = {
  plan_name: string;
  suite_types: string[];
  methods: ExperimentMethod[];
  rows: ExperimentMatrixRow[];
  runnable_row_count: number;
  blocked_row_count: number;
  warnings: string[];
  v4_realism_candidates: Record<string, unknown>[];
  next_step: string;
};
export type V4DerivedRequestPreview = {
  source: string;
  v4_request: V4RealismSmokeRequest;
  runnable: boolean;
  warnings: string[];
};
export type SelectedMatrixRowRequest = {
  row_id: string;
  suite_type: string;
  method_id: string;
  method_role: string;
  config_source: string;
  method_config_id?: string | null;
  resolved_method_name: string;
  validation_status: string;
  workload_id: string;
  topology_id: string;
  topology_mode: string;
  nodes: number;
  shards: number;
  validators_per_shard: number;
  tx_count: number;
  seed: number;
  repeat_index: number;
  runtime_target: string;
  runnable: boolean;
  warnings: string[];
};
export type RunSuiteExecutionRequest = {
  run_mode: "dry_run" | "execute" | string;
  selected_rows: SelectedMatrixRowRequest[];
  include_v4_realism?: boolean;
  v4_request_override?: V4RealismSmokeRequest | null;
  max_execute_rows?: number;
};
export type ChildRunResult = {
  row_id: string;
  suite_type: string;
  method_id: string;
  status: string;
  runner: string;
  run_id?: string | null;
  summary: Record<string, unknown>;
  artifacts: Record<string, unknown>[];
  warnings: string[];
  blocked_reason?: string | null;
};
export type RunSuiteExecutionResponse = {
  run_group_id: string;
  run_mode: string;
  selected_row_count: number;
  started_row_count: number;
  blocked_row_count: number;
  child_runs: ChildRunResult[];
  warnings: string[];
  next_step: string;
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
  controlled_experiment_enabled?: boolean;
  metaverse_suite_enabled?: boolean;
  workload_source?: "synthetic" | "metaverse" | "saved_workload" | "existing_trace_preview" | string;
  trace_path?: string;
  trace_schema?: string;
  trace_field_mapping?: Record<string, unknown>;
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
  zipf_alpha?: number;
  submit_rate?: number;
  arrival_rate?: number;
  key_space_size?: number;
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
  fault_injection_enabled?: boolean;
  fault_profile?: "none" | "node_failure" | "node_recovery" | "network_delay" | "network_drop" | "target_congestion" | "relay_fault" | "mixed_fault" | string;
  fault_seed?: number;
  fault_start_round?: number;
  fault_duration_rounds?: number;
  failed_node_count?: number;
  message_delay_ms?: number;
  message_drop_ratio?: number;
  target_congestion_ratio?: number;
  relay_fault_mode?: "none" | "proof_fail" | "timeout" | "target_reject" | string;
  observability_enabled?: boolean;
  observability_level?: "basic" | "detailed" | string;
  reproducibility_bundle_enabled?: boolean;
  paper_mapping_enabled?: boolean;
  final_artifact_catalog_enabled?: boolean;
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
export type V3SavedConfigKind = "module" | "workload" | "topology" | "method" | "formal_plan";
export type V3SavedConfig = {
  config_id: string;
  config_kind: V3SavedConfigKind;
  name: string;
  description: string;
  owner_label: string;
  tags: string[];
  created_at: string;
  updated_at: string;
  version: number;
  payload: Record<string, unknown>;
  validation_status: "unknown" | "valid" | "runnable" | "blocked" | string;
  last_validation: Record<string, unknown>;
  last_smoke_run_id: string;
  source: "user_saved" | "builtin_seed" | "imported" | string;
  truth_boundary: string;
};
export type V3SavedConfigPayload = {
  config_kind: V3SavedConfigKind;
  name: string;
  description?: string;
  owner_label?: string;
  tags?: string[];
  payload: Record<string, unknown>;
  validation_status?: "unknown" | "valid" | "runnable" | "blocked" | string;
  last_validation?: Record<string, unknown>;
  last_smoke_run_id?: string;
  source?: "user_saved" | "builtin_seed" | "imported" | string;
};
export type V3FormalExperimentType = "ablation" | "hotspot_sensitivity" | "cross_shard_sensitivity" | "shard_scalability" | "control_overhead" | "workload_comparison";
export type V3RuntimeEvidenceMode = "logical_single_process" | "local_multi_process_validation";
export type V3FormalMetatrackBenchmarkRequest = {
  draft: V3ComposerDraftRequest;
  experiment_type: V3FormalExperimentType;
  formal_tx_count: number;
  seed_base: number;
  seed_count: number;
  baseline_ids: string[];
  hotspot_ratio_points: number[];
  cross_shard_ratio_points: number[];
  shard_count_points: number[];
  workload_scenario_points: string[];
  method_config_ids: string[];
  workload_config_ids: string[];
  topology_config_ids: string[];
  zipf_alpha: number;
  runtime_evidence_mode: V3RuntimeEvidenceMode;
  enable_faults_for_formal_run: boolean;
  max_run_count: number;
  max_total_tx_count: number;
};
export type V3FormalMetatrackBenchmarkPreview = {
  is_valid: boolean;
  is_runnable: boolean;
  errors: string[];
  warnings: string[];
  matrix: Record<string, unknown>[];
  seed_list: number[];
  run_count: number;
  total_tx_count: number;
  baseline_count: number;
  method_count?: number;
  workload_count?: number;
  topology_count?: number;
  scan_point_count: number;
  experiment_type: string;
  formal_tx_count: number;
  baseline_ids: string[];
  method_config_ids?: string[];
  workload_config_ids?: string[];
  topology_config_ids?: string[];
  runtime_evidence_mode: string;
  contains_preview_or_planned_plugin: boolean;
  exceeds_recommended_range: boolean;
  includes_fault_injection: boolean;
  truth_boundary: string;
};
export type V3FormalMetatrackBenchmarkRunResponse = {
  run_id: string;
  status: string;
  run_mode: string;
  output_dir: string;
  summary: V3RuntimeSummary;
  preview: V3FormalMetatrackBenchmarkPreview;
  artifacts: V2Artifact[];
};
export type V3FormalRunHistoryItem = {
  run_id: string;
  created_at: string;
  updated_at: string;
  status: string;
  experiment_type: string;
  formal_tx_count: number | string;
  run_count: number | string;
  completed_run_count: number | string;
  failed_run_count: number | string;
  total_tx_count: number | string;
  runtime_evidence_mode: string;
  method_count: number | string;
  workload_count: number | string;
  topology_count: number | string;
  output_dir: string;
  summary_available: boolean;
  chart_preview_available: boolean;
  zip_download_url: string;
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

export function artifactPreviewURL(downloadURL: string): string {
  return `${requestBaseURL}${downloadURL}`;
}

export function artifactDownloadURL(downloadURL: string): string {
  return `${requestBaseURL}${downloadURL}`;
}

export function formalArtifactsZipURL(runId: string): string {
  return `${requestBaseURL}/api/v3/composer/formal-metatrack/${encodeURIComponent(runId)}/artifacts.zip`;
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

export async function listV3SavedConfigs(kind?: V3SavedConfigKind): Promise<V3SavedConfig[]> {
  const query = kind ? `?kind=${encodeURIComponent(kind)}` : "";
  const response = await request<{ items: V3SavedConfig[] }>(`/api/v3/saved-configs${query}`);
  return response.items;
}

export async function getV3SavedConfig(configId: string): Promise<V3SavedConfig> {
  return request<V3SavedConfig>(`/api/v3/saved-configs/${encodeURIComponent(configId)}`);
}

export async function createV3SavedConfig(payload: V3SavedConfigPayload): Promise<V3SavedConfig> {
  return request<V3SavedConfig>("/api/v3/saved-configs", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) });
}

export async function updateV3SavedConfig(configId: string, payload: Partial<V3SavedConfigPayload>): Promise<V3SavedConfig> {
  return request<V3SavedConfig>(`/api/v3/saved-configs/${encodeURIComponent(configId)}`, { method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) });
}

export async function deleteV3SavedConfig(configId: string): Promise<{ deleted: boolean; config_id: string }> {
  return request<{ deleted: boolean; config_id: string }>(`/api/v3/saved-configs/${encodeURIComponent(configId)}`, { method: "DELETE" });
}

export async function previewV3FormalMetatrackBenchmark(payload: V3FormalMetatrackBenchmarkRequest): Promise<V3FormalMetatrackBenchmarkPreview> {
  return request<V3FormalMetatrackBenchmarkPreview>("/api/v3/composer/formal-metatrack/preview", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) });
}

export async function runV3FormalMetatrackBenchmark(payload: V3FormalMetatrackBenchmarkRequest): Promise<V3FormalMetatrackBenchmarkRunResponse> {
  return request<V3FormalMetatrackBenchmarkRunResponse>("/api/v3/composer/formal-metatrack/run", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) });
}

export async function fetchV3FormalMetatrackRuns(limit = 20): Promise<V3FormalRunHistoryItem[]> {
  const response = await request<{ runs: V3FormalRunHistoryItem[] }>(`/api/v3/composer/formal-metatrack/runs?limit=${encodeURIComponent(String(limit))}`);
  return response.runs;
}

export async function fetchV3FormalMetatrackRunResult(runId: string): Promise<V3FormalMetatrackBenchmarkRunResponse> {
  return request<V3FormalMetatrackBenchmarkRunResponse>(`/api/v3/composer/formal-metatrack/runs/${encodeURIComponent(runId)}`);
}

export async function fetchV3DraftRuns(limit = 20): Promise<V3DraftRunSummary[]> {
  const response = await request<{ runs: V3DraftRunSummary[] }>(`/api/v3/composer/draft-runs?limit=${encodeURIComponent(String(limit))}`);
  return response.runs;
}

export async function fetchV3DraftRunDetail(runId: string): Promise<V3DraftRunDetail> {
  return request<V3DraftRunDetail>(`/api/v3/composer/draft-runs/${encodeURIComponent(runId)}`);
}

export async function fetchExperimentProfiles(): Promise<ExperimentProfile[]> {
  const response = await request<{ items: ExperimentProfile[] }>("/api/experiment-flow/profiles");
  return response.items;
}

export async function fetchExperimentTopologies(): Promise<ExperimentTopology[]> {
  const response = await request<{ items: ExperimentTopology[] }>("/api/experiment-flow/topologies");
  return response.items;
}

export async function fetchExperimentWorkloads(): Promise<ExperimentWorkload[]> {
  const response = await request<{ items: ExperimentWorkload[] }>("/api/experiment-flow/workloads");
  return response.items;
}

export async function fetchExperimentMethods(includeSaved = true): Promise<ExperimentMethod[]> {
  const response = await request<{ items: ExperimentMethod[] }>(`/api/experiment-flow/methods?include_saved=${includeSaved ? "true" : "false"}`);
  return response.items;
}

export async function fetchRecommendedRun(): Promise<ExperimentRunPlanPreview> {
  return request<ExperimentRunPlanPreview>("/api/experiment-flow/recommended-run");
}

export async function previewExperimentRunPlan(payload: ExperimentRunPlanRequest): Promise<ExperimentRunPlanPreview> {
  return request<ExperimentRunPlanPreview>("/api/experiment-flow/preview-run-plan", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) });
}

export async function previewExperimentRunMatrix(payload: ExperimentSuiteRequest): Promise<ExperimentRunMatrixPreview> {
  return request<ExperimentRunMatrixPreview>("/api/experiment-flow/preview-run-matrix", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) });
}

export async function deriveV4RealismRequest(payload: ExperimentSuiteRequest): Promise<V4DerivedRequestPreview> {
  return request<V4DerivedRequestPreview>("/api/experiment-flow/derive-v4-realism-request", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) });
}

export async function executeSelectedRunMatrix(payload: RunSuiteExecutionRequest): Promise<RunSuiteExecutionResponse> {
  return request<RunSuiteExecutionResponse>("/api/experiment-flow/execute-selected-matrix", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) });
}

export async function fetchV4RealismStatus(): Promise<V4RealismStatus> {
  return request<V4RealismStatus>("/api/v4/realism/status");
}

export async function runV4RealismSmoke(payload: V4RealismSmokeRequest = { nodes: 4, shards: 1, tx_count: 10, enable_cross_shard: true, enable_faults: true, fault_profile: "network_delay", blockemulator_tx_limit: 10, run_duration_ms: 1000 }): Promise<V4RealismSmokeResponse> {
  return request<V4RealismSmokeResponse>("/api/v4/realism/smoke", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) });
}

export async function fetchV4RealismSummary(runId: string): Promise<Record<string, unknown>> {
  return request<Record<string, unknown>>(`/api/v4/realism/runs/${encodeURIComponent(runId)}/summary`);
}

export async function fetchV4RealismArtifacts(runId: string): Promise<{ run_id: string; artifacts: V4RealismArtifact[] }> {
  return request<{ run_id: string; artifacts: V4RealismArtifact[] }>(`/api/v4/realism/runs/${encodeURIComponent(runId)}/artifacts`);
}

export function v4RealismArtifactURL(downloadURL: string): string {
  return `${requestBaseURL}${downloadURL}`;
}

export type V5PluginMetric = { key: string; type: string; unit: string; aggregation: string; visualization: string; description: string };
export type V5PluginManifest = { plugin_id: string; category: string; version: string; display_name: string; description: string; implementation_status: string; supported_backends: string[]; config_schema: { type?: string; properties?: Record<string, V5SchemaField> }; default_config: Record<string, unknown>; capabilities: string[]; requirements: string[]; conflicts: string[]; metrics: V5PluginMetric[]; runtime_factory: string; runtime_adapter: string; truth_boundary: string };
export type V5SchemaField = { type?: string; title?: string; description?: string; default?: unknown; minimum?: number; maximum?: number; enum?: string[]; readOnly?: boolean; advanced?: boolean };
export type V5PluginSelection = { category: string; plugin_id: string; config: Record<string, unknown> };
export type V5ExperimentSpec = { schema_version?: "v5_experiment_spec_v1"; name: string; execution_backend: "preview" | "simulation" | "real_cluster"; plugin_selections: V5PluginSelection[]; topology: { nodes: number; shards: number; validators_per_shard: number }; tx_count: number; seed: number; duration_ms: number; fault_policy?: Record<string, unknown>; requested_metrics?: string[]; saved_config_id?: string | null; source_composer_draft?: Record<string, unknown> };
export type V5CompatibilityResult = { valid: boolean; blockers: string[]; warnings: string[]; resolved_plugins: V5PluginSelection[]; resource_estimate: Record<string, unknown> };
export type V5CompiledRunPlan = Record<string, unknown> & { plan_id: string; plan_digest: string; node_configs: Record<string, unknown>[]; plugin_snapshot: V5PluginManifest[]; no_fallback: boolean };
export type V5RealClusterResult = { run_id: string; status: string; summary: Record<string, unknown>; artifacts: V2Artifact[]; stdout: string; stderr: string; no_fallback: boolean };
export type V5FormalSuite = "main_experiment" | "comparison_experiment" | "ablation_experiment" | "workload_sensitivity" | "topology_scaling" | "fault_recovery_experiment";
export type V5FormalMethod = { method_id: string; display_name: string; plugin_overrides: Record<string, string> };
export type V5FormalExperimentPlan = {
  name: string;
  saved_config_id?: string;
  base_spec: V5ExperimentSpec;
  suites: V5FormalSuite[];
  methods: V5FormalMethod[];
  seeds: number[];
  repeats: number;
  topology_points?: Array<Record<string, number>>;
  workload_points?: Array<Record<string, number>>;
  fault_points?: Array<Record<string, unknown>>;
};
export type V5FormalRunRequest = { execution_backend: "preview" | "simulation" | "real_cluster"; plan: V5FormalExperimentPlan };
export type V5FormalMatrixRow = {
  child_run_id: string;
  suite_type: V5FormalSuite;
  method: V5FormalMethod;
  method_config_id: string;
  workload_point: Record<string, unknown>;
  topology_point: { nodes?: number; shards?: number; validators_per_shard?: number };
  fault_point: Record<string, unknown>;
  seed: number;
  repeat_index: number;
  scan_variable: string;
  scan_value: string;
  comparison_group_id?: string;
  execution_backend: string;
  estimated_processes: number;
  estimated_transactions: number;
  runnable: boolean;
  blockers: string[];
  warnings: string[];
};
export type V5FormalPreviewResponse = { execution_backend: string; rows: V5FormalMatrixRow[]; paper_candidate: boolean };
export type V5FormalAggregate = { count?: number | null; mean?: number | null; median?: number | null; std?: number | null; min?: number | null; max?: number | null; ci95_low?: number | null; ci95_high?: number | null; completed_count?: number | null; failed_count?: number | null; missing_count?: number | null };
export type V5FinalityEvidence = { [key: string]: unknown; logical_transaction_count?: number; submitted_unique_tx_count?: number; terminal_unique_tx_count?: number; incomplete_unique_tx_count?: number; finalized_unique_logical_tx_count?: number; intra_shard_committed_unique_count?: number; intra_shard_terminal_unique_count?: number; cross_shard_requested_unique_count?: number; cross_shard_target_committed_unique_count?: number; cross_shard_finalized_unique_count?: number; cross_shard_refunded_unique_count?: number; cross_shard_failed_unique_count?: number; throughput_tps?: number; p50_finality_ms?: number; p95_finality_ms?: number; p99_finality_ms?: number; metric_truth?: string; tcp_send_latency_excluded?: boolean };
export type V5RealClusterSummary = Record<string, unknown> & { runtime_stage?: string; runtime_truth?: string; ready_to_commit?: boolean; one_node_one_os_process?: boolean; independent_tcp_ports?: boolean; all_shards_active?: boolean; per_shard_multiple_blocks?: boolean; real_client_submission?: boolean; real_cross_shard_network?: boolean; real_pbft_style_messages?: boolean; real_signed_tx?: boolean; persistent_state?: boolean; plugin_driven_runtime?: boolean; state_root_consistent?: boolean; no_fallback?: boolean; orphan_process_count?: number; distinct_process_count?: number; expected_process_count?: number; shard_count?: number; shard_blocks?: Record<string, number>; finality_evidence?: V5FinalityEvidence };
export type V5RuntimeArtifact = { name: string; size_bytes: number; truth_category: string; download_url: string };
export type V5FormalChildResult = { run_id?: string; status?: string; output_dir?: string; summary?: V5RealClusterSummary; artifacts?: V5RuntimeArtifact[]; no_fallback?: boolean; stdout?: string; stderr?: string };
export type V5FormalChildRun = V5FormalMatrixRow & {
  run_group_id: string;
  status: string;
  attempt?: number;
  paper_candidate?: boolean;
  error?: string;
  result?: V5FormalChildResult;
  metrics?: Record<string, unknown>;
};
export type V5FormalRunGroup = {
  run_group_id: string;
  status: string;
  execution_backend: string;
  runtime_truth: string;
  total_child_runs: number;
  completed_child_runs: number;
  plan?: V5FormalExperimentPlan;
  matrix?: V5FormalMatrixRow[];
  aggregate?: V5FormalAggregate;
  cancel_requested?: boolean;
  plan_config_id?: string;
  bundle_path?: string;
  created_at?: string;
  updated_at?: string;
};
export type V5FormalRunGroupDetail = { group: V5FormalRunGroup; children: V5FormalChildRun[] };
export type V5FormalArtifactCatalog = { run_group_id: string; status: "ready" | "pending"; bundle_ready: boolean; bundle_size_bytes: number; file_count: number; files: Array<{ name: string; size_bytes: number }> };

export async function fetchV5PluginCatalog(backend?: string): Promise<V5PluginManifest[]> {
  const query = backend ? `?backend=${encodeURIComponent(backend)}` : "";
  const response = await request<{ items: V5PluginManifest[] }>(`/api/v5/plugins${query}`);
  return response.items;
}

export async function validateV5ExperimentSpec(payload: V5ExperimentSpec): Promise<V5CompatibilityResult> {
  return request<V5CompatibilityResult>("/api/v5/experiment-spec/validate", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) });
}

export async function compileV5ExperimentSpec(payload: V5ExperimentSpec): Promise<V5CompiledRunPlan> {
  return request<V5CompiledRunPlan>("/api/v5/experiment-spec/compile", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) });
}

export async function runV5RealCluster(payload: V5ExperimentSpec): Promise<V5RealClusterResult> {
  return request<V5RealClusterResult>("/api/v5/real-cluster/run", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) });
}

export async function previewV5FormalRun(payload: V5FormalRunRequest): Promise<V5FormalPreviewResponse> {
  return request<V5FormalPreviewResponse>("/api/v5/formal/preview", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) });
}

export async function createV5FormalRunGroup(payload: V5FormalRunRequest): Promise<V5FormalRunGroup> {
  return request<V5FormalRunGroup>("/api/v5/formal/run-groups", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) });
}

export async function fetchV5FormalRunGroup(groupId: string): Promise<V5FormalRunGroupDetail> {
  return request<V5FormalRunGroupDetail>(`/api/v5/formal/run-groups/${encodeURIComponent(groupId)}`);
}

export async function listV5FormalRunGroups(): Promise<V5FormalRunGroup[]> {
  return request<V5FormalRunGroup[]>("/api/v5/formal/run-groups");
}

export async function fetchV5FormalChildRun(groupId: string, childId: string): Promise<V5FormalChildRun> {
  return request<V5FormalChildRun>(`/api/v5/formal/run-groups/${encodeURIComponent(groupId)}/children/${encodeURIComponent(childId)}`);
}

export async function fetchV5FormalGroupMetrics(groupId: string): Promise<V5FormalAggregate> {
  return request<V5FormalAggregate>(`/api/v5/formal/run-groups/${encodeURIComponent(groupId)}/metrics`);
}

export async function fetchV5FormalArtifactCatalog(groupId: string): Promise<V5FormalArtifactCatalog> {
  return request<V5FormalArtifactCatalog>(`/api/v5/formal/run-groups/${encodeURIComponent(groupId)}/artifacts`);
}

export function v5FormalBundleURL(groupId: string): string {
  return `${requestBaseURL}/api/v5/formal/run-groups/${encodeURIComponent(groupId)}/bundle`;
}

export function v5RealClusterArtifactURL(downloadURL: string): string {
  return `${requestBaseURL}${downloadURL}`;
}

async function request<T = unknown>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${requestBaseURL}${path}`, init);
  if (!response.ok) {
    const body = await response.text();
    throw new Error(`${response.status} ${response.statusText}${body ? `: ${body}` : ""}`);
  }
  return response.json() as Promise<T>;
}
