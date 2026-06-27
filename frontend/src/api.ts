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
export type V3ModuleStatus = "fixed" | "variable" | "disabled" | "planned" | "output" | string;
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
  stage: string;
  chain_mode: string;
  runnable: boolean;
  preview_only: boolean;
  description: string;
  variable_modules: string[];
  fixed_modules: string[];
  disabled_modules: string[];
  planned_modules: string[];
  output_modules: string[];
};
export type V3SmokeRunResponse = V2SweepRunResponse & { runtime_mode?: string };

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

async function request<T = unknown>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${requestBaseURL}${path}`, init);
  if (!response.ok) {
    const body = await response.text();
    throw new Error(`${response.status} ${response.statusText}${body ? `: ${body}` : ""}`);
  }
  return response.json() as Promise<T>;
}
