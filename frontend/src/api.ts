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

async function request<T = unknown>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${requestBaseURL}${path}`, init);
  if (!response.ok) {
    const body = await response.text();
    throw new Error(`${response.status} ${response.statusText}${body ? `: ${body}` : ""}`);
  }
  return response.json() as Promise<T>;
}
