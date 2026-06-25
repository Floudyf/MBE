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

async function request<T = unknown>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${requestBaseURL}${path}`, init);
  if (!response.ok) {
    const body = await response.text();
    throw new Error(`${response.status} ${response.statusText}${body ? `: ${body}` : ""}`);
  }
  return response.json() as Promise<T>;
}
