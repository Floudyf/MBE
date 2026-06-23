const configuredBaseURL = import.meta.env.VITE_API_BASE_URL;

// In development, Vite proxies relative requests to this default local backend.
export const API_BASE_URL = configuredBaseURL ?? "http://127.0.0.1:8000";
const requestBaseURL = configuredBaseURL ?? "";

const experimentPath = "/api/v0/experiments/v0_default_asset_hotspot";

export type Summary = Record<string, string>;
export type V1Template = { name: string; stage: string; runnable: boolean; description: string };
export type V1Experiment = { id: string; stage: string; runnable: boolean; implemented: boolean; description: string; template: string };

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

async function request<T = unknown>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${requestBaseURL}${path}`, init);
  if (!response.ok) {
    const body = await response.text();
    throw new Error(`${response.status} ${response.statusText}${body ? `: ${body}` : ""}`);
  }
  return response.json() as Promise<T>;
}
