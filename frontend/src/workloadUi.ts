export const WORKLOAD_TX_OPTIONS = [10_000, 50_000, 100_000, 250_000] as const;
export const WORKLOAD_ALPHA_OPTIONS = [0.0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2, 1.4] as const;

export function formatHash(value: unknown): string {
  const text = typeof value === "string" ? value : "";
  return text ? `${text.slice(0, 12)}...${text.slice(-8)}` : "未提供";
}

export function formatTimeRange(start?: number | null, end?: number | null): string {
  if (!start || !end) return "未提供";
  return `${new Date(start).toISOString().slice(0, 10)} - ${new Date(end).toISOString().slice(0, 10)}`;
}

export function truthText(label: unknown): string {
  if (label === "real_observed") return "真实观测成交数据的确定性连续窗口";
  if (label === "real_derived_resampled") return "从真实观测成交行中确定性重采样形成的偏斜负载";
  if (label === "synthetic_generated") return "确定性合成负载";
  return String(label ?? "未提供");
}

export function valueText(value: unknown): string {
  if (value === undefined || value === null || value === "") return "未提供";
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "number") return Number.isInteger(value) ? value.toLocaleString() : value.toLocaleString(undefined, { maximumFractionDigits: 4 });
  return String(value);
}
