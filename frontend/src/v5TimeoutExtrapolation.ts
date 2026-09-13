export type TimeoutExtrapolationEvidence = {
  measurement_type?: string;
  qualified_for_tps_estimate?: boolean;
  estimated_end_to_end_tps?: number | null;
  tail_regression_tps?: number | null;
  tail_cv?: number | null;
  tail_relative_drift?: number | null;
  tail_terminal_r2?: number | null;
  estimated_remaining_seconds?: number | null;
  estimated_total_duration_seconds?: number | null;
  observed_duration_ms?: number | null;
  terminal_at_timeout?: number | null;
  incomplete_at_timeout?: number | null;
  reasons?: string[];
};

type ChildLike = {
  status?: string;
  execution_status?: string;
  formal_eligibility?: boolean;
  metrics?: Record<string, unknown>;
  result?: { summary?: Record<string, unknown> };
};

export type ReportedTpsSample = {
  value: number;
  mode: "observed_complete" | "timeout_extrapolated";
  estimated: boolean;
  evidence: TimeoutExtrapolationEvidence | null;
};

function record(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

function finiteNumber(value: unknown): number | null {
  if (value === null || value === undefined || value === "" || typeof value === "boolean") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

export function timeoutExtrapolationEvidence(child: ChildLike): TimeoutExtrapolationEvidence | null {
  const summary = record(child.result?.summary);
  const evidence = record(summary.timeout_extrapolation);
  return Object.keys(evidence).length ? evidence as TimeoutExtrapolationEvidence : null;
}

export function reportedTpsSample(child: ChildLike): ReportedTpsSample | null {
  const summary = record(child.result?.summary);
  const execution = child.execution_status ?? String(summary.execution_status ?? child.status ?? "");
  const eligible = child.formal_eligibility ?? (typeof summary.formal_eligibility === "boolean" ? summary.formal_eligibility : undefined);
  if (child.status === "completed" && execution === "completed" && eligible === true) {
    const metrics = record(child.metrics);
    const finality = record(summary.finality_evidence);
    const observed = finiteNumber(metrics.end_to_end_tps ?? finality.end_to_end_tps ?? metrics.throughput_tps);
    if (observed !== null && observed > 0) {
      return { value: observed, mode: "observed_complete", estimated: false, evidence: null };
    }
  }

  const evidence = timeoutExtrapolationEvidence(child);
  if (!evidence || evidence.qualified_for_tps_estimate !== true || evidence.measurement_type !== "timeout_extrapolated") return null;
  const estimated = finiteNumber(evidence.estimated_end_to_end_tps);
  if (estimated === null || estimated <= 0) return null;
  return { value: estimated, mode: "timeout_extrapolated", estimated: true, evidence };
}

export function observedP99FinalityMs(child: ChildLike): number | null {
  const summary = record(child.result?.summary);
  const execution = child.execution_status ?? String(summary.execution_status ?? child.status ?? "");
  const eligible = child.formal_eligibility ?? (typeof summary.formal_eligibility === "boolean" ? summary.formal_eligibility : undefined);
  if (child.status !== "completed" || execution !== "completed" || eligible !== true) return null;
  const metrics = record(child.metrics);
  const finality = record(summary.finality_evidence);
  const value = finiteNumber(metrics.p99_finality_ms ?? finality.p99_finality_ms ?? metrics.p99_latency_ms);
  return value !== null && value >= 0 ? value : null;
}
