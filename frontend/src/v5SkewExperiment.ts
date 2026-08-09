import type { V5FormalChildRun, V5WorkloadDatasetSummary } from "./api";

export type SkewExperimentPreset = "single_theta" | "theta_main";
export type ThetaSweepPoint = { tx_count: number; target_theta: number };
export type ThetaTpsPoint = { theta: number; tps: number; sampleCount: number };
export type ThetaTpsSeries = { methodId: string; methodName: string; points: ThetaTpsPoint[] };

export function thetaOptionsForDataset(
  datasets: V5WorkloadDatasetSummary[],
  datasetId: string,
  variantMode: string,
): number[] {
  const dataset = datasets.find((item) => item.dataset_id === datasetId);
  const definitions = dataset?.variant_definitions ?? [];
  const definition = definitions.find((item) => item.variant_mode === variantMode)
    ?? definitions.find((item) => item.kind === "derived" && (item.parameters ?? []).some((field) => field.name === "target_theta"));
  const field = (definition?.parameters ?? []).find((item) => item.name === "target_theta");
  const values = (field?.options ?? [])
    .map((value) => typeof value === "number" ? value : Number(value))
    .filter((value) => Number.isFinite(value));
  return [...new Set(values)].sort((left, right) => left - right);
}

export function buildThetaSweepPoints(txCount: number, thetaValues: number[]): ThetaSweepPoint[] {
  return [...new Set(thetaValues)]
    .filter((value) => Number.isFinite(value))
    .sort((left, right) => left - right)
    .map((target_theta) => ({ tx_count: txCount, target_theta }));
}

export function compactCount(value: number): string {
  if (value >= 1000 && value % 1000 === 0) return `${value / 1000}K`;
  return String(value);
}

function finiteNumber(value: unknown): number | null {
  const numeric = typeof value === "number" ? value : Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function childTPS(child: V5FormalChildRun): number | null {
  return finiteNumber(
    child.metrics?.end_to_end_tps
      ?? child.result?.summary?.finality_evidence?.end_to_end_tps
      ?? child.metrics?.throughput_tps,
  );
}

export function buildThetaTpsSeries(children: V5FormalChildRun[]): ThetaTpsSeries[] {
  const buckets = new Map<string, { methodName: string; theta: Map<number, number[]> }>();
  for (const child of children) {
    const execution = child.execution_status ?? child.result?.summary?.execution_status ?? child.status;
    const eligible = child.formal_eligibility ?? child.result?.summary?.formal_eligibility;
    if (child.status !== "completed" || execution !== "completed" || eligible !== true) continue;
    const theta = finiteNumber(child.workload_point?.target_theta);
    const tps = childTPS(child);
    if (theta === null || tps === null) continue;
    const methodId = child.method_config_id || child.method.method_id;
    const bucket = buckets.get(methodId) ?? { methodName: child.method.display_name || methodId, theta: new Map<number, number[]>() };
    const samples = bucket.theta.get(theta) ?? [];
    samples.push(tps);
    bucket.theta.set(theta, samples);
    buckets.set(methodId, bucket);
  }
  return [...buckets.entries()]
    .map(([methodId, bucket]) => ({
      methodId,
      methodName: bucket.methodName,
      points: [...bucket.theta.entries()]
        .map(([theta, samples]) => ({
          theta,
          tps: samples.reduce((sum, value) => sum + value, 0) / samples.length,
          sampleCount: samples.length,
        }))
        .sort((left, right) => left.theta - right.theta),
    }))
    .sort((left, right) => left.methodName.localeCompare(right.methodName));
}
