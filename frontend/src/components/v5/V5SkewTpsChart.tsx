// MBE_TIMEOUT_EXTRAPOLATION_FRONTEND_20260830_V5
// MBE_V5_RESULTS_UI_TRUTH_CN_FINAL_20260814_V5
// MBE_V5_RESULTS_UI_FINAL_CN_CLOSURE_20260814_V4
import type { V5FormalChildRun } from "../../api";
import { observedP99FinalityMs, reportedTpsSample } from "../../v5TimeoutExtrapolation";

const palette = ["#2563eb", "#dc2626", "#059669", "#7c3aed", "#d97706", "#0891b2", "#be185d", "#4f46e5", "#65a30d"];
type MetricKind = "tps" | "p99";
type MetricSample = { value: number; estimated: boolean };
type MetricPoint = { theta: number; value: number; sampleCount: number; seedCount: number; ciLow: number | null; ciHigh: number | null; observedCount: number; timeoutExtrapolatedCount: number };
type MetricSeries = { methodId: string; methodName: string; points: MetricPoint[] };

export default function V5SkewTpsChart({ children, plannedThetaValues = [] }: { children: V5FormalChildRun[]; plannedThetaValues?: number[] }) {
  const methodEntries = [...new Map(children.map((child) => {
    const methodId = child.method_config_id || child.method.method_id;
    return [methodId, { methodId, methodName: shortMethodName(methodId, child.method.display_name || methodId) }] as const;
  })).values()].sort((left, right) => left.methodName.localeCompare(right.methodName));
  const thetaValues = [...new Set([
    ...plannedThetaValues.filter((value) => Number.isFinite(value)),
    ...children.map((child) => finiteNumber(child.workload_point?.target_theta)).filter((value): value is number => value !== null),
  ])].sort((left, right) => left - right);
  if (thetaValues.length < 2 || !methodEntries.length) return null;
  const tpsSeries = buildMetricSeries(children, methodEntries, "tps");
  const p99Series = buildMetricSeries(children, methodEntries, "p99");
  return <>
    <ThetaMetricChart testId="v5-skew-tps-chart" title="不同偏斜度下各方案 TPS" unit="TPS" metric="tps" series={tpsSeries} thetaValues={thetaValues} />
    <ThetaMetricChart testId="v5-skew-p99-chart" title="不同偏斜度下各方案 P99 Finality" unit="ms" metric="p99" series={p99Series} thetaValues={thetaValues} />
  </>;
}

function buildMetricSeries(children: V5FormalChildRun[], methods: Array<{ methodId: string; methodName: string }>, metric: MetricKind): MetricSeries[] {
  const buckets = new Map<string, Map<number, Map<string, MetricSample[]>>>();
  for (const child of children) {
    const sample = metricSample(child, metric);
    if (sample === null) continue;
    const theta = finiteNumber(child.workload_point?.target_theta);
    if (theta === null) continue;
    const methodId = child.method_config_id || child.method.method_id;
    const byTheta = buckets.get(methodId) ?? new Map<number, Map<string, MetricSample[]>>();
    const bySeed = byTheta.get(theta) ?? new Map<string, MetricSample[]>();
    const seed = String(child.seed ?? "");
    bySeed.set(seed, [...(bySeed.get(seed) ?? []), sample]);
    byTheta.set(theta, bySeed);
    buckets.set(methodId, byTheta);
  }
  return methods.map((method) => ({
    ...method,
    points: [...(buckets.get(method.methodId) ?? new Map<number, Map<string, MetricSample[]>>()).entries()]
      .map(([theta, bySeed]) => {
        const seedMeans = [...bySeed.values()].filter((values) => values.length > 0).map((values) => average(values.map((item) => item.value)));
        const onlySeed = bySeed.size <= 1;
        const rawSamples = [...bySeed.values()].flat();
        const samples = onlySeed ? rawSamples.map((item) => item.value) : seedMeans;
        const value = average(samples);
        const ci = studentT95(samples);
        return {
          theta,
          value,
          sampleCount: samples.length,
          seedCount: bySeed.size,
          ciLow: ci?.[0] ?? null,
          ciHigh: ci?.[1] ?? null,
          observedCount: rawSamples.filter((item) => !item.estimated).length,
          timeoutExtrapolatedCount: rawSamples.filter((item) => item.estimated).length,
        };
      })
      .sort((left, right) => Number(left.theta) - Number(right.theta)),
  }));
}

function metricSample(child: V5FormalChildRun, metric: MetricKind): MetricSample | null {
  if (metric === "tps") {
    const sample = reportedTpsSample(child);
    return sample ? { value: sample.value, estimated: sample.estimated } : null;
  }
  const value = observedP99FinalityMs(child);
  return value === null ? null : { value, estimated: false };
}

const t95: Record<number, number> = { 1: 12.706204736, 2: 4.30265273, 3: 3.182446305, 4: 2.776445105, 5: 2.570581836, 6: 2.446911851, 7: 2.364624252, 8: 2.306004135, 9: 2.262157163, 10: 2.228138852, 11: 2.20098516, 12: 2.17881283, 13: 2.160368656, 14: 2.144786688, 15: 2.131449546, 16: 2.119905299, 17: 2.109815578, 18: 2.10092204, 19: 2.093024054 };
function average(values: number[]): number { return values.reduce((sum, value) => sum + value, 0) / values.length; }
function studentT95(values: number[]): [number, number] | null {
  if (values.length < 2) return null;
  const mean = average(values);
  const variance = values.reduce((sum, value) => sum + (value - mean) ** 2, 0) / (values.length - 1);
  const critical = t95[values.length - 1] ?? 1.96;
  const margin = critical * Math.sqrt(variance) / Math.sqrt(values.length);
  return [mean - margin, mean + margin];
}

function finiteNumber(value: unknown): number | null {
  const numeric = typeof value === "number" ? value : Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function ThetaMetricChart({ testId, title, unit, metric, series, thetaValues }: { testId: string; title: string; unit: string; metric: MetricKind; series: MetricSeries[]; thetaValues: number[] }) {
  const width = 960;
  const height = 420;
  const left = 72;
  const right = 24;
  const top = 24;
  const bottom = 62;
  const plotWidth = width - left - right;
  const plotHeight = height - top - bottom;
  const allValues = series.flatMap((item) => item.points.map((point) => point.value));
  const yMax = Math.max(1, ...allValues) * 1.08;
  const thetaMin = thetaValues[0];
  const thetaMax = thetaValues[thetaValues.length - 1];
  const x = (theta: number) => left + (thetaMax === thetaMin ? 0 : ((theta - thetaMin) / (thetaMax - thetaMin)) * plotWidth);
  const y = (value: number) => top + plotHeight - (value / yMax) * plotHeight;
  const yTicks = Array.from({ length: 6 }, (_, index) => (yMax * index) / 5);
  return <article className="final-card wide analysis-chart" data-testid={testId}>
    <div className="section-heading"><div><h3>{title}</h3><p className="muted">横轴固定使用构造目标账户写偏斜 θ（target_account_write_theta）；单个随机种子时只跨重复运行统计，多个随机种子时先计算各随机种子的重复均值，再对这些均值计算 Student-t 95% 置信区间。{metric === "tps" ? " ○* 表示运行达到 60 分钟 hard timeout 后，尾部稳定性通过门禁并外推得到的预计完整工作负载 TPS；原始 child 仍保持失败/超时状态。" : " 延迟只使用完整实测样本，超时外推样本不会用于 P99。"}</p></div></div>
    <div className="table-wrap">
      <svg data-testid={`${testId}-svg`} viewBox={`0 0 ${width} ${height}`} role="img" aria-label={`${title} line chart`} style={{ width: "100%", minWidth: 720 }}>
        {yTicks.map((tick) => <g key={tick}>
          <line x1={left} x2={width - right} y1={y(tick)} y2={y(tick)} stroke="currentColor" opacity="0.12" />
          <text x={left - 10} y={y(tick) + 4} textAnchor="end" fontSize="12">{formatTick(tick)}</text>
        </g>)}
        <line x1={left} x2={left} y1={top} y2={top + plotHeight} stroke="currentColor" opacity="0.6" />
        <line x1={left} x2={width - right} y1={top + plotHeight} y2={top + plotHeight} stroke="currentColor" opacity="0.6" />
        {thetaValues.map((theta) => <g key={theta}>
          <line x1={x(theta)} x2={x(theta)} y1={top + plotHeight} y2={top + plotHeight + 5} stroke="currentColor" />
          <text x={x(theta)} y={top + plotHeight + 22} textAnchor="middle" fontSize="12">{theta.toFixed(1)}</text>
        </g>)}
        <text x={left + plotWidth / 2} y={height - 12} textAnchor="middle" fontSize="13">目标账户写偏斜 θ</text>
        <text x="16" y={top + plotHeight / 2} transform={`rotate(-90 16 ${top + plotHeight / 2})`} textAnchor="middle" fontSize="13">{unit}</text>
        {series.map((item, seriesIndex) => {
          const pointByTheta = new Map(item.points.map((point) => [point.theta, point]));
          const segments: MetricPoint[][] = [];
          let current: MetricPoint[] = [];
          for (const theta of thetaValues) {
            const point = pointByTheta.get(theta);
            if (point) current.push(point);
            else if (current.length) { segments.push(current); current = []; }
          }
          if (current.length) segments.push(current);
          const color = palette[seriesIndex % palette.length];
          return <g key={item.methodId} data-method-id={item.methodId}>
            {segments.filter((segment) => segment.length > 1).map((segment, index) => <polyline key={index} points={segment.map((point) => `${x(point.theta)},${y(point.value)}`).join(" ")} fill="none" stroke={color} strokeWidth="2.5" />)}
            {item.points.map((point) => {
              const estimated = point.timeoutExtrapolatedCount > 0;
              return <g key={point.theta} data-testid={estimated ? "v5-timeout-extrapolated-marker" : undefined}>
                {point.ciLow !== null && point.ciHigh !== null && <><line x1={x(point.theta)} x2={x(point.theta)} y1={y(Math.min(point.ciHigh, yMax))} y2={y(Math.max(0, point.ciLow))} stroke={color} strokeWidth="1.3" /><line x1={x(point.theta) - 5} x2={x(point.theta) + 5} y1={y(Math.min(point.ciHigh, yMax))} y2={y(Math.min(point.ciHigh, yMax))} stroke={color} /><line x1={x(point.theta) - 5} x2={x(point.theta) + 5} y1={y(Math.max(0, point.ciLow))} y2={y(Math.max(0, point.ciLow))} stroke={color} /></>}
                <circle cx={x(point.theta)} cy={y(point.value)} r={estimated ? "5" : "4"} fill={estimated ? "none" : color} stroke={color} strokeWidth={estimated ? "2.2" : "1"} />
                {estimated && <text x={x(point.theta) + 7} y={y(point.value) - 7} fontSize="13" fontWeight="700" fill={color}>*</text>}
                <title>{`${item.methodName} θ=${point.theta.toFixed(1)} ${unit}=${point.value.toFixed(2)} 样本数=${point.sampleCount} 种子数=${point.seedCount}${estimated ? ` 超时外推样本=${point.timeoutExtrapolatedCount}` : ""}${point.ciLow === null ? "" : ` 95%CI=[${point.ciLow.toFixed(2)}, ${point.ciHigh?.toFixed(2)}]`}`}</title>
              </g>;
            })}
            {thetaValues.filter((theta) => !pointByTheta.has(theta)).map((theta) => <g key={`missing-${theta}`}>
              <text x={x(theta)} y={top + plotHeight - 6 - (seriesIndex % 5) * 11} textAnchor="middle" fontSize="14" fill={color}>×</text>
              <title>{`${item.methodName} θ=${theta.toFixed(1)} missing/failed/invalid`}</title>
            </g>)}
          </g>;
        })}
      </svg>
    </div>
    <div className="button-row" data-testid={`${testId}-legend`}>
      {series.map((item, index) => <span key={item.methodId}><strong style={{ color: palette[index % palette.length] }}>●</strong> {item.methodName}</span>)}
      {metric === "tps" && <span data-testid="v5-timeout-extrapolation-legend"><strong>○*</strong> 60min 超时后稳定尾部外推</span>}
      <span><strong>×</strong> 缺失 / 失败 / 无效</span>
    </div>
  </article>;
}

function shortMethodName(methodId: string, value: string): string {
  const id = methodId.toLowerCase();
  if (id === "hash_serial") return "Serial";
  if (id === "hash_block_stm") return "Block-STM";
  if (id === "hash_aria") return "Aria";
  if (id === "hash_groundhog") return "Groundhog";
  if (id === "hash_cg") return "CG/Nezha";
  if (id === "hash_acg") return "ACG/Nezha";
  if (id === "hash_bsx") return "BSX";
  if (id === "hash_batch_si") return "Batch-SI";
  return value;
}

function formatTick(value: number): string {
  if (value >= 1000000) return `${(value / 1000000).toFixed(1)}M`;
  if (value >= 1000) return `${(value / 1000).toFixed(value >= 10000 ? 0 : 1)}K`;
  return value.toFixed(value >= 100 ? 0 : 1);
}
