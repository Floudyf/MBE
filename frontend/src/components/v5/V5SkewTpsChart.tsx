import type { V5FormalChildRun } from "../../api";

const palette = ["#2563eb", "#dc2626", "#059669", "#7c3aed", "#d97706", "#0891b2", "#be185d", "#4f46e5", "#65a30d"];
type MetricKind = "tps" | "p99";
type MetricPoint = { theta: number; value: number; sampleCount: number };
type MetricSeries = { methodId: string; methodName: string; points: MetricPoint[] };

export default function V5SkewTpsChart({ children, plannedThetaValues = [] }: { children: V5FormalChildRun[]; plannedThetaValues?: number[] }) {
  const methodEntries = [...new Map(children.map((child) => {
    const methodId = child.method_config_id || child.method.method_id;
    return [methodId, { methodId, methodName: child.method.display_name || methodId }] as const;
  })).values()].sort((left, right) => left.methodName.localeCompare(right.methodName));
  const thetaValues = [...new Set([
    ...plannedThetaValues.filter((value) => Number.isFinite(value)),
    ...children.map((child) => finiteNumber(child.workload_point?.target_theta)).filter((value): value is number => value !== null),
  ])].sort((left, right) => left - right);
  if (thetaValues.length < 2 || !methodEntries.length) return null;
  const tpsSeries = buildMetricSeries(children, methodEntries, "tps");
  const p99Series = buildMetricSeries(children, methodEntries, "p99");
  return <>
    <ThetaMetricChart testId="v5-skew-tps-chart" title="不同偏斜度下各方案 TPS" unit="TPS" series={tpsSeries} thetaValues={thetaValues} />
    <ThetaMetricChart testId="v5-skew-p99-chart" title="不同偏斜度下各方案 P99 Finality" unit="ms" series={p99Series} thetaValues={thetaValues} />
  </>;
}

function buildMetricSeries(children: V5FormalChildRun[], methods: Array<{ methodId: string; methodName: string }>, metric: MetricKind): MetricSeries[] {
  const buckets = new Map<string, Map<number, number[]>>();
  for (const child of children) {
    const execution = child.execution_status ?? child.result?.summary?.execution_status ?? child.status;
    const eligible = child.formal_eligibility ?? child.result?.summary?.formal_eligibility;
    if (child.status !== "completed" || execution !== "completed" || eligible !== true) continue;
    const theta = finiteNumber(child.workload_point?.target_theta);
    const value = metricValue(child, metric);
    if (theta === null || value === null) continue;
    const methodId = child.method_config_id || child.method.method_id;
    const byTheta = buckets.get(methodId) ?? new Map<number, number[]>();
    const values = byTheta.get(theta) ?? [];
    values.push(value);
    byTheta.set(theta, values);
    buckets.set(methodId, byTheta);
  }
  return methods.map((method) => ({
    ...method,
    points: [...(buckets.get(method.methodId) ?? new Map<number, number[]>()).entries()]
      .map(([theta, values]) => ({ theta, value: values.reduce((sum, current) => sum + current, 0) / values.length, sampleCount: values.length }))
      .sort((left, right) => left.theta - right.theta),
  }));
}

function metricValue(child: V5FormalChildRun, metric: MetricKind): number | null {
  if (metric === "tps") return finiteNumber(child.metrics?.end_to_end_tps ?? child.result?.summary?.finality_evidence?.end_to_end_tps ?? child.metrics?.throughput_tps);
  return finiteNumber(child.metrics?.p99_finality_ms ?? child.result?.summary?.finality_evidence?.p99_finality_ms ?? child.metrics?.p99_latency_ms);
}

function finiteNumber(value: unknown): number | null {
  const numeric = typeof value === "number" ? value : Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function ThetaMetricChart({ testId, title, unit, series, thetaValues }: { testId: string; title: string; unit: string; series: MetricSeries[]; thetaValues: number[] }) {
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
    <div className="section-heading"><div><h3>{title}</h3><p className="muted">横轴固定使用正式计划中的目标偏斜度 θ；每个 θ 内仅对 repeats/seeds 聚合。圆点表示正式有效样本，缺失 / 失败 / 无效点以 × 标记并保持断线。</p></div></div>
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
        <text x={left + plotWidth / 2} y={height - 12} textAnchor="middle" fontSize="13">偏斜度 θ</text>
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
            {item.points.map((point) => <g key={point.theta}><circle cx={x(point.theta)} cy={y(point.value)} r="4" fill={color} /><title>{`${item.methodName} θ=${point.theta.toFixed(1)} ${unit}=${point.value.toFixed(2)} n=${point.sampleCount}`}</title></g>)}
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
      <span><strong>×</strong> 缺失 / 失败 / 无效</span>
    </div>
  </article>;
}

function formatTick(value: number): string {
  if (value >= 1000000) return `${(value / 1000000).toFixed(1)}M`;
  if (value >= 1000) return `${(value / 1000).toFixed(value >= 10000 ? 0 : 1)}K`;
  return value.toFixed(value >= 100 ? 0 : 1);
}
