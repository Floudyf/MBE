import type { V5FormalChildRun } from "../../api";
import { buildThetaTpsSeries } from "../../v5SkewExperiment";

const palette = ["#2563eb", "#dc2626", "#059669", "#7c3aed", "#d97706", "#0891b2", "#be185d", "#4f46e5", "#65a30d"];

export default function V5SkewTpsChart({ children }: { children: V5FormalChildRun[] }) {
  const series = buildThetaTpsSeries(children);
  const thetaValues = [...new Set(series.flatMap((item) => item.points.map((point) => point.theta)))].sort((a, b) => a - b);
  if (thetaValues.length < 2 || !series.length) return null;

  const width = 960;
  const height = 420;
  const left = 72;
  const right = 24;
  const top = 24;
  const bottom = 62;
  const plotWidth = width - left - right;
  const plotHeight = height - top - bottom;
  const allTPS = series.flatMap((item) => item.points.map((point) => point.tps));
  const yMax = Math.max(1, ...allTPS) * 1.08;
  const thetaMin = thetaValues[0];
  const thetaMax = thetaValues[thetaValues.length - 1];
  const x = (theta: number) => left + (thetaMax === thetaMin ? 0 : ((theta - thetaMin) / (thetaMax - thetaMin)) * plotWidth);
  const y = (tps: number) => top + plotHeight - (tps / yMax) * plotHeight;
  const yTicks = Array.from({ length: 6 }, (_, index) => (yMax * index) / 5);

  return <article className="final-card wide analysis-chart" data-testid="v5-skew-tps-chart">
    <div className="section-heading"><div><h3>不同偏斜度下各方案 TPS</h3><p className="muted">横轴为目标偏斜度 θ；每个方案一条线。仅使用执行完成且 formal_eligibility=true 的 child；缺失点保持断线。</p></div></div>
    <div className="table-wrap">
      <svg data-testid="v5-skew-tps-chart-svg" viewBox={`0 0 ${width} ${height}`} role="img" aria-label="TPS vs skew theta line chart" style={{ width: "100%", minWidth: 720 }}>
        {yTicks.map((tick) => <g key={tick}>
          <line x1={left} x2={width - right} y1={y(tick)} y2={y(tick)} stroke="currentColor" opacity="0.12" />
          <text x={left - 10} y={y(tick) + 4} textAnchor="end" fontSize="12">{tick.toFixed(tick >= 100 ? 0 : 1)}</text>
        </g>)}
        <line x1={left} x2={left} y1={top} y2={top + plotHeight} stroke="currentColor" opacity="0.6" />
        <line x1={left} x2={width - right} y1={top + plotHeight} y2={top + plotHeight} stroke="currentColor" opacity="0.6" />
        {thetaValues.map((theta) => <g key={theta}>
          <line x1={x(theta)} x2={x(theta)} y1={top + plotHeight} y2={top + plotHeight + 5} stroke="currentColor" />
          <text x={x(theta)} y={top + plotHeight + 22} textAnchor="middle" fontSize="12">{theta.toFixed(1)}</text>
        </g>)}
        <text x={left + plotWidth / 2} y={height - 12} textAnchor="middle" fontSize="13">偏斜度 θ</text>
        <text x="16" y={top + plotHeight / 2} transform={`rotate(-90 16 ${top + plotHeight / 2})`} textAnchor="middle" fontSize="13">TPS</text>
        {series.map((item, seriesIndex) => {
          const pointByTheta = new Map(item.points.map((point) => [point.theta, point]));
          const segments: typeof item.points[] = [];
          let current: typeof item.points = [];
          for (const theta of thetaValues) {
            const point = pointByTheta.get(theta);
            if (point) current.push(point);
            else if (current.length) { segments.push(current); current = []; }
          }
          if (current.length) segments.push(current);
          const color = palette[seriesIndex % palette.length];
          return <g key={item.methodId} data-method-id={item.methodId}>
            {segments.filter((segment) => segment.length > 1).map((segment, index) => <polyline key={index} points={segment.map((point) => `${x(point.theta)},${y(point.tps)}`).join(" ")} fill="none" stroke={color} strokeWidth="2.5" />)}
            {item.points.map((point) => <g key={point.theta}><circle cx={x(point.theta)} cy={y(point.tps)} r="4" fill={color} /><title>{`${item.methodName} θ=${point.theta.toFixed(1)} TPS=${point.tps.toFixed(2)} n=${point.sampleCount}`}</title></g>)}
          </g>;
        })}
      </svg>
    </div>
    <div className="button-row" data-testid="v5-skew-tps-legend">
      {series.map((item, index) => <span key={item.methodId}><strong style={{ color: palette[index % palette.length] }}>●</strong> {item.methodName}</span>)}
    </div>
  </article>;
}
