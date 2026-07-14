import type { V5FormalAnalysis } from "../../api";

const width = 520;
const height = 220;
const chartPadding = { top: 24, right: 24, bottom: 54, left: 52 };

export default function V5AnalysisPanel({ analysis }: { analysis: V5FormalAnalysis | null }) {
  if (!analysis) return null;
  if (!analysis.charts.length || !analysis.groups.length) return <article className="final-card wide"><h2>实验分析</h2><p>当前实验类型没有可绘制的分组数据。</p></article>;
  return <article className="final-card wide" data-testid="v5-analysis-panel"><h2>实验分析</h2><p className="muted">图表和下方数据表都来自后端按真实子实验分组的结果。</p>{analysis.charts.map((chart, index) => <Chart key={`${chart.kind}-${chart.suite_type}-${index}`} chart={chart} />)}<AnalysisTable groups={analysis.groups} /></article>;
}

type AnalysisRow = Record<string, unknown>;
type AnalysisChart = { suite_type: string; kind: "summary" | "bar" | "line"; rows: AnalysisRow[] };

function Chart({ chart }: { chart: AnalysisChart }) {
  const title = chartTitle(chart);
  if (chart.kind === "summary") return <section className="analysis-summary"><h3>{title}</h3><p>当前实验只有摘要数据，不绘制虚假趋势线。</p></section>;
  if (!chart.rows.length) return <section><h3>{title}</h3><p>当前实验类型没有可绘制的分组数据。</p></section>;
  return chart.kind === "bar" ? <BarChart chart={chart} /> : <LineChart chart={chart} />;
}

function BarChart({ chart }: { chart: AnalysisChart }) {
  const title = chartTitle(chart);
  const values = chart.rows.map((row) => numeric(row.mean_tps));
  const max = Math.max(...values, 1);
  const innerWidth = width - chartPadding.left - chartPadding.right;
  const innerHeight = height - chartPadding.top - chartPadding.bottom;
  const slot = innerWidth / chart.rows.length;
  return <section className="analysis-chart" data-testid="v5-analysis-chart"><h3>{title}</h3><svg data-testid="v5-analysis-bar-chart" role="img" aria-label={`${title} TPS 柱状图`} viewBox={`0 0 ${width} ${height}`}><title>{title} TPS 柱状图</title><Axes max={max} />{chart.rows.map((row, index) => { const value = values[index]; const barHeight = value / max * innerHeight; const x = chartPadding.left + index * slot + slot * 0.2; const y = chartPadding.top + innerHeight - barHeight; return <g key={`${String(row.method_id)}-${index}`}><rect x={x} y={y} width={slot * 0.6} height={barHeight} /><text x={x + slot * 0.3} y={y - 5} textAnchor="middle">{format(value)}</text><text x={x + slot * 0.3} y={height - 24} textAnchor="middle">{String(row.method_name || row.method_id || "方法")}</text></g>; })}</svg></section>;
}

function LineChart({ chart }: { chart: AnalysisChart }) {
  const title = chartTitle(chart);
  const rows = [...chart.rows].sort(compareScanValue);
  const values = rows.map((row) => numeric(row.mean_tps));
  const max = Math.max(...values, 1);
  const innerWidth = width - chartPadding.left - chartPadding.right;
  const innerHeight = height - chartPadding.top - chartPadding.bottom;
  const points = rows.map((row, index) => ({ row, value: values[index], x: chartPadding.left + (rows.length === 1 ? innerWidth / 2 : index / (rows.length - 1) * innerWidth), y: chartPadding.top + innerHeight - values[index] / max * innerHeight }));
  return <section className="analysis-chart" data-testid="v5-analysis-chart"><h3>{title}</h3><svg data-testid="v5-analysis-line-chart" role="img" aria-label={`${title} TPS 折线图`} viewBox={`0 0 ${width} ${height}`}><title>{title} TPS 折线图</title><Axes max={max} /><polyline fill="none" points={points.map((point) => `${point.x},${point.y}`).join(" ")} />{points.map((point, index) => <g key={`${String(point.row.scan_value)}-${index}`}><circle cx={point.x} cy={point.y} r="4" /><text x={point.x} y={point.y - 8} textAnchor="middle">{format(point.value)}</text><text x={point.x} y={height - 24} textAnchor="middle">{String(point.row.scan_value ?? "—")}</text></g>)}</svg></section>;
}

function Axes({ max }: { max: number }) { return <><line x1={chartPadding.left} y1={chartPadding.top} x2={chartPadding.left} y2={height - chartPadding.bottom} /><line x1={chartPadding.left} y1={height - chartPadding.bottom} x2={width - chartPadding.right} y2={height - chartPadding.bottom} /><text x="8" y={chartPadding.top + 8}>TPS</text><text x="8" y={height - chartPadding.bottom}>{format(max)}</text></>; }

function AnalysisTable({ groups }: { groups: AnalysisRow[] }) { return <div className="table-wrap"><table data-testid="v5-analysis-table"><thead><tr><th>方法</th><th>扫描点</th><th>样本</th><th>平均 TPS</th><th>P99</th><th>95% CI</th></tr></thead><tbody>{groups.map((group, index) => <tr key={`${String(group.method_id)}-${String(group.scan_value)}-${index}`}><td>{String(group.method_name ?? "—")}</td><td>{String(group.scan_value ?? "—")}</td><td>{String(group.sample_count ?? "—")}</td><td>{format(group.mean_tps)}</td><td>{format(group.mean_p99_ms)}</td><td>{format(group.ci95_low_tps)} - {format(group.ci95_high_tps)}</td></tr>)}</tbody></table></div>; }

function compareScanValue(a: AnalysisRow, b: AnalysisRow) { const left = Number(a.scan_value); const right = Number(b.scan_value); return Number.isFinite(left) && Number.isFinite(right) ? left - right : 0; }
function chartTitle(chart: AnalysisChart) { return chart.kind === "bar" ? "方法性能对比" : chart.kind === "line" ? "扫描点性能趋势" : "实验摘要"; }
function numeric(value: unknown) { const result = Number(value); return Number.isFinite(result) ? result : 0; }
function format(value: unknown) { const number = numeric(value); return Number.isFinite(number) ? number.toFixed(2) : "—"; }
