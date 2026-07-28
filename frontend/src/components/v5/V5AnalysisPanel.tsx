import type { V5FormalAnalysis, V5PaperMetricRow, V5PaperResultAnalysis } from "../../api";

const width = 520;
const height = 260;
const chartPadding = { top: 26, right: 24, bottom: 64, left: 58 };
const methodOrder = ["hash_serial", "hash_block_stm", "metatrack_serial", "metatrack_block_stm"];
const methodColors: Record<string, string> = {
  hash_serial: "#6382AA",
  hash_block_stm: "#BE9A62",
  metatrack_serial: "#FA8095",
  metatrack_block_stm: "#56B76A",
};

export default function V5AnalysisPanel({ analysis }: { analysis: V5FormalAnalysis | null }) {
  if (!analysis) return null;
  if (analysis.paper_result_analysis) return <PaperAnalysisPanel analysis={analysis.paper_result_analysis} />;
  if (!analysis.charts.length || !analysis.groups.length) return <article className="final-card wide"><h2>实验分析</h2><p>当前实验类型没有可绘制的分组数据。</p></article>;
  return <article className="final-card wide" data-testid="v5-analysis-panel"><h2>实验分析</h2><p className="muted">图表和下方数据表都来自后端按真实子实验分组的结果。</p>{analysis.charts.map((chart, index) => <LegacyChart key={`${chart.kind}-${chart.suite_type}-${index}`} chart={chart} />)}<LegacyAnalysisTable groups={analysis.groups} /></article>;
}

function PaperAnalysisPanel({ analysis }: { analysis: V5PaperResultAnalysis }) {
  const tps = sortedRows(analysis.metrics.end_to_end_tps ?? []);
  const p99 = sortedRows(analysis.metrics.p99_finality_ms ?? []);
  return <article className="final-card wide" data-testid="v5-analysis-panel">
    <h2>Paper Result Analysis</h2>
    <p className="muted">正式主图只使用通过 fairness 和 metric completeness gate 的样本；单样本显示 n=1，不绘制虚假置信区间。</p>
    <div className="analysis-paper-grid">
      <PaperBarChart title="End-to-End TPS" rows={tps} metric="end_to_end_tps" unit="TPS" higherBetter />
      <PaperBarChart title="P99 Finality Latency" rows={p99} metric="p99_finality_ms" unit="ms" />
    </div>
    <details>
      <summary>查看分析数据</summary>
      <PaperAnalysisTable rows={[...tps, ...p99]} />
      {analysis.excluded_samples.length > 0 && <p className="file-error">Excluded samples: {analysis.excluded_samples.length}. Download JSON/CSV artifacts for exact reasons.</p>}
    </details>
  </article>;
}

function PaperBarChart({ title, rows, metric, unit, higherBetter = false }: { title: string; rows: V5PaperMetricRow[]; metric: string; unit: string; higherBetter?: boolean }) {
  const values = rows.map((row) => numeric(row.mean));
  const max = Math.max(...values, 1);
  const innerWidth = width - chartPadding.left - chartPadding.right;
  const innerHeight = height - chartPadding.top - chartPadding.bottom;
  const slot = rows.length ? innerWidth / rows.length : innerWidth;
  const svgId = `v5-paper-chart-${metric}`;
  return <section className="analysis-chart" data-testid={svgId}>
    <div className="section-heading"><div><h3>{title}</h3><p className="muted">{higherBetter ? "Higher is better" : "Lower is better"} · unit: {unit}</p></div></div>
    {!rows.length ? <p className="file-error">数据不完整，无法绘制正式主图。</p> : <svg id={svgId} data-testid={`${svgId}-svg`} role="img" aria-label={`${title} chart`} viewBox={`0 0 ${width} ${height}`}>
      <title>{title}</title>
      <rect x="0" y="0" width={width} height={height} fill="#ffffff" />
      <Axes max={max} unit={unit} />
      {rows.map((row, index) => {
        const value = values[index];
        const barHeight = value / max * innerHeight;
        const x = chartPadding.left + index * slot + slot * 0.22;
        const y = chartPadding.top + innerHeight - barHeight;
        const color = methodColors[row.method_id] ?? "#6382AA";
        return <g key={row.method_id}>
          <rect x={x} y={y} width={slot * 0.56} height={barHeight} rx="2" fill={color} />
          <text x={x + slot * 0.28} y={Math.max(14, y - 6)} textAnchor="middle" fontSize="11" fill="#243044">{format(value)}</text>
          <text x={x + slot * 0.28} y={height - 34} textAnchor="middle" fontSize="10" fill="#243044">{shortName(row.method_name)}</text>
          <text x={x + slot * 0.28} y={height - 18} textAnchor="middle" fontSize="10" fill="#667085">{row.valid_sample_count === 1 ? "n=1" : `n=${row.valid_sample_count}`}</text>
        </g>;
      })}
    </svg>}
    <div className="button-row">
      <button type="button" className="ghost-button" onClick={() => downloadCSV(`${metric}.csv`, rows)}>Download CSV</button>
      <button type="button" className="ghost-button" onClick={() => downloadSVG(`${metric}.svg`, svgId)}>Download SVG</button>
      <button type="button" className="ghost-button" onClick={() => void downloadPNG(`${metric}.png`, svgId)}>Download PNG</button>
      <button type="button" className="ghost-button" onClick={() => downloadPDF(`${metric}.pdf`, title, rows)}>Download PDF</button>
    </div>
  </section>;
}

function PaperAnalysisTable({ rows }: { rows: V5PaperMetricRow[] }) {
  return <div className="table-wrap"><table data-testid="v5-paper-analysis-table"><thead><tr><th>Metric</th><th>Method</th><th>Samples</th><th>Mean</th><th>Median</th><th>Std</th><th>CI95</th><th>Note</th></tr></thead><tbody>{rows.map((row) => <tr key={`${row.metric}-${row.method_id}`}><td>{row.metric}</td><td>{row.method_name}</td><td>{row.valid_sample_count}</td><td>{format(row.mean)}</td><td>{format(row.median)}</td><td>{format(row.std)}</td><td>{format(row.ci95_low)} - {format(row.ci95_high)}</td><td>{row.statistical_note}</td></tr>)}</tbody></table></div>;
}

type AnalysisRow = Record<string, unknown>;
type AnalysisChart = { suite_type: string; kind: "summary" | "bar" | "line"; rows: AnalysisRow[] };

function LegacyChart({ chart }: { chart: AnalysisChart }) {
  const title = chartTitle(chart);
  if (chart.kind === "summary") return <section className="analysis-summary"><h3>{title}</h3><p>当前实验只有摘要数据，不绘制虚假趋势线。</p></section>;
  if (!chart.rows.length) return <section><h3>{title}</h3><p>当前实验类型没有可绘制的分组数据。</p></section>;
  return <section className="analysis-chart" data-testid="v5-analysis-chart"><h3>{title}</h3><svg data-testid="v5-analysis-bar-chart" role="img" aria-label={`${title} TPS 柱状图`} viewBox={`0 0 ${width} ${height}`}><title>{title} TPS 柱状图</title><Axes max={Math.max(...chart.rows.map((row) => numeric(row.mean_tps)), 1)} unit="TPS" /></svg></section>;
}

function Axes({ max, unit }: { max: number; unit: string }) {
  const innerHeight = height - chartPadding.top - chartPadding.bottom;
  return <><line x1={chartPadding.left} y1={chartPadding.top} x2={chartPadding.left} y2={chartPadding.top + innerHeight} stroke="#D0D5DD" /><line x1={chartPadding.left} y1={chartPadding.top + innerHeight} x2={width - chartPadding.right} y2={chartPadding.top + innerHeight} stroke="#D0D5DD" /><line x1={chartPadding.left} y1={chartPadding.top + innerHeight / 2} x2={width - chartPadding.right} y2={chartPadding.top + innerHeight / 2} stroke="#EAECF0" /><text x="8" y={chartPadding.top + 8} fontSize="11" fill="#667085">{unit}</text><text x="8" y={chartPadding.top + innerHeight / 2 + 4} fontSize="10" fill="#98A2B3">{format(max / 2)}</text><text x="8" y={chartPadding.top + innerHeight + 4} fontSize="10" fill="#98A2B3">0</text></>;
}

function LegacyAnalysisTable({ groups }: { groups: AnalysisRow[] }) { return <div className="table-wrap"><table data-testid="v5-analysis-table"><thead><tr><th>方法</th><th>扫描点</th><th>样本</th><th>平均 TPS</th><th>P99</th><th>95% CI</th></tr></thead><tbody>{groups.map((group, index) => <tr key={`${String(group.method_id)}-${String(group.scan_value)}-${index}`}><td>{String(group.method_name ?? "—")}</td><td>{String(group.scan_value ?? "—")}</td><td>{String(group.sample_count ?? "—")}</td><td>{format(group.mean_tps)}</td><td>{format(group.mean_p99_ms)}</td><td>{format(group.ci95_low_tps)} - {format(group.ci95_high_tps)}</td></tr>)}</tbody></table></div>; }

function sortedRows(rows: V5PaperMetricRow[]): V5PaperMetricRow[] { return [...rows].sort((a, b) => orderOf(a.method_id) - orderOf(b.method_id)); }
function orderOf(methodId: string): number { const index = methodOrder.indexOf(methodId); return index >= 0 ? index : methodOrder.length; }
function chartTitle(chart: AnalysisChart) { return chart.kind === "bar" ? "方法性能对比" : chart.kind === "line" ? "扫描点性能趋势" : "实验摘要"; }
function numeric(value: unknown) { const result = Number(value); return Number.isFinite(result) ? result : 0; }
function format(value: unknown) { if (value === null || value === undefined || value === "") return "—"; const number = Number(value); return Number.isFinite(number) ? number.toFixed(2) : "—"; }
function shortName(value: string): string { return value.replace("MetaTrack with Block-STM backend", "Combined").replace("Block-STM", "B-STM"); }

function downloadCSV(filename: string, rows: V5PaperMetricRow[]) {
  const header = ["method_id", "method_name", "valid_sample_count", "excluded_sample_count", "mean", "median", "std", "min", "max", "ci95_low", "ci95_high", "statistical_note"];
  const lines = [header.join(","), ...rows.map((row) => header.map((key) => csvCell((row as unknown as Record<string, unknown>)[key])).join(","))];
  downloadBlob(filename, "text/csv;charset=utf-8", `${lines.join("\n")}\n`);
}
function downloadSVG(filename: string, svgId: string) { const svg = document.getElementById(svgId); if (svg) downloadBlob(filename, "image/svg+xml;charset=utf-8", svg.outerHTML); }
async function downloadPNG(filename: string, svgId: string) {
  const svg = document.getElementById(svgId);
  if (!svg) return;
  const image = new Image();
  const url = URL.createObjectURL(new Blob([svg.outerHTML], { type: "image/svg+xml;charset=utf-8" }));
  await new Promise<void>((resolve, reject) => { image.onload = () => resolve(); image.onerror = reject; image.src = url; });
  const canvas = document.createElement("canvas");
  canvas.width = width; canvas.height = height;
  canvas.getContext("2d")?.drawImage(image, 0, 0);
  URL.revokeObjectURL(url);
  const blob = await new Promise<Blob | null>((resolve) => canvas.toBlob(resolve, "image/png"));
  if (blob) downloadObjectURL(filename, blob);
}
function downloadPDF(filename: string, title: string, rows: V5PaperMetricRow[]) {
  const body = `${title}\n\n${rows.map((row) => `${row.method_name}: ${format(row.mean)} (${row.statistical_note})`).join("\n")}`;
  downloadBlob(filename, "application/pdf", minimalPDF(body));
}
function minimalPDF(text: string): string {
  const escaped = text.replace(/[()\\]/g, "\\$&").replace(/\r?\n/g, ") Tj T* (");
  const stream = `BT /F1 12 Tf 50 760 Td (${escaped}) Tj ET`;
  return `%PDF-1.4\n1 0 obj<< /Type /Catalog /Pages 2 0 R >>endobj\n2 0 obj<< /Type /Pages /Kids [3 0 R] /Count 1 >>endobj\n3 0 obj<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources<< /Font<< /F1 4 0 R >> >> /Contents 5 0 R >>endobj\n4 0 obj<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>endobj\n5 0 obj<< /Length ${stream.length} >>stream\n${stream}\nendstream endobj\ntrailer<< /Root 1 0 R >>\n%%EOF\n`;
}
function csvCell(value: unknown): string { return `"${String(value ?? "").replace(/"/g, "\"\"")}"`; }
function downloadBlob(filename: string, type: string, content: string) { downloadObjectURL(filename, new Blob([content], { type })); }
function downloadObjectURL(filename: string, blob: Blob) { const url = URL.createObjectURL(blob); const link = document.createElement("a"); link.href = url; link.download = filename; link.click(); URL.revokeObjectURL(url); }
