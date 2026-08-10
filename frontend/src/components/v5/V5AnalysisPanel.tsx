import { useState, type ReactNode } from "react";

import type { V5FormalAnalysis, V5PaperMetricRow, V5PaperResultAnalysis } from "../../api";

const baseWidth = 520;
const height = 286;
const chartPadding = { top: 30, right: 24, bottom: 88, left: 58 };
const methodOrder = [
  "hash_serial",
  "hash_block_stm",
  "hash_aria",
  "hash_groundhog",
  "hash_batch_si",
  "stateless_hash_serial",
  "stateless_hash_block_stm",
  "metatrack_serial",
  "metatrack_block_stm",
];
const methodColors: Record<string, string> = {
  hash_serial: "#6382AA",
  hash_block_stm: "#BE9A62",
  hash_aria: "#7C6BB0",
  hash_groundhog: "#A56B46",
  hash_batch_si: "#8B5FBF",
  stateless_hash_serial: "#3E9AA5",
  stateless_hash_block_stm: "#D1784A",
  metatrack_serial: "#FA8095",
  metatrack_block_stm: "#56B76A",
};
const methodLabels: Record<string, string[]> = {
  hash_serial: ["Stateful Hash", "Serial"],
  hash_block_stm: ["Stateful Hash", "Block-STM"],
  hash_aria: ["Stateful Hash", "Aria"],
  hash_groundhog: ["Stateful Hash", "Groundhog"],
  hash_batch_si: ["Stateful Hash", "Batch-SI"],
  stateless_hash_serial: ["Stateless Hash", "Serial"],
  stateless_hash_block_stm: ["Stateless Hash", "Block-STM"],
  metatrack_serial: ["MetaTrack", "Serial"],
  metatrack_block_stm: ["MetaTrack", "Block-STM"],
};
type AnalysisView = "observed" | "paper";
type SampleStatus = "paper_eligible" | "comparison_excluded" | "completed_invalid" | "blocked_incompatible" | "execution_failed";

export default function V5AnalysisPanel({ analysis }: { analysis: V5FormalAnalysis | null }) {
  if (!analysis) return null;
  const workloadSensitivity = analysis.groups.some((row) => row.suite_type === "workload_sensitivity");
  if (analysis.paper_result_analysis && workloadSensitivity) return <SensitivityAnalysisNotice analysis={analysis.paper_result_analysis} />;
  if (analysis.paper_result_analysis) return <PaperAnalysisPanel analysis={analysis.paper_result_analysis} />;
  if (!analysis.charts.length || !analysis.groups.length) return <article className="final-card wide"><h2>实验分析</h2><p>当前实验类型没有可绘制的分组数据。</p></article>;
  return <article className="final-card wide" data-testid="v5-analysis-panel"><h2>实验分析</h2><p className="muted">图表和下方数据表都来自后端按真实子实验分组的结果。</p>{analysis.charts.map((chart, index) => <LegacyChart key={`${chart.kind}-${chart.suite_type}-${index}`} chart={chart} />)}<LegacyAnalysisTable groups={analysis.groups} /></article>;
}

function SensitivityAnalysisNotice({ analysis }: { analysis: V5PaperResultAnalysis }) {
  const counts = analysis.status_counts;
  return <article className="final-card wide" data-testid="v5-sensitivity-analysis-notice">
    <h2>偏斜度敏感性实验分析</h2>
    <p className="muted">不同 θ 是不同实验条件，不能合并成一个方法总平均或跨 θ 的 95% 置信区间。论文结果请使用下方 θ–TPS、θ–P99 曲线与 sensitivity_summary.csv；每个 θ 内仅对 repeats/seeds 聚合。</p>
    {counts && <div className="analysis-status-strip">
      <StatusCount label="执行失败" value={counts.execution_failed} tone="failed" />
      <StatusCount label="兼容性阻止" value={counts.blocked_incompatible} tone="blocked" />
      <StatusCount label="完成但无效" value={counts.completed_invalid} tone="invalid" />
      <StatusCount label="论文有效" value={counts.paper_eligible} tone="eligible" />
    </div>}
  </article>;
}

function PaperAnalysisPanel({ analysis }: { analysis: V5PaperResultAnalysis }) {
  const [view, setView] = useState<AnalysisView>("paper");
  const source = view === "observed" && analysis.observed_metrics ? analysis.observed_metrics : analysis.metrics;
  const tps = sortedRows(source.end_to_end_tps ?? []);
  const p95 = sortedRows(source.p95_finality_ms ?? []);
  const p99 = sortedRows(source.p99_finality_ms ?? []);
  const [latencyMetric, setLatencyMetric] = useState<"p95_finality_ms" | "p99_finality_ms">("p99_finality_ms");
  const latencyRows = latencyMetric === "p95_finality_ms" ? p95 : p99;
  const latencyTitle = latencyMetric === "p95_finality_ms" ? "P95 Finality Latency" : "P99 Finality Latency";
  const counts = analysis.status_counts;
  return <article className="final-card wide" data-testid="v5-analysis-panel">
    <div className="section-heading">
      <div><h2>Paper Result Analysis</h2><p className="muted">默认展示论文有效样本；“全部运行结果”保留兼容性阻止、完成但无效和执行失败的原始观察证据，并明确标注样本状态。</p></div>
      <div className="segmented-control" data-testid="v5-analysis-view-toggle">
        <button type="button" className={view === "observed" ? "active" : ""} onClick={() => setView("observed")}>全部运行结果</button>
        <button type="button" className={view === "paper" ? "active" : ""} onClick={() => setView("paper")}>论文有效样本</button>
      </div>
    </div>
    {counts && <div className="analysis-status-strip" data-testid="v5-analysis-status-counts">
      <StatusCount label="执行失败" value={counts.execution_failed} tone="failed" />
      <StatusCount label="兼容性阻止" value={counts.blocked_incompatible} tone="blocked" />
      <StatusCount label="完成但无效" value={counts.completed_invalid} tone="invalid" />
      <StatusCount label="有效但比较受限" value={counts.comparison_excluded} tone="excluded" />
      <StatusCount label="论文有效" value={counts.paper_eligible} tone="eligible" />
    </div>}
    <div className="analysis-status-legend" aria-label="sample status legend">
      <span className="eligible">论文有效</span><span className="excluded">有效但比较受限</span><span className="invalid">完成但结果无效</span><span className="blocked">兼容性阻止</span><span className="failed">执行失败</span>
    </div>
    <div className="analysis-paper-grid">
      <PaperBarChart title="End-to-End TPS" rows={tps} metric="end_to_end_tps" unit="TPS" higherBetter view={view} />
      <PaperBarChart title={latencyTitle} rows={latencyRows} metric={latencyMetric} unit="ms" view={view} controls={<div className="segmented-control" data-testid="v5-latency-percentile-toggle">
        <button type="button" className={latencyMetric === "p95_finality_ms" ? "active" : ""} onClick={() => setLatencyMetric("p95_finality_ms")}>P95</button>
        <button type="button" className={latencyMetric === "p99_finality_ms" ? "active" : ""} onClick={() => setLatencyMetric("p99_finality_ms")}>P99</button>
      </div>} />
    </div>
    <details>
      <summary>查看分析数据</summary>
      <PaperAnalysisTable rows={[...tps, ...p95, ...p99]} />
      {analysis.excluded_samples.length > 0 && <div className="analysis-exclusion-summary" data-testid="v5-analysis-exclusions"><strong>未进入论文有效样本：{analysis.excluded_samples.length}</strong><p>执行失败、完成但无效、比较受限分别统计，不能合并理解成“失败”。JSON/CSV工件保留每个样本的具体原因。</p></div>}
    </details>
  </article>;
}

function StatusCount({ label, value, tone }: { label: string; value: number; tone: string }) {
  return <div className={`analysis-status-count ${tone}`}><strong>{value}</strong><span>{label}</span></div>;
}

function PaperBarChart({ title, rows, metric, unit, higherBetter = false, controls, view }: { title: string; rows: V5PaperMetricRow[]; metric: string; unit: string; higherBetter?: boolean; controls?: ReactNode; view: AnalysisView }) {
  const numericValues = rows.map((row) => finiteNumber(row.mean)).filter((value): value is number => value !== null);
  const max = Math.max(...numericValues, 1);
  const chartWidth = Math.max(baseWidth, chartPadding.left + chartPadding.right + rows.length * 112);
  const innerWidth = chartWidth - chartPadding.left - chartPadding.right;
  const innerHeight = height - chartPadding.top - chartPadding.bottom;
  const slot = rows.length ? innerWidth / rows.length : innerWidth;
  const svgId = `v5-paper-chart-${metric}`;
  return <section className="analysis-chart" data-testid={svgId}>
    <div className="section-heading"><div><h3>{title}</h3><p className="muted">{higherBetter ? "Higher is better" : "Lower is better"} · unit: {unit} · {view === "observed" ? "observed runs" : "paper eligible"}</p></div>{controls}</div>
    {!rows.length ? <p className="file-error">当前视图没有可绘制样本。</p> : <div className="analysis-chart-scroll"><svg id={svgId} data-testid={`${svgId}-svg`} role="img" aria-label={`${title} chart`} viewBox={`0 0 ${chartWidth} ${height}`} data-chart-width={chartWidth}>
      <title>{title}</title>
      <defs><pattern id={`${svgId}-invalid`} width="8" height="8" patternUnits="userSpaceOnUse" patternTransform="rotate(45)"><rect width="8" height="8" fill="#F2F4F7" /><line x1="0" y1="0" x2="0" y2="8" stroke="#D92D20" strokeWidth="3" /></pattern></defs>
      <rect x="0" y="0" width={chartWidth} height={height} fill="#ffffff" />
      <Axes max={max} unit={unit} chartWidth={chartWidth} />
      {rows.map((row, index) => {
        const value = finiteNumber(row.mean);
        const status = rowStatus(row);
        const center = chartPadding.left + index * slot + slot / 2;
        const barWidth = Math.min(52, slot * 0.56);
        const x = center - barWidth / 2;
        const barHeight = value === null ? 0 : value / max * innerHeight;
        const y = chartPadding.top + innerHeight - barHeight;
        const color = methodColors[row.method_id] ?? "#6382AA";
        const labels = methodLabels[row.method_id] ?? [shortName(row.method_name)];
        return <g key={row.method_id} data-sample-status={status}>
          {value !== null && <rect x={x} y={y} width={barWidth} height={Math.max(barHeight, 1)} rx="3" fill={status === "completed_invalid" ? `url(#${svgId}-invalid)` : color} fillOpacity={status === "comparison_excluded" ? 0.62 : 1} stroke={status === "comparison_excluded" ? "#B54708" : status === "completed_invalid" ? "#D92D20" : "none"} strokeWidth="2" />}
          {value === null && <><line x1={center - 10} y1={chartPadding.top + innerHeight - 22} x2={center + 10} y2={chartPadding.top + innerHeight - 2} stroke="#D92D20" strokeWidth="3" /><line x1={center + 10} y1={chartPadding.top + innerHeight - 22} x2={center - 10} y2={chartPadding.top + innerHeight - 2} stroke="#D92D20" strokeWidth="3" /></>}
          <text x={center} y={value === null ? chartPadding.top + innerHeight - 28 : Math.max(14, y - 7)} textAnchor="middle" fontSize="11" fill="#243044">{value === null ? "—" : format(value)}</text>
          <text x={center} y={height - 55} textAnchor="middle" fontSize="10" fill="#243044">{labels.map((label, labelIndex) => <tspan key={label} x={center} dy={labelIndex === 0 ? 0 : 13}>{label}</tspan>)}</text>
          <text x={center} y={height - 18} textAnchor="middle" fontSize="9.5" fill={statusColor(status)}>{statusLabel(status)} · n={row.observed_sample_count ?? row.valid_sample_count}</text>
        </g>;
      })}
    </svg></div>}
    <div className="button-row">
      <button type="button" className="ghost-button" onClick={() => downloadCSV(`${metric}.csv`, rows)}>Download CSV</button>
      <button type="button" className="ghost-button" onClick={() => downloadSVG(`${metric}.svg`, svgId)}>Download SVG</button>
      <button type="button" className="ghost-button" onClick={() => void downloadPNG(`${metric}.png`, svgId)}>Download PNG</button>
      <button type="button" className="ghost-button" onClick={() => downloadPDF(`${metric}.pdf`, title, rows)}>Download PDF</button>
    </div>
  </section>;
}

function PaperAnalysisTable({ rows }: { rows: V5PaperMetricRow[] }) {
  return <div className="table-wrap"><table data-testid="v5-paper-analysis-table"><thead><tr><th>Metric</th><th>Method</th><th>Status</th><th>Observed</th><th>Paper valid</th><th>Mean</th><th>Median</th><th>Std</th><th>CI95</th><th>Note</th></tr></thead><tbody>{rows.map((row) => <tr key={`${row.metric}-${row.method_id}`}><td>{row.metric}</td><td>{row.method_name}</td><td>{statusLabel(rowStatus(row))}</td><td>{row.observed_sample_count ?? row.raw_values.length}</td><td>{row.valid_sample_count}</td><td>{format(row.mean)}</td><td>{format(row.median)}</td><td>{format(row.std)}</td><td>{format(row.ci95_low)} - {format(row.ci95_high)}</td><td>{row.statistical_note}</td></tr>)}</tbody></table></div>;
}

type AnalysisRow = Record<string, unknown>;
type AnalysisChart = { suite_type: string; kind: "summary" | "bar" | "line"; rows: AnalysisRow[] };

function LegacyChart({ chart }: { chart: AnalysisChart }) {
  const title = chartTitle(chart);
  if (chart.kind === "summary") return <section className="analysis-summary"><h3>{title}</h3><p>当前实验只有摘要数据，不绘制虚假趋势线。</p></section>;
  if (!chart.rows.length) return <section><h3>{title}</h3><p>当前实验类型没有可绘制的分组数据。</p></section>;
  return <section className="analysis-chart" data-testid="v5-analysis-chart"><h3>{title}</h3><svg data-testid="v5-analysis-bar-chart" role="img" aria-label={`${title} TPS 柱状图`} viewBox={`0 0 ${baseWidth} ${height}`}><title>{title} TPS 柱状图</title><Axes max={Math.max(...chart.rows.map((row) => numeric(row.mean_tps)), 1)} unit="TPS" chartWidth={baseWidth} /></svg></section>;
}

function Axes({ max, unit, chartWidth }: { max: number; unit: string; chartWidth: number }) {
  const innerHeight = height - chartPadding.top - chartPadding.bottom;
  return <><line x1={chartPadding.left} y1={chartPadding.top} x2={chartPadding.left} y2={chartPadding.top + innerHeight} stroke="#D0D5DD" /><line x1={chartPadding.left} y1={chartPadding.top + innerHeight} x2={chartWidth - chartPadding.right} y2={chartPadding.top + innerHeight} stroke="#D0D5DD" /><line x1={chartPadding.left} y1={chartPadding.top + innerHeight / 2} x2={chartWidth - chartPadding.right} y2={chartPadding.top + innerHeight / 2} stroke="#EAECF0" /><text x="8" y={chartPadding.top + 8} fontSize="11" fill="#667085">{unit}</text><text x="8" y={chartPadding.top + innerHeight / 2 + 4} fontSize="10" fill="#98A2B3">{format(max / 2)}</text><text x="8" y={chartPadding.top + innerHeight + 4} fontSize="10" fill="#98A2B3">0</text></>;
}

function LegacyAnalysisTable({ groups }: { groups: AnalysisRow[] }) { return <div className="table-wrap"><table data-testid="v5-analysis-table"><thead><tr><th>方法</th><th>扫描点</th><th>样本</th><th>平均 TPS</th><th>P99</th><th>95% CI</th></tr></thead><tbody>{groups.map((group, index) => <tr key={`${String(group.method_id)}-${String(group.scan_value)}-${index}`}><td>{String(group.method_name ?? "—")}</td><td>{String(group.scan_value ?? "—")}</td><td>{String(group.sample_count ?? "—")}</td><td>{format(group.mean_tps)}</td><td>{format(group.mean_p99_ms)}</td><td>{format(group.ci95_low_tps)} - {format(group.ci95_high_tps)}</td></tr>)}</tbody></table></div>; }

function sortedRows(rows: V5PaperMetricRow[]): V5PaperMetricRow[] { return [...rows].sort((a, b) => orderOf(a.method_id) - orderOf(b.method_id)); }
function orderOf(methodId: string): number { const index = methodOrder.indexOf(methodId); return index >= 0 ? index : methodOrder.length; }
function chartTitle(chart: AnalysisChart) { return chart.kind === "bar" ? "方法性能对比" : chart.kind === "line" ? "扫描点性能趋势" : "实验摘要"; }
function numeric(value: unknown) { const result = Number(value); return Number.isFinite(result) ? result : 0; }
function finiteNumber(value: unknown): number | null { if (value === null || value === undefined || value === "") return null; const result = Number(value); return Number.isFinite(result) ? result : null; }
function format(value: unknown) { const number = finiteNumber(value); return number === null ? "—" : number.toFixed(2); }
function shortName(value: string): string { return value.replace("MetaTrack with Block-STM backend", "MetaTrack + B-STM").replace("Block-STM", "B-STM"); }
function rowStatus(row: V5PaperMetricRow): SampleStatus { return (row.sample_status ?? "paper_eligible") as SampleStatus; }
function statusLabel(status: SampleStatus): string { return ({ paper_eligible: "有效", comparison_excluded: "比较受限", completed_invalid: "结果无效", blocked_incompatible: "兼容性阻止", execution_failed: "执行失败" } as Record<SampleStatus, string>)[status]; }
function statusColor(status: SampleStatus): string { return ({ paper_eligible: "#027A48", comparison_excluded: "#B54708", completed_invalid: "#D92D20", blocked_incompatible: "#667085", execution_failed: "#D92D20" } as Record<SampleStatus, string>)[status]; }

function downloadCSV(filename: string, rows: V5PaperMetricRow[]) {
  const header = ["method_id", "method_name", "sample_status", "observed_sample_count", "valid_sample_count", "excluded_sample_count", "metric", "metric_unit", "mean", "median", "std", "min", "max", "ci95_low", "ci95_high", "statistical_note"];
  const lines = [header.join(","), ...rows.map((row) => header.map((key) => csvCell((row as unknown as Record<string, unknown>)[key])).join(","))];
  downloadBlob(filename, "text/csv;charset=utf-8", `${lines.join("\n")}\n`);
}
function downloadSVG(filename: string, svgId: string) { const svg = serializeSVG(svgId); if (svg) downloadBlob(filename, "image/svg+xml;charset=utf-8", svg); }
async function downloadPNG(filename: string, svgId: string) {
  const svgElement = document.getElementById(svgId) as SVGSVGElement | null;
  const svg = serializeSVG(svgId);
  if (!svg || !svgElement) return;
  const viewBox = svgElement.viewBox.baseVal;
  const image = new Image();
  const url = URL.createObjectURL(new Blob([svg], { type: "image/svg+xml;charset=utf-8" }));
  await new Promise<void>((resolve, reject) => { image.onload = () => resolve(); image.onerror = reject; image.src = url; });
  const canvas = document.createElement("canvas");
  canvas.width = Math.max(1, Math.round(viewBox.width || baseWidth)); canvas.height = Math.max(1, Math.round(viewBox.height || height));
  canvas.getContext("2d")?.drawImage(image, 0, 0, canvas.width, canvas.height);
  URL.revokeObjectURL(url);
  const blob = await new Promise<Blob | null>((resolve) => canvas.toBlob(resolve, "image/png"));
  if (blob) downloadObjectURL(filename, blob);
}
function serializeSVG(svgId: string): string | null {
  const svg = document.getElementById(svgId) as SVGSVGElement | null;
  if (!svg) return null;
  const clone = svg.cloneNode(true) as SVGElement;
  clone.setAttribute("xmlns", "http://www.w3.org/2000/svg");
  const viewBox = svg.viewBox.baseVal;
  clone.setAttribute("width", String(viewBox.width || baseWidth));
  clone.setAttribute("height", String(viewBox.height || height));
  return `<?xml version="1.0" encoding="UTF-8"?>\n${new XMLSerializer().serializeToString(clone)}`;
}
function downloadPDF(filename: string, title: string, rows: V5PaperMetricRow[]) {
  const body = `${title}\n\n${rows.map((row) => `${row.method_name}: ${format(row.mean)} [${statusLabel(rowStatus(row))}] (${row.statistical_note})`).join("\n")}`;
  downloadBlob(filename, "application/pdf", minimalPDF(body));
}
function minimalPDF(text: string): string {
  const escaped = text.replace(/[()\\]/g, "\\$&").replace(/\r?\n/g, ") Tj T* (");
  const stream = `BT /F1 12 Tf 50 760 Td (${escaped}) Tj ET`;
  return `%PDF-1.4\n1 0 obj<< /Type /Catalog /Pages 2 0 R >>endobj\n2 0 obj<< /Type /Pages /Kids [3 0 R] /Count 1 >>endobj\n3 0 obj<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources<< /Font<< /F1 4 0 R >> >> /Contents 5 0 R >>endobj\n4 0 obj<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>endobj\n5 0 obj<< /Length ${stream.length} >>stream\n${stream}\nendstream endobj\ntrailer<< /Root 1 0 R >>\n%%EOF\n`;
}
function csvCell(value: unknown): string { return `"${String(value ?? "").replace(/"/g, "\"\"")}"`; }
function downloadBlob(filename: string, type: string, content: string) { downloadObjectURL(filename, new Blob([content], { type })); }
function downloadObjectURL(filename: string, blob: Blob) {
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  link.style.display = "none";
  document.body.appendChild(link);
  link.click();
  link.remove();
  window.setTimeout(() => URL.revokeObjectURL(url), 0);
}
