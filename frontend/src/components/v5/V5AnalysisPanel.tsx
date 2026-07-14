import type { V5FormalAnalysis } from "../../api";
import { suiteLabel } from "../../v5Labels";

export default function V5AnalysisPanel({ analysis }: { analysis: V5FormalAnalysis | null }) {
  if (!analysis?.groups.length) return <section className="final-card wide" data-testid="v5-analysis-panel"><h2>基础实验分析</h2><p className="muted">当前实验类型没有可绘制的分组数据；单点主实验只显示摘要，不伪造曲线。</p></section>;
  return <section className="final-card wide" data-testid="v5-analysis-panel"><h2>基础实验分析</h2>
    {analysis.charts.map((chart) => <section key={chart.suite_type}><h3>{suiteLabel(chart.suite_type)} / {kindLabel(chart.kind)}</h3><div className="table-wrap"><table><thead><tr><th>方法</th><th>扫描值</th><th>平均 TPS</th><th>P99（ms）</th><th>95% CI</th><th>失败数</th></tr></thead><tbody>{chart.rows.map((row, index) => <tr key={index}><td>{String(row.method_name ?? "—")}</td><td>{String(row.scan_value || row.method_name || "—")}</td><td>{value(row.mean_tps)}</td><td>{value(row.mean_p99_ms)}</td><td>{value(row.ci95_low_tps)} / {value(row.ci95_high_tps)}</td><td>{value(row.failed_count)}</td></tr>)}</tbody></table></div></section>)}
  </section>;
}
function kindLabel(kind: string): string { return ({ bar: "分组柱状对比", line: "趋势曲线", summary: "摘要" } as Record<string, string>)[kind] ?? kind; }
function value(value: unknown): string { return value === undefined || value === null ? "—" : String(value); }
