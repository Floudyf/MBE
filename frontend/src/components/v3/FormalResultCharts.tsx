import type { V3RuntimeSummary } from "../../api";

type ChartGroup = {
  x: string;
  series: string;
  metric: string;
  mean: number;
  ci95?: number | null;
  count?: number;
};

type ChartPreview = {
  primary_metric?: string;
  available_metrics?: string[];
  groups?: ChartGroup[];
  diagnostics?: Record<string, string>;
  data_files?: Record<string, string>;
};

const charts = [
  ["throughput_tps", "Throughput TPS"],
  ["avg_latency_ms", "Average latency"],
  ["p95_latency_ms", "P95 latency"],
  ["p99_latency_ms", "P99 latency"],
] as const;

type Props = {
  summary: V3RuntimeSummary;
};

export default function FormalResultCharts({ summary }: Props) {
  const preview = readChartPreview(summary);
  const groups = preview.groups || [];
  return (
    <section className="v3-chart-dashboard">
      <div className="v3-section-head">
        <div>
          <p className="eyebrow">结果图表预览</p>
          <h4>正式实验趋势速览</h4>
        </div>
        <span className="v3-status-badge status-fixed">来源 formal_paper_figure_data.csv</span>
      </div>
      <p className="muted">图表预览用于快速查看趋势，不替代 CSV、复现包或论文绘图脚本。</p>
      {preview.diagnostics?.reason && (
        <div className="v3-warning-card">指标抽取诊断见 formal_metric_extraction_report.csv；缺失指标汇总见 formal_missing_metrics.csv。</div>
      )}
      <div className="v3-chart-grid">
        {charts.map(([metric, title]) => (
          <MetricBarChart key={metric} title={title} metric={metric} groups={groups.filter((group) => group.metric === metric)} />
        ))}
      </div>
      <ResultInterpretation summary={summary} groups={groups} />
    </section>
  );
}

function MetricBarChart({ title, metric, groups }: { title: string; metric: string; groups: ChartGroup[] }) {
  if (groups.length === 0) {
    return (
      <div className="v3-chart-card">
        <h5>{title}</h5>
        <p className="muted">该指标暂无可预览数据。</p>
        <small>metric: {metric}</small>
      </div>
    );
  }
  const maxValue = Math.max(...groups.map((group) => Number(group.mean) || 0), 1);
  const xValues = Array.from(new Set(groups.map((group) => group.x)));
  const series = Array.from(new Set(groups.map((group) => group.series)));
  return (
    <div className="v3-chart-card">
      <h5>{title}</h5>
      <div className="v3-bar-chart" role="img" aria-label={`${title} grouped bar chart`}>
        {xValues.map((xValue) => (
          <div key={xValue} className="v3-bar-group">
            <div className="v3-bars">
              {series.map((seriesName, seriesIndex) => {
                const row = groups.find((group) => group.x === xValue && group.series === seriesName);
                const height = row ? Math.max(4, (Number(row.mean) / maxValue) * 100) : 0;
                return (
                  <span
                    key={seriesName}
                    className={`v3-bar series-${seriesIndex % 6}`}
                    style={{ height: `${height}%` }}
                    title={`${seriesName}: ${formatNumber(row?.mean)}${row?.ci95 != null ? ` ± ${formatNumber(row.ci95)}` : ""}`}
                  />
                );
              })}
            </div>
            <small title={xValue}>{xValue}</small>
          </div>
        ))}
      </div>
      <div className="v3-chart-legend">
        {series.map((seriesName, index) => <span key={seriesName}><i className={`series-${index % 6}`} />{seriesName}</span>)}
      </div>
      <small>数据源：formal_paper_figure_data.csv</small>
    </div>
  );
}

function ResultInterpretation({ summary, groups }: { summary: V3RuntimeSummary; groups: ChartGroup[] }) {
  const failed = Number(summary.failed_run_count || 0);
  const seedList = Array.isArray(summary.seed_list) ? summary.seed_list : [];
  const notes: string[] = [];
  if (seedList.length < 3) {
    notes.push("当前 seed 数少于 3，结果更适合链路确认，不作为稳定统计结论。");
  }
  if (failed > 0) {
    notes.push("存在失败子运行，先查看 formal_failed_runs.csv 和 formal_child_artifact_index.csv。");
  }
  const throughput = groups.filter((group) => group.metric === "throughput_tps");
  const xValues = Array.from(new Set(throughput.map((group) => group.x)));
  for (const xValue of xValues.slice(0, 2)) {
    const rows = throughput.filter((group) => group.x === xValue);
    const series = Array.from(new Set(rows.map((row) => row.series)));
    if (series.length !== 2) continue;
    const first = rows.find((row) => row.series === series[0]);
    const second = rows.find((row) => row.series === series[1]);
    if (!first || !second || !first.mean) continue;
    const change = ((second.mean - first.mean) / first.mean) * 100;
    notes.push(`本次结果显示：在 ${xValue} 下，${second.series} 的 throughput_tps 相比 ${first.series} ${change >= 0 ? "高" : "低"} ${Math.abs(change).toFixed(1)}%。`);
  }
  if (notes.length === 0) {
    notes.push("当前结果已生成图表预览数据；对比解读需要恰好两个 series 且指标完整。");
  }
  return (
    <div className="v3-warning-card">
      <strong>自动结果解读</strong>
      {notes.map((note) => <p key={note}>{note}</p>)}
    </div>
  );
}

function readChartPreview(summary: V3RuntimeSummary): ChartPreview {
  const value = summary.chart_preview;
  if (value && typeof value === "object") return value as ChartPreview;
  return {};
}

function formatNumber(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(Number(value))) return "-";
  return Number(value).toFixed(Number(value) >= 100 ? 1 : 3);
}
