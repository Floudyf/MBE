import type { V2Artifact } from "../../api";

type Props = {
  summary?: Record<string, unknown> | null;
  artifacts?: V2Artifact[];
};

const candidateMetrics = [
  ["throughput_tps", "TPS"],
  ["tps", "TPS"],
  ["avg_latency_ms", "Avg Latency"],
  ["p95_latency_ms", "P95"],
  ["p99_latency_ms", "P99"],
  ["tx_count", "Tx"],
  ["success_count", "Success"],
  ["cross_shard_tx_count", "Cross-shard"],
  ["fault_event_count", "Faults"],
] as const;

export default function ResultChartPanel({ summary = null, artifacts = [] }: Props) {
  const metrics = candidateMetrics
    .map(([key, label]) => ({ key, label, value: numberValue(summary?.[key]) }))
    .filter((item) => item.value > 0);
  const max = Math.max(...metrics.map((item) => item.value), 1);
  const csvCount = artifacts.filter((artifact) => artifact.name.endsWith(".csv")).length;

  return (
    <section className="result-chart-panel">
      <div className="stage-flow-head">
        <div>
          <p className="eyebrow">Result Charts</p>
          <h3>指标图表</h3>
        </div>
        <span className="status-badge badge-preview">lightweight</span>
      </div>
      {metrics.length ? (
        <div className="result-bar-chart">
          {metrics.map((metric) => (
            <div key={metric.key} className="result-bar-row">
              <span>{metric.label}</span>
              <div className="result-bar-track"><i style={{ width: `${Math.max(6, (metric.value / max) * 100)}%` }} /></div>
              <b>{formatMetric(metric.value)}</b>
            </div>
          ))}
        </div>
      ) : (
        <p className="muted">暂无可渲染图表；可下载 CSV 进一步分析。</p>
      )}
      <dl className="stage-flow-kpis">
        <div><dt>artifacts</dt><dd>{artifacts.length}</dd></div>
        <div><dt>csv files</dt><dd>{csvCount}</dd></div>
        <div><dt>report</dt><dd>{artifacts.some((artifact) => artifact.name.endsWith(".md")) ? "available" : "unknown"}</dd></div>
      </dl>
    </section>
  );
}

function numberValue(value: unknown): number {
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function formatMetric(value: number) {
  if (value >= 1000) return value.toLocaleString(undefined, { maximumFractionDigits: 0 });
  return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
}
