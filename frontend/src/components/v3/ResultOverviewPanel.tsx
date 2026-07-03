import MetricCard from "./MetricCard";
import MiniBarChart from "./MiniBarChart";

type Props = {
  summary: Record<string, unknown>;
};

export default function ResultOverviewPanel({ summary }: Props) {
  const failure = summary.failure_count ?? summary.failed_count;
  return (
    <section className="result-overview">
      <div className="metric-card-grid">
        <MetricCard label="交易总数" value={summary.tx_count} />
        <MetricCard label="成功交易" value={summary.success_count} />
        <MetricCard label="失败交易" value={failure} />
        <MetricCard label="平均延迟" value={summary.avg_latency_ms} hint="ms" />
        <MetricCard label="P95 延迟" value={summary.p95_latency_ms} hint="ms" />
        <MetricCard label="P99 延迟" value={summary.p99_latency_ms} hint="ms" />
        <MetricCard label="吞吐 TPS" value={summary.throughput_tps} />
        <MetricCard label="跨片交易数" value={summary.cross_shard_tx_count} />
        <MetricCard label="Relay 锁定数" value={summary.relay_source_lock_count} />
        <MetricCard label="Relay 证书数" value={summary.relay_certificate_count} />
        <MetricCard label="目标提交数" value={summary.relay_target_commit_count} />
        <MetricCard label="Relay 退款数" value={summary.relay_refund_count} />
        <MetricCard label="状态证明验证数" value={summary.state_proof_verified_count} />
        <MetricCard label="Benchmark 运行次数" value={summary.benchmark_run_count} />
      </div>
      <div className="chart-grid">
        <MiniBarChart
          title="延迟指标预览"
          data={[
            { label: "平均", value: summary.avg_latency_ms },
            { label: "P95", value: summary.p95_latency_ms },
            { label: "P99", value: summary.p99_latency_ms },
          ]}
        />
        <MiniBarChart
          title="关键计数预览"
          data={[
            { label: "跨片交易", value: summary.cross_shard_tx_count },
            { label: "Relay 成功", value: summary.relay_success_count },
            { label: "Relay 失败", value: summary.relay_failed_count },
            { label: "证明验证", value: summary.state_proof_verified_count },
            { label: "Witness 验证", value: summary.witness_verified_count },
            { label: "Benchmark", value: summary.benchmark_run_count },
          ]}
        />
        <MiniBarChart
          title="成功 / 失败"
          data={[
            { label: "成功", value: summary.success_count },
            { label: "失败", value: failure },
          ]}
        />
      </div>
    </section>
  );
}
