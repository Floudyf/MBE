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
        <MetricCard label="节点运行模式" value={summary.node_runtime_mode} />
        <MetricCard label="计划进程数" value={summary.planned_process_count} />
        <MetricCard label="启动进程数" value={summary.started_process_count} />
        <MetricCard label="失败进程数" value={summary.failed_process_count} />
        <MetricCard label="网络消息数" value={summary.network_message_count} />
        <MetricCard label="分片数" value={summary.shard_count} />
        <MetricCard label="委员会数" value={summary.committee_count} />
        <MetricCard label="Epoch 数" value={summary.epoch_count} />
        <MetricCard label="重配置事件数" value={summary.reconfiguration_event_count} />
        <MetricCard label="跨片交易数" value={summary.cross_shard_tx_count} />
        <MetricCard label="Relay 成功数" value={summary.relay_success_count} />
        <MetricCard label="状态证明验证数" value={summary.state_proof_verified_count} />
        <MetricCard label="Benchmark 运行数" value={summary.benchmark_run_count} />
        <MetricCard label="元宇宙场景" value={summary.metaverse_scenario_selected} />
        <MetricCard label="元宇宙交易数" value={summary.metaverse_tx_count} />
        <MetricCard label="元宇宙用户数" value={summary.metaverse_user_count} />
        <MetricCard label="虚拟资产数" value={summary.metaverse_asset_count} />
        <MetricCard label="场景数" value={summary.metaverse_scene_count} />
        <MetricCard label="跨场景次数" value={summary.metaverse_cross_scene_count} />
        <MetricCard label="元宇宙跨片次数" value={summary.metaverse_cross_shard_count} />
        <MetricCard label="链下确认次数" value={summary.metaverse_offchain_confirmation_count} />
        <MetricCard label="跨元宇宙次数" value={summary.metaverse_cross_metaverse_count} />
        <MetricCard label="Baseline 数" value={summary.baseline_count} />
        <MetricCard label="Seed 数" value={summary.seed_count} />
        <MetricCard label="Paper 表格" value={summary.paper_table_available} />
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
          title="Runtime realism 预览"
          data={[
            { label: "计划进程", value: summary.planned_process_count },
            { label: "启动进程", value: summary.started_process_count },
            { label: "失败进程", value: summary.failed_process_count },
            { label: "网络消息", value: summary.network_message_count },
            { label: "Epoch", value: summary.epoch_count },
          ]}
        />
        <MiniBarChart
          title="元宇宙场景指标"
          data={[
            { label: "跨场景", value: summary.metaverse_cross_scene_count },
            { label: "跨片", value: summary.metaverse_cross_shard_count },
            { label: "链下确认", value: summary.metaverse_offchain_confirmation_count },
            { label: "跨元宇宙", value: summary.metaverse_cross_metaverse_count },
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
