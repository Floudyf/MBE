import type { V3DraftSmokeRunResponse } from "../../api";
import ArtifactGroups from "./ArtifactGroups";
import HelpTip from "./HelpTip";
import ResultOverviewPanel from "./ResultOverviewPanel";

type Props = {
  result?: V3DraftSmokeRunResponse | null;
};

const rawMetricGroups = [
  { title: "交易池", keys: ["queue_wait_ms", "txpool_avg_wait_ms", "txpool_p95_wait_ms", "txpool_peak_size", "txpool_admitted_count", "txpool_rejected_count"] },
  { title: "区块生成", keys: ["block_count", "avg_block_size", "max_block_size", "empty_block_count", "block_interval_ms", "avg_block_interval_ms"] },
  { title: "共识与网络", keys: ["consensus_runtime_selected", "consensus_message_count", "consensus_over_network_enabled", "proposal_preview_count", "vote_preview_count", "light_quorum_reached_count", "network_adapter_selected", "typed_message_count", "network_error_count"] },
  { title: "PBFT 预览", keys: ["pbft_preview_enabled", "pbft_over_network_enabled", "pbft_preprepare_count", "pbft_prepare_count", "pbft_commit_count", "pbft_quorum_reached_count", "pbft_finalized_block_count"] },
  { title: "跨片 / Relay MVP", keys: ["cross_shard_protocol_selected", "cross_shard_tx_count", "cross_shard_ratio", "cross_shard_message_count", "relay_preview_count", "relay_mvp_enabled", "relay_mvp_tx_count", "relay_source_lock_count", "relay_certificate_count", "relay_proof_verified_count", "relay_proof_failed_count", "relay_target_commit_count", "relay_source_finalized_count", "relay_refund_count", "relay_abort_count", "relay_success_count", "relay_failed_count", "relay_avg_latency_ms", "relay_mvp_truth", "cross_shard_completed_count", "cross_shard_failed_count"] },
  { title: "状态真实性", keys: ["state_backend_selected", "state_root_count", "state_key_count", "state_update_count", "state_proof_generated_count", "state_proof_verified_count", "state_proof_failed_count", "witness_generated_count", "witness_verified_count", "witness_failed_count"] },
  { title: "Benchmark", keys: ["benchmark_template_selected", "baseline_profile_selected", "benchmark_run_count", "sweep_parameter_count", "repeat_count", "benchmark_artifact_count", "baseline_comparison_count", "reproducibility_manifest_available", "benchmark_report_available", "paper_grade_benchmark"] },
  { title: "元宇宙实验套件", keys: ["metaverse_suite_enabled", "metaverse_scenario_selected", "metaverse_tx_count", "metaverse_user_count", "metaverse_asset_count", "metaverse_item_count", "metaverse_avatar_count", "metaverse_scene_count", "metaverse_count", "metaverse_hotspot_ratio", "metaverse_cross_scene_ratio", "metaverse_cross_shard_ratio", "metaverse_cross_scene_count", "metaverse_cross_shard_count", "metaverse_burst_count", "metaverse_offchain_confirmation_count", "metaverse_offchain_failure_count", "metaverse_cross_metaverse_count", "baseline_matrix_enabled", "baseline_count", "multi_seed_enabled", "seed_count", "paper_export_enabled", "paper_table_available", "paper_figure_data_available", "metaverse_experiment_truth"] },
  { title: "节点拓扑", keys: ["shard_count", "validators_per_shard", "logical_node_count", "validator_node_count", "executor_node_count", "storage_node_count", "supervisor_node_count"] },
];

export default function DraftRunResultPanel({ result }: Props) {
  if (!result) return null;
  const summary = result.summary || {};
  const normalized = result.validation?.normalized_draft || {};
  const artifactNames = new Set((result.artifacts || []).map((artifact) => artifact.name));
  const expectedArtifacts = readStringArray(summary.expected_artifacts || normalized.expected_artifacts);

  return (
    <section className="final-card wide v3-draft-run-result result-console">
      <div className="v3-section-head">
        <div>
          <p className="eyebrow">配置草稿试运行结果</p>
          <h3>运行摘要</h3>
        </div>
        <span className={`v3-status-badge status-${result.validation?.is_runnable ? "variable" : "planned"}`}>
          {result.validation?.is_runnable ? "可运行配置" : "不可运行配置"}
        </span>
      </div>

      <dl className="v3-result-grid compact">
        <div><dt>运行 ID</dt><dd>{result.run_id}</dd></div>
        <div><dt>运行模式</dt><dd>配置草稿试运行</dd></div>
        <div><dt>实验模板</dt><dd>{String(summary.experiment_template || normalized.experiment_template || normalized.template_id || "-")}</dd></div>
        <div><dt>快速验证预设</dt><dd>{String(summary.preset_id || normalized.preset_id || "默认")}</dd></div>
        <div><dt>校验结果</dt><dd>{result.validation?.is_valid ? "通过" : "未通过"}</dd></div>
        <div><dt>真实性边界</dt><dd>本地快速验证，非论文级实验</dd></div>
      </dl>

      <ResultOverviewPanel summary={summary} />

      <section className="result-section">
        <div className="v3-section-head">
          <h3>核心产物状态</h3>
          <HelpTip title="历史运行缺少产物">
            老运行可能没有 V3.8、V3.9 或 V3.10 新增产物，页面会显示“历史运行缺少该产物”，不会视为错误。
          </HelpTip>
        </div>
        <div className="artifact-pill-grid">
          {["summary.json", "report.md", "cross_shard_summary.json", "state_authenticity_summary.json", "benchmark_summary.json", "metaverse_experiment_summary.json", "paper_export_manifest.json"].map((name) => (
            <span key={name} className={artifactNames.has(name) ? "artifact-pill ok" : "artifact-pill missing"}>
              {name}: {artifactNames.has(name) ? "可下载" : "历史运行缺少该产物"}
            </span>
          ))}
        </div>
      </section>

      <details className="v3-foldout">
        <summary className="v3-foldout-summary">
          <span>详细指标</span>
          <small>默认折叠，保留 raw metrics 供调试和复核</small>
        </summary>
        <div className="raw-metric-groups">
          {rawMetricGroups.map((group) => (
            <section key={group.title} className="raw-metric-group">
              <h4>{group.title}</h4>
              <dl className="v3-summary-preview">
                {group.keys.filter((key) => key in summary).map((key) => (
                  <div key={key}><dt>{key}</dt><dd>{String(summary[key])}</dd></div>
                ))}
              </dl>
            </section>
          ))}
        </div>
      </details>

      <details className="v3-foldout">
        <summary className="v3-foldout-summary">
          <span>实际插件选择</span>
          <small>英文 ID 保留在开发者视图中</small>
        </summary>
        <dl className="v3-summary-preview">
          {Object.entries(readPluginSelection(normalized)).map(([moduleId, plugin]) => (
            <div key={moduleId}><dt>{moduleId}</dt><dd>{plugin}</dd></div>
          ))}
        </dl>
      </details>

      <ArtifactGroups
        artifacts={result.artifacts || []}
        title="产物下载"
        expectedArtifacts={expectedArtifacts}
        defaultOpen={false}
      />

      <p className="muted">配置草稿试运行用于检查配置、运行链路、summary 和产物输出是否正常，不代表论文级正式实验。</p>
    </section>
  );
}

function readPluginSelection(normalized: Record<string, unknown>): Record<string, string> {
  const selection = normalized.plugin_selection;
  if (!selection || typeof selection !== "object" || Array.isArray(selection)) return {};
  return Object.fromEntries(Object.entries(selection).map(([key, value]) => [key, String(value)]));
}

function readStringArray(value: unknown): string[] {
  if (Array.isArray(value)) return value.map(String);
  if (typeof value === "string" && value.trim()) return value.split(",").map((item) => item.trim()).filter(Boolean);
  return [];
}
