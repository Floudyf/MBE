import { useState } from "react";

import { fetchV3DraftRunDetail, fetchV3DraftRuns, type V3DraftRunDetail, type V3DraftRunSummary } from "../../api";
import ArtifactGroups from "./ArtifactGroups";

const summaryKeys = ["tx_count", "success_count", "failure_count", "failed_count", "avg_latency_ms", "p95_latency_ms", "p99_latency_ms", "queue_wait_ms", "txpool_avg_wait_ms", "txpool_p95_wait_ms", "txpool_peak_size", "txpool_admitted_count", "txpool_rejected_count", "block_count", "avg_block_size", "empty_block_count", "avg_block_interval_ms", "blockproducer_count_cut_count", "blockproducer_time_cut_count", "blockproducer_drain_cut_count", "avg_consensus_latency_ms", "p95_consensus_latency_ms", "consensus_message_count", "avg_consensus_message_count", "consensus_round_count", "finalized_block_count", "failed_block_count", "view_change_count", "routing_plugin", "cross_shard_ratio", "routing_decision_count", "cross_shard_tx_count", "avg_touched_shards", "coaccess_group_count", "hotspot_key_count", "execution_plugin", "fast_track_count", "conservative_track_count", "blocked_tx_count", "dependency_edge_count", "avg_execution_latency_ms", "p95_execution_latency_ms", "state_access_plugin", "remote_state_access_ratio", "cache_hit_rate", "prefetch_hit_rate", "avg_state_access_latency_ms", "witness_estimated_count", "proof_estimated_count", "commit_plugin", "aggregation_ratio", "constraint_failed_count", "avg_commit_latency_ms", "p95_commit_latency_ms", "experiment_template", "preset_id", "preset_name", "ablation_stage", "enabled_metatrack_components", "variable_module", "fairness_validated"];

export default function DraftRunHistoryPanel() {
  const [runs, setRuns] = useState<V3DraftRunSummary[]>([]);
  const [selected, setSelected] = useState<V3DraftRunDetail | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  async function refresh() {
    try {
      setLoading(true);
      const nextRuns = await fetchV3DraftRuns(20);
      setRuns(nextRuns);
      setError("");
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setLoading(false);
    }
  }

  async function openRun(runId: string) {
    try {
      setLoading(true);
      const detail = await fetchV3DraftRunDetail(runId);
      setSelected(detail);
      setError("");
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setLoading(false);
    }
  }

  return (
    <details className="final-card wide v3-foldout v3-draft-history">
      <summary className="v3-foldout-summary">
        <span>最近配置草稿试运行</span>
        <small>仅为本地 .cache 历史，不是正式结果数据库</small>
      </summary>
      <div className="v3-history-toolbar">
        <p className="muted">配置草稿试运行历史用于本地调试、演示和配置追踪，不代表论文级正式实验依据。</p>
        <button type="button" className="v3-secondary-button" disabled={loading} onClick={refresh}>
          {loading ? "加载中..." : "刷新历史"}
        </button>
      </div>
      {error && <p className="file-error">{error}</p>}
      {runs.length === 0 && <p className="muted">暂无配置草稿试运行记录。运行后可点击刷新。</p>}
      {runs.length > 0 && (
        <div className="v3-history-list">
          {runs.map((run) => (
            <button key={run.run_id} type="button" className="v3-history-row" onClick={() => openRun(run.run_id)}>
              <span>
                <strong>{run.run_id}</strong>
                <small>{run.created_at}</small>
              </span>
              <span>{run.template_id}</span>
              <span>{run.preset_id || String(run.summary_preview?.preset_id || "历史默认预设")}</span>
              <span>{String(run.summary_preview?.ablation_stage || "历史消融")}</span>
              <span>{run.variable_module || String(run.summary_preview?.variable_module || "历史模板")}</span>
              <span>{String(run.summary_preview?.routing_plugin || run.summary_preview?.cross_shard_ratio || "历史路由")}</span>
              <span>{String(run.summary_preview?.execution_plugin || run.summary_preview?.fast_track_count || "历史执行")}</span>
              <span>{String(run.summary_preview?.state_access_plugin || run.summary_preview?.remote_state_access_ratio || "历史状态访问")}</span>
              <span>{String(run.summary_preview?.commit_plugin || run.summary_preview?.aggregation_ratio || "历史提交")}</span>
              <span>{pluginSummary(run.selected_plugins)}</span>
              <b>{run.artifact_count} 个产物</b>
            </button>
          ))}
        </div>
      )}
      {selected && (
        <div className="v3-history-detail">
          <div className="v3-section-head">
            <div>
              <p className="eyebrow">草稿试运行详情</p>
              <h3>{selected.run_id}</h3>
            </div>
            <span className={`v3-status-badge status-${selected.validation?.is_runnable ? "variable" : "planned"}`}>
              {selected.validation?.is_runnable ? "可运行" : "不可运行"}
            </span>
          </div>
          <div className="v3-summary-preview">
            {summaryKeys.filter((key) => key in selected.summary_preview).map((key) => (
              <div key={key}><dt>{key}</dt><dd>{String(selected.summary_preview[key])}</dd></div>
            ))}
          </div>
          {selected.missing_files.length > 0 && (
            <p className="file-error">缺少文件：{selected.missing_files.join(", ")}</p>
          )}
          <ArtifactGroups
            artifacts={selected.artifacts}
            title="所选草稿试运行产物"
            emptyMessage="该历史运行没有可下载产物。"
            expectedArtifacts={readStringArray(selected.summary_preview.expected_artifacts)}
            defaultOpen
            embedded
          />
        </div>
      )}
    </details>
  );
}

function readStringArray(value: unknown): string[] {
  if (Array.isArray(value)) return value.map(String);
  if (typeof value === "string" && value.trim()) return value.split(",").map((item) => item.trim()).filter(Boolean);
  return [];
}

function pluginSummary(plugins: Record<string, string>): string {
  const keys = ["Consensus", "Routing", "Execution", "StateAccess", "Commit"];
  const pairs = keys.filter((key) => plugins[key]).map((key) => `${key}:${plugins[key]}`);
  return pairs.length ? pairs.join(" / ") : "暂无插件摘要";
}
