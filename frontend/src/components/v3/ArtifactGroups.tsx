import { v2ArtifactDownloadURL, type V2Artifact } from "../../api";

type Props = {
  artifacts: V2Artifact[];
  title?: string;
  emptyMessage?: string;
  defaultOpen?: boolean;
  embedded?: boolean;
  expectedArtifacts?: string[];
};

const groups = [
  { title: "配置草稿", files: ["composer_draft.json", "normalized_draft.json", "draft_validation.json", "generated_experiment_profile.json", "generated_experiment_profile.yaml", "generated_plugin_profile.json", "generated_plugin_profile.yaml"] },
  { title: "运行摘要", files: ["summary.csv", "summary.json", "report.md", "latency.csv", "metatrack_summary.csv", "metatrack_summary.json", "metatrack_ablation_report.md"] },
  { title: "交易池日志", files: ["txpool_log.csv"] },
  { title: "共识日志", files: ["consensus_log.csv"] },
  { title: "路由日志", files: ["routing_log.csv"] },
  { title: "执行日志", files: ["execution_log.csv"] },
  { title: "状态访问日志", files: ["state_access_log.csv"] },
  { title: "提交日志", files: ["state_commit_log.csv"] },
  { title: "节点级运行产物", files: ["node_topology.csv", "node_log.csv", "network_log.csv", "consensus_message_log.csv"] },
  { title: "本地启动预览产物", files: ["node_address_table.csv", "topology.json", "launch_nodes_windows.bat", "launch_nodes_linux.sh", "launcher_readme.md"] },
  { title: "节点进程预览产物", files: ["node_process_status.csv", "node_process_manifest.json", "node_process_log_sample.log"] },
  { title: "Local Multi-process Runtime artifacts", files: ["address_table.json", "multi_process_manifest.json", "node_process_log.csv", "node_lifecycle_log.csv", "network_message_log.csv", "node_process_status.json", "local_multi_process_summary.json", "node_stdout.log", "node_stderr.log"] },
  { title: "Committee / Epoch artifacts", files: ["shard_assignment_log.csv", "committee_assignment_log.csv", "committee_summary.json", "epoch_log.csv", "reconfiguration_plan.json", "reshard_plan_log.csv", "reconfiguration_summary.json"] },
  { title: "Metaverse Workload artifacts", files: ["metaverse_workload_catalog.json", "metaverse_workload_config.json", "metaverse_trace_meta.json", "scenario_summary.csv", "hotspot_distribution.csv", "cross_scene_transfer_log.csv", "offchain_confirmation_log.csv", "cross_metaverse_transfer_log.csv", "metaverse_experiment_summary.json"] },
  { title: "Benchmark Matrix artifacts", files: ["baseline_matrix.csv", "multi_seed_summary.csv", "benchmark_suite_summary.json"] },
  { title: "Paper Export artifacts", files: ["paper_table_latency.csv", "paper_table_throughput.csv", "paper_table_cross_shard.csv", "paper_table_offchain_confirmation.csv", "paper_figure_data.csv", "paper_export_manifest.json"] },
  { title: "Fault Injection artifacts", files: ["fault_injection_config.json", "fault_injection_log.csv", "node_failure_log.csv", "node_recovery_log.csv", "network_fault_log.csv", "target_congestion_log.csv", "relay_fault_observation_log.csv", "fault_injection_summary.json"] },
  { title: "Observability artifacts", files: ["observability_summary.json", "observability_timeline.csv", "component_health_summary.csv", "runtime_component_status.json"] },
  { title: "Reproducibility / Final Closure artifacts", files: ["final_artifact_catalog.json", "final_artifact_catalog.md", "v3_final_reproducibility_manifest.json", "v3_reproducibility_guide.md", "v3_experiment_manual.md", "v3_paper_experiment_mapping.md", "v3_final_summary.json"] },
  { title: "网络通信产物", files: ["tcp_adapter_status.csv", "network_send_log.csv", "network_receive_log.csv", "typed_message_log.csv"] },
  { title: "轻量共识网络产物", files: ["consensus_network_light_log.csv", "network_consensus_summary.json"] },
  { title: "PBFT 状态机预览产物", files: ["pbft_state_log.csv", "pbft_message_log.csv", "quorum_log.csv", "finalized_block_log.csv"] },
  { title: "PBFT 网络预览产物", files: ["consensus_network_log.csv", "pbft_network_summary.json"] },
  { title: "跨片 skeleton 产物", files: ["cross_shard_tx_log.csv", "cross_shard_message_log.csv", "relay_preview_log.csv", "cross_shard_status.csv", "cross_shard_summary.json"] },
  { title: "Relay MVP 产物", files: ["relay_state_machine_log.csv", "source_lock_log.csv", "relay_certificate_log.csv", "relay_proof_verification_log.csv", "target_verification_log.csv", "target_commit_log.csv", "source_finalize_log.csv", "cross_shard_timeout_refund_log.csv", "cross_shard_failure_log.csv", "relay_mvp_summary.json"] },
  { title: "状态真实性产物", files: ["state_storage_log.csv", "state_version_log.csv", "state_root_log.csv", "state_proof_log.csv", "state_proof_verification_log.csv", "witness_log.csv", "witness_verification_log.csv", "state_authenticity_summary.json"] },
  { title: "Benchmark 产物", files: ["benchmark_template_catalog.json", "baseline_profile_catalog.json", "benchmark_plan.json", "benchmark_run_index.csv", "sweep_matrix.csv", "sweep_summary.csv", "sweep_summary.json", "aggregate_summary.csv", "baseline_comparison.csv", "reproducibility_manifest.json", "benchmark_report.md", "benchmark_summary.json"] },
  { title: "链运行日志", files: ["runtime.log", "block_log.csv", "tx_results.csv"] },
  { title: "MetaTrack 指标", files: ["metatrack_latency.csv", "metatrack_mechanism_metrics.csv"] },
  { title: "受控对照输出", files: ["run_index.csv", "aggregate_summary.csv", "ablation_report.md", "realism_readiness.json", "realism_readiness.md"] },
  { title: "使用的配置", files: ["used_chain_profile.yaml", "used_plugin_profile.yaml", "used_experiment_profile.yaml", "used_chain_profile.json", "used_plugin_profile.json", "used_experiment_profile.json"] },
];

export default function ArtifactGroups({
  artifacts,
  title = "产物下载",
  emptyMessage = "运行后会在这里显示 summary、日志、指标和生成配置。",
  defaultOpen = false,
  embedded = false,
  expectedArtifacts = [],
}: Props) {
  const byName = new Map(artifacts.map((artifact) => [artifact.name, artifact]));

  return (
    <details className={embedded ? "v3-foldout" : "final-card wide v3-foldout"} open={defaultOpen}>
      <summary className="v3-foldout-summary">
        <span>{title}</span>
        <small>{artifacts.length ? `已找到 ${artifacts.length} 个产物` : emptyMessage}</small>
      </summary>
      {artifacts.length === 0 && <p className="muted">{emptyMessage}</p>}
      {artifacts.length > 0 && (
        <div className="v3-artifact-groups">
          {expectedArtifacts.length > 0 && (
            <div className="v3-artifact-group">
              <strong>预期产物</strong>
              <ul>
                {expectedArtifacts.map((name) => renderArtifact(name, byName))}
              </ul>
            </div>
          )}
          {groups.map((group) => (
            <div key={group.title} className="v3-artifact-group">
              <strong>{group.title}</strong>
              <ul>{group.files.map((name) => renderArtifact(name, byName))}</ul>
            </div>
          ))}
        </div>
      )}
    </details>
  );
}

function renderArtifact(name: string, byName: Map<string, V2Artifact>) {
  const artifact = byName.get(name);
  return (
    <li key={name} className={artifact ? "" : "missing"}>
      {artifact ? <a href={v2ArtifactDownloadURL(artifact.download_url)}>{name}</a> : <span>{name}</span>}
    </li>
  );
}
