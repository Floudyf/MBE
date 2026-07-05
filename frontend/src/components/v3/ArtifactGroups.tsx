import { artifactDownloadURL, artifactPreviewURL, type V2Artifact } from "../../api";

type Props = {
  artifacts: V2Artifact[];
  title?: string;
  emptyMessage?: string;
  defaultOpen?: boolean;
  embedded?: boolean;
  expectedArtifacts?: string[];
};

const groups = [
  { title: "正式性能实验核心结果", defaultOpen: true, files: ["formal_benchmark_config.json", "formal_matrix_preview.json", "formal_run_matrix.csv", "formal_run_index.csv", "formal_run_manifest.json", "formal_progress.json", "formal_failed_runs.csv", "formal_child_artifact_index.csv", "formal_metric_extraction_report.csv", "formal_metric_extraction_report.json", "formal_missing_metrics.csv", "formal_chart_preview.json", "formal_raw_summary.csv", "formal_aggregate_summary.csv", "formal_workload_comparison.csv", "summary.json"] },
  { title: "论文画图数据", defaultOpen: true, files: ["formal_latency_summary.csv", "formal_throughput_summary.csv", "formal_overhead_summary.csv", "formal_confidence_interval.csv", "formal_paper_figure_data.csv", "formal_chart_preview.json"] },
  { title: "正式实验复现包", defaultOpen: false, files: ["formal_reproducibility_manifest.json", "formal_benchmark_report.md"] },
  { title: "配置草稿", files: ["composer_draft.json", "normalized_draft.json", "draft_validation.json", "generated_experiment_profile.json", "generated_experiment_profile.yaml", "generated_plugin_profile.json", "generated_plugin_profile.yaml"] },
  { title: "运行摘要", files: ["summary.csv", "summary.json", "report.md", "latency.csv", "metatrack_summary.csv", "metatrack_summary.json", "metatrack_ablation_report.md"] },
  { title: "链运行日志", files: ["runtime.log", "block_log.csv", "tx_results.csv"] },
  { title: "MetaTrack 指标", files: ["metatrack_latency.csv", "metatrack_mechanism_metrics.csv"] },
  { title: "受控快速验证输出", files: ["run_index.csv", "aggregate_summary.csv", "ablation_report.md", "realism_readiness.json", "realism_readiness.md"] },
  { title: "运行组件日志", files: ["txpool_log.csv", "consensus_log.csv", "routing_log.csv", "execution_log.csv", "state_access_log.csv", "state_commit_log.csv"] },
  { title: "节点与网络产物", files: ["node_topology.csv", "node_log.csv", "network_log.csv", "consensus_message_log.csv", "node_address_table.csv", "topology.json", "launch_nodes_windows.bat", "launch_nodes_linux.sh", "launcher_readme.md", "node_process_status.csv", "node_process_manifest.json", "node_process_log_sample.log"] },
  { title: "Local Multi-process Runtime artifacts", files: ["address_table.json", "multi_process_manifest.json", "node_process_log.csv", "node_lifecycle_log.csv", "network_message_log.csv", "node_process_status.json", "local_multi_process_summary.json", "node_stdout.log", "node_stderr.log"] },
  { title: "Committee / Epoch artifacts", files: ["shard_assignment_log.csv", "committee_assignment_log.csv", "committee_summary.json", "epoch_log.csv", "reconfiguration_plan.json", "reshard_plan_log.csv", "reconfiguration_summary.json"] },
  { title: "Metaverse Workload artifacts", files: ["metaverse_workload_catalog.json", "metaverse_workload_config.json", "metaverse_trace_meta.json", "scenario_summary.csv", "hotspot_distribution.csv", "cross_scene_transfer_log.csv", "offchain_confirmation_log.csv", "cross_metaverse_transfer_log.csv", "metaverse_experiment_summary.json"] },
  { title: "Benchmark / Paper Export artifacts", files: ["baseline_matrix.csv", "multi_seed_summary.csv", "benchmark_suite_summary.json", "paper_table_latency.csv", "paper_table_throughput.csv", "paper_table_cross_shard.csv", "paper_table_offchain_confirmation.csv", "paper_figure_data.csv", "paper_export_manifest.json"] },
  { title: "Fault / Observability / Reproducibility artifacts", files: ["fault_injection_config.json", "fault_injection_log.csv", "node_failure_log.csv", "node_recovery_log.csv", "network_fault_log.csv", "target_congestion_log.csv", "relay_fault_observation_log.csv", "fault_injection_summary.json", "observability_summary.json", "observability_timeline.csv", "component_health_summary.csv", "runtime_component_status.json", "final_artifact_catalog.json", "final_artifact_catalog.md", "v3_final_reproducibility_manifest.json", "v3_reproducibility_guide.md", "v3_experiment_manual.md", "v3_paper_experiment_mapping.md", "v3_final_summary.json"] },
  { title: "Cross-shard / Relay / State Authenticity artifacts", files: ["cross_shard_tx_log.csv", "cross_shard_message_log.csv", "relay_preview_log.csv", "cross_shard_status.csv", "cross_shard_summary.json", "relay_state_machine_log.csv", "source_lock_log.csv", "relay_certificate_log.csv", "relay_proof_verification_log.csv", "target_verification_log.csv", "target_commit_log.csv", "source_finalize_log.csv", "cross_shard_timeout_refund_log.csv", "cross_shard_failure_log.csv", "relay_mvp_summary.json", "state_storage_log.csv", "state_version_log.csv", "state_root_log.csv", "state_proof_log.csv", "state_proof_verification_log.csv", "witness_log.csv", "witness_verification_log.csv", "state_authenticity_summary.json"] },
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
            <ArtifactGroup title="预期产物" files={expectedArtifacts} byName={byName} defaultOpen />
          )}
          {groups.map((group) => (
            <ArtifactGroup key={group.title} title={group.title} files={group.files} byName={byName} defaultOpen={Boolean(group.defaultOpen)} />
          ))}
        </div>
      )}
    </details>
  );
}

function ArtifactGroup({ title, files, byName, defaultOpen = false }: { title: string; files: string[]; byName: Map<string, V2Artifact>; defaultOpen?: boolean }) {
  return (
    <details className="v3-artifact-group v3-foldout" open={defaultOpen}>
      <summary className="v3-foldout-summary">
        <span>{title}</span>
        <small>{countFound(files, byName)} / {files.length}</small>
      </summary>
      <ul>{files.map((name) => renderArtifact(name, byName))}</ul>
    </details>
  );
}

function renderArtifact(name: string, byName: Map<string, V2Artifact>) {
  const artifact = byName.get(name);
  return (
    <li key={name} className={artifact ? "" : "missing"}>
      {artifact ? (
        <span className="artifact-row-actions">
          <span>{name}</span>
          <a href={artifactPreviewURL(artifact.download_url)} target="_blank" rel="noreferrer">预览</a>
          <a href={artifactDownloadURL(artifact.download_url)} download={name}>下载</a>
        </span>
      ) : <span>{name}</span>}
    </li>
  );
}

function countFound(files: string[], byName: Map<string, V2Artifact>): number {
  return files.filter((name) => byName.has(name)).length;
}
