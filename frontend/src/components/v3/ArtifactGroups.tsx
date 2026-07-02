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
  { title: "Draft config", files: ["composer_draft.json", "normalized_draft.json", "draft_validation.json", "generated_experiment_profile.json", "generated_experiment_profile.yaml", "generated_plugin_profile.json", "generated_plugin_profile.yaml"] },
  { title: "Run summary", files: ["summary.csv", "summary.json", "report.md", "latency.csv", "metatrack_summary.csv", "metatrack_summary.json", "metatrack_ablation_report.md"] },
  { title: "Runtime queue logs", files: ["txpool_log.csv"] },
  { title: "Runtime consensus logs", files: ["consensus_log.csv"] },
  { title: "Runtime routing logs", files: ["routing_log.csv"] },
  { title: "Runtime execution logs", files: ["execution_log.csv"] },
  { title: "Runtime state access logs", files: ["state_access_log.csv"] },
  { title: "Runtime commit logs", files: ["state_commit_log.csv"] },
  { title: "Node-level runtime artifacts", files: ["node_topology.csv", "node_log.csv", "network_log.csv", "consensus_message_log.csv"] },
  { title: "Local launcher artifacts", files: ["node_address_table.csv", "topology.json", "launch_nodes_windows.bat", "launch_nodes_linux.sh", "launcher_readme.md"] },
  { title: "Local node process preview artifacts", files: ["node_process_status.csv", "node_process_manifest.json", "node_process_log_sample.log"] },
  { title: "NetworkAdapter typed message artifacts", files: ["tcp_adapter_status.csv", "network_send_log.csv", "network_receive_log.csv", "typed_message_log.csv"] },
  { title: "Consensus-light over NetworkAdapter artifacts", files: ["consensus_network_light_log.csv", "network_consensus_summary.json"] },
  { title: "PBFT state machine preview artifacts", files: ["pbft_state_log.csv", "pbft_message_log.csv", "quorum_log.csv", "finalized_block_log.csv"] },
  { title: "PBFT over NetworkAdapter artifacts", files: ["consensus_network_log.csv", "pbft_network_summary.json"] },
  { title: "Chain runtime logs", files: ["runtime.log", "block_log.csv", "tx_results.csv"] },
  { title: "MetaTrack metrics", files: ["metatrack_latency.csv", "metatrack_mechanism_metrics.csv"] },
  { title: "Controlled smoke outputs", files: ["run_index.csv", "aggregate_summary.csv", "ablation_report.md", "realism_readiness.json", "realism_readiness.md"] },
  { title: "Used profiles", files: ["used_chain_profile.yaml", "used_plugin_profile.yaml", "used_experiment_profile.yaml", "used_chain_profile.json", "used_plugin_profile.json", "used_experiment_profile.json"] },
];

export default function ArtifactGroups({
  artifacts,
  title = "Artifacts and downloads",
  emptyMessage = "Run Smoke to show summaries, logs, metrics, and generated profiles here.",
  defaultOpen = false,
  embedded = false,
  expectedArtifacts = [],
}: Props) {
  const byName = new Map(artifacts.map((artifact) => [artifact.name, artifact]));

  return (
    <details className={embedded ? "v3-foldout" : "final-card wide v3-foldout"} open={defaultOpen}>
      <summary className="v3-foldout-summary">
        <span>{title}</span>
        <small>{artifacts.length ? `${artifacts.length} artifacts found` : emptyMessage}</small>
      </summary>
      {artifacts.length === 0 && <p className="muted">{emptyMessage}</p>}
      {artifacts.length > 0 && (
        <div className="v3-artifact-groups">
          {expectedArtifacts.length > 0 && (
            <div className="v3-artifact-group">
              <strong>Preset expected artifacts</strong>
              <ul>
                {expectedArtifacts.map((name) => {
                  const artifact = byName.get(name);
                  return (
                    <li key={name} className={artifact ? "" : "missing"}>
                      {artifact ? <a href={v2ArtifactDownloadURL(artifact.download_url)}>{name}</a> : <span>{name} (legacy missing)</span>}
                    </li>
                  );
                })}
              </ul>
            </div>
          )}
          {groups.map((group) => (
            <div key={group.title} className="v3-artifact-group">
              <strong>{group.title}</strong>
              <ul>
                {group.files.map((name) => {
                  const artifact = byName.get(name);
                  return (
                    <li key={name} className={artifact ? "" : "missing"}>
                      {artifact ? <a href={v2ArtifactDownloadURL(artifact.download_url)}>{name}</a> : <span>{name}</span>}
                    </li>
                  );
                })}
              </ul>
            </div>
          ))}
        </div>
      )}
    </details>
  );
}
