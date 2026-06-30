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
  { title: "Chain runtime logs", files: ["runtime.log", "block_log.csv", "tx_results.csv", "state_commit_log.csv"] },
  { title: "MetaTrack metrics", files: ["metatrack_latency.csv", "metatrack_mechanism_metrics.csv"] },
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
