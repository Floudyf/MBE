import { v2ArtifactDownloadURL, type V2Artifact } from "../../api";

type Props = {
  artifacts: V2Artifact[];
};

const groups = [
  { title: "实验汇总", files: ["summary.csv", "summary.json", "report.md", "metatrack_summary.csv", "metatrack_summary.json", "metatrack_ablation_report.md"] },
  { title: "链运行日志", files: ["block_log.csv", "tx_results.csv", "state_commit_log.csv"] },
  { title: "MetaTrack 指标", files: ["metatrack_latency.csv", "metatrack_mechanism_metrics.csv"] },
  { title: "使用的配置", files: ["used_chain_profile.yaml", "used_plugin_profile.yaml", "used_experiment_profile.yaml", "used_chain_profile.json", "used_plugin_profile.json", "used_experiment_profile.json"] },
];

export default function ArtifactGroups({ artifacts }: Props) {
  const byName = new Map(artifacts.map((artifact) => [artifact.name, artifact]));

  return (
    <details className="final-card wide v3-foldout">
      <summary className="v3-foldout-summary">
        <span>实验产物与下载</span>
        <small>{artifacts.length ? `已发现 ${artifacts.length} 个产物` : "运行 Smoke 后将在此显示 summary、日志、指标和使用的配置。"}</small>
      </summary>
      {artifacts.length === 0 && <p className="muted">运行 Smoke 后将在此显示 summary、日志、指标和使用的配置。</p>}
      {artifacts.length > 0 && (
        <div className="v3-artifact-groups">
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
