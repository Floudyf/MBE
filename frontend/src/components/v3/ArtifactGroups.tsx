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
    <section className="final-card wide">
      <p className="eyebrow">实验产物</p>
      <h3>运行输出与下载</h3>
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
    </section>
  );
}
