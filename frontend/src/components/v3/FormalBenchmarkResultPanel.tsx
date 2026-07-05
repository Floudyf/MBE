import { artifactDownloadURL, artifactPreviewURL, formalArtifactsZipURL, type V2Artifact, type V3FormalMetatrackBenchmarkRunResponse } from "../../api";
import ArtifactGroups from "./ArtifactGroups";
import FormalResultCharts from "./FormalResultCharts";

type Props = {
  result?: V3FormalMetatrackBenchmarkRunResponse | null;
};

export default function FormalBenchmarkResultPanel({ result }: Props) {
  if (!result) return null;
  const summary = result.summary || {};
  const paperCandidate = Boolean(summary.paper_candidate_eligible);
  const reasons = readArray(summary.paper_candidate_reasons);
  return (
    <section className="final-card wide result-console">
      <div className="v3-section-head">
        <div>
          <p className="eyebrow">正式性能实验结果</p>
          <h3>MetaTrack 受控基准实验</h3>
        </div>
        <span className={`v3-status-badge status-${paperCandidate ? "variable" : "fixed"}`}>
          {paperCandidate ? "论文候选结果" : "受控基准实验"}
        </span>
      </div>
      {!paperCandidate && (
        <div className="v3-warning-card">当前结果为受控基准实验结果，尚未满足论文候选条件。</div>
      )}
      <dl className="v3-result-grid compact">
        <div><dt>运行 ID</dt><dd>{result.run_id}</dd></div>
        <div><dt>运行模式</dt><dd>{result.run_mode}</dd></div>
        <div><dt>实验类型</dt><dd>{String(summary.experiment_type || "-")}</dd></div>
        <div><dt>证据等级</dt><dd>{String(summary.experiment_evidence_level || "-")}</dd></div>
        <div><dt>是否论文候选</dt><dd>{paperCandidate ? "是" : "否"}</dd></div>
        <div><dt>每组交易数</dt><dd>{String(summary.formal_tx_count || "-")}</dd></div>
        <div><dt>seed_list</dt><dd>{readArray(summary.seed_list).join(", ") || "-"}</dd></div>
        <div><dt>总运行组数</dt><dd>{String(summary.run_count || "-")}</dd></div>
        <div><dt>总交易数</dt><dd>{String(summary.total_tx_count || "-")}</dd></div>
        <div><dt>基线数量</dt><dd>{String(summary.baseline_count || "-")}</dd></div>
        <div><dt>方案数量</dt><dd>{String(summary.method_count || summary.baseline_count || "-")}</dd></div>
        <div><dt>负载数量</dt><dd>{String(summary.workload_count || "-")}</dd></div>
        <div><dt>拓扑数量</dt><dd>{String(summary.topology_count || "-")}</dd></div>
        <div><dt>扫描变量</dt><dd>{String(summary.scan_variable || "-")}</dd></div>
        <div><dt>运行真实性等级</dt><dd>{String(summary.runtime_evidence_mode || "-")}</dd></div>
        <div><dt>完成组数</dt><dd>{String(summary.completed_run_count || 0)}</dd></div>
        <div><dt>失败组数</dt><dd>{String(summary.failed_run_count || 0)}</dd></div>
        <div><dt>当前运行索引</dt><dd>{String(summary.current_run_index ?? "-")}</dd></div>
        <div><dt>失败子运行</dt><dd>{String(summary.failed_child_run_count ?? summary.failed_run_count ?? 0)}</dd></div>
      </dl>
      <div className="v3-warning-card">正式运行会输出 formal_run_manifest.json、formal_progress.json、formal_failed_runs.csv 和 formal_child_artifact_index.csv，用于定位子运行状态和失败原因。</div>
      <FormalResultCharts summary={summary} />
      <section className="v3-config-section">
        <div className="v3-section-head">
          <div>
            <p className="eyebrow">数据文件说明</p>
            <h4>论文画图与复现入口</h4>
          </div>
          <a className="v3-secondary-button" href={formalArtifactsZipURL(result.run_id)} download={`formal_metatrack_results_${result.run_id}.zip`}>下载全部 ZIP</a>
        </div>
        <p className="muted">实验输出目录：{result.output_dir}</p>
        <DataFileList artifacts={result.artifacts || []} />
      </section>
      {reasons.length > 0 && (
        <details className="v3-foldout">
          <summary className="v3-foldout-summary">论文候选结果判定</summary>
          <ul className="v3-check-list compact">{reasons.map((reason) => <li key={reason}>{reason}</li>)}</ul>
        </details>
      )}
      <details className="v3-foldout">
        <summary className="v3-foldout-summary">真实性边界</summary>
        <p className="muted">本地 emulator 受控基准实验，不是生产链，不是多服务器部署，不是 Fabric/EVM live 后端，不是 BlockEmulator 后端。</p>
      </details>
      <ArtifactGroups artifacts={result.artifacts || []} title="正式性能实验产物" defaultOpen />
    </section>
  );
}

function DataFileList({ artifacts }: { artifacts: V2Artifact[] }) {
  const byName = new Map(artifacts.map((artifact) => [artifact.name, artifact]));
  const files = [
    ["formal_paper_figure_data.csv", "论文画图数据"],
    ["formal_workload_comparison.csv", "不同负载对比"],
    ["formal_aggregate_summary.csv", "全部聚合统计"],
    ["formal_raw_summary.csv", "每个子运行原始统计"],
    ["formal_child_artifact_index.csv", "子运行目录索引"],
    ["formal_reproducibility_manifest.json", "复现实验配置"],
    ["formal_chart_preview.json", "图表预览数据"],
  ];
  return (
    <ul className="v3-data-file-list">
      {files.map(([name, label]) => {
        const artifact = byName.get(name);
        return (
          <li key={name} className={artifact ? "" : "missing"}>
            <span><strong>{label}</strong><small>{name}</small></span>
            {artifact ? (
              <span className="artifact-row-actions">
                <a href={artifactPreviewURL(artifact.download_url)} target="_blank" rel="noreferrer">预览</a>
                <a href={artifactDownloadURL(artifact.download_url)} download={name}>下载</a>
              </span>
            ) : <small>未生成</small>}
          </li>
        );
      })}
    </ul>
  );
}

function readArray(value: unknown): string[] {
  if (Array.isArray(value)) return value.map(String);
  if (typeof value === "string" && value.trim()) return value.split(",").map((item) => item.trim()).filter(Boolean);
  return [];
}
