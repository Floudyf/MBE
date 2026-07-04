import type { V3FormalMetatrackBenchmarkRunResponse } from "../../api";
import ArtifactGroups from "./ArtifactGroups";

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
        <div><dt>扫描变量</dt><dd>{String(summary.scan_variable || "-")}</dd></div>
        <div><dt>运行真实性等级</dt><dd>{String(summary.runtime_evidence_mode || "-")}</dd></div>
        <div><dt>完成组数</dt><dd>{String(summary.completed_run_count || 0)}</dd></div>
        <div><dt>失败组数</dt><dd>{String(summary.failed_run_count || 0)}</dd></div>
      </dl>
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

function readArray(value: unknown): string[] {
  if (Array.isArray(value)) return value.map(String);
  if (typeof value === "string" && value.trim()) return value.split(",").map((item) => item.trim()).filter(Boolean);
  return [];
}
