import type { V3FormalMetatrackBenchmarkPreview } from "../../api";

type Props = {
  preview?: V3FormalMetatrackBenchmarkPreview | null;
};

export default function FormalExperimentMatrixPreview({ preview }: Props) {
  if (!preview) {
    return (
      <section className="v3-config-section">
        <h4>实验矩阵预览</h4>
        <p className="muted">配置正式性能实验参数后，可预览基线、seed 和扫描点组合。</p>
      </section>
    );
  }
  const sampleRows = (preview.matrix || []).slice(0, 8);
  return (
    <section className="v3-config-section">
      <div className="v3-section-head">
        <h4>实验矩阵预览</h4>
        <span className={`v3-status-badge status-${preview.is_runnable ? "variable" : "planned"}`}>
          {preview.is_runnable ? "可运行" : "需要调整"}
        </span>
      </div>
      <dl className="v3-result-grid compact">
        <div><dt>实验类型</dt><dd>{preview.experiment_type}</dd></div>
        <div><dt>基线数量</dt><dd>{preview.baseline_count}</dd></div>
        <div><dt>seed 数量</dt><dd>{preview.seed_list.length}</dd></div>
        <div><dt>扫描点数量</dt><dd>{preview.scan_point_count}</dd></div>
        <div><dt>总运行组数</dt><dd>{preview.run_count}</dd></div>
        <div><dt>每组交易数</dt><dd>{preview.formal_tx_count}</dd></div>
        <div><dt>总交易数</dt><dd>{preview.total_tx_count}</dd></div>
        <div><dt>运行真实性等级</dt><dd>{preview.runtime_evidence_mode}</dd></div>
        <div><dt>故障注入</dt><dd>{preview.includes_fault_injection ? "包含" : "不包含"}</dd></div>
        <div><dt>preview/planned 插件</dt><dd>{preview.contains_preview_or_planned_plugin ? "包含" : "不包含"}</dd></div>
      </dl>
      {(preview.errors.length > 0 || preview.exceeds_recommended_range) && (
        <div className="v3-warning-card">
          {preview.exceeds_recommended_range && <p>总运行组数或总交易数偏大，建议减少 seed_count 或扫描点。</p>}
          {preview.errors.map((error) => <p key={error}>{error}</p>)}
        </div>
      )}
      <details className="v3-foldout">
        <summary className="v3-foldout-summary">完整矩阵样例</summary>
        <div className="v3-summary-preview">
          {sampleRows.map((row) => (
            <div key={String(row.run_index)}>
              <dt>{String(row.baseline_id)} / seed {String(row.seed)}</dt>
              <dd>{String(row.scan_variable)}={String(row.scan_value)}</dd>
            </div>
          ))}
        </div>
      </details>
    </section>
  );
}
