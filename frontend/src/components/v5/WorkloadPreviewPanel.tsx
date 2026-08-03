import type { V5WorkloadPreview } from "../../api";
import { formatTimeRange, valueText } from "../../workloadUi";

export default function WorkloadPreviewPanel({ preview, dirty, error, onPreview, disabled }: { preview: V5WorkloadPreview | null; dirty: boolean; error: string; onPreview: () => void; disabled: boolean }) {
  const selectedWindow = preview?.selected_window_preview;
  const selectedCounts = selectedWindow?.operation_counts ?? selectedWindow?.category_counts ?? preview?.operation_counts ?? preview?.category_counts ?? {};
  const selectedPercentages = selectedWindow?.category_percentages ?? {};
  const selectedCount = selectedWindow?.actual_selected_count ?? preview?.tx_count ?? 0;
  return <article className="final-card wide" data-testid="workload-preview-panel">
    <div className="section-heading">
      <div><h3>Workload Preview</h3><p className="muted">预览由后端 API 生成，浏览器不物化数据。</p></div>
      <button type="button" data-testid="workload-preview-button" onClick={onPreview} disabled={disabled}>预览负载</button>
    </div>
    {dirty && <p className="notice" data-testid="workload-preview-dirty">配置已变化，请重新预览。</p>}
    {error && <p className="file-error" data-testid="workload-preview-error">{error}</p>}
    {preview ? <>
      {preview.blockers.length ? <ul className="boundary-list file-error">{preview.blockers.map((item) => <li key={item}>{item}</li>)}</ul> : null}
      {preview.warnings.length ? <ul className="boundary-list">{preview.warnings.map((item) => <li key={item}>{item}</li>)}</ul> : null}
      <dl className="stage-flow-kpis workload-kpis">
        <Metric label="source_type" value={preview.source_type} />
        <Metric label="plugin_id" value={preview.plugin_id} />
        <Metric label="dataset_id" value={preview.dataset_id} />
        <Metric label="requested / actual count" value={`${selectedWindow?.requested_tx_count ?? preview.tx_count} / ${selectedCount}`} />
        <Metric label="selection mode" value="contiguous_window" />
        <Metric label="selected time range" value={formatTimeRange(selectedWindow?.selected_time_range?.start_ms ?? preview.selected_time_range.start_ms, selectedWindow?.selected_time_range?.end_ms ?? preview.selected_time_range.end_ms)} />
        <Metric label="selection digest" value={selectedWindow?.selection_digest} />
        <Metric label="expected cross-shard" value={JSON.stringify(preview.expected_cross_shard)} />
        <Metric label="actual cross-shard" value={`${valueText(selectedWindow?.cross_shard_count)} / ${valueText(selectedWindow?.cross_shard_ratio)}`} />
        <Metric label="cache status" value={JSON.stringify(preview.materialization_cache_status)} />
        <Metric label="shard load distribution" value={JSON.stringify(selectedWindow?.shard_distribution ?? preview.shard_distribution)} />
        <Metric label="natural skew" value={JSON.stringify(preview.natural_skew)} />
        <Metric label="derived skew" value={JSON.stringify(selectedWindow?.realized_skew ?? preview.derived_skew)} />
      </dl>
      <div className="workload-detail-grid">
        {Object.entries(selectedCounts).map(([key, value]) => {
          const percentage = selectedPercentages[key] ?? (selectedCount ? value / selectedCount : 0);
          return <section key={key}><h4>{key}</h4><p>{valueText(value)} tx</p><p className="muted">{(percentage * 100).toFixed(2)}%</p></section>;
        })}
      </div>
    </> : <p className="muted">尚未预览。</p>}
  </article>;
}

function Metric({ label, value }: { label: string; value: unknown }) { return <div><dt>{label}</dt><dd>{valueText(value)}</dd></div>; }
