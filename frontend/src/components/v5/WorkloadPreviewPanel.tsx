import type { V5WorkloadPreview } from "../../api";
import { formatTimeRange, valueText } from "../../workloadUi";

type ObjectRecord = Record<string, unknown>;

export default function WorkloadPreviewPanel({ preview, dirty, error, onPreview, disabled }: { preview: V5WorkloadPreview | null; dirty: boolean; error: string; onPreview: () => void; disabled: boolean }) {
  const selectedWindow = preview?.selected_window_preview;
  const selectedCounts = selectedWindow?.operation_counts ?? selectedWindow?.category_counts ?? preview?.operation_counts ?? preview?.category_counts ?? {};
  const selectedPercentages = selectedWindow?.category_percentages ?? {};
  const selectedCount = selectedWindow?.actual_selected_count ?? preview?.tx_count ?? 0;
  return <article className="final-card wide workload-preview-panel" data-testid="workload-preview-panel">
    <div className="section-heading">
      <div><h3>Workload Preview</h3><p className="muted">预览由后端 API 生成。常用信息优先显示为可读摘要；完整原始字段可按需展开查看。</p></div>
      <button type="button" data-testid="workload-preview-button" onClick={onPreview} disabled={disabled}>预览负载</button>
    </div>
    {dirty && <p className="notice" data-testid="workload-preview-dirty">配置已变化，请重新预览。</p>}
    {error && <p className="file-error" data-testid="workload-preview-error">{error}</p>}
    {preview ? <>
      {preview.blockers.length ? <ul className="boundary-list file-error">{preview.blockers.map((item) => <li key={item}>{item}</li>)}</ul> : null}
      {preview.warnings.length ? <ul className="boundary-list">{preview.warnings.map((item) => <li key={item}>{item}</li>)}</ul> : null}
      <dl className="stage-flow-kpis workload-kpis workload-preview-kpis">
        <Metric label="负载来源" value={preview.source_type} />
        <Metric label="负载插件" value={preview.plugin_id} />
        <Metric label="数据集" value={preview.dataset_id} />
        <Metric label="请求 / 实际交易数" value={`${selectedWindow?.requested_tx_count ?? preview.tx_count} / ${selectedCount}`} />
        <Metric label="选择方式" value="contiguous_window" />
        <Metric label="选中时间范围" value={formatTimeRange(selectedWindow?.selected_time_range?.start_ms ?? preview.selected_time_range.start_ms, selectedWindow?.selected_time_range?.end_ms ?? preview.selected_time_range.end_ms)} />
        <Metric label="选择摘要" value={selectedWindow?.selection_digest} compact />
        <Metric label="预期跨片" value={summarizeCrossShard(preview.expected_cross_shard)} rawValue={preview.expected_cross_shard} />
        <Metric label="实际跨片" value={`${valueText(selectedWindow?.cross_shard_count)} / ${formatRatio(selectedWindow?.cross_shard_ratio)}`} />
        <Metric label="物化缓存" value={summarizeCache(preview.materialization_cache_status)} rawValue={preview.materialization_cache_status} />
        <Metric label="分片负载" value={summarizeDistribution(selectedWindow?.shard_distribution ?? preview.shard_distribution)} rawValue={selectedWindow?.shard_distribution ?? preview.shard_distribution} />
        <Metric label="原始偏斜" value={summarizeSkew(preview.natural_skew)} rawValue={preview.natural_skew} />
        <Metric label="构造后偏斜" value={summarizeSkew(selectedWindow?.realized_skew ?? preview.derived_skew)} rawValue={selectedWindow?.realized_skew ?? preview.derived_skew} wide />
      </dl>
      <div className="workload-detail-grid">
        {Object.entries(selectedCounts).map(([key, value]) => {
          const numericValue = Number(value);
          const percentage = selectedPercentages[key] ?? (selectedCount && Number.isFinite(numericValue) ? numericValue / selectedCount : 0);
          return <section key={key}><h4>{key}</h4><p>{valueText(value)} tx</p><p className="muted">{(percentage * 100).toFixed(2)}%</p></section>;
        })}
      </div>
    </> : <p className="muted">尚未预览。</p>}
  </article>;
}

function Metric({ label, value, rawValue, wide = false, compact = false }: { label: string; value: unknown; rawValue?: unknown; wide?: boolean; compact?: boolean }) {
  const rawAvailable = isStructured(rawValue) && Object.keys(rawValue as ObjectRecord).length > 0;
  return <div className={`${wide ? "workload-metric-wide" : ""} ${compact ? "workload-metric-compact" : ""}`.trim()}>
    <dt>{label}</dt>
    <dd title={typeof value === "string" ? value : undefined}>{valueText(value)}</dd>
    {rawAvailable && <details className="workload-raw-details"><summary>原始字段</summary><pre>{safeJson(rawValue)}</pre></details>}
  </div>;
}

function summarizeCache(value: unknown): string {
  const record = asRecord(value);
  if (!record) return "未提供";
  const required = record.required;
  const hit = record.cache_hit;
  if (required === false) return "无需物化缓存";
  if (hit === true) return "已命中缓存，可直接复用";
  if (hit === false) return "缓存未命中，本次会重新物化";
  if (required === true) return "需要物化，缓存命中状态待确定";
  return compactRecord(record, 3);
}

function summarizeCrossShard(value: unknown): string {
  const record = asRecord(value);
  if (!record) return valueText(value);
  const count = firstDefined(record, ["expected_cross_shard_count", "cross_shard_count", "expected_count", "count"]);
  const ratio = firstDefined(record, ["expected_cross_shard_ratio", "cross_shard_ratio", "expected_ratio", "ratio"]);
  if (count !== undefined || ratio !== undefined) return `${valueText(count)} 笔 / ${formatRatio(ratio)}`;
  const required = record.required;
  if (typeof required === "boolean") return required ? "需要跨片语义" : "不要求跨片语义";
  return compactRecord(record, 4);
}

function summarizeDistribution(value: unknown): string {
  const record = asRecord(value);
  if (!record) return valueText(value);
  const entries = Object.entries(record).filter(([, item]) => item !== undefined && item !== null);
  if (!entries.length) return "无分片分布数据";
  return entries.slice(0, 8).map(([key, item]) => `${key}: ${valueText(item)}`).join(" · ") + (entries.length > 8 ? ` · +${entries.length - 8}` : "");
}

function summarizeSkew(value: unknown): string {
  const record = asRecord(value);
  if (!record) return valueText(value);
  const flattened = flattenRecord(record, "", 0);
  if (!flattened.length) return "无偏斜统计";
  const priority = ["target_account_write_theta", "measured_account_write_theta", "measured_account_touch_theta", "target_theta", "realized_theta", "mle_theta", "theta", "target_alpha", "realized_alpha", "alpha", "skew_axis", "axis", "access_profile", "identity_count", "active_key_count", "unique_key_count", "top1_share", "top_1_share"];
  flattened.sort((a, b) => rankKey(a[0], priority) - rankKey(b[0], priority) || a[0].localeCompare(b[0]));
  const selected = flattened.slice(0, 6);
  return selected.map(([key, item]) => `${friendlyKey(key)}: ${compactScalar(item)}`).join(" · ") + (flattened.length > selected.length ? ` · 另 ${flattened.length - selected.length} 项` : "");
}

function flattenRecord(record: ObjectRecord, prefix: string, depth: number): Array<[string, unknown]> {
  const out: Array<[string, unknown]> = [];
  for (const [key, item] of Object.entries(record)) {
    if (item === undefined || item === null || item === "") continue;
    const path = prefix ? `${prefix}.${key}` : key;
    if (isStructured(item) && depth < 1) out.push(...flattenRecord(item as ObjectRecord, path, depth + 1));
    else if (!Array.isArray(item)) out.push([path, item]);
  }
  return out;
}

function compactRecord(record: ObjectRecord, limit: number): string {
  const entries = Object.entries(record).filter(([, item]) => item !== undefined && item !== null && item !== "");
  if (!entries.length) return "未提供";
  return entries.slice(0, limit).map(([key, item]) => `${friendlyKey(key)}: ${compactScalar(item)}`).join(" · ") + (entries.length > limit ? ` · 另 ${entries.length - limit} 项` : "");
}

function compactScalar(value: unknown): string {
  if (typeof value === "boolean") return value ? "是" : "否";
  if (typeof value === "number") return value.toLocaleString(undefined, { maximumFractionDigits: 4 });
  if (typeof value === "string") return value.length > 80 ? `${value.slice(0, 77)}…` : value;
  if (Array.isArray(value)) return value.length ? `${value.slice(0, 4).map(compactScalar).join(", ")}${value.length > 4 ? `, +${value.length - 4}` : ""}` : "[]";
  if (isStructured(value)) return compactRecord(value as ObjectRecord, 3);
  return valueText(value);
}

function friendlyKey(path: string): string {
  const key = path.split(".").pop() ?? path;
  const labels: Record<string, string> = {
    target_account_write_theta: "目标账户写 θ",
    measured_account_write_theta: "实测账户写 θ",
    measured_account_touch_theta: "实测账户触点 θ",
    measured_account_access_theta: "实测账户触点 θ",
    target_theta: "构造目标 θ",
    realized_theta: "实现 θ",
    mle_theta: "MLE θ",
    theta: "θ",
    target_alpha: "目标 α",
    realized_alpha: "实现 α",
    alpha: "α",
    skew_axis: "偏斜轴",
    axis: "轴",
    access_profile: "访问画像",
    identity_count: "活跃身份",
    active_key_count: "活跃键",
    unique_key_count: "唯一键",
    top1_share: "Top-1 占比",
    top_1_share: "Top-1 占比",
    required: "需要",
    cache_hit: "缓存命中",
    cache_root: "缓存目录",
  };
  return labels[key] ?? key.replace(/_/g, " ");
}

function rankKey(path: string, priority: string[]): number {
  const key = path.split(".").pop() ?? path;
  const index = priority.indexOf(key);
  return index >= 0 ? index : priority.length + 1;
}

function firstDefined(record: ObjectRecord, keys: string[]): unknown {
  for (const key of keys) if (record[key] !== undefined && record[key] !== null) return record[key];
  return undefined;
}

function formatRatio(value: unknown): string {
  if (typeof value !== "number") return valueText(value);
  if (value >= 0 && value <= 1) return `${(value * 100).toFixed(2)}%`;
  return value.toLocaleString(undefined, { maximumFractionDigits: 4 });
}

function isStructured(value: unknown): value is ObjectRecord { return typeof value === "object" && value !== null && !Array.isArray(value); }
function asRecord(value: unknown): ObjectRecord | null { return isStructured(value) ? value : null; }
function safeJson(value: unknown): string { try { return JSON.stringify(value, null, 2); } catch { return valueText(value); } }
