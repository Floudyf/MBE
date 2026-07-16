import { useEffect, useState } from "react";

import { fetchV5WorkloadDataset, fetchV5WorkloadDatasets, type V5WorkloadDatasetDetail, type V5WorkloadDatasetSummary } from "../api";
import { formatHash, formatTimeRange, truthText } from "../workloadUi";

export default function WorkloadLibraryPage() {
  const [items, setItems] = useState<V5WorkloadDatasetSummary[]>([]);
  const [selectedId, setSelectedId] = useState("");
  const [detail, setDetail] = useState<V5WorkloadDatasetDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [detailLoading, setDetailLoading] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => { void load(); }, []);
  async function load() {
    setLoading(true);
    try {
      const next = await fetchV5WorkloadDatasets();
      setItems(next);
      setError("");
      const first = next[0]?.dataset_id ?? "";
      setSelectedId(first);
      if (first) await loadDetail(first);
    } catch (caught) {
      setError(message(caught));
    } finally {
      setLoading(false);
    }
  }
  async function loadDetail(datasetId: string) {
    setDetailLoading(true);
    try {
      setDetail(await fetchV5WorkloadDataset(datasetId));
      setError("");
    } catch (caught) {
      setDetail(null);
      setError(message(caught));
    } finally {
      setDetailLoading(false);
    }
  }
  return <section className="page-grid workload-library-page" data-testid="v5-workload-library-page">
    <article className="final-card wide page-hero">
      <p className="eyebrow">V5 Workload Library</p>
      <h2>负载数据集注册表</h2>
      <p>这里展示后端 Registry 返回的负载数据源、可用性、验证状态和真实性边界。浏览器不读取完整 CSV，也不计算 canonical hash 或派生偏斜。</p>
      {loading && <p className="notice">正在加载数据集注册表...</p>}
      {error && <p className="file-error" data-testid="workload-library-error">{error}</p>}
    </article>
    <article className="final-card wide">
      <h3>数据集</h3>
      {items.length ? <div className="selectable-card-grid">
        {items.map((item) => <button type="button" key={item.dataset_id} data-testid={`workload-dataset-${item.dataset_id}`} className={`dataset-choice ${item.dataset_id === selectedId ? "selected" : ""}`} onClick={() => { setSelectedId(item.dataset_id); void loadDetail(item.dataset_id); }}>
          <strong>{item.display_name}</strong>
          <small>{item.dataset_id}</small>
          <span className={item.selectable ? "file-present" : "file-missing"}>{item.selectable ? "selectable" : item.available ? "validation failed" : "unavailable"}</span>
        </button>)}
      </div> : !loading && <p className="muted">没有注册的数据集。</p>}
    </article>
    <article className="final-card wide">
      <h3>注册详情</h3>
      {detailLoading && <p className="notice">正在加载详情...</p>}
      {detail ? <DatasetDetail detail={detail} /> : <p className="muted">请选择数据集。</p>}
    </article>
  </section>;
}

function DatasetDetail({ detail }: { detail: V5WorkloadDatasetDetail }) {
  return <>
    <dl className="stage-flow-kpis workload-kpis">
      <Metric label="display_name" value={detail.display_name} />
      <Metric label="dataset_id" value={detail.dataset_id} />
      <Metric label="source_platform" value={detail.source_platform} />
      <Metric label="source_chain" value={detail.source_chain} />
      <Metric label="dataset_type" value={detail.dataset_type} />
      <Metric label="truth_label" value={`${detail.truth_label} - ${truthText(detail.truth_label)}`} />
      <Metric label="row_count" value={detail.row_count} />
      <Metric label="time_range" value={formatTimeRange(detail.time_start_ms, detail.time_end_ms)} />
      <Metric label="source_sha256" value={formatHash(detail.source_sha256)} />
      <Metric label="validation_status" value={detail.validation_status} />
      <Metric label="available" value={detail.available} />
      <Metric label="selectable" value={detail.selectable} />
      <Metric label="verification" value={`${detail.verification_method}; samples=${detail.verification_sample_count}; ${detail.verification_results}`} />
    </dl>
    {!detail.available && <p className="file-error" data-testid="dataset-unavailable">完整本地母文件不可用或尚未通过校验，因此运行页不会允许选择该数据集。</p>}
    {detail.blockers.length ? <ul className="boundary-list">{detail.blockers.map((item) => <li key={item}>{item}</li>)}</ul> : null}
    <div className="workload-detail-grid">
      <Info title="Included categories" values={detail.included_categories} />
      <Info title="Excluded categories" values={detail.excluded_categories} />
      <Info title="Operation counts" values={Object.entries(detail.operation_counts ?? detail.category_counts).map(([key, value]) => `${key}: ${value}`)} />
      <Info title="Adapter" values={[detail.adapter_id ?? "未提供", ...(detail.supported_skew_axes ?? []).map((axis) => `skew_axis: ${axis}`)]} />
      <Info title="Supported variants" values={detail.variants.map((item) => `${String(item.variant_mode)} ${Array.isArray(item.target_alpha_values) ? `alpha=${item.target_alpha_values.join(",")}` : ""}`)} />
      <Info title="Truth boundary" values={[detail.usage_note, ...detail.warnings]} />
    </div>
  </>;
}

function Metric({ label, value }: { label: string; value: unknown }) { return <div><dt>{label}</dt><dd>{value === undefined || value === null || value === "" ? "未提供" : String(value)}</dd></div>; }
function Info({ title, values }: { title: string; values: string[] }) { return <section><h4>{title}</h4>{values.length ? <ul className="boundary-list">{values.map((value) => <li key={value}>{value}</li>)}</ul> : <p className="muted">未提供</p>}</section>; }
function message(value: unknown): string { return value instanceof Error ? value.message : String(value); }
