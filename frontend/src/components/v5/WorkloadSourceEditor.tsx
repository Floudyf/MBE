import type { V5WorkloadDatasetSummary, V5WorkloadVariantDefinition, V5WorkloadVariantParameter } from "../../api";

export type WorkloadMode = "synthetic" | "dataset_original" | "dataset_derived";
export type WorkloadParameterValue = string | number | boolean;
export type WorkloadEditorState = {
  mode: WorkloadMode;
  datasetId: string;
  variantMode: string;
  variantParameters: Record<string, WorkloadParameterValue>;
  txCount: number;
  useFullDataset: boolean;
  seedText: string;
  replayMode: "max_throughput" | "fixed_rate";
  targetSubmissionTPS: number;
  targetAlpha: number;
  crossShardRatio: number;
  timeoutEvery: number;
  timeoutEnabled: boolean;
  skewAxis: string;
};

function variantKind(definition: V5WorkloadVariantDefinition): "original" | "derived" {
  return definition.kind === "derived" ? "derived" : "original";
}

function definitionsForMode(dataset: V5WorkloadDatasetSummary | undefined, mode: WorkloadMode): V5WorkloadVariantDefinition[] {
  const kind = mode === "dataset_derived" ? "derived" : "original";
  return (dataset?.variant_definitions ?? []).filter((item) => variantKind(item) === kind);
}

// MBE_HENE_WINDOW_UI_V1: prefer a historical original window on NEW Henesys selections.
// Previously saved original_window drafts are never silently changed.
const HENE_HISTORY_DATASETS = new Set(["hene_20250612_calls_v2","hene_20250824_calls_v2","henesys_20250612_v4","henesys_20250824_v4"]);
function preferredHeneDefinition(dataset: V5WorkloadDatasetSummary | undefined, mode: WorkloadMode): V5WorkloadVariantDefinition | undefined {
  const definitions = definitionsForMode(dataset, mode);
  return (mode === "dataset_original" && HENE_HISTORY_DATASETS.has(dataset?.dataset_id ?? "")
    ? definitions.find((item) => item.variant_mode === "historical_window") : undefined) ?? definitions[0];
}

function initialParameters(definition: V5WorkloadVariantDefinition | undefined): Record<string, WorkloadParameterValue> {
  return Object.fromEntries((definition?.parameters ?? []).flatMap((field) => field.default === undefined || field.default === null ? [] : [[field.name, field.default as WorkloadParameterValue]]));
}

function optionLabel(field: V5WorkloadVariantParameter, value: WorkloadParameterValue): string {
  return field.option_labels?.[String(value)] ?? String(value);
}

// MT_COUNT_UI_V1: manifest is the only source of permissible real replay counts.
export function validDatasetCounts(dataset: V5WorkloadDatasetSummary | undefined): number[] {
  if (!dataset || !Number.isSafeInteger(dataset.row_count) || dataset.row_count <= 0) return [];
  return [...new Set(dataset.supported_tx_counts ?? [])]
    .filter((n) => Number.isSafeInteger(n) && n > 0 && n <= dataset.row_count && (dataset.allow_full_dataset !== false || n < dataset.row_count))
    .sort((a, b) => a - b);
}

export function chooseDatasetCount(dataset: V5WorkloadDatasetSummary | undefined, current: number): number {
  const valid = validDatasetCounts(dataset);
  if (valid.includes(current)) return current;
  return valid.includes(10_000) ? 10_000 : (valid.find((n) => n >= 1_000) ?? valid[0] ?? 0);
}

function countLabel(value: number): string {
  return `${value.toLocaleString("en-US")} 笔${value >= 1_000 && value % 1_000 === 0 ? `（${value / 1_000}K）` : ""}`;
}

// MBE_MV_UIFIX_V1 formal-run catalog visibility only.
const PUBLISHED_V5_DATASETS = new Set([
  "alien_worlds_layered_v2_controlled",
  "alien_worlds_layered_v2_historical",
  "axie_day_layered_v4",
  "dcl_sales_layered_v4",
  "axie_infinity_controlled_prefix_rmw_v1",
  "tapos_layered_v4", // MBE_MV_TAPOS_V4_V1
  "henesys_20250612_v4",
  "henesys_20250824_v4",


  // MBE_FW_UIACCESS_V1 six independent real replay projections
  "farmersworld_20260908_v4",
  "fw_20260908_roles_v2",
  "hene_20250612_calls_v2",
  "hene_20250824_calls_v2",
]);

// MBE_FW_PICKERFIX_V1_EDITOR: internal alternative projections stay in the backend/library, not the formal picker.
const FORMAL_HIDDEN_SEMANTIC = new Set([
  "farmersworld_20260908_v4", "hene_20250612_calls_v2", "hene_20250824_calls_v2",
]);
const FORMAL_DATASET_LABELS: Record<string, string> = {
  "fw_20260908_roles_v2": "Farmers World（WAX · 2026-09-08 · 真实交易访问建模）",
  "henesys_20250612_v4": "MapleStory N · Henesys（2025-06-12 · 真实交易访问建模）",
  "henesys_20250824_v4": "MapleStory N · Henesys（2025-08-24 · 真实交易访问建模）",
};
const FORMAL_SEMANTIC_FALLBACK: Record<string, string> = {
  "farmersworld_20260908_v4": "fw_20260908_roles_v2",
  "hene_20250612_calls_v2": "henesys_20250612_v4",
  "hene_20250824_calls_v2": "henesys_20250824_v4",
};
export function canonicalFormalDatasetId(id: string): string {
  return FORMAL_SEMANTIC_FALLBACK[id] ?? id;
}

export function publishedWorkloadDatasets(datasets: V5WorkloadDatasetSummary[], mode: WorkloadMode): V5WorkloadDatasetSummary[] {
  if (mode === "synthetic") return [];
  return datasets.filter((dataset) => PUBLISHED_V5_DATASETS.has(dataset.dataset_id) && !FORMAL_HIDDEN_SEMANTIC.has(dataset.dataset_id) && definitionsForMode(dataset, mode).length > 0);
}

export default function WorkloadSourceEditor({ state, datasets, onChange }: { state: WorkloadEditorState; datasets: V5WorkloadDatasetSummary[]; onChange: (state: WorkloadEditorState) => void }) {
  const visibleDatasets = publishedWorkloadDatasets(datasets, state.mode);
  const dataset = visibleDatasets.find((item) => item.dataset_id === state.datasetId);
  const modeDefinitions = definitionsForMode(dataset, state.mode);
  const selectedDefinition = modeDefinitions.find((item) => item.variant_mode === state.variantMode) ?? modeDefinitions[0];
  const hasOriginalDataset = publishedWorkloadDatasets(datasets, "dataset_original").some((item) => item.selectable);
  const hasDerivedDataset = publishedWorkloadDatasets(datasets, "dataset_derived").some((item) => item.selectable);
  const datasetDisabled = state.mode !== "synthetic" && (!dataset || !dataset.selectable || !selectedDefinition || !(state.useFullDataset ? (dataset.allow_full_dataset !== false) : validDatasetCounts(dataset).includes(state.txCount)));
  const supportedCounts = validDatasetCounts(dataset);
  const supportsFull = Boolean(dataset && dataset.allow_full_dataset !== false && dataset.row_count > 0);
  const visibleNumericCounts = supportedCounts.filter((n) => !supportsFull || n !== dataset?.row_count);
  const shownCount = supportsFull && (state.useFullDataset || state.txCount === dataset?.row_count)
    ? "Full" : String(state.txCount);
  const formatTotal = (n: number | undefined) => Number.isSafeInteger(n) && (n ?? 0) > 0 ? (n as number).toLocaleString("en-US") : "未知";

  function patch(next: Partial<WorkloadEditorState>) { onChange({ ...state, ...next }); }

  function selectMode(mode: WorkloadMode) {
    if (mode === "synthetic") { patch({ mode, useFullDataset: false }); return; }
    const choices = publishedWorkloadDatasets(datasets, mode);
    const candidateDataset = choices.find((item) => item.dataset_id === state.datasetId && item.selectable) ?? choices.find((item) => item.selectable);
    const definition = preferredHeneDefinition(candidateDataset, mode);
    const parameters = initialParameters(definition);
    patch({
      mode,
      datasetId: candidateDataset?.dataset_id ?? state.datasetId,
      variantMode: definition?.variant_mode ?? "",
      variantParameters: parameters,
      useFullDataset: false,
      txCount: chooseDatasetCount(candidateDataset, state.txCount),
      targetAlpha: typeof parameters.target_alpha === "number" ? parameters.target_alpha : state.targetAlpha,
      skewAxis: typeof parameters.skew_axis === "string" ? parameters.skew_axis : state.skewAxis,
    });
  }

  function selectDataset(datasetId: string) {
    const nextDataset = visibleDatasets.find((item) => item.dataset_id === datasetId);
    if (!nextDataset) return;
    const definition = preferredHeneDefinition(nextDataset, state.mode);
    const parameters = initialParameters(definition);
    patch({
      datasetId,
      variantMode: definition?.variant_mode ?? "",
      variantParameters: parameters,
      useFullDataset: false,
      txCount: chooseDatasetCount(nextDataset, state.txCount),
      targetAlpha: typeof parameters.target_alpha === "number" ? parameters.target_alpha : state.targetAlpha,
      skewAxis: typeof parameters.skew_axis === "string" ? parameters.skew_axis : state.skewAxis,
    });
  }

  function selectVariant(variantMode: string) {
    const definition = modeDefinitions.find((item) => item.variant_mode === variantMode);
    const parameters = initialParameters(definition);
    patch({
      variantMode,
      variantParameters: parameters,
      targetAlpha: typeof parameters.target_alpha === "number" ? parameters.target_alpha : state.targetAlpha,
      skewAxis: typeof parameters.skew_axis === "string" ? parameters.skew_axis : state.skewAxis,
    });
  }

  function patchParameter(field: V5WorkloadVariantParameter, value: WorkloadParameterValue) {
    const variantParameters = { ...state.variantParameters, [field.name]: value };
    patch({
      variantParameters,
      targetAlpha: field.name === "target_alpha" && typeof value === "number" ? value : state.targetAlpha,
      skewAxis: field.name === "skew_axis" && typeof value === "string" ? value : state.skewAxis,
    });
  }

  return <article className="final-card wide" data-testid="workload-source-editor">
    <h3>负载来源</h3>
    <div className="segmented-control" role="group" aria-label="负载来源">
      <button type="button" data-testid="workload-mode-synthetic" className={state.mode === "synthetic" ? "selected" : ""} onClick={() => selectMode("synthetic")}>模拟负载</button>
      <button type="button" data-testid="workload-mode-original" className={state.mode === "dataset_original" ? "selected" : ""} disabled={!hasOriginalDataset} onClick={() => selectMode("dataset_original")}>真实原始负载</button>
      <button type="button" data-testid="workload-mode-derived" className={state.mode === "dataset_derived" ? "selected" : ""} disabled={!hasDerivedDataset} onClick={() => selectMode("dataset_derived")}>真实派生/重构负载</button>
    </div>

    {state.mode !== "synthetic" && <div className="experiment-condition-grid">
      <label><span>数据集</span><select aria-label="dataset_id" value={state.datasetId} onChange={(event) => selectDataset(event.target.value)}>{visibleDatasets.map((item) => <option key={item.dataset_id} value={item.dataset_id} disabled={!item.selectable}>{FORMAL_DATASET_LABELS[item.dataset_id] ?? item.display_name} / {item.selectable ? "可用" : "不可用"}</option>)}</select></label>
      <label><span>数据来源平台</span><input value={dataset?.source_platform ?? "registry"} readOnly /></label>
      <label><span>数据来源链</span><input value={dataset?.source_chain ?? "registry"} readOnly /></label>
      {modeDefinitions.length > 1 && <label><span>负载模式</span><select aria-label="variant_mode" value={selectedDefinition?.variant_mode ?? ""} onChange={(event) => selectVariant(event.target.value)}>{modeDefinitions.map((item) => <option key={item.variant_mode} value={item.variant_mode}>{item.display_name ?? item.variant_mode}</option>)}</select></label>}
      {modeDefinitions.length <= 1 && <label><span>负载模式</span><input aria-label="variant_mode" value={selectedDefinition?.variant_mode ?? "未提供"} readOnly /></label>}
      <div className="readonly-field" data-testid="dataset-total-tx-count"><span>数据集总交易数</span><strong>{formatTotal(dataset?.row_count)} 笔</strong></div>
      <div className="readonly-field" data-testid="dataset-max-tx-count"><span>最大可选规模</span><strong>{formatTotal(supportsFull ? dataset?.row_count : supportedCounts[supportedCounts.length - 1])} 笔</strong></div>
      <label><span>交易规模</span><select aria-label="dataset tx count" value={shownCount} disabled={datasetDisabled || (!supportsFull && supportedCounts.length === 0)} onChange={(event) => event.target.value === "Full" ? patch({ useFullDataset: true, txCount: dataset?.row_count ?? 0 }) : patch({ useFullDataset: false, txCount: globalThis.Number(event.target.value) })}>
        {[["小规模 / 调试", visibleNumericCounts.filter((n) => n < 10_000)], ["正式实验", visibleNumericCounts.filter((n) => n >= 10_000 && n <= 100_000)], ["大规模 / 扩展性", visibleNumericCounts.filter((n) => n > 100_000)]] .map(([group, values]) => (values as number[]).length > 0 && <optgroup key={group as string} label={group as string}>{(values as number[]).map((value) => <option key={value} value={value}>{countLabel(value)}</option>)}</optgroup>)}
        {supportsFull && <option value="Full">完整数据集：{countLabel(dataset!.row_count)}</option>}
      </select></label>
      {dataset && !supportsFull && <div className="readonly-field" data-testid="dataset-full-mode-note"><span>全量执行</span><strong>未开放；仅使用已验证规模</strong></div>}
      <label><span>seed</span><input aria-label="dataset seed" value={state.seedText} onChange={(event) => patch({ seedText: event.target.value })} /></label>
      {(selectedDefinition?.parameters ?? []).map((field) => {
        const value = state.variantParameters[field.name] ?? field.default ?? "";
        const options = field.options ?? [];
        if (field.type === "enum" || field.type === "number_enum") return <label key={field.name}><span>{field.label ?? field.name}</span><select aria-label={field.name} value={String(value)} onChange={(event) => patchParameter(field, field.type === "number_enum" ? globalThis.Number(event.target.value) : event.target.value)}>{options.map((option) => <option key={String(option)} value={String(option)}>{optionLabel(field, option as WorkloadParameterValue)}</option>)}</select></label>;
        return <label key={field.name}><span>{field.label ?? field.name}</span><input aria-label={field.name} value={String(value)} onChange={(event) => patchParameter(field, field.type === "number" ? globalThis.Number(event.target.value) : event.target.value)} /></label>;
      })}
      <label><span>truth label</span><input value={dataset?.truth_label ?? "未提供"} readOnly /></label>
      <label><span>selection_mode</span><input value={selectedDefinition?.selection_mode ?? "contiguous_window"} readOnly /></label>
      {HENE_HISTORY_DATASETS.has(dataset?.dataset_id ?? "") && selectedDefinition?.variant_mode === "original_window" &&
        <div role="alert">原始前缀从第 0 行开始，没有 TxAllo 所需的评测前历史。若包含 TxAllo，请在“负载模式”选择 historical_window，并为所有对照方法使用同一窗口。</div>}
      {HENE_HISTORY_DATASETS.has(dataset?.dataset_id ?? "") && selectedDefinition?.variant_mode === "historical_window" &&
        <div role="status">历史连续窗口：请确认所有对照方法的交易范围、seed 和物化摘要相同；TxAllo 只使用该窗口之前的真实历史。</div>}
    </div>}

    {state.mode === "synthetic" && <div className="experiment-condition-grid">
      <label><span>交易数量</span><input aria-label="tx_count" type="number" min={1} value={state.txCount} onChange={(event) => patch({ txCount: globalThis.Number(event.target.value) })} /></label>
      <label><span>跨片交易比例</span><input aria-label="cross_shard_ratio" type="number" min={0} max={1} step={0.01} value={state.crossShardRatio} onChange={(event) => patch({ crossShardRatio: globalThis.Number(event.target.value) })} /></label>
      <label><span>seed</span><input aria-label="seeds" value={state.seedText} onChange={(event) => patch({ seedText: event.target.value })} /></label>
      <label><input type="checkbox" checked={state.timeoutEnabled} onChange={(event) => patch({ timeoutEnabled: event.target.checked })} /> 启用 timeout_every</label>
      {state.timeoutEnabled && <label><span>timeout_every</span><input aria-label="timeout_every" type="number" min={0} value={state.timeoutEvery} onChange={(event) => patch({ timeoutEvery: globalThis.Number(event.target.value) })} /></label>}
    </div>}
    <div className="experiment-condition-grid" data-testid="workload-replay-settings">
      <label><span>交易注入模式</span><select aria-label="replay_mode" value={state.replayMode} onChange={(event) => patch({ replayMode: event.target.value as WorkloadEditorState["replayMode"] })}><option value="max_throughput">最大吞吐</option><option value="fixed_rate">固定速率</option></select></label>
      {state.replayMode === "fixed_rate" && <label><span>目标注入速率 (tx/s)</span><input aria-label="target_submission_tps" type="number" min={1} max={1_000_000} value={state.targetSubmissionTPS} onChange={(event) => patch({ targetSubmissionTPS: globalThis.Number(event.target.value) })} /></label>}
    </div>
    {datasetDisabled && <p className="file-error" data-testid="workload-selection-blocked">所选数据集或变体不可用，不能启动 RunGroup。</p>}
  </article>;
}
