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

function initialParameters(definition: V5WorkloadVariantDefinition | undefined): Record<string, WorkloadParameterValue> {
  return Object.fromEntries((definition?.parameters ?? []).flatMap((field) => field.default === undefined || field.default === null ? [] : [[field.name, field.default as WorkloadParameterValue]]));
}

function optionLabel(field: V5WorkloadVariantParameter, value: WorkloadParameterValue): string {
  return field.option_labels?.[String(value)] ?? String(value);
}

function countLabel(value: number): string {
  if (value >= 1000 && value % 1000 === 0) return `${value / 1000}K`;
  return String(value);
}

export default function WorkloadSourceEditor({ state, datasets, onChange }: { state: WorkloadEditorState; datasets: V5WorkloadDatasetSummary[]; onChange: (state: WorkloadEditorState) => void }) {
  const dataset = datasets.find((item) => item.dataset_id === state.datasetId);
  const modeDefinitions = definitionsForMode(dataset, state.mode);
  const selectedDefinition = modeDefinitions.find((item) => item.variant_mode === state.variantMode) ?? modeDefinitions[0];
  const hasOriginalDataset = datasets.some((item) => item.selectable && definitionsForMode(item, "dataset_original").length > 0);
  const hasDerivedDataset = datasets.some((item) => item.selectable && definitionsForMode(item, "dataset_derived").length > 0);
  const datasetDisabled = state.mode !== "synthetic" && (!dataset || !dataset.selectable || !selectedDefinition);
  const supportedCounts = (dataset?.supported_tx_counts?.length ? dataset.supported_tx_counts : [1_000, 10_000, 50_000, 100_000, 250_000])
    .filter((value) => value >= 1_000 || value === state.txCount)
    .sort((a, b) => a - b);

  function patch(next: Partial<WorkloadEditorState>) { onChange({ ...state, ...next }); }

  function selectMode(mode: WorkloadMode) {
    if (mode === "synthetic") { patch({ mode, useFullDataset: false }); return; }
    const candidateDataset = dataset?.selectable && definitionsForMode(dataset, mode).length ? dataset : datasets.find((item) => item.selectable && definitionsForMode(item, mode).length);
    const definition = definitionsForMode(candidateDataset, mode)[0];
    const parameters = initialParameters(definition);
    patch({
      mode,
      datasetId: candidateDataset?.dataset_id ?? state.datasetId,
      variantMode: definition?.variant_mode ?? "",
      variantParameters: parameters,
      useFullDataset: false,
      txCount: candidateDataset?.supported_tx_counts?.includes(state.txCount) ? state.txCount : (candidateDataset?.supported_tx_counts?.find((value) => value >= 1_000) ?? state.txCount),
      targetAlpha: typeof parameters.target_alpha === "number" ? parameters.target_alpha : state.targetAlpha,
      skewAxis: typeof parameters.skew_axis === "string" ? parameters.skew_axis : state.skewAxis,
    });
  }

  function selectDataset(datasetId: string) {
    const nextDataset = datasets.find((item) => item.dataset_id === datasetId);
    const definition = definitionsForMode(nextDataset, state.mode)[0] ?? nextDataset?.variant_definitions?.[0];
    const parameters = initialParameters(definition);
    patch({
      datasetId,
      variantMode: definition?.variant_mode ?? "",
      variantParameters: parameters,
      useFullDataset: false,
      txCount: nextDataset?.supported_tx_counts?.includes(state.txCount) ? state.txCount : (nextDataset?.supported_tx_counts?.find((value) => value >= 1_000) ?? state.txCount),
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
      <label><span>数据集</span><select aria-label="dataset_id" value={state.datasetId} onChange={(event) => selectDataset(event.target.value)}>{datasets.map((item) => <option key={item.dataset_id} value={item.dataset_id} disabled={!item.selectable}>{item.display_name} / {item.selectable ? "selectable" : "unavailable"}</option>)}</select></label>
      <label><span>数据来源平台</span><input value={dataset?.source_platform ?? "registry"} readOnly /></label>
      <label><span>数据来源链</span><input value={dataset?.source_chain ?? "registry"} readOnly /></label>
      {modeDefinitions.length > 1 && <label><span>负载模式</span><select aria-label="variant_mode" value={selectedDefinition?.variant_mode ?? ""} onChange={(event) => selectVariant(event.target.value)}>{modeDefinitions.map((item) => <option key={item.variant_mode} value={item.variant_mode}>{item.display_name ?? item.variant_mode}</option>)}</select></label>}
      {modeDefinitions.length <= 1 && <label><span>负载模式</span><input aria-label="variant_mode" value={selectedDefinition?.variant_mode ?? "未提供"} readOnly /></label>}
      <label><span>交易规模</span><select aria-label="dataset tx count" value={state.useFullDataset ? "Full" : String(state.txCount)} onChange={(event) => event.target.value === "Full" ? patch({ useFullDataset: true, txCount: dataset?.row_count ?? state.txCount }) : patch({ useFullDataset: false, txCount: globalThis.Number(event.target.value) })}>{supportedCounts.map((value) => <option key={value} value={value}>{countLabel(value)}</option>)}{dataset?.allow_full_dataset !== false && <option value="Full">Full</option>}</select></label>
      <label><span>seed</span><input aria-label="dataset seed" value={state.seedText} onChange={(event) => patch({ seedText: event.target.value })} /></label>
      {(selectedDefinition?.parameters ?? []).map((field) => {
        const value = state.variantParameters[field.name] ?? field.default ?? "";
        const options = field.options ?? [];
        if (field.type === "enum" || field.type === "number_enum") return <label key={field.name}><span>{field.label ?? field.name}</span><select aria-label={field.name} value={String(value)} onChange={(event) => patchParameter(field, field.type === "number_enum" ? globalThis.Number(event.target.value) : event.target.value)}>{options.map((option) => <option key={String(option)} value={String(option)}>{optionLabel(field, option as WorkloadParameterValue)}</option>)}</select></label>;
        return <label key={field.name}><span>{field.label ?? field.name}</span><input aria-label={field.name} value={String(value)} onChange={(event) => patchParameter(field, field.type === "number" ? globalThis.Number(event.target.value) : event.target.value)} /></label>;
      })}
      <label><span>truth label</span><input value={dataset?.truth_label ?? "未提供"} readOnly /></label>
      <label><span>selection_mode</span><input value={selectedDefinition?.selection_mode ?? "contiguous_window"} readOnly /></label>
      <label><span>replay_mode</span><input value="max_throughput" readOnly /></label>
    </div>}

    {state.mode === "synthetic" && <div className="experiment-condition-grid">
      <label><span>交易数量</span><input aria-label="tx_count" type="number" min={1} value={state.txCount} onChange={(event) => patch({ txCount: globalThis.Number(event.target.value) })} /></label>
      <label><span>跨片交易比例</span><input aria-label="cross_shard_ratio" type="number" min={0} max={1} step={0.01} value={state.crossShardRatio} onChange={(event) => patch({ crossShardRatio: globalThis.Number(event.target.value) })} /></label>
      <label><span>seed</span><input aria-label="seeds" value={state.seedText} onChange={(event) => patch({ seedText: event.target.value })} /></label>
      <label><input type="checkbox" checked={state.timeoutEnabled} onChange={(event) => patch({ timeoutEnabled: event.target.checked })} /> 启用 timeout_every</label>
      {state.timeoutEnabled && <label><span>timeout_every</span><input aria-label="timeout_every" type="number" min={0} value={state.timeoutEvery} onChange={(event) => patch({ timeoutEvery: globalThis.Number(event.target.value) })} /></label>}
    </div>}
    {datasetDisabled && <p className="file-error" data-testid="workload-selection-blocked">所选数据集或变体不可用，不能启动 RunGroup。</p>}
  </article>;
}
