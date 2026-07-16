import type { V5WorkloadDatasetSummary } from "../../api";
import { WORKLOAD_ALPHA_OPTIONS, WORKLOAD_TX_OPTIONS } from "../../workloadUi";

export type WorkloadMode = "synthetic" | "dataset_original" | "dataset_derived";
export type WorkloadEditorState = {
  mode: WorkloadMode;
  datasetId: string;
  txCount: number;
  useFullDataset: boolean;
  seedText: string;
  targetAlpha: number;
  crossShardRatio: number;
  timeoutEvery: number;
  timeoutEnabled: boolean;
};

export default function WorkloadSourceEditor({ state, datasets, onChange }: { state: WorkloadEditorState; datasets: V5WorkloadDatasetSummary[]; onChange: (state: WorkloadEditorState) => void }) {
  const dataset = datasets.find((item) => item.dataset_id === state.datasetId);
  const hasSelectableDataset = datasets.some((item) => item.selectable);
  const datasetDisabled = state.mode !== "synthetic" && (!dataset || !dataset.selectable);
  function patch(next: Partial<WorkloadEditorState>) { onChange({ ...state, ...next }); }
  return <article className="final-card wide" data-testid="workload-source-editor">
    <h3>负载来源</h3>
    <div className="segmented-control" role="group" aria-label="负载来源">
      <button type="button" data-testid="workload-mode-synthetic" className={state.mode === "synthetic" ? "selected" : ""} onClick={() => patch({ mode: "synthetic", useFullDataset: false })}>确定性签名合成负载</button>
      <button type="button" data-testid="workload-mode-original" className={state.mode === "dataset_original" ? "selected" : ""} disabled={!hasSelectableDataset} onClick={() => patch({ mode: "dataset_original" })}>Decentraland 原始销售负载</button>
      <button type="button" data-testid="workload-mode-derived" className={state.mode === "dataset_derived" ? "selected" : ""} disabled={!hasSelectableDataset} onClick={() => patch({ mode: "dataset_derived" })}>Decentraland 合约 Zipf 派生偏斜负载</button>
    </div>
    {state.mode !== "synthetic" && <div className="experiment-condition-grid">
      <label><span>dataset_id</span><select aria-label="dataset_id" value={state.datasetId} onChange={(event) => patch({ datasetId: event.target.value })}>{datasets.map((item) => <option key={item.dataset_id} value={item.dataset_id} disabled={!item.selectable}>{item.display_name} / {item.dataset_id} / {item.selectable ? "selectable" : "unavailable"}</option>)}</select></label>
      <label><span>交易规模</span><select aria-label="dataset tx count" value={state.useFullDataset ? "Full" : String(state.txCount)} onChange={(event) => event.target.value === "Full" ? patch({ useFullDataset: true, txCount: dataset?.row_count ?? state.txCount }) : patch({ useFullDataset: false, txCount: globalThis.Number(event.target.value) })}>{WORKLOAD_TX_OPTIONS.map((value) => <option key={value} value={value}>{value / 1000}K</option>)}<option value="Full">Full</option></select></label>
      <label><span>seed</span><input aria-label="dataset seed" value={state.seedText} onChange={(event) => patch({ seedText: event.target.value })} /></label>
      {state.mode === "dataset_derived" && <label><span>target_alpha</span><select aria-label="target_alpha" value={state.targetAlpha} onChange={(event) => patch({ targetAlpha: globalThis.Number(event.target.value) })}>{WORKLOAD_ALPHA_OPTIONS.map((value) => <option key={value} value={value}>{value.toFixed(1)}</option>)}</select></label>}
      <label><span>selection_mode</span><input value="contiguous_window" readOnly /></label>
      <label><span>replay_mode</span><input value="max_throughput" readOnly /></label>
    </div>}
    {state.mode === "synthetic" && <div className="experiment-condition-grid">
      <label><span>交易数量</span><input aria-label="tx_count" type="number" min={1} value={state.txCount} onChange={(event) => patch({ txCount: globalThis.Number(event.target.value) })} /></label>
      <label><span>跨片交易比例</span><input aria-label="cross_shard_ratio" type="number" min={0} max={1} step={0.01} value={state.crossShardRatio} onChange={(event) => patch({ crossShardRatio: globalThis.Number(event.target.value) })} /></label>
      <label><span>seed</span><input aria-label="seeds" value={state.seedText} onChange={(event) => patch({ seedText: event.target.value })} /></label>
      <label><input type="checkbox" checked={state.timeoutEnabled} onChange={(event) => patch({ timeoutEnabled: event.target.checked })} /> 启用 timeout_every</label>
      {state.timeoutEnabled && <label><span>timeout_every</span><input aria-label="timeout_every" type="number" min={0} value={state.timeoutEvery} onChange={(event) => patch({ timeoutEvery: globalThis.Number(event.target.value) })} /></label>}
    </div>}
    {datasetDisabled && <p className="file-error" data-testid="workload-selection-blocked">所选数据集不可用或未通过验证，不能启动 RunGroup。</p>}
  </article>;
}
