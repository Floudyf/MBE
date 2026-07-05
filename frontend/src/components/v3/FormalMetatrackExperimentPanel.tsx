import { useMemo, useState } from "react";
import type { V3FormalExperimentType, V3FormalMetatrackBenchmarkPreview, V3FormalMetatrackBenchmarkRequest, V3RuntimeEvidenceMode, V3SavedConfig } from "../../api";
import { toComposerDraftRequest, type ComposerDraft } from "./composerDraft";
import FormalExperimentMatrixPreview from "./FormalExperimentMatrixPreview";
import { IntegerSliderField, MultiSelectChipGroup, PresetChipGroup, RatioSliderField, SliderNumberField } from "./SliderFields";

type Props = {
  draft?: ComposerDraft | null;
  savedConfigs?: V3SavedConfig[];
  preview?: V3FormalMetatrackBenchmarkPreview | null;
  running?: boolean;
  previewing?: boolean;
  error?: string;
  onPreview: (payload: V3FormalMetatrackBenchmarkRequest) => void;
  onRun: (payload: V3FormalMetatrackBenchmarkRequest) => void;
};

const experimentTypes: [V3FormalExperimentType, string][] = [
  ["ablation", "消融实验"],
  ["hotspot_sensitivity", "热点偏斜敏感性"],
  ["cross_shard_sensitivity", "跨片比例敏感性"],
  ["shard_scalability", "分片数量扩展性"],
  ["control_overhead", "控制面开销实验"],
  ["workload_comparison", "不同负载场景对比"],
];
const baselineOptions = [
  "baseline_hash_serial",
  "baseline_hash_prefetch",
  "baseline_hash_dual_track",
  "baseline_hash_aggregation",
  "metatrack_full",
];
const workloadScenarioOptions = [
  { id: "asset_transfer", label: "资产转移" },
  { id: "avatar_update", label: "Avatar 更新" },
  { id: "scene_hotspot", label: "场景热点" },
  { id: "item_transfer", label: "道具转移" },
  { id: "cross_scene_migration", label: "跨场景迁移" },
  { id: "mixed_metaverse", label: "混合元宇宙" },
];

export default function FormalMetatrackExperimentPanel({ draft, savedConfigs = [], preview, running = false, previewing = false, error = "", onPreview, onRun }: Props) {
  const [experimentType, setExperimentType] = useState<V3FormalExperimentType>("ablation");
  const [methodSource, setMethodSource] = useState<"builtin" | "saved" | "mixed">("builtin");
  const [formalTxCount, setFormalTxCount] = useState(10000);
  const [seedBase, setSeedBase] = useState(42);
  const [seedCount, setSeedCount] = useState(5);
  const [baselineIds, setBaselineIds] = useState<string[]>(baselineOptions);
  const [hotspotPoints, setHotspotPoints] = useState("0.0, 0.2, 0.4, 0.6, 0.8");
  const [crossShardPoints, setCrossShardPoints] = useState("0.0, 0.2, 0.4, 0.6");
  const [shardPoints, setShardPoints] = useState("1, 2, 4, 8");
  const [workloadScenarioPoints, setWorkloadScenarioPoints] = useState<string[]>(["scene_hotspot", "cross_scene_migration", "mixed_metaverse"]);
  const [methodConfigIds, setMethodConfigIds] = useState<string[]>([]);
  const [workloadConfigIds, setWorkloadConfigIds] = useState<string[]>([]);
  const [topologyConfigIds, setTopologyConfigIds] = useState<string[]>([]);
  const [zipfAlpha, setZipfAlpha] = useState(0.8);
  const [runtimeEvidenceMode, setRuntimeEvidenceMode] = useState<V3RuntimeEvidenceMode>("logical_single_process");
  const [enableFaults, setEnableFaults] = useState(false);
  const seedList = useMemo(() => Array.from({ length: seedCount }, (_, index) => seedBase + index), [seedBase, seedCount]);
  const payload = draft ? buildPayload() : null;

  function buildPayload(): V3FormalMetatrackBenchmarkRequest {
    return {
      draft: toComposerDraftRequest(draft as ComposerDraft),
      experiment_type: experimentType,
      formal_tx_count: formalTxCount,
      seed_base: seedBase,
      seed_count: seedCount,
      baseline_ids: methodSource === "saved" ? [] : baselineIds,
      hotspot_ratio_points: parseFloatList(hotspotPoints),
      cross_shard_ratio_points: parseFloatList(crossShardPoints),
      shard_count_points: parseIntList(shardPoints),
      workload_scenario_points: workloadScenarioPoints,
      method_config_ids: methodSource === "builtin" ? [] : methodConfigIds,
      workload_config_ids: workloadConfigIds,
      topology_config_ids: topologyConfigIds,
      zipf_alpha: zipfAlpha,
      runtime_evidence_mode: runtimeEvidenceMode,
      enable_faults_for_formal_run: enableFaults,
      max_run_count: 200,
      max_total_tx_count: 20000000,
    };
  }

  function toggleBaseline(id: string) {
    setBaselineIds((current) => current.includes(id) ? current.filter((item) => item !== id) : [...current, id]);
  }
  function toggleSaved(id: string, values: string[], setValues: (value: string[]) => void) {
    setValues(values.includes(id) ? values.filter((item) => item !== id) : [...values, id]);
  }
  function applyPreset(id: string) {
    if (id === "link_check") {
      setExperimentType("workload_comparison");
      setFormalTxCount(1000);
      setSeedCount(1);
      setRuntimeEvidenceMode("local_multi_process_validation");
      setHotspotPoints("0.4");
      setCrossShardPoints("0.3");
      setShardPoints("4");
      setWorkloadScenarioPoints(["scene_hotspot", "cross_scene_migration", "mixed_metaverse"]);
      setEnableFaults(false);
    }
    if (id === "local_realism") {
      setExperimentType("workload_comparison");
      setFormalTxCount(5000);
      setSeedCount(3);
      setRuntimeEvidenceMode("local_multi_process_validation");
      setHotspotPoints("0.4");
      setCrossShardPoints("0.3");
      setShardPoints("4");
      setWorkloadScenarioPoints(["scene_hotspot", "cross_scene_migration", "mixed_metaverse"]);
      setEnableFaults(false);
    }
    if (id === "paper_candidate") {
      setExperimentType("workload_comparison");
      setFormalTxCount(20000);
      setSeedCount(3);
      setRuntimeEvidenceMode("logical_single_process");
      setHotspotPoints("0.4");
      setCrossShardPoints("0.3");
      setShardPoints("4");
      setWorkloadScenarioPoints(["scene_hotspot", "cross_scene_migration", "mixed_metaverse"]);
      setEnableFaults(false);
    }
  }
  function applyScenarioPreset(id: string) {
    const presets: Record<string, string[]> = {
      recommended: ["scene_hotspot", "cross_scene_migration", "mixed_metaverse"],
      all: workloadScenarioOptions.map((item) => item.id),
      hotspot: ["scene_hotspot"],
      migration: ["cross_scene_migration"],
    };
    setWorkloadScenarioPoints(presets[id] || workloadScenarioPoints);
  }

  const methodConfigs = savedConfigs.filter((config) => config.config_kind === "method");
  const workloadConfigs = savedConfigs.filter((config) => config.config_kind === "workload");
  const topologyConfigs = savedConfigs.filter((config) => config.config_kind === "topology");

  return (
    <section className="final-card wide formal-benchmark-panel">
      <div className="v3-section-head">
        <div>
          <p className="eyebrow">论文实验设计</p>
          <h3>MetaTrack 正式性能实验</h3>
        </div>
        <span className="v3-status-badge status-variable">受控基准实验</span>
      </div>
      <p className="muted">该入口用于生成受控性能实验数据。它不同于快速验证，会按显式交易数量、多随机种子、固定基线和单变量扫描运行。</p>
      <PresetChipGroup
        label="推荐实验方案"
        items={[
          { id: "link_check", label: "最真实链路确认" },
          { id: "local_realism", label: "本地真实性对比" },
          { id: "paper_candidate", label: "论文候选预跑" },
        ]}
        onSelect={applyPreset}
      />
      <div className="topology-field-grid formal-field-grid">
        <label className="field-card">
          <span>实验类型</span>
          <select value={experimentType} onChange={(event) => setExperimentType(event.target.value as V3FormalExperimentType)}>
            {experimentTypes.map(([value, label]) => <option key={value} value={value}>{label}</option>)}
          </select>
          <small>默认单变量扫描，不做全因子组合。</small>
        </label>
        <label className="field-card">
          <span>方案来源</span>
          <select value={methodSource} onChange={(event) => setMethodSource(event.target.value as "builtin" | "saved" | "mixed")}>
            <option value="builtin">内置基线</option>
            <option value="saved">已保存方案</option>
            <option value="mixed">内置基线 + 已保存方案</option>
          </select>
          <small>正式实验可直接复用配置库中的 method 方案。</small>
        </label>
        <SliderNumberField label="交易数量" value={formalTxCount} min={1000} max={1000000} step={1000} helper="正式性能实验每组运行的交易数量，不受 Draft Smoke 限制。" onChange={setFormalTxCount} />
        <IntegerSliderField label="随机种子数量" value={seedCount} min={1} max={10} helper="多 seed 用于受控统计聚合。" onChange={setSeedCount} />
        <label className="field-card">
          <span>seed_base</span>
          <input type="number" value={seedBase} onChange={(event) => setSeedBase(Number(event.target.value))} />
          <small>seed_list: [{seedList.join(", ")}]</small>
        </label>
        <SliderNumberField label="Zipf 偏斜参数" value={zipfAlpha} min={0} max={2} step={0.05} onChange={setZipfAlpha} />
        <label className="field-card">
          <span>运行真实性等级</span>
          <select value={runtimeEvidenceMode} onChange={(event) => setRuntimeEvidenceMode(event.target.value as V3RuntimeEvidenceMode)}>
            <option value="logical_single_process">逻辑单进程：主性能实验推荐</option>
            <option value="local_multi_process_validation">本地多进程：原型真实性验证</option>
          </select>
          <small>本地多进程受本机调度影响，不作为主性能图默认模式。</small>
        </label>
      </div>
      <details className="v3-foldout">
        <summary className="v3-foldout-summary">对照基线配置</summary>
        {methodSource !== "saved" && (
        <div className="v3-checkbox-grid">
          {baselineOptions.map((id) => (
            <label key={id} className="checkbox-card field-card">
              <span>{id}</span>
              <input type="checkbox" checked={baselineIds.includes(id)} onChange={() => toggleBaseline(id)} />
            </label>
          ))}
        </div>
        )}
        {methodSource !== "builtin" && (
          <div className="v3-checkbox-grid">
            {methodConfigs.map((config) => (
              <label key={config.config_id} className="checkbox-card field-card">
                <span>{config.name}<small>{config.config_id}</small></span>
                <input type="checkbox" checked={methodConfigIds.includes(config.config_id)} onChange={() => toggleSaved(config.config_id, methodConfigIds, setMethodConfigIds)} />
              </label>
            ))}
            {methodConfigs.length === 0 && <p className="muted">暂无已保存方案；先在 11 模块下方保存完整方案。</p>}
          </div>
        )}
      </details>
      <details className="v3-foldout">
        <summary className="v3-foldout-summary">已保存负载 / 拓扑</summary>
        <div className="topology-field-grid">
          <SavedConfigChecks title="已保存负载" configs={workloadConfigs} selected={workloadConfigIds} onToggle={(id) => toggleSaved(id, workloadConfigIds, setWorkloadConfigIds)} />
          <SavedConfigChecks title="已保存拓扑" configs={topologyConfigs} selected={topologyConfigIds} onToggle={(id) => toggleSaved(id, topologyConfigIds, setTopologyConfigIds)} />
        </div>
      </details>
      <details className="v3-foldout">
        <summary className="v3-foldout-summary">高级扫描点</summary>
        <div className="topology-field-grid formal-field-grid">
          {experimentType === "workload_comparison" ? (
            <>
              <RatioSliderField label="固定热点比例" value={parseFloatList(hotspotPoints)[0] ?? 0.4} onChange={(value) => setHotspotPoints(String(value))} />
              <RatioSliderField label="固定跨片比例" value={parseFloatList(crossShardPoints)[0] ?? 0.3} onChange={(value) => setCrossShardPoints(String(value))} />
              <IntegerSliderField label="固定分片数量" value={parseIntList(shardPoints)[0] ?? 4} min={1} max={32} onChange={(value) => setShardPoints(String(value))} />
              <PresetChipGroup label="负载场景快捷" items={[
                { id: "recommended", label: "推荐三场景" },
                { id: "all", label: "全场景" },
                { id: "hotspot", label: "只看热点" },
                { id: "migration", label: "只看迁移" },
              ]} onSelect={applyScenarioPreset} />
              <MultiSelectChipGroup label="负载场景" options={workloadScenarioOptions} selected={workloadScenarioPoints} onChange={setWorkloadScenarioPoints} />
            </>
          ) : (
            <>
              {experimentType === "hotspot_sensitivity" && <TextList label="热点比例扫描" value={hotspotPoints} onChange={setHotspotPoints} />}
              {experimentType === "cross_shard_sensitivity" && <TextList label="跨片比例扫描" value={crossShardPoints} onChange={setCrossShardPoints} />}
              {experimentType === "shard_scalability" && <TextList label="分片数量扫描" value={shardPoints} onChange={setShardPoints} />}
            </>
          )}
          <label className="field-card checkbox-card">
            <span>正式实验包含故障注入</span>
            <input type="checkbox" checked={enableFaults} onChange={(event) => setEnableFaults(event.target.checked)} />
            <small>主性能实验默认不启用故障注入。</small>
          </label>
        </div>
      </details>
      <div className="v3-warning-card">资源保护：最多 200 个运行组、总交易数最多 20000000、seed 数量最多 10、每组交易数最多 1000000、扫描点最多 20。</div>
      <div className="v3-run-buttons">
        <button type="button" className="v3-secondary-button" disabled={!payload || previewing || running} onClick={() => payload && onPreview(payload)}>
          {previewing ? "预览中..." : "预览实验矩阵"}
        </button>
        <button type="button" disabled={!payload || running || !preview?.is_runnable} onClick={() => payload && onRun(payload)}>
          {running ? "正式性能实验运行中..." : "运行正式性能实验"}
        </button>
      </div>
      {error && <p className="file-error">{error}</p>}
      <FormalExperimentMatrixPreview preview={preview} />
    </section>
  );
}

function TextList({ label, value, onChange }: { label: string; value: string; onChange: (value: string) => void }) {
  return (
    <label className="field-card">
      <span>{label}</span>
      <input value={value} onChange={(event) => onChange(event.target.value)} />
      <small>逗号分隔的显式实验点。</small>
    </label>
  );
}

function SavedConfigChecks({ title, configs, selected, onToggle }: { title: string; configs: V3SavedConfig[]; selected: string[]; onToggle: (id: string) => void }) {
  return (
    <div className="field-card">
      <span>{title}</span>
      {configs.length === 0 && <small>暂无配置</small>}
      {configs.map((config) => (
        <label key={config.config_id} className="checkbox-card compact">
          <span>{config.name}</span>
          <input type="checkbox" checked={selected.includes(config.config_id)} onChange={() => onToggle(config.config_id)} />
        </label>
      ))}
    </div>
  );
}

function parseFloatList(value: string): number[] {
  return value.split(",").map((item) => Number(item.trim())).filter((item) => Number.isFinite(item));
}

function parseIntList(value: string): number[] {
  return value.split(",").map((item) => Math.trunc(Number(item.trim()))).filter((item) => Number.isFinite(item));
}
