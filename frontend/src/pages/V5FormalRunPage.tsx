import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";

import {
  cancelV5FormalRunGroup,
  createV5FormalRunGroup,
  fetchV5FormalRunGroup,
  fetchV5PluginCatalog,
  fetchV5WorkloadDatasets,
  previewV5FormalRun,
  previewV5Workload,
  validateV5ExperimentSpec,
  type V5CompatibilityResult,
  type V5ExperimentSpec,
  type V5FormalMethod,
  type V5FormalPreviewResponse,
  type V5FormalRunGroupDetail,
  type V5FormalRunRequest,
  type V5FormalSuite,
  type V5PluginManifest,
  type V5PluginSelection,
  type V5WorkloadDatasetSummary,
  type V5WorkloadPreview,
  type V5WorkloadSourceSpec,
} from "../api";
import WorkloadPreviewPanel from "../components/v5/WorkloadPreviewPanel";
import WorkloadSourceEditor, { type WorkloadEditorState } from "../components/v5/WorkloadSourceEditor";
import { backendLabel, blockerLabel, faultModeLabel, statusLabel, suiteLabel } from "../v5Labels";
import { BATCH_SI_ABLATION_METHOD_IDS, FORMAL_METHOD_DEFINITIONS, FORMAL_SUITE_DEFINITIONS, PARALLEL_WORKER_OPTIONS, methodDefinition } from "../v5FormalExperimentCatalog";
import { V5_BUILTIN_METHODS, V5_DEFAULT_METHOD_IDS, applyV5MethodSelections, defaultV5PluginSelections } from "../v5MethodProfile";
import { buildThetaSweepPoints, compactCount, thetaOptionsForDataset, type SkewExperimentPreset } from "../v5SkewExperiment"; // V5_SKEW_MAIN_PRESET_V1
import "../v5UiPolish.css";

const recentGroupKey = "mbe.v5FormalRunGroupId";
const formalRunDraftKey = "mbe.v5FormalRunDraft.v1";
const groundhogMethodId = "hash_groundhog";
const alphaValues = [0, 0.2, 0.4, 0.6, 0.8, 1, 1.2, 1.4];

type Topology = { nodes: number; shards: number; validators_per_shard: number };
type TopologyPoint = Topology & { worker_count?: number };
type BlockProduction = { block_size: number; block_interval_ms: number };

function deriveValidatorsPerShard(nodes: number, shards: number, fallback: number): number {
  return nodes > 0 && shards > 0 && nodes % shards === 0 ? nodes / shards : fallback;
}

function topologyWithNodes(current: Topology, nodes: number): Topology {
  return { ...current, nodes, validators_per_shard: deriveValidatorsPerShard(nodes, current.shards, current.validators_per_shard) };
}

function topologyWithShards(current: Topology, shards: number): Topology {
  return { ...current, shards, validators_per_shard: deriveValidatorsPerShard(current.nodes, shards, current.validators_per_shard) };
}
type WorkloadPoint = { tx_count: number; cross_shard_ratio?: number; timeout_every?: number; target_alpha?: number; target_theta?: number };
type FaultMode = "disabled" | "delay_only" | "network_drop";
type FaultPoint = { mode: FaultMode; delay_ms?: number; drop_rate?: number; drop_message_types?: string[] };
type Props = { onOpenResults?: (groupId: string) => void; onPreferredMethodConsumed?: () => void; onPreferredMethodUnavailable?: (methodId: string) => void; preferredMethodId?: string };

const defaultWorkload: WorkloadEditorState = {
  mode: "synthetic",
  datasetId: "",
  variantMode: "",
  variantParameters: {},
  txCount: 10_000,
  useFullDataset: false,
  seedText: "11",
  targetAlpha: 1,
  crossShardRatio: 0,
  timeoutEvery: 0,
  timeoutEnabled: false,
  skewAxis: "contract",
};

const defaultTopology: Topology = { nodes: 8, shards: 2, validators_per_shard: 4 };
const defaultBlockProduction: BlockProduction = { block_size: 100, block_interval_ms: 75 };

type FormalRunDraftV1 = {
  schema_version: "mbe_v5_formal_run_draft_v1";
  saved_at: string;
  selectedMethods: string[];
  selectedSuite: V5FormalSuite;
  workerCount: number;
  topology: Topology;
  blockProduction: BlockProduction;
  workload: WorkloadEditorState;
  skewPreset: SkewExperimentPreset;
  repeats: number;
  workloadPoints: WorkloadPoint[];
  topologyPoints: TopologyPoint[];
  faultPoints: FaultPoint[];
};

function cloneDefaultWorkload(): WorkloadEditorState {
  return { ...defaultWorkload, variantParameters: {} };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function finiteNumber(value: unknown, fallback: number): number {
  return typeof value === "number" && Number.isFinite(value) ? value : fallback;
}

function positiveInteger(value: unknown, fallback: number, maximum = Number.MAX_SAFE_INTEGER): number {
  const number = finiteNumber(value, fallback);
  return Number.isInteger(number) && number > 0 ? Math.min(number, maximum) : fallback;
}

function optionalFiniteNumber(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function readWorkloadPoints(value: unknown): WorkloadPoint[] {
  if (!Array.isArray(value)) return [];
  const out: WorkloadPoint[] = [];
  for (const item of value) {
    if (!isRecord(item) || typeof item.tx_count !== "number" || !Number.isInteger(item.tx_count) || item.tx_count < 1) continue;
    const point: WorkloadPoint = { tx_count: item.tx_count };
    const crossShard = optionalFiniteNumber(item.cross_shard_ratio); if (crossShard !== undefined) point.cross_shard_ratio = crossShard;
    const timeout = optionalFiniteNumber(item.timeout_every); if (timeout !== undefined) point.timeout_every = Math.max(0, Math.trunc(timeout));
    const alpha = optionalFiniteNumber(item.target_alpha); if (alpha !== undefined) point.target_alpha = alpha;
    const theta = optionalFiniteNumber(item.target_theta); if (theta !== undefined) point.target_theta = theta;
    out.push(point);
  }
  return out;
}

function readTopologyPoints(value: unknown): TopologyPoint[] {
  if (!Array.isArray(value)) return [];
  const out: TopologyPoint[] = [];
  for (const item of value) {
    if (!isRecord(item)) continue;
    if (![item.nodes, item.shards, item.validators_per_shard].every((entry) => typeof entry === "number" && Number.isInteger(entry) && entry > 0)) continue;
    const point: TopologyPoint = { nodes: item.nodes as number, shards: item.shards as number, validators_per_shard: item.validators_per_shard as number };
    if (typeof item.worker_count === "number" && Number.isInteger(item.worker_count) && item.worker_count > 0) point.worker_count = item.worker_count;
    out.push(point);
  }
  return out;
}

function readFaultPoints(value: unknown): FaultPoint[] {
  if (!Array.isArray(value)) return [];
  const out: FaultPoint[] = [];
  for (const item of value) {
    if (!isRecord(item) || !["disabled", "delay_only", "network_drop"].includes(String(item.mode))) continue;
    const point: FaultPoint = { mode: item.mode as FaultMode };
    const delay = optionalFiniteNumber(item.delay_ms); if (delay !== undefined) point.delay_ms = delay;
    const drop = optionalFiniteNumber(item.drop_rate); if (drop !== undefined) point.drop_rate = drop;
    if (Array.isArray(item.drop_message_types)) point.drop_message_types = item.drop_message_types.filter((entry): entry is string => typeof entry === "string");
    out.push(point);
  }
  return out;
}

function readFormalRunDraft(): FormalRunDraftV1 | null {
  if (typeof window === "undefined") return null;
  try {
    const raw = window.localStorage.getItem(formalRunDraftKey);
    if (!raw) return null;
    const parsed: unknown = JSON.parse(raw);
    if (!isRecord(parsed) || parsed.schema_version !== "mbe_v5_formal_run_draft_v1") return null;
    const suite = FORMAL_SUITE_DEFINITIONS.some((item) => item.id === parsed.selectedSuite) ? parsed.selectedSuite as V5FormalSuite : "comparison_experiment";
    const methods = Array.isArray(parsed.selectedMethods) ? parsed.selectedMethods.filter((item): item is string => typeof item === "string" && item.length > 0) : [];
    const topologyRaw = isRecord(parsed.topology) ? parsed.topology : {};
    const blockRaw = isRecord(parsed.blockProduction) ? parsed.blockProduction : {};
    const workloadRaw = isRecord(parsed.workload) ? parsed.workload : {};
    const variantParameters = isRecord(workloadRaw.variantParameters)
      ? Object.fromEntries(Object.entries(workloadRaw.variantParameters).filter(([, value]) => ["string", "number", "boolean"].includes(typeof value))) as Record<string, string | number | boolean>
      : {};
    const mode = ["synthetic", "dataset_original", "dataset_derived"].includes(String(workloadRaw.mode)) ? workloadRaw.mode as WorkloadEditorState["mode"] : defaultWorkload.mode;
    const skewPreset = parsed.skewPreset === "theta_main" ? "theta_main" : "single_theta";
    return {
      schema_version: "mbe_v5_formal_run_draft_v1",
      saved_at: typeof parsed.saved_at === "string" ? parsed.saved_at : "",
      selectedMethods: methods.length ? methods : [...V5_DEFAULT_METHOD_IDS],
      selectedSuite: suite,
      workerCount: positiveInteger(parsed.workerCount, 4, 8),
      topology: {
        nodes: positiveInteger(topologyRaw.nodes, defaultTopology.nodes, 128),
        shards: positiveInteger(topologyRaw.shards, defaultTopology.shards, 64),
        validators_per_shard: positiveInteger(topologyRaw.validators_per_shard, defaultTopology.validators_per_shard, 128),
      },
      blockProduction: {
        block_size: positiveInteger(blockRaw.block_size, defaultBlockProduction.block_size, 5000),
        block_interval_ms: positiveInteger(blockRaw.block_interval_ms, defaultBlockProduction.block_interval_ms, 5000),
      },
      workload: {
        mode,
        datasetId: typeof workloadRaw.datasetId === "string" ? workloadRaw.datasetId : "",
        variantMode: typeof workloadRaw.variantMode === "string" ? workloadRaw.variantMode : "",
        variantParameters,
        txCount: positiveInteger(workloadRaw.txCount, defaultWorkload.txCount, 10_000_000),
        useFullDataset: workloadRaw.useFullDataset === true,
        seedText: typeof workloadRaw.seedText === "string" ? workloadRaw.seedText : defaultWorkload.seedText,
        targetAlpha: finiteNumber(workloadRaw.targetAlpha, defaultWorkload.targetAlpha),
        crossShardRatio: finiteNumber(workloadRaw.crossShardRatio, defaultWorkload.crossShardRatio),
        timeoutEvery: Math.max(0, Math.trunc(finiteNumber(workloadRaw.timeoutEvery, defaultWorkload.timeoutEvery))),
        timeoutEnabled: workloadRaw.timeoutEnabled === true,
        skewAxis: typeof workloadRaw.skewAxis === "string" ? workloadRaw.skewAxis : defaultWorkload.skewAxis,
      },
      skewPreset,
      repeats: positiveInteger(parsed.repeats, 1, 20),
      workloadPoints: readWorkloadPoints(parsed.workloadPoints),
      topologyPoints: readTopologyPoints(parsed.topologyPoints),
      faultPoints: readFaultPoints(parsed.faultPoints),
    };
  } catch {
    return null;
  }
}

export default function V5FormalRunPage({ onOpenResults, onPreferredMethodConsumed, onPreferredMethodUnavailable, preferredMethodId = "" }: Props) {
  const restoredDraft = useMemo(() => readFormalRunDraft(), []);
  const [catalog, setCatalog] = useState<V5PluginManifest[]>([]);
  const [datasets, setDatasets] = useState<V5WorkloadDatasetSummary[]>([]);
  const [selectedMethods, setSelectedMethods] = useState<string[]>(() => restoredDraft?.selectedMethods ?? [...V5_DEFAULT_METHOD_IDS]);
  const [selectedSuite, setSelectedSuite] = useState<V5FormalSuite>(() => restoredDraft?.selectedSuite ?? "comparison_experiment");
  const [workerCount, setWorkerCount] = useState(() => restoredDraft?.workerCount ?? 4);
  const [topology, setTopology] = useState<Topology>(() => restoredDraft?.topology ?? { ...defaultTopology });
  const [blockProduction, setBlockProduction] = useState<BlockProduction>(() => restoredDraft?.blockProduction ?? { ...defaultBlockProduction });
  const [workload, setWorkload] = useState<WorkloadEditorState>(() => restoredDraft?.workload ?? cloneDefaultWorkload());
  const [skewPreset, setSkewPreset] = useState<SkewExperimentPreset>(() => restoredDraft?.skewPreset ?? "single_theta");
  const [repeats, setRepeats] = useState(() => restoredDraft?.repeats ?? 1);
  const [workloadPoints, setWorkloadPoints] = useState<WorkloadPoint[]>(() => restoredDraft?.workloadPoints ?? []);
  const [topologyPoints, setTopologyPoints] = useState<TopologyPoint[]>(() => restoredDraft?.topologyPoints ?? []);
  const [faultPoints, setFaultPoints] = useState<FaultPoint[]>(() => restoredDraft?.faultPoints ?? []);
  const [workloadPreview, setWorkloadPreview] = useState<V5WorkloadPreview | null>(null);
  const [workloadPreviewDirty, setWorkloadPreviewDirty] = useState(true);
  const [workloadPreviewError, setWorkloadPreviewError] = useState("");
  const [preview, setPreview] = useState<V5FormalPreviewResponse | null>(null);
  const [previewRequest, setPreviewRequest] = useState<V5FormalRunRequest | null>(null);
  const [methodCompatibility, setMethodCompatibility] = useState<Record<string, V5CompatibilityResult>>({});
  const [groupDetail, setGroupDetail] = useState<V5FormalRunGroupDetail | null>(null);
  const [groupId, setGroupId] = useState("");
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [catalogError, setCatalogError] = useState("");
  const [busy, setBusy] = useState(false);
  const [cancelBusy, setCancelBusy] = useState(false);
  const [draftStatus, setDraftStatus] = useState(restoredDraft ? `已恢复上次实验配置${restoredDraft.saved_at ? `（${restoredDraft.saved_at.replace("T", " ").slice(0, 19)}）` : ""}。` : "实验配置会自动保存在本机浏览器。");
  const pollTimer = useRef<number | null>(null);
  const formRevision = useRef(0);
  const preferredConsumed = useRef(false);

  const methods = useMemo(() => V5_BUILTIN_METHODS, []);
  const seeds = useMemo(() => parseSeeds(workload.seedText), [workload.seedText]);
  const thetaOptions = useMemo(() => thetaOptionsForDataset(datasets, workload.datasetId, workload.variantMode), [datasets, workload.datasetId, workload.variantMode]);
  const thetaMainActive = skewPreset === "theta_main" && workload.mode === "dataset_derived" && thetaOptions.length > 1;
  const thetaMainTransactionsPerMethod = thetaOptions.length * workload.txCount;
  const catalogReady = useMemo(() => {
    const categories = new Set(catalog.map((item) => item.category));
    return catalog.length > 0 && defaultV5PluginSelections(catalog).length === categories.size;
  }, [catalog]);
  const selectedRaw = methods.filter((method) => selectedMethods.includes(method.method_id));
  const selected = selectedRaw;
  const groundhogSelected = selectedMethods.includes(groundhogMethodId);
  const previewRunnable = Boolean(preview?.rows.length && preview.rows.every((row) => row.runnable && !row.blockers.length));
  const previewBlockers = useMemo(() => preview?.rows.flatMap((row) => row.blockers.map((blocker) => `${row.method.display_name}: ${blockerLabel(blocker)}`)) ?? [], [preview]);
  const workloadRunnable = Boolean(workloadPreview && !workloadPreviewDirty && !workloadPreview.blockers.length && !workloadPreviewError);
  const resources = estimateFormalResources(selectedSuite, selected.length, seeds.length, repeats, topology, workload.txCount, workloadPoints, topologyPoints, faultPoints);
  const estimatedPrimaryBlockCount = Math.max(1, Math.ceil(workload.txCount / Math.max(1, blockProduction.block_size)));
  const currentSource = currentWorkloadSource();
  const configurationBlocker = formError({
    catalogReady,
    selected,
    selectedSuite,
    topology,
    blockProduction,
    source: currentSource,
    seeds,
    repeats,
    workloadPoints,
    topologyPoints,
    faultPoints,
    estimatedChildren: resources.children,
    workloadRunnable,
    workerCount,
  });
  const startBlocker = configurationBlocker
    ?? (!preview || !previewRequest ? "请先生成正式实验矩阵预览。" : null)
    ?? (!previewRunnable ? (previewBlockers.join("；") || "正式实验矩阵包含不可运行项。") : null)
    ?? (workloadPreviewDirty ? "配置已经变化，请重新生成 workload preview 和正式矩阵。" : null);

  useEffect(() => { void loadCatalog(); const stored = window.localStorage.getItem(recentGroupKey); if (stored) void queryGroup(stored, true); return stopPolling; }, []);
  useEffect(() => { if (datasets.length && !workload.datasetId) setWorkload((current) => ({ ...current, datasetId: datasets[0].dataset_id })); }, [datasets.length, workload.datasetId]);
  useEffect(() => { if (groupId && groupDetail && !terminal(groupDetail.group.status)) schedulePolling(groupId); else stopPolling(); return stopPolling; }, [groupId, groupDetail?.group.status]);
  useEffect(() => {
    const timer = window.setTimeout(() => {
      const draft: FormalRunDraftV1 = {
        schema_version: "mbe_v5_formal_run_draft_v1",
        saved_at: new Date().toISOString(),
        selectedMethods, selectedSuite, workerCount, topology, blockProduction, workload, skewPreset, repeats, workloadPoints, topologyPoints, faultPoints,
      };
      window.localStorage.setItem(formalRunDraftKey, JSON.stringify(draft));
      setDraftStatus("当前实验配置已自动保存。");
    }, 150);
    return () => window.clearTimeout(timer);
  }, [selectedMethods, selectedSuite, workerCount, topology, blockProduction, workload, skewPreset, repeats, workloadPoints, topologyPoints, faultPoints]);

  async function loadCatalog() {
    setBusy(true);
    try {
      const [pluginResponse, datasetResponse] = await Promise.all([fetchV5PluginCatalog("real_cluster"), fetchV5WorkloadDatasets()]);
      setCatalog(pluginResponse);
      setDatasets(datasetResponse);
      const categories = new Set(pluginResponse.map((item) => item.category));
      setCatalogError(pluginResponse.length > 0 && defaultV5PluginSelections(pluginResponse).length === categories.size ? "" : "真实集群插件目录不完整。");
      const available = V5_BUILTIN_METHODS.map((item) => item.method_id);
      if (preferredMethodId && available.includes(preferredMethodId)) { setSelectedMethods([preferredMethodId]); preferredConsumed.current = true; onPreferredMethodConsumed?.(); }
      else {
        setSelectedMethods((current) => {
          const filtered = current.filter((methodId) => available.includes(methodId));
          return filtered.length ? filtered : V5_DEFAULT_METHOD_IDS.filter((methodId) => available.includes(methodId));
        });
        if (preferredMethodId && !preferredConsumed.current) { preferredConsumed.current = true; onPreferredMethodUnavailable?.(preferredMethodId); }
      }
    } catch (caught) {
      setCatalogError(errorMessage(caught));
      setSelectedMethods((current) => current.length ? current : [...V5_DEFAULT_METHOD_IDS]);
    } finally {
      setBusy(false);
    }
  }

  function resetFormalRunDraft() {
    window.localStorage.removeItem(formalRunDraftKey);
    setSelectedMethods([...V5_DEFAULT_METHOD_IDS]);
    setSelectedSuite("comparison_experiment");
    setWorkerCount(4);
    setTopology({ ...defaultTopology });
    setBlockProduction({ ...defaultBlockProduction });
    setWorkload({ ...cloneDefaultWorkload(), datasetId: datasets[0]?.dataset_id ?? "" });
    setSkewPreset("single_theta");
    setRepeats(1);
    setWorkloadPoints([]);
    setTopologyPoints([]);
    setFaultPoints([]);
    invalidateAll();
    setDraftStatus("已恢复系统默认配置；后续修改仍会自动保存。");
    setMessage("");
    setError("");
  }

  function invalidateAll() {
    formRevision.current += 1;
    setWorkloadPreviewDirty(true);
    setPreview(null);
    setPreviewRequest(null);
    setMethodCompatibility({});
  }
  function update(fn: () => void) { fn(); invalidateAll(); }
  function updateWorkload(next: WorkloadEditorState) {
    setWorkload(next);
    if (skewPreset === "theta_main") {
      const values = thetaOptionsForDataset(datasets, next.datasetId, next.variantMode);
      if (next.mode === "dataset_derived" && values.length > 1) {
        setSelectedSuite("workload_sensitivity");
        setWorkloadPoints(buildThetaSweepPoints(next.txCount, values));
      } else {
        setSkewPreset("single_theta");
        setWorkloadPoints([]);
      }
    }
    invalidateAll();
  }
  function selectSkewPreset(next: SkewExperimentPreset) {
    if (next === "theta_main") {
      const values = thetaOptionsForDataset(datasets, workload.datasetId, workload.variantMode);
      if (values.length < 2) { setError("当前真实派生负载没有可展开的 target_theta 扫描点。"); return; }
      setSkewPreset(next);
      setSelectedSuite("workload_sensitivity");
      setWorkloadPoints(buildThetaSweepPoints(workload.txCount, values));
      const visible = selectedMethods.filter((methodId) => methodDefinition(methodId)?.comparisonVisible);
      setSelectedMethods(visible.length ? visible : [...V5_DEFAULT_METHOD_IDS]);
    } else {
      setSkewPreset(next);
      if (selectedSuite === "workload_sensitivity") {
        setSelectedSuite("comparison_experiment");
        setWorkloadPoints([]);
        const visible = selectedMethods.filter((methodId) => methodDefinition(methodId)?.comparisonVisible);
        setSelectedMethods(visible.length ? visible : [...V5_DEFAULT_METHOD_IDS]);
      }
    }
    invalidateAll();
  }

  function selectSuite(suite: V5FormalSuite) {
    if (suite !== "workload_sensitivity" && skewPreset === "theta_main") setSkewPreset("single_theta");
    setSelectedSuite(suite);
    if (suite === "workload_sensitivity" && workloadPoints.length < 2) {
      const base = defaultWorkloadPoint(workload);
      const second = { ...base, tx_count: Math.max(1, Math.round(base.tx_count * 1.5)) };
      if (typeof base.target_alpha === "number") second.target_alpha = snapAlpha(base.target_alpha + 0.2);
      if (typeof base.target_theta === "number") second.target_theta = Math.min(1.2, Math.round((base.target_theta + 0.2) * 10) / 10);
      setWorkloadPoints([base, second]);
    }
    if (suite === "topology_scaling" && topologyPoints.length < 2) {
      setTopologyPoints([{ ...topology, worker_count: 1 }, { ...topology, worker_count: workerCount }]);
    }
    if (suite === "fault_recovery_experiment" && faultPoints.length < 2) {
      setFaultPoints([{ mode: "disabled" }, { mode: "delay_only", delay_ms: 5 }]);
    }
    if (suite === "ablation_experiment") {
      setSelectedMethods([...BATCH_SI_ABLATION_METHOD_IDS]);
    } else if (suite === "main_experiment") {
      const currentMain = selectedMethods.find((methodId) => methodDefinition(methodId)?.mainVisible);
      setSelectedMethods([currentMain ?? "hash_batch_si"]);
    } else if (suite === "fault_recovery_experiment") {
      const current = selectedMethods.find((methodId) => methodDefinition(methodId)?.comparisonVisible);
      setSelectedMethods([current ?? "hash_batch_si"]);
    } else {
      const visible = selectedMethods.filter((methodId) => methodDefinition(methodId)?.comparisonVisible);
      setSelectedMethods(visible.length ? visible : [...V5_DEFAULT_METHOD_IDS]);
    }
    invalidateAll();
  }

  function toggleMethodSelection(methodId: string) {
    const mode = FORMAL_SUITE_DEFINITIONS.find((item) => item.id === selectedSuite)?.methodMode ?? "multiple";
    if (mode === "single") {
      setSelectedMethods([methodId]);
    } else if (mode === "ablation") {
      if (methodId === "hash_batch_si") return;
      setSelectedMethods((current) => {
        const next = toggle(methodId, current);
        return next.includes("hash_batch_si") ? next : ["hash_batch_si", ...next];
      });
    } else {
      setSelectedMethods((current) => toggle(methodId, current));
    }
    invalidateAll();
  }

  function currentWorkloadSource(): V5WorkloadSourceSpec | null {
    const seed = seeds[0];
    if (!globalThis.Number.isInteger(seed)) return null;
    if (workload.mode === "synthetic") {
      return { source_type: "synthetic", plugin_id: "deterministic_signed_synthetic", requested_tx_count: workload.txCount, seed, selection_mode: "contiguous_window", replay_mode: "max_throughput" };
    }
    const dataset = datasets.find((item) => item.dataset_id === workload.datasetId);
    if (!dataset?.selectable) return null;
    const definitions = dataset.variant_definitions ?? [];
    const expectedKind = workload.mode === "dataset_derived" ? "derived" : "original";
    const definition = definitions.find((item) => item.variant_mode === workload.variantMode) ?? definitions.find((item) => (item.kind ?? "original") === expectedKind);
    if (!definition) return null;
    const variantParameters = { ...workload.variantParameters };
    const targetAlpha = typeof variantParameters.target_alpha === "number" ? variantParameters.target_alpha : undefined;
    const skewAxis = typeof variantParameters.skew_axis === "string" ? variantParameters.skew_axis : undefined;
    return {
      source_type: "dataset",
      plugin_id: "canonical_trace_replay",
      dataset_id: dataset.dataset_id,
      variant_mode: definition.variant_mode,
      requested_tx_count: workload.useFullDataset ? dataset.row_count : workload.txCount,
      use_full_dataset: workload.useFullDataset,
      seed,
      selection_mode: definition.selection_mode ?? "contiguous_window",
      replay_mode: "max_throughput",
      skew_axis: skewAxis,
      target_alpha: targetAlpha,
      variant_parameters: variantParameters,
      source_sha256: dataset.source_sha256,
    };
  }

  function methodSpec(
  method: V5FormalMethod,
  source: V5WorkloadSourceSpec,
): V5ExperimentSpec {
  const base: V5ExperimentSpec = {
    name: "v5_formal_real_cluster",
    execution_backend: "real_cluster",
    plugin_selections: defaultV5PluginSelections(catalog),
    topology,
    tx_count: source.requested_tx_count,
    seed: source.seed,
    workload_source: source,
    duration_ms: 3_600_000,
    fault_policy: {
      mode: "disabled",
    },
    requested_metrics: [],
  };

  const spec = applyV5MethodSelections(base, method, catalog);

  return {
    ...spec,
    plugin_selections: patchWorkerSelections(
      patchBlockProducerSelections(
        patchWorkloadSelections(
          spec.plugin_selections,
          source,
        ),
        blockProduction,
      ),
      workerCount,
    ),
  };
}

  function patchWorkloadSelections(selections: V5PluginSelection[], source: V5WorkloadSourceSpec): V5PluginSelection[] {
    return selections.map((selection) => {
      if (selection.category !== "workload") return selection;
      if (source?.source_type === "dataset") return { ...selection, plugin_id: "canonical_trace_replay", config: {} };
      return { ...selection, plugin_id: "deterministic_signed_synthetic", config: { ...selection.config, cross_shard_ratio: workload.crossShardRatio, timeout_every: workload.timeoutEnabled ? workload.timeoutEvery : 0 } };
    });
  }

  function patchBlockProducerSelections(selections: V5PluginSelection[], production: BlockProduction): V5PluginSelection[] {
    return selections.map((selection) => {
      if (selection.category !== "block_producer") return selection;
      return { ...selection, config: { ...selection.config, block_size: production.block_size, interval_ms: production.block_interval_ms } };
    });
  }

  function patchWorkerSelections(selections: V5PluginSelection[], workers: number): V5PluginSelection[] {
    return selections.map((selection) => {
      if (selection.category !== "block_executor") return selection;
      return { ...selection, config: { ...selection.config, worker_count: selection.plugin_id === "serial_block_executor" ? 1 : workers } };
    });
  }

  function buildRequest(): V5FormalRunRequest | null {
    const source = currentWorkloadSource();
    if (!source) return null;
    const base_spec: V5ExperimentSpec = {
      name: "v5_formal_real_cluster",
      execution_backend: "real_cluster",
      plugin_selections: patchBlockProducerSelections(
        patchWorkloadSelections(defaultV5PluginSelections(catalog), source),
        blockProduction,
      ),
      topology,
      tx_count: source.requested_tx_count,
      seed: source.seed,
      workload_source: source,
      duration_ms: 3_600_000,
      fault_policy: { mode: "disabled" },
      requested_metrics: [],
    };
    const e2e = new URLSearchParams(window.location.search).get("e2e") === "1";
    return { execution_backend: "real_cluster", plan: { name: "v5_formal_real_cluster", base_spec, suites: [selectedSuite], methods: selected, seeds, repeats, worker_count: workerCount, workload_points: cleanWorkloadPoints(workloadPoints), topology_points: topologyPoints, fault_points: faultPoints, source_label: e2e ? "e2e" : "user", tags: e2e ? ["e2e"] : [] } };
  }

  async function previewWorkload() {
    const source = currentWorkloadSource();
    if (!source) { setWorkloadPreviewError("workload_source 无法构造，请检查数据集可用性和 seed。"); return; }
    setBusy(true);
    try {
      const response = await previewV5Workload(source);
      setWorkloadPreview(response);
      setWorkloadPreviewError("");
      setWorkloadPreviewDirty(Boolean(response.blockers.length));
      setPreview(null);
      setPreviewRequest(null);
    } catch (caught) {
      setWorkloadPreview(null);
      setWorkloadPreviewError(errorMessage(caught));
      setWorkloadPreviewDirty(true);
    } finally {
      setBusy(false);
    }
  }

  async function previewMatrix() {
    const request = buildRequest();
    const source = currentWorkloadSource();
    let localWorkloadRunnable = workloadRunnable;
    if (source && (!workloadPreview || workloadPreviewDirty || workloadPreviewError)) {
      try {
        const nextPreview = await previewV5Workload(source);
        setWorkloadPreview(nextPreview);
        setWorkloadPreviewError("");
        setWorkloadPreviewDirty(Boolean(nextPreview.blockers.length));
        localWorkloadRunnable = !nextPreview.blockers.length;
      } catch (caught) {
        setWorkloadPreview(null);
        setWorkloadPreviewError(errorMessage(caught));
        setWorkloadPreviewDirty(true);
        localWorkloadRunnable = false;
      }
    }
    const form = formError({ catalogReady, selected, selectedSuite, topology, blockProduction, source, seeds, repeats, workloadPoints, topologyPoints, faultPoints, estimatedChildren: resources.children, workloadRunnable: localWorkloadRunnable, workerCount });
    if (!request || form) { setError(form ?? "无法构造 Formal RunGroup 请求。"); return; }
    setBusy(true);
    try {
      const compatibility = Object.fromEntries(await Promise.all(selected.map(async (method) => {
        const spec = methodSpec(method, request.plan.base_spec.workload_source ?? currentWorkloadSource()!);
        return [method.method_id, await validateV5ExperimentSpec(spec)] as const;
      })));
      setMethodCompatibility(compatibility);
      const response = await previewV5FormalRun(request);
      setPreview(response);
      setPreviewRequest(request);
      setError("");
    } catch (caught) {
      setPreview(null);
      setPreviewRequest(null);
      setError(errorMessage(caught));
    } finally {
      setBusy(false);
    }
  }

  async function startGroup() {
    if (!previewRequest || !previewRunnable || workloadPreviewDirty) { setError("配置已变化，请重新预览。"); return; }
    setBusy(true);
    try {
      const group = await createV5FormalRunGroup(previewRequest);
      setGroupId(group.run_group_id);
      window.localStorage.setItem(recentGroupKey, group.run_group_id);
      setMessage(`RunGroup 已启动：${group.run_group_id}`);
      schedulePolling(group.run_group_id);
      try {
        const detail = await fetchV5FormalRunGroup(group.run_group_id);
        setGroupDetail(detail);
      } catch {
        setGroupDetail(null);
      }
      schedulePolling(group.run_group_id);
      setError("");
    } catch (caught) { setError(errorMessage(caught)); } finally { setBusy(false); }
  }

  async function cancelGroup() {
    if (!groupId || !groupDetail || terminal(groupDetail.group.status) || groupDetail.group.status === "cancelling") return;
    setCancelBusy(true);
    try {
      const summary = await cancelV5FormalRunGroup(groupId);
      setMessage(`已请求强制停止 RunGroup：${groupId}（${summary.status}）`);
      setError("");
      await queryGroup(groupId, true);
      schedulePolling(groupId);
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setCancelBusy(false);
    }
  }

  async function queryGroup(id = groupId, silent = false) {
    if (!id) return;
    try {
      const detail = await fetchV5FormalRunGroup(id);
      setGroupDetail(detail);
      setGroupId(id);
      if (!silent) setMessage(`已刷新 RunGroup：${id}`);
      if (!terminal(detail.group.status)) schedulePolling(id);
    } catch (caught) { if (!silent) setError(errorMessage(caught)); }
  }
  function schedulePolling(id: string) { stopPolling(); pollTimer.current = window.setTimeout(() => { void queryGroup(id, true); }, 1500); }
  function stopPolling() { if (pollTimer.current !== null) { window.clearTimeout(pollTimer.current); pollTimer.current = null; } }

  return <section className="page-grid v5-formal-run-page" data-testid="v5-formal-run-page">
    <article className="overview-hero wide">
      <p className="eyebrow">V5 Formal RunGroup</p>
      <h2>运行正式实验</h2>
      <p>负载来源进入 immutable Child ExperimentSpec。数据集 preview 和 Formal Matrix preview 都必须随配置变化重新生成。</p>
      <div className="button-row">
        <span className="muted" data-testid="v5-run-draft-status">{draftStatus}</span>
        <button type="button" className="ghost-button" data-testid="v5-reset-run-draft" onClick={resetFormalRunDraft}>恢复系统默认配置</button>
      </div>
      {catalogError && <p className="file-error">{catalogError}</p>}
      {message && <p className="notice">{message}</p>}
      {error && <p className="file-error">{error}</p>}
      <CurrentMethods suite={selectedSuite} methods={selected} preferredMethodId={preferredMethodId} workerCount={workerCount} childCount={resources.children} />
    </article>

    <article className="final-card wide">
      <h3>① 选择实验类型</h3>
      <div className="formal-suite-grid">{FORMAL_SUITE_DEFINITIONS.map((suite) => {
        const checked = selectedSuite === suite.id;
        return <button type="button" key={suite.id} data-testid={`v5-suite-${suite.id}`} aria-pressed={checked} className={`experiment-choice-card ${checked ? "selected" : ""}`} onClick={() => selectSuite(suite.id)}>
          <input className="compatibility-checkbox" type="checkbox" aria-label={`${suite.title} ${suite.id}`} defaultChecked={checked} onClick={(event) => event.stopPropagation()} onChange={(event) => { if (event.target.checked) selectSuite(suite.id); }} />
          <span className="choice-check">{checked ? "✓" : ""}</span><strong>{suite.title}</strong><small>{suite.description}</small>
        </button>;
      })}</div>
    </article>

    <article className="final-card wide">
      <div className="section-heading"><div><h3>② 选择实验对象</h3><p className="muted">{FORMAL_SUITE_DEFINITIONS.find((item) => item.id === selectedSuite)?.description}</p></div></div>
      {selectedSuite === "comparison_experiment" && <div className="button-row compact-actions"><button type="button" className="ghost-button" onClick={() => update(() => setSelectedMethods(["hash_serial", "hash_aria", "hash_block_stm", "hash_groundhog", "hash_batch_si"]))}>论文五方法</button><button type="button" className="ghost-button" onClick={() => update(() => setSelectedMethods(["stateless_hash_serial", "stateless_hash_block_stm", "metatrack_serial", "metatrack_block_stm"]))}>MetaTrack 对照组</button></div>}
      {selectedSuite === "ablation_experiment" && <div className="ablation-target-grid" data-testid="v5-ablation-targets"><button type="button" className="experiment-choice-card selected" aria-pressed="true"><span className="choice-check">✓</span><strong>Batch-SI</strong><small>已注册完整版本与四个针对性消融。</small></button><button type="button" className="experiment-choice-card unavailable" disabled><strong>MetaTrack</strong><small>消融定义尚未在当前方法注册表中闭合，暂不生成无效实验。</small></button></div>}
      <div className="formal-method-groups">
        {(["stateful", "batch_si", "stateless", "metatrack"] as const).map((family) => {
          const definitions = FORMAL_METHOD_DEFINITIONS.filter((definition) => definition.family === family && (selectedSuite === "ablation_experiment" ? definition.ablationTarget === "batch_si" : selectedSuite === "main_experiment" ? definition.mainVisible : definition.comparisonVisible));
          if (!definitions.length) return null;
          const familyTitle = family === "stateful" ? "有状态执行方法" : family === "batch_si" ? (selectedSuite === "ablation_experiment" ? "Batch-SI 消融变体" : "Batch-SI") : family === "stateless" ? "无状态执行方法" : "MetaTrack 方法";
          return <section key={family} className="method-family"><h4>{familyTitle}</h4><div className="selectable-card-grid">{definitions.map((definition) => {
            const method = methods.find((item) => item.method_id === definition.methodId);
            if (!method) return null;
            const checked = selectedMethods.includes(method.method_id);
            const forced = selectedSuite === "ablation_experiment" && definition.isFullVariant;
            return <button type="button" key={method.method_id} data-testid={`v5-run-method-${method.method_id}`} aria-pressed={checked} className={`method-choice-card ${checked ? "selected" : ""}`} onClick={() => toggleMethodSelection(method.method_id)} disabled={forced}>
              <input className="compatibility-checkbox" type="checkbox" aria-label={`${definition.title} ${method.method_id}`} checked={checked} disabled={forced} onClick={(event) => event.stopPropagation()} onChange={() => toggleMethodSelection(method.method_id)} />
              <span className="choice-check">{checked ? "✓" : ""}</span><strong>{definition.title}</strong><small>{definition.description}</small>{forced && <em>完整版本（强制对照）</em>}
            </button>;
          })}</div></section>;
        })}
      </div>
      {!selected.length && <p className="file-error">尚未选择执行方法。</p>}
      {selectedSuite === "ablation_experiment" && <p className="notice">完整 Batch-SI 始终作为主方法；消融变体只复用 Batch-SI 自己的实现，不调用其他方案算法代码。</p>}
      {groundhogSelected && <p className="notice" data-testid="v5-groundhog-topology-notice">Groundhog 使用当前选择的节点数、分片数和每片验证节点数。核心复现要求跨片交易比例为 0；多分片时，各分片独立运行 Groundhog。</p>}
    </article>

    <WorkloadSourceEditor state={workload} datasets={datasets} onChange={updateWorkload} />
    {workload.mode === "dataset_derived" && thetaOptions.length > 1 && <article className="final-card wide" data-testid="v5-skew-main-preset-panel">
      <div className="section-heading"><div><h3>真实派生/重构负载 · 实验预设</h3><p className="muted">偏斜度主实验直接使用数据集 manifest 已注册的 target_theta 档位，不生成或插值不存在的负载。</p></div></div>
      <div className="experiment-condition-grid">
        <label><span>实验预设</span><select data-testid="v5-skew-experiment-preset" value={skewPreset} onChange={(event) => selectSkewPreset(event.target.value as SkewExperimentPreset)}><option value="single_theta">单偏斜度实验</option><option value="theta_main">偏斜度主实验（θ={thetaOptions[0].toFixed(1)}–{thetaOptions[thetaOptions.length - 1].toFixed(1)}，共 {thetaOptions.length} 点）</option></select></label>
        {thetaMainActive && <><div className="readonly-field"><span>每个偏斜度交易规模</span><strong>{compactCount(workload.txCount)}</strong></div><div className="readonly-field"><span>偏斜度点数</span><strong>{thetaOptions.length}</strong></div><div className="readonly-field"><span>单方法总交易量</span><strong>{compactCount(thetaMainTransactionsPerMethod)}</strong></div></>}
      </div>
      {thetaMainActive && <p className="notice" data-testid="v5-skew-main-summary">已自动切换到负载敏感性实验并生成 {thetaOptions.length} 个独立 child 点：{thetaOptions.map((value) => `θ=${value.toFixed(1)}`).join("、")}。当前“交易规模”按每个 θ 解释；正式矩阵不会把这些点拼成一个大 trace。</p>}
    </article>}

    <article className="final-card wide">
      <h3>Block Production</h3>
      <div className="experiment-condition-grid">
        <NumericInput label="Transactions per block" aria="block size" value={blockProduction.block_size} min={10} max={5000} step={10} onChange={(value) => update(() => setBlockProduction({ ...blockProduction, block_size: boundedInteger(value, 10, 5000) }))} />
        <NumericInput label="Block interval (ms)" aria="block interval ms" value={blockProduction.block_interval_ms} min={25} max={5000} step={25} onChange={(value) => update(() => setBlockProduction({ ...blockProduction, block_interval_ms: boundedInteger(value, 25, 5000) }))} />
      </div>
      <div className="button-row">
        {[100, 250, 500, 1000].map((value) => <button type="button" key={value} className="ghost-button" onClick={() => update(() => setBlockProduction({ ...blockProduction, block_size: value }))}>{value} tx</button>)}
        {[50, 75, 100, 200, 500].map((value) => <button type="button" key={value} className="ghost-button" onClick={() => update(() => setBlockProduction({ ...blockProduction, block_interval_ms: value }))}>{value} ms</button>)}
      </div>
      <p className="muted">Smaller interval values cut blocks faster. Estimated primary block count: <strong data-testid="v5-estimated-block-count">{estimatedPrimaryBlockCount}</strong>.</p>
    </article>

    <article className="final-card wide">
      <div className="section-heading"><div><h3>③ 集群拓扑与节点内执行资源</h3><p className="muted">节点数决定独立 OS 进程数；Worker 是每个节点内部的执行并发上限，不代表节点数量。</p></div></div>
      <div className="experiment-condition-grid">
        <NumericInput label="节点数" aria="nodes" value={topology.nodes} min={1} onChange={(value) => update(() => setTopology((current) => topologyWithNodes(current, boundedInteger(value, 1, 128))))} />
        <NumericInput label="分片数" aria="shards" value={topology.shards} min={1} onChange={(value) => update(() => setTopology((current) => topologyWithShards(current, boundedInteger(value, 1, 64))))} />
        <NumericInput label="每片验证节点数" aria="validators per shard" value={topology.validators_per_shard} min={1} onChange={(value) => update(() => setTopology({ ...topology, validators_per_shard: boundedInteger(value, 1, 128) }))} />
        <NumericInput label="重复次数" aria="repeats" value={repeats} min={1} max={20} onChange={(value) => update(() => setRepeats(boundedInteger(value, 1, 20)))} />
      </div>
      <div className="resource-summary-grid">
        <div><span>预计节点进程</span><strong data-testid="v5-process-count">{topology.nodes}</strong><small>固定关系：1 节点 = 1 独立进程</small></div>
        <div><span>节点内 Worker</span><strong data-testid="v5-worker-count">{workerCount}</strong><small>并行方法使用；Serial 有效并发度固定为 1</small></div>
      </div>
      <div className="worker-option-group" data-testid="v5-worker-options">
        <span>节点内执行并发度（Workers）</span>
        <div className="segmented-control">{PARALLEL_WORKER_OPTIONS.map((value) => <button type="button" key={value} aria-pressed={workerCount === value} className={workerCount === value ? "selected" : ""} onClick={() => update(() => setWorkerCount(value))}>{value}</button>)}</div>
      </div>
      <p className="muted">普通方法对比和消融使用同一 Worker 上限保证公平；拓扑与资源扩展实验可在扫描点中分别设置 1、2、4、8。</p>
    </article>

    <WorkloadPreviewPanel preview={workloadPreview} dirty={workloadPreviewDirty} error={workloadPreviewError} onPreview={() => void previewWorkload()} disabled={busy} />

    {selectedSuite === "workload_sensitivity" && thetaMainActive && <article className="final-card wide" data-testid="v5-auto-theta-points"><div className="section-heading"><div><h3>自动偏斜度扫描点</h3><p className="muted">预设锁定为 manifest 中的 {thetaOptions.length} 个 θ 点；需要手动编辑时切回“单偏斜度实验”。</p></div></div><p>{thetaOptions.map((value) => `θ=${value.toFixed(1)} / ${compactCount(workload.txCount)}`).join("；")}</p></article>}

    {selectedSuite === "workload_sensitivity" && !thetaMainActive && <PointEditor title="负载扫描点" onAdd={() => update(() => setWorkloadPoints((items) => [...items, defaultWorkloadPoint(workload)]))}>{workloadPoints.map((point, index) => <div key={index} className="experiment-condition-grid">
      <NumericInput label="交易数量" value={point.tx_count} onChange={(value) => update(() => setWorkloadPoints(replace(workloadPoints, index, { ...point, tx_count: value })))} />
      {workload.mode === "dataset_derived" && typeof workload.variantParameters.target_alpha === "number" && <NumericInput label="target_alpha" value={point.target_alpha ?? workload.targetAlpha} step={0.2} onChange={(value) => update(() => setWorkloadPoints(replace(workloadPoints, index, { ...point, target_alpha: snapAlpha(value) })))} />}
      {workload.mode === "dataset_derived" && typeof workload.variantParameters.target_theta === "number" && <NumericInput label="target_theta" value={point.target_theta ?? Number(workload.variantParameters.target_theta)} step={0.1} min={0} max={1.2} onChange={(value) => update(() => setWorkloadPoints(replace(workloadPoints, index, { ...point, target_theta: Math.round(value * 10) / 10 })))} />}
      {workload.mode === "synthetic" && <><NumericInput label="跨片交易比例" value={point.cross_shard_ratio ?? 0} step={0.01} onChange={(value) => update(() => setWorkloadPoints(replace(workloadPoints, index, { ...point, cross_shard_ratio: value })))} /><NumericInput label="timeout_every" value={point.timeout_every ?? 0} onChange={(value) => update(() => setWorkloadPoints(replace(workloadPoints, index, { ...point, timeout_every: value })))} /></>}
      <button type="button" onClick={() => update(() => setWorkloadPoints(workloadPoints.filter((_, item) => item !== index)))}>删除点</button>
    </div>)}</PointEditor>}

    {selectedSuite === "topology_scaling" && <PointEditor title="拓扑与 Worker 扫描点" onAdd={() => update(() => setTopologyPoints((items) => [...items, { ...topology, worker_count: workerCount }]))}>{topologyPoints.map((point, index) => <div key={index} className="experiment-condition-grid topology-point-row">
      <NumericInput label="节点数" value={point.nodes} min={1} onChange={(value) => update(() => setTopologyPoints(replace(topologyPoints, index, topologyWithNodes(point, boundedInteger(value, 1, 128)))))} />
      <NumericInput label="分片数" value={point.shards} min={1} onChange={(value) => update(() => setTopologyPoints(replace(topologyPoints, index, topologyWithShards(point, boundedInteger(value, 1, 64)))))} />
      <NumericInput label="每片验证节点数" value={point.validators_per_shard} min={1} onChange={(value) => update(() => setTopologyPoints(replace(topologyPoints, index, { ...point, validators_per_shard: boundedInteger(value, 1, 128) })))} />
      <label><span>节点内 Workers</span><select value={point.worker_count ?? workerCount} onChange={(event) => update(() => setTopologyPoints(replace(topologyPoints, index, { ...point, worker_count: globalThis.Number(event.target.value) })))}>{PARALLEL_WORKER_OPTIONS.map((value) => <option key={value} value={value}>{value}</option>)}</select></label>
      <div className="readonly-field"><span>预计进程</span><strong>{point.nodes}</strong></div>
      <button type="button" onClick={() => update(() => setTopologyPoints(topologyPoints.filter((_, item) => item !== index)))}>删除点</button>
    </div>)}</PointEditor>}

    {selectedSuite === "fault_recovery_experiment" && <PointEditor title="故障扫描点" onAdd={() => update(() => setFaultPoints((items) => [...items, { mode: "disabled" }]))}>{faultPoints.map((point, index) => <div key={index} className="experiment-condition-grid">
      <label><span>故障模式</span><select value={point.mode} onChange={(event) => update(() => setFaultPoints(replace(faultPoints, index, defaultFaultPoint(event.target.value as FaultMode))))}>{(["disabled", "delay_only", "network_drop"] as FaultMode[]).map((mode) => <option key={mode} value={mode}>{faultModeLabel(mode)} ({mode})</option>)}</select></label>
      {point.mode !== "disabled" && <NumericInput label="delay_ms" value={point.delay_ms ?? 5} min={0} max={1000} onChange={(value) => update(() => setFaultPoints(replace(faultPoints, index, { ...point, delay_ms: value })))} />}
      {point.mode === "network_drop" && <NumericInput label="drop_rate" value={point.drop_rate ?? 0.1} min={0.01} max={1} step={0.01} onChange={(value) => update(() => setFaultPoints(replace(faultPoints, index, { ...point, drop_rate: value })))} />}
      <button type="button" onClick={() => update(() => setFaultPoints(faultPoints.filter((_, item) => item !== index)))}>删除点</button>
    </div>)}</PointEditor>}

    <article className="final-card wide">
      <div className="section-heading"><div><h3>④ 预检、矩阵预览与启动</h3><p className="muted">配置变化后，负载预览和正式矩阵都会失效，必须重新生成。</p></div></div>
      <p>预计子实验：<strong data-testid="v5-estimated-children">{resources.children}</strong>；预计节点进程启动次数：<strong data-testid="v5-estimated-process-starts">{resources.processStarts}</strong>；预计交易总量：<strong data-testid="v5-estimated-transactions">{resources.transactions}</strong></p>
      <div className="preflight-panel" data-testid="v5-formal-preflight">
        <PreflightItem ok={Boolean(selectedSuite)} text={`实验类型：${FORMAL_SUITE_DEFINITIONS.find((item) => item.id === selectedSuite)?.title ?? "未选择"}`} />
        <PreflightItem ok={selected.length > 0 && (selectedSuite !== "comparison_experiment" || selected.length >= 2) && (selectedSuite !== "ablation_experiment" || selected.length >= 2)} text={`实验方法：已选择 ${selected.length} 个`} />
        <PreflightItem ok={workloadRunnable} text={workloadRunnable ? "Workload preview 已通过" : "Workload preview 尚未通过或已经失效"} />
        <PreflightItem ok={topology.nodes === topology.shards * topology.validators_per_shard} text={`拓扑：${topology.nodes} 节点 / ${topology.shards} 分片 / 每片 ${topology.validators_per_shard} 节点；${workerCount} Workers`} />
        <PreflightItem ok={Boolean(preview && previewRequest && previewRunnable)} text={preview && previewRequest ? `正式矩阵：${preview.rows.length} 行${previewRunnable ? "，全部可运行" : "，存在阻断"}` : "正式矩阵尚未生成"} />
      </div>
      <div className="button-row"><button type="button" data-testid="v5-formal-preview-button" onClick={() => void previewMatrix()} disabled={busy || !catalogReady || !selected.length}>预览正式实验矩阵</button><button type="button" data-testid="v5-start-run-group-button" className="v3-secondary-button" onClick={() => void startGroup()} disabled={busy || Boolean(startBlocker)}>启动真实集群实验组</button></div>
      {startBlocker && <p className="file-error" data-testid="v5-start-blocker">无法启动：{startBlocker}</p>}
      {preview && !previewRunnable && <p className="file-error" data-testid="v5-formal-preview-blockers">启动被预览阻断：{previewBlockers.join("；") || "存在不可运行的矩阵行。"}</p>}
      {Object.entries(methodCompatibility).map(([id, result]) => <p key={id} className={result.valid ? "muted" : "file-error"}>方法 {methodDefinition(id)?.title ?? id}：{result.valid ? "兼容" : result.blockers.map(blockerLabel).join("；")}</p>)}
      {preview && <PreviewTable preview={preview} source={currentSource} datasets={datasets} />}
    </article>

    <article className="final-card wide">
      <div className="section-heading"><div><h3>实验组状态</h3><p className="muted">最近的实验组 ID 保存在浏览器，只用于刷新后的查询。强制停止会请求后端终止当前 supervisor 与节点进程树，并保留已生成的诊断产物。</p></div><div className="button-row"><button type="button" onClick={() => void queryGroup()} disabled={!groupId || busy}>重新查询</button><button type="button" data-testid="v5-cancel-run-group-button" onClick={() => void cancelGroup()} disabled={!groupId || !groupDetail || terminal(groupDetail.group.status) || groupDetail.group.status === "cancelling" || cancelBusy}>{groupDetail?.group.status === "cancelling" ? "正在停止…" : cancelBusy ? "正在请求…" : "强制停止实验"}</button></div></div>
      {groupId && <p><strong>run_group_id：</strong><code>{groupId}</code> {onOpenResults && <button type="button" onClick={() => onOpenResults(groupId)}>查看结果与产物</button>}</p>}
      {groupDetail && <GroupStatus detail={groupDetail} />}
    </article>
  </section>;
}

function PreviewTable({ preview, source, datasets }: { preview: V5FormalPreviewResponse; source: V5WorkloadSourceSpec | null; datasets: V5WorkloadDatasetSummary[] }) {
  return <div className="table-wrap formal-matrix-wrap">
    <p data-testid="v5-formal-preview-summary"><strong>执行后端：</strong>{preview.execution_backend}；<strong>矩阵行数：</strong>{preview.rows.length}</p>
    <p className="muted table-scroll-hint">矩阵列较多，保持字段横向可读；可在表格内左右滚动查看完整内容。</p>
    <table className="v5-formal-preview-table">
      <thead>
        <tr>
          <th>实验类型</th>
          <th>方法</th>
          <th>source_type</th>
          <th>dataset_id</th>
          <th>variant</th>
          <th>count</th>
          <th>seed</th>
          <th>axis</th>
          <th>alpha</th>
          <th>block_size</th>
          <th>block_interval_ms</th>
          <th>estimated_blocks</th>
          <th>nodes</th>
          <th>workers</th>
          <th>truth</th>
          <th>materialization</th>
          <th>兼容性</th>
        </tr>
      </thead>
      <tbody>{preview.rows.map((row) => <tr key={row.child_run_id} data-method-config-id={row.method_config_id}>
        <td>{suiteLabel(row.suite_type)}</td>
        <td className="matrix-method-cell">{row.method.display_name}</td>
        <td>{source?.source_type ?? "synthetic"}</td>
        <td className="matrix-dataset-cell">{source?.dataset_id ?? "synthetic"}</td>
        <td className="matrix-variant-cell">{source?.variant_mode ?? "synthetic"}</td>
        <td>{row.estimated_transactions}</td>
        <td>{row.seed}</td>
        <td>{stringValue(source?.skew_axis ?? source?.variant_parameters?.skew_axis ?? source?.variant_parameters?.access_profile)}</td>
        <td>{stringValue(row.workload_point.target_alpha ?? row.workload_point.target_theta ?? source?.target_alpha ?? source?.variant_parameters?.target_theta)}</td>
        <td>{stringValue(row.block_size)}</td>
        <td>{stringValue(row.block_interval_ms)}</td>
        <td>{stringValue(row.estimated_block_count)}</td>
        <td>{stringValue(row.topology_point.nodes)}</td>
        <td>{stringValue(row.topology_point.worker_count ?? row.method.plugin_config_overrides?.block_executor?.worker_count)}</td>
        <td className="matrix-truth-cell">{source?.source_type === "dataset" ? (datasets.find((item) => item.dataset_id === source.dataset_id)?.truth_label ?? "dataset_truth_label_unavailable") : "synthetic_generated"}</td>
        <td className="matrix-materialization-cell">{source?.source_type === "dataset" ? "child_start_before_materialization" : "not_required"}</td>
        <td className="matrix-compatibility-cell">{row.runnable ? "可运行" : row.blockers.map(blockerLabel).join("；") || "已阻止"}</td>
      </tr>)}</tbody>
    </table>
  </div>;
}

function CurrentMethods({ suite, methods, preferredMethodId, workerCount, childCount }: { suite: V5FormalSuite; methods: V5FormalMethod[]; preferredMethodId: string; workerCount: number; childCount: number }) {
  const suiteTitle = FORMAL_SUITE_DEFINITIONS.find((item) => item.id === suite)?.title ?? suite;
  return <div data-testid="v5-run-preferred-method" className="current-experiment-summary">
    <div><span>当前实验</span><strong>{suiteTitle}</strong></div>
    <div><span>已选方法</span><strong>{methods.length ? methods.map((method) => methodDefinition(method.method_id)?.title ?? method.display_name).join("、") : "未选择"}</strong></div>
    <div><span>节点内并发</span><strong>{workerCount} Workers</strong></div>
    <div><span>预计子实验</span><strong>{childCount}</strong></div>
    {preferredMethodId && methods.some((method) => method.method_id === preferredMethodId) && <small>包含从实验设计页带入的方法。</small>}
  </div>;
}

function PreflightItem({ ok, text }: { ok: boolean; text: string }) {
  return <div className={ok ? "preflight-item ok" : "preflight-item blocked"}><span aria-hidden="true">{ok ? "✓" : "×"}</span><span>{text}</span></div>;
}

function NumericInput({ label, aria, value, onChange, step = 1, min = 0, max }: { label: string; aria?: string; value: number; onChange: (value: number) => void; step?: number; min?: number; max?: number }) {
  return <label><span>{label}</span><input aria-label={aria ?? label} type="number" min={min} max={max} step={step} value={value} onChange={(event) => onChange(globalThis.Number(event.target.value))} /></label>;
}

function PointEditor({ title, onAdd, children }: { title: string; onAdd: () => void; children: ReactNode }) {
  return <article className="final-card wide" data-testid={`v5-point-editor-${title}`}><div className="section-heading"><h3>{title}</h3><button type="button" onClick={onAdd}>添加扫描点</button></div>{children}</article>;
}

function GroupStatus({ detail }: { detail: V5FormalRunGroupDetail }) {
  const failed = detail.children.filter((child) => ["failed", "blocked"].includes(child.status)).length;
  return <><p data-testid="v5-formal-group-summary"><strong>状态：</strong>{statusLabel(detail.group.status)}；<strong>执行后端：</strong>{backendLabel(detail.group.execution_backend)}；<strong>子实验：</strong>{detail.group.completed_child_runs}/{detail.group.total_child_runs}；<strong>失败：</strong>{failed}</p><div className="table-wrap"><table data-testid="v5-formal-child-table"><thead><tr><th>子实验</th><th>实验类型</th><th>方法</th><th>种子</th><th>交易</th><th>执行状态</th><th>产物状态</th><th>正式结果</th><th>无回退</th><th>阻断原因</th></tr></thead><tbody>{detail.children.map((child) => {
    const execution = child.execution_status ?? child.result?.summary?.execution_status ?? child.status;
    const artifact = child.artifact_status ?? child.result?.summary?.artifact_status;
    const eligible = child.formal_eligibility ?? child.result?.summary?.formal_eligibility;
    const blockers = [...(child.execution_gate?.blockers ?? child.result?.summary?.execution_gate?.blockers ?? []), ...(child.artifact_gate?.blockers ?? child.result?.summary?.artifact_gate?.blockers ?? [])];
    const executionLabel = execution === "completed" ? "已完成" : execution === "failed" ? "失败" : String(execution ?? "未提供");
    const artifactLabel = artifact === "complete" ? "完整" : artifact === "incomplete" ? "不完整" : String(artifact ?? "未提供");
    return <tr key={child.child_run_id}><td>{child.child_run_id}</td><td>{suiteLabel(child.suite_type)}</td><td>{child.method.display_name}</td><td>{child.seed}</td><td>{child.estimated_transactions}</td><td>{executionLabel}</td><td>{artifactLabel}</td><td>{eligible === true ? "可用" : eligible === false ? "不可用" : "未提供"}</td><td>{child.result?.summary?.no_fallback === undefined ? "未提供" : String(child.result.summary.no_fallback)}</td><td>{blockers.length ? blockers.join("; ") : (child.error ?? "无")}</td></tr>;
  })}</tbody></table></div></>;
}

function formError(input: { catalogReady: boolean; selected: V5FormalMethod[]; selectedSuite: V5FormalSuite; topology: Topology; blockProduction: BlockProduction; source: V5WorkloadSourceSpec | null; seeds: number[]; repeats: number; workloadPoints: WorkloadPoint[]; topologyPoints: TopologyPoint[]; faultPoints: FaultPoint[]; estimatedChildren: number; workloadRunnable: boolean; workerCount: number }): string | null {
  if (!input.catalogReady) return "真实集群插件目录不完整，无法预览。";
  if (!input.selectedSuite) return "请选择一种实验类型。";
  if (!input.selected.length) return "请至少选择一个执行方法。";
  if (!input.seeds.length) return "随机种子必须是一到十个不重复整数。";
  if (!input.source) return "workload_source 无法构造，请检查数据集和 seed。";
  if (!input.workloadRunnable) return "配置已变化，请先重新运行 workload preview。";
  if (input.topology.nodes < 1 || input.topology.shards < 1 || input.topology.validators_per_shard < 1 || input.topology.nodes !== input.topology.shards * input.topology.validators_per_shard) return "节点数必须等于分片数乘以每片验证节点数。";
  if (!PARALLEL_WORKER_OPTIONS.includes(input.workerCount as (typeof PARALLEL_WORKER_OPTIONS)[number])) return "Worker 数量必须为 1、2、4 或 8。";
  if (!globalThis.Number.isInteger(input.repeats) || input.repeats < 1 || input.repeats > 20) return "重复次数必须是 1 到 20 的整数。";
  if (!globalThis.Number.isInteger(input.blockProduction.block_size) || input.blockProduction.block_size < 10 || input.blockProduction.block_size > 5000) return "block_size 必须是 10 到 5000 的整数。";
  if (!globalThis.Number.isInteger(input.blockProduction.block_interval_ms) || input.blockProduction.block_interval_ms < 25 || input.blockProduction.block_interval_ms > 5000) return "block_interval_ms 必须是 25 到 5000 的整数。";
  if (input.selectedSuite === "comparison_experiment" && input.selected.length < 2) return "方法对比实验至少需要两个方法。";
  if (input.selectedSuite === "ablation_experiment" && (input.selected.length < 2 || !input.selected.some((method) => method.method_id === "hash_batch_si"))) return "Batch-SI 消融至少需要完整版本和一个消融变体。";
  if (input.selectedSuite === "main_experiment" && input.selected.length !== 1) return "主实验只能选择一个研究方案。";
  if (input.selectedSuite === "fault_recovery_experiment" && input.selected.length !== 1) return "故障与恢复实验只能选择一个方法。";
  if (input.selectedSuite === "workload_sensitivity" && input.workloadPoints.length < 2) return "负载敏感性实验至少需要两个负载扫描点。";
  if (input.selectedSuite === "topology_scaling") {
    if (input.topologyPoints.length < 2) return "拓扑与资源扩展实验至少需要两个扫描点。";
    const invalid = input.topologyPoints.find((point) => point.nodes < 1 || point.shards < 1 || point.validators_per_shard < 1 || point.nodes !== point.shards * point.validators_per_shard || !PARALLEL_WORKER_OPTIONS.includes((point.worker_count ?? input.workerCount) as (typeof PARALLEL_WORKER_OPTIONS)[number]));
    if (invalid) return "拓扑扫描点必须满足节点数=分片数×每片验证节点数，Worker 为 1、2、4 或 8。";
  }
  if (input.selectedSuite === "fault_recovery_experiment" && (input.faultPoints.length < 2 || !input.faultPoints.some((item) => item.mode === "disabled") || !input.faultPoints.some((item) => item.mode !== "disabled"))) return "故障实验需要无故障基准点和至少一个故障点。";
  if (input.estimatedChildren > 100) return "正式矩阵超过 100 个子实验硬上限。";
  return null;
}

function defaultWorkloadPoint(workload: WorkloadEditorState): WorkloadPoint {
  if (workload.mode === "dataset_derived" && typeof workload.variantParameters.target_theta === "number") return { tx_count: workload.txCount, target_theta: Number(workload.variantParameters.target_theta) };
  if (workload.mode === "dataset_derived" && typeof workload.variantParameters.target_alpha === "number") return { tx_count: workload.txCount, target_alpha: Number(workload.variantParameters.target_alpha) };
  if (workload.mode === "dataset_derived" || workload.mode === "dataset_original") return { tx_count: workload.txCount };
  return { tx_count: workload.txCount, cross_shard_ratio: workload.crossShardRatio, timeout_every: workload.timeoutEnabled ? workload.timeoutEvery : 0 };
}
function defaultFaultPoint(mode: FaultMode): FaultPoint { if (mode === "delay_only") return { mode, delay_ms: 5 }; if (mode === "network_drop") return { mode, drop_rate: 0.1, delay_ms: 0 }; return { mode: "disabled" }; }
function estimateFormalResources(selectedSuite: V5FormalSuite, methods: number, seeds: number, repeats: number, topology: Topology, txCount: number, workloadPoints: WorkloadPoint[], topologyPoints: TopologyPoint[], faultPoints: FaultPoint[]): { children: number; processStarts: number; transactions: number } {
  const factor = methods * seeds * repeats;
  const points = selectedSuite === "workload_sensitivity"
    ? workloadPoints.map((point) => ({ nodes: topology.nodes, txCount: point.tx_count }))
    : selectedSuite === "topology_scaling"
      ? topologyPoints.map((point) => ({ nodes: point.nodes, txCount }))
      : selectedSuite === "fault_recovery_experiment"
        ? faultPoints.map(() => ({ nodes: topology.nodes, txCount }))
        : [{ nodes: topology.nodes, txCount }];
  return {
    children: points.length * factor,
    processStarts: points.reduce((sum, point) => sum + point.nodes * factor, 0),
    transactions: points.reduce((sum, point) => sum + point.txCount * factor, 0),
  };
}
function parseSeeds(value: string): number[] { const values = value.split(",").map((item) => item.trim()).filter(Boolean).map((item) => globalThis.Number(item)); return !values.length || values.length > 10 || values.some((item) => !globalThis.Number.isInteger(item)) ? [] : [...new Set(values)]; }
function boundedInteger(value: number, min: number, max: number): number { if (!globalThis.Number.isFinite(value)) return min; return Math.min(max, Math.max(min, Math.round(value))); }
function snapAlpha(value: number): number { return alphaValues.reduce((best, item) => Math.abs(item - value) < Math.abs(best - value) ? item : best, 0); }
function toggle<T>(item: T, values: T[]): T[] { return values.includes(item) ? values.filter((value) => value !== item) : [...values, item]; }
function replace<T>(items: T[], index: number, value: T): T[] { return items.map((item, current) => current === index ? value : item); }
function terminal(status: string): boolean { return ["completed", "completed_with_failures", "failed", "cancelled"].includes(status); }
function errorMessage(error: unknown): string { return error instanceof Error ? error.message : String(error); }
function stringValue(value: unknown): string { return value === undefined || value === null || value === "" ? "未提供" : String(value); }
function cleanWorkloadPoints(points: WorkloadPoint[]): Array<Record<string, number>> { return points.map((point) => Object.fromEntries(Object.entries(point).filter(([, value]) => typeof value === "number" && globalThis.Number.isFinite(value))) as Record<string, number>); }
