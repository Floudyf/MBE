import { useEffect, useMemo, useRef, useState } from "react";

import {
  fetchExperimentProfiles,
  fetchExperimentTopologies,
  fetchExperimentWorkloads,
  fetchV3ComposerPreview,
  fetchV3ComposerTemplates,
  createV3SavedConfig,
  deleteV3SavedConfig,
  listV3SavedConfigs,
  previewV3FormalMetatrackBenchmark,
  runV3ControlledSmoke,
  runV3ComposerDraftSmoke,
  runV3ComposerSmoke,
  runV3FormalMetatrackBenchmark,
  validateV3ComposerDraft,
  type V2Artifact,
  type ExperimentProfile,
  type ExperimentTopology,
  type ExperimentWorkload,
  type V3ComposerPreviewResponse,
  type V3ControlledSmokeRunResponse,
  type V3DraftModuleStatus,
  type V3DraftSmokeRunResponse,
  type V3DraftValidationResponse,
  type V3FormalMetatrackBenchmarkPreview,
  type V3FormalMetatrackBenchmarkRequest,
  type V3FormalMetatrackBenchmarkRunResponse,
  type V3SavedConfig,
  type V3SavedConfigKind,
  type V3TemplateSummary,
} from "../api";
import ArtifactGroups from "../components/v3/ArtifactGroups";
import DraftRunHistoryPanel from "../components/v3/DraftRunHistoryPanel";
import DraftRunResultPanel from "../components/v3/DraftRunResultPanel";
import FairnessScopePanel from "../components/v3/FairnessScopePanel";
import FormalBenchmarkResultPanel from "../components/v3/FormalBenchmarkResultPanel";
import FormalMetatrackExperimentPanel from "../components/v3/FormalMetatrackExperimentPanel";
import FormalRunHistoryPanel from "../components/v3/FormalRunHistoryPanel";
import HelpTip from "../components/v3/HelpTip";
import PluginMatrixTable from "../components/v3/PluginMatrixTable";
import RunLevelPanel from "../components/v3/RunLevelPanel";
import RunProgressPanel, { type RunProgressMode } from "../components/v3/RunProgressPanel";
import RuntimeTopologyPanel from "../components/v3/RuntimeTopologyPanel";
import SaveCurrentConfigPanel from "../components/v3/SaveCurrentConfigPanel";
import SavedConfigLibraryPanel from "../components/v3/SavedConfigLibraryPanel";
import SingleChainComposer from "../components/v3/SingleChainComposer";
import { createComposerDraft, summarizeDraft, toComposerDraftRequest, updateDraftTopology, type ComposerDraft } from "../components/v3/composerDraft";
import { labelFor, profileLabels, templateLabels, yesNo } from "../components/v3/localization";

const fallbackStageMetadata = {
  currentStage: "V3-final Fault, Observability, and Reproducibility Closure",
  latestRuntime: "deterministic fault injection MVP, observability summary, final artifact catalog, reproducibility guide, experiment manual, and paper experiment mapping",
  runtimeTruth: "v3_final_emulator_closure_not_production_system",
  nextStage: "V3 maintenance only; do not start V4 unless explicitly requested",
};
const controlledSmokeDescription = "按固定顺序运行五组 MetaTrack 快速验证方案。Workload、seed、TxPool、BlockProducer、Consensus、CommitteeEpoch、StateStorage、MetricsReport 保持固定，只改变 Routing、Execution、StateAccess、Commit。输出 aggregate summary 与 realism readiness；这是快速验证级别的受控对照。";

const metatrackLockedModules = {
  Workload: "synthetic_hotspot",
  TxPool: "fifo_pool",
  BlockProducer: "time_or_count_block_producer",
  Consensus: "simple_leader",
  CommitteeEpoch: "disabled",
  StateStorage: "hash_state_storage",
  MetricsReport: "basic_metrics",
};

const metatrackExpectedArtifacts = ["routing_log.csv", "execution_log.csv", "state_access_log.csv", "state_commit_log.csv", "summary.csv", "summary.json"];

const fallbackTemplates: V3TemplateSummary[] = [
  {
    template_id: "metatrack_ablation",
    template_name: "MetaTrack 消融实验",
    stage: "V3.4.9",
    chain_mode: "single_chain",
    runnable: true,
    preview_only: false,
    description: "MetaTrack 本地受控消融配置，用于快速验证路由、执行、状态访问和提交组合。",
    variable_modules: ["Routing", "Execution", "StateAccess", "Commit"],
    fixed_modules: ["Workload", "TxPool", "BlockProducer", "Consensus", "StateStorage"],
    disabled_modules: ["CommitteeEpoch"],
    planned_modules: [],
    output_modules: ["MetricsReport"],
    default_preset_id: "metatrack_baseline_smoke",
    locked_modules: metatrackLockedModules,
    presets: [
      { preset_id: "metatrack_baseline_smoke", preset_name: "基线快速验证", ablation_stage: "baseline", enabled_metatrack_components: [], controlled_modules: ["Routing", "Execution", "StateAccess", "Commit"], default_plugin_selection: { Routing: "hash_sharding", Execution: "serial_execution", StateAccess: "direct_fetch", Commit: "normal_commit" }, locked_modules: metatrackLockedModules, expected_artifacts: metatrackExpectedArtifacts },
      { preset_id: "metatrack_routing_only_smoke", preset_name: "路由快速验证", ablation_stage: "routing_only", enabled_metatrack_components: ["routing"], controlled_modules: ["Routing", "Execution", "StateAccess", "Commit"], default_plugin_selection: { Routing: "metatrack_coaccess_routing", Execution: "serial_execution", StateAccess: "direct_fetch", Commit: "normal_commit" }, locked_modules: metatrackLockedModules, expected_artifacts: metatrackExpectedArtifacts },
      { preset_id: "metatrack_full_smoke", preset_name: "完整 MetaTrack 快速验证", ablation_stage: "full", enabled_metatrack_components: ["routing", "execution", "state_access", "commit"], controlled_modules: ["Routing", "Execution", "StateAccess", "Commit"], default_plugin_selection: { Routing: "metatrack_coaccess_routing", Execution: "metatrack_dual_track_execution", StateAccess: "access_list_prefetch", Commit: "constraint_checked_aggregation" }, locked_modules: metatrackLockedModules, expected_artifacts: metatrackExpectedArtifacts },
    ],
    truthfulness_note: "快速验证用于检查配置、运行链路和产物输出，不代表论文级正式实验。",
  },
];

const profileOptions = [
  { id: "metatrack_go_backed_ablation_smoke", label: "MetaTrack Go-backed 消融快速验证" },
  { id: "single_chain_role_separation_smoke", label: "单链角色拆分快速验证" },
  { id: "single_chain_composer_preview", label: "单链 Composer 预览" },
];
const currentRunPlanStorageKey = "mbe.currentRunPlanSelection";

type Props = {
  onRunCompleted?: (runId: string) => void;
  onNextToRunExperiment?: () => void;
};

function moduleStatusForLockedPlugin(moduleId: string, plugin: string): V3DraftModuleStatus {
  if (moduleId === "MetricsReport") return "output";
  if (moduleId === "CommitteeEpoch" || plugin === "disabled") return "disabled";
  return "fixed";
}

function CurrentWorkflowStatus({ draft, formalPreview, formalResult, backendValidation, draftRunResult, currentMethodName, currentWorkloadName, currentTopologyName }: { draft?: ComposerDraft | null; formalPreview?: V3FormalMetatrackBenchmarkPreview | null; formalResult?: V3FormalMetatrackBenchmarkRunResponse | null; backendValidation?: V3DraftValidationResponse | null; draftRunResult?: V3DraftSmokeRunResponse | null; currentMethodName?: string; currentWorkloadName?: string; currentTopologyName?: string }) {
  const topology = draft?.topology;
  const formalState = formalResult ? "已完成" : formalPreview ? (formalPreview.is_runnable ? "可运行" : "已预览矩阵") : "未配置";
  const workloadSource = topology?.metaverse_suite_enabled ? "元宇宙场景化" : draft?.modules.Workload?.plugin === "existing_trace" ? "真实 trace 预览" : "可控合成";
  const step = formalResult ? "正式实验完成" : formalPreview ? "已预览正式矩阵" : currentMethodName ? "已保存" : draftRunResult ? "快速验证通过" : backendValidation ? "已校验" : "配置中";
  return (
    <section className="final-card wide current-config-summary">
      <div className="v3-section-head">
        <div>
          <p className="eyebrow">当前工作流状态</p>
          <h3>配置 → 校验 → 快速验证 → 保存 → 正式实验</h3>
        </div>
        <span className="v3-status-badge status-fixed">本地 emulator</span>
      </div>
      <dl className="v3-result-grid compact">
        <div><dt>当前步骤</dt><dd>{step}</dd></div>
        <div><dt>当前完整方案</dt><dd>{currentMethodName || "未保存"}</dd></div>
        <div><dt>当前负载</dt><dd>{topology?.workload_source || workloadSource}{currentWorkloadName ? ` / ${currentWorkloadName}` : ""}</dd></div>
        <div><dt>当前拓扑</dt><dd>{currentTopologyName || "当前草稿"}</dd></div>
        <div><dt>插件选择模式</dt><dd>{topology?.controlled_experiment_enabled ? "受控对照" : "自由配置"}</dd></div>
        <div><dt>运行类型</dt><dd>{formalResult ? "正式性能实验" : "快速验证 / 可预览正式实验"}</dd></div>
        <div><dt>节点运行模式</dt><dd>{topology?.node_runtime_mode || "logical_single_process"}</dd></div>
        <div><dt>网络通信方式</dt><dd>{topology?.network_adapter || topology?.network_mode || "in_memory_message_bus"}</dd></div>
        <div><dt>跨片协议</dt><dd>{topology?.cross_shard_protocol || "none"}</dd></div>
        <div><dt>状态后端</dt><dd>{topology?.state_backend || "memory_kv"}</dd></div>
        <div><dt>负载来源</dt><dd>{workloadSource}</dd></div>
        <div><dt>正式实验状态</dt><dd>{formalState}</dd></div>
        <div><dt>运行真实性等级</dt><dd>本地 emulator，不是生产链</dd></div>
      </dl>
      <p className="muted">逻辑单进程：主性能实验推荐，用于控制变量。本地多进程：原型真实性验证，不作为主性能结论。本地 TCP 预览：消息路径预览，不是生产网络。</p>
    </section>
  );
}

export default function V3ComposerPage({ onRunCompleted, onNextToRunExperiment }: Props) {
  const [profileId, setProfileId] = useState("metatrack_go_backed_ablation_smoke");
  const [flowProfiles, setFlowProfiles] = useState<ExperimentProfile[]>([]);
  const [flowTopologies, setFlowTopologies] = useState<ExperimentTopology[]>([]);
  const [flowWorkloads, setFlowWorkloads] = useState<ExperimentWorkload[]>([]);
  const [runPlanSelection, setRunPlanSelection] = useState({ profile_id: "v4_3_realism_default", topology_id: "local_8_nodes_2_shards", workload_id: "small_test" });
  const [runPlanMessage, setRunPlanMessage] = useState("");
  const [preview, setPreview] = useState<V3ComposerPreviewResponse | null>(null);
  const [templates, setTemplates] = useState<V3TemplateSummary[]>(fallbackTemplates);
  const [artifacts, setArtifacts] = useState<V2Artifact[]>([]);
  const [draftRunResult, setDraftRunResult] = useState<V3DraftSmokeRunResponse | null>(null);
  const [controlledResult, setControlledResult] = useState<V3ControlledSmokeRunResponse | null>(null);
  const [formalPreview, setFormalPreview] = useState<V3FormalMetatrackBenchmarkPreview | null>(null);
  const [formalResult, setFormalResult] = useState<V3FormalMetatrackBenchmarkRunResponse | null>(null);
  const [formalHistoryRefreshKey, setFormalHistoryRefreshKey] = useState(0);
  const [savedConfigs, setSavedConfigs] = useState<V3SavedConfig[]>([]);
  const [loadingSavedConfigs, setLoadingSavedConfigs] = useState(false);
  const [currentMethodName, setCurrentMethodName] = useState("");
  const [currentWorkloadName, setCurrentWorkloadName] = useState("");
  const [currentTopologyName, setCurrentTopologyName] = useState("");
  const [draft, setDraft] = useState<ComposerDraft | null>(null);
  const [backendValidation, setBackendValidation] = useState<V3DraftValidationResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [runningBuiltin, setRunningBuiltin] = useState(false);
  const [validatingDraft, setValidatingDraft] = useState(false);
  const [runningDraft, setRunningDraft] = useState(false);
  const [runningControlled, setRunningControlled] = useState(false);
  const [previewingFormal, setPreviewingFormal] = useState(false);
  const [runningFormal, setRunningFormal] = useState(false);
  const [progressMode, setProgressMode] = useState<RunProgressMode>("idle");
  const [progressStep, setProgressStep] = useState(0);
  const [error, setError] = useState("");
  const [draftError, setDraftError] = useState("");
  const [formalError, setFormalError] = useState("");
  const [savedConfigError, setSavedConfigError] = useState("");
  const formalResultRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => { void loadPreview(profileId); }, [profileId]);
  useEffect(() => { void refreshSavedConfigs(); }, []);
  useEffect(() => { void loadExperimentFlowCatalog(); }, []);

  async function loadExperimentFlowCatalog() {
    try {
      const [profiles, topologies, workloads] = await Promise.all([
        fetchExperimentProfiles(),
        fetchExperimentTopologies(),
        fetchExperimentWorkloads(),
      ]);
      setFlowProfiles(profiles);
      setFlowTopologies(topologies);
      setFlowWorkloads(workloads);
      const saved = window.localStorage.getItem(currentRunPlanStorageKey);
      if (saved) {
        const parsed = JSON.parse(saved) as typeof runPlanSelection;
        if (parsed.profile_id && parsed.topology_id && parsed.workload_id) setRunPlanSelection(parsed);
      } else {
        const profile = profiles.find((item) => item.profile_id === "v4_3_realism_default") || profiles[0];
        if (profile) {
          setRunPlanSelection({
            profile_id: profile.profile_id,
            topology_id: profile.default_topology_id,
            workload_id: profile.default_workload_id,
          });
        }
      }
      setRunPlanMessage("");
    } catch (caught) {
      setRunPlanMessage(caught instanceof Error ? caught.message : String(caught));
    }
  }

  function saveCurrentRunPlanSelection() {
    window.localStorage.setItem(currentRunPlanStorageKey, JSON.stringify(runPlanSelection));
    setRunPlanMessage("当前实验计划默认项已保存到本地。");
  }

  function showFormalResult(result: V3FormalMetatrackBenchmarkRunResponse) {
    setFormalResult(result);
    setFormalPreview(result.preview);
    setArtifacts(result.artifacts || []);
    window.setTimeout(() => formalResultRef.current?.scrollIntoView({ behavior: "smooth", block: "start" }), 0);
  }

  async function loadPreview(nextProfileId: string) {
    try {
      setLoading(true);
      const [nextPreview, nextTemplates] = await Promise.all([
        fetchV3ComposerPreview(nextProfileId),
        fetchV3ComposerTemplates().catch(() => fallbackTemplates),
      ]);
      setPreview(nextPreview);
      setDraft(nextPreview.composer_preview ? createComposerDraft(nextPreview.composer_preview) : null);
      setBackendValidation(null);
      setDraftError("");
      setTemplates(nextTemplates.length ? nextTemplates : fallbackTemplates);
      setError("");
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setLoading(false);
    }
  }

  async function refreshSavedConfigs() {
    try {
      setLoadingSavedConfigs(true);
      setSavedConfigs(await listV3SavedConfigs());
      setSavedConfigError("");
    } catch (caught) {
      setSavedConfigError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setLoadingSavedConfigs(false);
    }
  }

  function updateDraft(nextDraft: ComposerDraft) {
    setDraft(nextDraft);
    setBackendValidation(null);
    setDraftError("");
  }

  function selectExperimentTemplate(templateId: string) {
    if (!draft) return;
    const template = templates.find((item) => item.template_id === templateId);
    if (!template) return;
    const presetId = template.default_preset_id || template.presets?.[0]?.preset_id || "";
    const preset = template.presets?.find((item) => item.preset_id === presetId);
    applyTemplateAndPreset(template, preset, presetId);
  }

  function selectPreset(presetId: string) {
    if (!draft) return;
    const template = templates.find((item) => item.template_id === draft.templateId);
    const preset = template?.presets?.find((item) => item.preset_id === presetId);
    if (!template) return;
    applyTemplateAndPreset(template, preset, presetId);
  }

  function applyTemplateAndPreset(template: V3TemplateSummary, preset: NonNullable<V3TemplateSummary["presets"]>[number] | undefined, presetId: string) {
    if (!draft) return;
    const controlledExperimentEnabled = draft.topology.controlled_experiment_enabled ?? false;
    const variableModule = template.variable_module || "";
    const allowedPlugins = template.allowed_variable_plugins || [];
    const lockedModules = controlledExperimentEnabled ? (preset?.locked_modules || template.locked_modules || {}) : {};
    const defaultSelection = preset?.default_plugin_selection || {};
    const controlledModules = new Set([...(template.variable_modules || []), ...(preset?.controlled_modules || [])]);
    const nextModules = Object.fromEntries(
      Object.entries(draft.modules).map(([moduleId, module]) => {
        if (defaultSelection[moduleId]) {
          const nextStatus: V3DraftModuleStatus = controlledExperimentEnabled
            ? (controlledModules.has(moduleId) ? "variable" : moduleStatusForLockedPlugin(moduleId, defaultSelection[moduleId]))
            : module.status;
          return [moduleId, { ...module, status: nextStatus, plugin: defaultSelection[moduleId], runnable: true }];
        }
        if (controlledExperimentEnabled && moduleId === variableModule) {
          const nextPlugin = allowedPlugins.includes(module.plugin) ? module.plugin : (allowedPlugins[0] || module.plugin);
          return [moduleId, { ...module, status: "variable" as const, plugin: nextPlugin, runnable: true }];
        }
        if (lockedModules[moduleId]) {
          return [moduleId, { ...module, status: moduleStatusForLockedPlugin(moduleId, lockedModules[moduleId]), plugin: lockedModules[moduleId], runnable: true }];
        }
        return [moduleId, module];
      }),
    );
    updateDraft(summarizeDraft({ templateId: template.template_id, presetId, modules: nextModules, topology: draft.topology }));
  }

  async function validateDraftOnServer(): Promise<V3DraftValidationResponse | null> {
    if (!draft) return null;
    try {
      setValidatingDraft(true);
      const result = await validateV3ComposerDraft(toComposerDraftRequest(draft));
      setBackendValidation(result);
      setDraftError("");
      return result;
    } catch (caught) {
      const message = caught instanceof Error ? caught.message : String(caught);
      setDraftError(message);
      return null;
    } finally {
      setValidatingDraft(false);
    }
  }

  async function runBuiltinTrial() {
    try {
      setRunningBuiltin(true);
      setProgressMode("draft");
      setProgressStep(2);
      const result = await runV3ComposerSmoke();
      setArtifacts(result.artifacts || []);
      setDraftRunResult(null);
      if (result.run_id) onRunCompleted?.(result.run_id);
      setProgressMode("success");
      setError("");
    } catch (caught) {
      setProgressMode("error");
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setRunningBuiltin(false);
    }
  }

  async function runDraftTrial() {
    if (!draft) return;
    try {
      setRunningDraft(true);
      setProgressMode("draft");
      setProgressStep(0);
      const validation = backendValidation?.is_runnable ? backendValidation : await validateDraftOnServer();
      if (!validation?.is_runnable) {
        setProgressMode("error");
        setDraftError("后端草稿校验未通过，不能运行当前配置草稿试运行。");
        return;
      }
      setProgressStep(2);
      const result = await runV3ComposerDraftSmoke(toComposerDraftRequest(draft));
      setProgressStep(5);
      setBackendValidation(result.validation);
      setArtifacts(result.artifacts || []);
      setDraftRunResult(result);
      if (result.run_id) onRunCompleted?.(result.run_id);
      setDraftError("");
      setError("");
      setProgressMode("success");
    } catch (caught) {
      setProgressMode("error");
      setDraftError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setRunningDraft(false);
    }
  }

  async function saveCurrentConfig(kind: V3SavedConfigKind, name: string, description: string, tags: string[]) {
    if (!draft) return;
    const draftRequest = toComposerDraftRequest(draft);
    const validationStatus = backendValidation?.is_runnable ? "runnable" : backendValidation?.is_valid ? "valid" : backendValidation ? "blocked" : "unknown";
    const payload = kind === "method"
      ? {
          draft: draftRequest,
          modules: draftRequest.modules,
          topology: draftRequest.topology,
          workload_source: draft.topology.workload_source || "synthetic",
          template_id: draft.templateId,
          preset_id: draft.presetId || "",
          last_validation: backendValidation || {},
          last_smoke_run_id: draftRunResult?.run_id || "",
        }
      : kind === "workload"
        ? workloadPayloadFromTopology(draft.topology)
        : { topology: draft.topology };
    try {
      const saved = await createV3SavedConfig({
        config_kind: kind,
        name,
        description,
        owner_label: "local_user",
        tags,
        payload,
        validation_status: validationStatus,
        last_validation: backendValidation || {},
        last_smoke_run_id: draftRunResult?.run_id || "",
      });
      if (kind === "method") setCurrentMethodName(saved.name);
      if (kind === "workload") setCurrentWorkloadName(saved.name);
      if (kind === "topology") setCurrentTopologyName(saved.name);
      await refreshSavedConfigs();
      setSavedConfigError("");
    } catch (caught) {
      setSavedConfigError(caught instanceof Error ? caught.message : String(caught));
    }
  }

  async function copySavedConfig(config: V3SavedConfig) {
    try {
      await createV3SavedConfig({
        config_kind: config.config_kind,
        name: `${config.name} copy`,
        description: config.description,
        owner_label: config.owner_label,
        tags: config.tags,
        payload: config.payload,
        validation_status: config.validation_status,
        last_validation: config.last_validation,
        last_smoke_run_id: config.last_smoke_run_id,
      });
      await refreshSavedConfigs();
    } catch (caught) {
      setSavedConfigError(caught instanceof Error ? caught.message : String(caught));
    }
  }

  async function removeSavedConfig(config: V3SavedConfig) {
    try {
      await deleteV3SavedConfig(config.config_id);
      await refreshSavedConfigs();
    } catch (caught) {
      setSavedConfigError(caught instanceof Error ? caught.message : String(caught));
    }
  }

  function loadSavedConfig(config: V3SavedConfig) {
    if (!draft) return;
    const payload = config.payload || {};
    if (config.config_kind === "method" && payload.draft && typeof payload.draft === "object") {
      const savedDraft = payload.draft as ReturnType<typeof toComposerDraftRequest>;
      const modules = Object.fromEntries(Object.entries(savedDraft.modules || {}).map(([moduleId, module]) => [moduleId, {
        moduleId,
        status: module.status,
        plugin: module.plugin,
        runnable: module.status !== "planned",
        params: module.params || {},
      }]));
      updateDraft(summarizeDraft({ templateId: savedDraft.template_id, presetId: savedDraft.preset_id, modules, topology: savedDraft.topology || draft.topology }));
      setBackendValidation((payload.last_validation as V3DraftValidationResponse) || null);
      setCurrentMethodName(config.name);
      return;
    }
    if (config.config_kind === "topology") {
      const topology = (payload.topology && typeof payload.topology === "object" ? payload.topology : payload) as ComposerDraft["topology"];
      updateDraft(updateDraftTopology(draft, { ...draft.topology, ...topology }));
      setCurrentTopologyName(config.name);
      return;
    }
    if (config.config_kind === "workload") {
      updateDraft(updateDraftTopology(draft, { ...draft.topology, ...payload }));
      setCurrentWorkloadName(config.name);
    }
  }

  async function runControlledTrial() {
    try {
      setRunningControlled(true);
      setProgressMode("controlled");
      setProgressStep(0);
      const result = await runV3ControlledSmoke();
      setProgressStep(3);
      setControlledResult(result);
      setArtifacts(result.artifacts || []);
      setDraftRunResult(null);
      if (result.run_id) onRunCompleted?.(result.run_id);
      setError("");
      setProgressMode("success");
    } catch (caught) {
      setProgressMode("error");
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setRunningControlled(false);
    }
  }

  async function previewFormalBenchmark(payload: V3FormalMetatrackBenchmarkRequest) {
    try {
      setPreviewingFormal(true);
      setFormalError("");
      const result = await previewV3FormalMetatrackBenchmark(payload);
      setFormalPreview(result);
    } catch (caught) {
      setFormalError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setPreviewingFormal(false);
    }
  }

  async function runFormalBenchmark(payload: V3FormalMetatrackBenchmarkRequest) {
    try {
      setRunningFormal(true);
      setProgressMode("controlled");
      setProgressStep(0);
      const result = await runV3FormalMetatrackBenchmark(payload);
      showFormalResult(result);
      setDraftRunResult(null);
      setFormalHistoryRefreshKey((value) => value + 1);
      setFormalError("");
      setProgressMode("success");
    } catch (caught) {
      setProgressMode("error");
      setFormalError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setRunningFormal(false);
    }
  }

  const composer = preview?.composer_preview;
  const profilePreview = preview?.profile_preview || {};
  const warnings = useMemo(() => {
    const value = profilePreview.warnings;
    return Array.isArray(value) ? value.map(String) : [];
  }, [profilePreview]);
  const selectedTemplate = useMemo(
    () => templates.find((template) => template.template_id === (draft?.templateId || composer?.template_id || preview?.experiment_template)),
    [composer?.template_id, draft?.templateId, preview?.experiment_template, templates],
  );
  const selectedPreset = useMemo(
    () => selectedTemplate?.presets?.find((preset) => preset.preset_id === (draft?.presetId || selectedTemplate.default_preset_id)) || selectedTemplate?.presets?.[0],
    [draft?.presetId, selectedTemplate],
  );
  const controlledExperimentEnabled = draft?.topology.controlled_experiment_enabled ?? false;
  const selectedLockedModules = controlledExperimentEnabled ? (selectedPreset?.locked_modules || selectedTemplate?.locked_modules || {}) : {};
  const selectedVariableModules = selectedPreset?.controlled_modules || (selectedTemplate?.variable_module ? [selectedTemplate.variable_module] : selectedTemplate?.variable_modules || []);
  const stageLabel = preview?.current_stage || preview?.stage || fallbackStageMetadata.currentStage;
  const latestRuntimeLabel = preview?.latest_runtime_stage || preview?.latest_completed_runtime_stage || fallbackStageMetadata.latestRuntime;
  const runtimeTruthLabel = preview?.runtime_truth || fallbackStageMetadata.runtimeTruth;
  const nextStageLabel = preview?.next_stage || fallbackStageMetadata.nextStage;

  return (
    <section className="page-grid v3-composer-page">
      <header className="final-card wide v3-composer-header console-hero">
        <div>
          <p className="eyebrow">实验设计</p>
          <h2>实验设计</h2>
          <p>以 11 模块区块链实验流水线作为唯一实验设计源。运行实验页只从当前设计派生矩阵和 V4 真实性验证请求。</p>
          <p className="muted">当前阶段：{stageLabel}</p>
          <p>{`运行能力：${latestRuntimeLabel}。`} <HelpTip title="真实性边界">V3-final 是本地 emulator 闭环：确定性故障注入、观测摘要和复现 bundle；不是多服务器部署、生产集群、生产共识网络或论文级性能结论。</HelpTip></p>
        </div>
        <div className="v3-boundary-badges">
          <span>本地快速验证</span>
          <span>中文控制台</span>
          <span>HelpTip 解释</span>
          <span>轻量图表</span>
          <span>V3-final closure</span>
        </div>
        <button type="button" className="v3-secondary-button" onClick={onNextToRunExperiment}>下一步：运行实验</button>
      </header>

      <CurrentWorkflowStatus
        draft={draft}
        formalPreview={formalPreview}
        formalResult={formalResult}
        backendValidation={backendValidation}
        draftRunResult={draftRunResult}
        currentMethodName={currentMethodName}
        currentWorkloadName={currentWorkloadName}
        currentTopologyName={currentTopologyName}
      />

      <section className="final-card wide">
        <p className="eyebrow">Experiment Plan Summary</p>
        <h3>当前实验计划摘要</h3>
        <p className="muted">完整实验计划来自 11 模块 Composer、Formal benchmark 配置、负载、拓扑、seed 和指标设置。下面的 catalog 只是默认推荐项 / 高级兼容设置，不是第二套完整实验计划源。</p>
        <dl className="v3-result-grid compact">
          <div><dt>当前方法</dt><dd>{currentMethodName || selectedPreset?.preset_id || "metatrack_full / draft"}</dd></div>
          <div><dt>当前负载</dt><dd>{currentWorkloadName || draft?.topology.workload_source || "Composer draft workload"}</dd></div>
          <div><dt>当前拓扑</dt><dd>{currentTopologyName || draft?.topology.node_runtime_mode || "Composer draft topology"}</dd></div>
          <div><dt>当前正式实验状态</dt><dd>{formalResult ? "已运行" : formalPreview ? "已预览" : "未预览"}</dd></div>
          <div><dt>可派生运行类型</dt><dd>快速验证 / 正式性能实验 / V4 真实性验证</dd></div>
        </dl>
        <details className="v3-foldout">
          <summary className="v3-foldout-summary">默认推荐项 / 高级兼容设置</summary>
          <p className="muted">这些字段来自 experiment-flow 静态 catalog，用于向运行实验页提供默认项；完整实验设计仍以 11 模块 Composer 为准。</p>
          <div className="form-grid">
          <label>
            <span>Profile</span>
            <select value={runPlanSelection.profile_id} onChange={(event) => {
              const nextProfile = flowProfiles.find((item) => item.profile_id === event.target.value);
              setRunPlanSelection({
                profile_id: event.target.value,
                topology_id: nextProfile?.default_topology_id || runPlanSelection.topology_id,
                workload_id: nextProfile?.default_workload_id || runPlanSelection.workload_id,
              });
            }}>
              {flowProfiles.map((item) => <option key={item.profile_id} value={item.profile_id}>{item.label}</option>)}
            </select>
          </label>
          <label>
            <span>Topology</span>
            <select value={runPlanSelection.topology_id} onChange={(event) => setRunPlanSelection({ ...runPlanSelection, topology_id: event.target.value })}>
              {flowTopologies.map((item) => <option key={item.topology_id} value={item.topology_id}>{item.label}</option>)}
            </select>
          </label>
          <label>
            <span>Workload</span>
            <select value={runPlanSelection.workload_id} onChange={(event) => setRunPlanSelection({ ...runPlanSelection, workload_id: event.target.value })}>
              {flowWorkloads.map((item) => <option key={item.workload_id} value={item.workload_id}>{item.label}{item.planned ? " - planned / dataset not attached" : ""}</option>)}
            </select>
          </label>
          </div>
        </details>
        <div className="button-row">
          <button type="button" onClick={saveCurrentRunPlanSelection}>保存实验计划</button>
          <button type="button" className="v3-secondary-button" onClick={onNextToRunExperiment}>下一步：运行实验</button>
        </div>
        {runPlanMessage && <p className="muted">{runPlanMessage}</p>}
      </section>

      <section className="final-card wide console-section-title">
        <p className="eyebrow">2. 控制台入口</p>
        <h3>入口与插件组合预设</h3>
        <p className="muted">快速验证用于检查配置链路；正式性能实验在“论文实验设计”中按显式参数、多 seed 和单变量扫描运行。</p>
      </section>

      <section className="final-card wide v3-template-bar">
        <label>
          <span>控制台入口</span>
          <select value={profileId} onChange={(event) => setProfileId(event.target.value)}>
            {profileOptions.map((item) => <option key={item.id} value={item.id}>{item.label}</option>)}
          </select>
        </label>
        <p>控制台入口决定 Composer 初始配置；正式性能实验参数在“论文实验设计”中配置。</p>
        <details className="v3-foldout">
          <summary className="v3-foldout-summary">实验身份详情</summary>
          <dl className="v3-identity-grid">
            <div><dt>论文实验方案</dt><dd>{labelFor(templateLabels, composer?.template_id || preview?.experiment_template || "-")}</dd></div>
            <div><dt>后端 stage</dt><dd>{stageLabel}</dd></div>
            <div><dt>前端 stage</dt><dd>{stageLabel}</dd></div>
            <div><dt>下一阶段</dt><dd>{nextStageLabel}</dd></div>
            <div><dt>Runtime truth</dt><dd>{runtimeTruthLabel}</dd></div>
            <div><dt>后端类型</dt><dd>{composer?.truth_labels?.backend_type || String(profilePreview.backend_type || "-")}</dd></div>
            <div><dt>真实性标签</dt><dd>{composer?.truth_labels?.truth_label || String(profilePreview.truth_label || "-")}</dd></div>
            <div><dt>是否可运行</dt><dd>{yesNo(preview?.runnable && composer?.runnable)}</dd></div>
            <div><dt>链模式</dt><dd>{composer?.chain_mode === "single_chain" ? "单链" : composer?.chain_mode || "-"}</dd></div>
          </dl>
        </details>
      </section>

      {draft && (
        <section className="final-card wide console-section-title">
          <p className="eyebrow">2. 插件组合预设</p>
          <h3>模板只填充推荐插件组合，默认不锁定模块</h3>
        </section>
      )}

      {draft && (
        <section className="final-card wide v3-template-bar">
          <label>
            <span>插件组合预设</span>
            <select value={draft.templateId} onChange={(event) => selectExperimentTemplate(event.target.value)}>
              {templates.filter((template) => template.runnable).map((template) => (
                <option key={template.template_id} value={template.template_id}>
                  {template.template_name || labelFor(templateLabels, template.template_id)}
                </option>
              ))}
            </select>
          </label>
          {selectedTemplate && (
            <div className="v3-template-lock-summary">
              <p className="muted">{selectedTemplate.description}</p>
              <p className="muted">模板用于快速填充推荐插件组合。默认不锁定模块；只有打开“受控对照模式”时，模板固定规则才会生效。</p>
              {selectedTemplate.presets && selectedTemplate.presets.length > 0 && (
                <label>
                  <span>快速验证方案 <HelpTip title="快速验证方案">用于确认配置和产物是否正常，不代表正式性能实验。</HelpTip></span>
                  <select value={selectedPreset?.preset_id || ""} onChange={(event) => selectPreset(event.target.value)}>
                    {selectedTemplate.presets.map((preset) => (
                      <option key={preset.preset_id} value={preset.preset_id}>
                        {preset.preset_name || preset.preset_id}
                      </option>
                    ))}
                  </select>
                </label>
              )}
              <dl className="v3-identity-grid">
                <div><dt>实验变量模块</dt><dd>{selectedTemplate.variable_module || selectedTemplate.variable_modules?.join(", ") || "-"}</dd></div>
                <div><dt>预设</dt><dd>{selectedPreset?.preset_id || selectedTemplate.default_preset_id || "默认快速验证"}</dd></div>
                <div><dt>消融阶段</dt><dd>{selectedPreset?.ablation_stage || "-"}</dd></div>
                <div><dt>启用组件</dt><dd>{(selectedPreset?.enabled_metatrack_components || []).join(", ") || "baseline / none"}</dd></div>
              </dl>
              {!controlledExperimentEnabled && (
                <p className="muted">当前为自由配置模式：模板固定规则未启用。</p>
              )}
              {controlledExperimentEnabled && Object.keys(selectedLockedModules).length > 0 && (
                <details className="v3-foldout">
                  <summary className="v3-foldout-summary">模板固定模块</summary>
                  <ul className="v3-check-list compact">
                    {Object.entries(selectedLockedModules).map(([moduleId, plugin]) => (
                      <li key={moduleId}><span>{moduleId}</span> <code>{plugin}</code></li>
                    ))}
                  </ul>
                </details>
              )}
            </div>
          )}
        </section>
      )}

      {draft && (
        <section className="final-card wide console-section-title">
          <p className="eyebrow">3. 负载配置 / 4. 运行拓扑配置</p>
          <h3>基础运行路径、负载语义与拓扑细节</h3>
        </section>
      )}

      {draft && <RuntimeTopologyPanel topology={draft.topology} onChange={(topology) => updateDraft(updateDraftTopology(draft, topology))} />}

      {loading && <p className="notice">正在加载 V3 Composer 预览...</p>}
      {error && <p className="file-error">{error}</p>}

      {composer && draft && (
        <>
          <section className="final-card wide console-section-title">
            <p className="eyebrow">5. 11 模块方案配置</p>
            <h3>主流程保持不变</h3>
            <p className="muted">{"Workload -> TxPool -> BlockProducer -> ConsensusRuntime -> CommitteeEpoch -> Routing/Sharding -> Execution -> StateAccess -> StateStorage -> Commit -> MetricsReport"}</p>
          </section>
          <SingleChainComposer
            preview={composer}
            draft={draft}
            onDraftChange={updateDraft}
            variableModule={selectedTemplate?.variable_module}
            variableModules={selectedVariableModules}
            lockedModules={selectedLockedModules}
            controlledExperimentEnabled={controlledExperimentEnabled}
          />
          <section className="final-card wide console-section-title">
            <p className="eyebrow">6. 校验与快速验证当前方案</p>
            <h3>先校验，再运行当前配置快速验证</h3>
            <p className="muted">快速验证通过后，可以把完整方案、负载或拓扑保存到配置库。</p>
          </section>
          <div className="final-card-grid v3-post-workbench">
            <RunLevelPanel
              runnable={Boolean(preview?.runnable && composer?.runnable)}
              running={runningBuiltin}
              onRunSmoke={runBuiltinTrial}
              draft={draft}
              backendValidation={backendValidation}
              validatingDraft={validatingDraft}
              runningDraft={runningDraft}
              draftError={draftError}
              onValidateDraft={validateDraftOnServer}
              onRunDraftSmoke={runDraftTrial}
            />
            <SaveCurrentConfigPanel
              disabled={!draft}
              validationStatus={backendValidation?.is_runnable ? "runnable" : backendValidation?.is_valid ? "valid" : backendValidation ? "blocked" : "unknown"}
              onSave={saveCurrentConfig}
            />
          </div>
          <section className="final-card wide console-section-title">
            <p className="eyebrow">7. 保存 / 加载配置</p>
            <h3>配置库用于复用正式实验方案</h3>
          </section>
          <SavedConfigLibraryPanel
            configs={savedConfigs}
            loading={loadingSavedConfigs}
            error={savedConfigError}
            onRefresh={refreshSavedConfigs}
            onLoad={loadSavedConfig}
            onCopy={copySavedConfig}
            onDelete={removeSavedConfig}
          />
        </>
      )}

      {draft && (
        <>
          <section className="final-card wide console-section-title">
            <p className="eyebrow">8. 正式实验设计</p>
            <h3>MetaTrack 正式性能实验矩阵</h3>
            <p className="muted">这里配置正式性能实验；快速验证入口保留在“运行与结果”中。</p>
          </section>
          <FormalMetatrackExperimentPanel
            draft={draft}
            savedConfigs={savedConfigs}
            preview={formalPreview}
            running={runningFormal}
            previewing={previewingFormal}
            error={formalError}
            onPreview={previewFormalBenchmark}
            onRun={runFormalBenchmark}
          />
        </>
      )}

      <section className="final-card wide console-section-title">
        <p className="eyebrow">9. 运行结果</p>
        <h3>先看进度与核心指标，再展开详细数据</h3>
      </section>

      <div className="final-card-grid v3-post-workbench">
        <FairnessScopePanel scope={composer?.fairness_scope || preview?.fairness_scope || {}} valid={Boolean(profilePreview.valid ?? preview?.runnable)} warnings={warnings} draft={draft} />
      </div>

      <RunProgressPanel mode={progressMode} activeStep={progressStep} error={draftError || error} />

      <section className="final-card wide v3-controlled-smoke">
        <div className="v3-section-head">
          <div>
            <p className="eyebrow">受控快速验证</p>
            <h3>MetaTrack 五组快速验证方案</h3>
          </div>
          <button type="button" className="v3-secondary-button" disabled={runningControlled} onClick={runControlledTrial}>
            {runningControlled ? "运行中..." : "运行受控快速验证"}
          </button>
        </div>
        <p className="muted">{controlledSmokeDescription}</p>
        <details className="v3-foldout">
          <summary className="v3-foldout-summary">真实性边界</summary>
          <p className="muted">本地 Go-backed modular research chain 快速验证；不是 Fabric live、不是 EVM live、不是 BlockEmulator backend、不是真实 PBFT/HotStuff/Raft、不是完整跨片协议，也不是论文级证据。</p>
        </details>
        {controlledResult && (
          <>
            <dl className="v3-result-grid">
              <div><dt>运行 ID</dt><dd>{controlledResult.run_id}</dd></div>
              <div><dt>运行模式</dt><dd>受控快速验证</dd></div>
              <div><dt>预设数量</dt><dd>{controlledResult.preset_order.length}</dd></div>
              <div><dt>后端真实性</dt><dd>{controlledResult.realism_readiness?.backend_truth || "local Go-backed quick trial"}</dd></div>
            </dl>
            <div className="v3-summary-preview">
              {controlledResult.aggregate_summary.map((row) => (
                <div key={String(row.preset_id)}>
                  <dt>{String(row.preset_id)}</dt>
                  <dd>{String(row.ablation_stage || "-")} / cross {String(row.cross_shard_ratio ?? "-")} / exec {String(row.avg_execution_latency_ms ?? "-")}</dd>
                </div>
              ))}
            </div>
            <ArtifactGroups artifacts={controlledResult.artifacts} title="受控对照产物" expectedArtifacts={["run_index.csv", "aggregate_summary.csv", "ablation_report.md", "realism_readiness.json", "realism_readiness.md"]} />
          </>
        )}
      </section>

      <FormalRunHistoryPanel refreshKey={formalHistoryRefreshKey} autoLoadLatest onSelectResult={showFormalResult} />
      <div ref={formalResultRef}>
        <FormalBenchmarkResultPanel result={formalResult} />
      </div>
      <DraftRunResultPanel result={draftRunResult} />
      <DraftRunHistoryPanel />

      {composer && (
        <section className="v3-supporting-sections">
          <section className="final-card wide console-section-title">
            <p className="eyebrow">11. 开发者详情</p>
            <h3>插件矩阵、模板列表与调试信息</h3>
          </section>
          <details className="final-card wide v3-foldout">
            <summary className="v3-foldout-summary">插件矩阵与开发者详情</summary>
            <PluginMatrixTable rows={composer.plugin_matrix || preview?.plugin_matrix || []} />
          </details>
          <details className="final-card wide v3-foldout">
            <summary className="v3-foldout-summary">论文实验方案列表</summary>
            <div className="v3-template-list">
              {templates.map((template) => (
                <div key={template.template_id} className="v3-template-row">
                  <span><strong>{template.template_name || labelFor(templateLabels, template.template_id)}</strong><small>{template.template_id}</small></span>
                  <span className={`v3-status-badge status-${template.runnable ? "variable" : "planned"}`}>{template.runnable ? "可运行" : template.preview_only ? "仅预览" : "规划中"}</span>
                </div>
              ))}
            </div>
          </details>
        </section>
      )}

      <section className="final-card wide console-section-title">
        <p className="eyebrow">10. 产物下载</p>
        <h3>正式实验、快速验证与历史兼容产物</h3>
        <p className="muted">优先查看 formal_* 论文数据表；历史 Draft Smoke 和 V3 兼容产物保留在折叠组内。</p>
      </section>

      <ArtifactGroups artifacts={artifacts} title="产物下载" expectedArtifacts={selectedPreset?.expected_artifacts} />
    </section>
  );
}

function workloadPayloadFromTopology(topology: ComposerDraft["topology"]): Record<string, unknown> {
  const keys = [
    "workload_source",
    "metaverse_scenario",
    "tx_count",
    "seed",
    "user_count",
    "asset_count",
    "item_count",
    "avatar_count",
    "scene_count",
    "metaverse_count",
    "hotspot_ratio",
    "cross_scene_ratio",
    "cross_shard_ratio",
    "burst_rate",
    "read_write_ratio",
    "zipf_alpha",
    "submit_rate",
    "arrival_rate",
    "key_space_size",
    "asset_skew",
    "scene_skew",
    "offchain_confirmation_enabled",
    "offchain_confirm_delay_ms",
    "offchain_failure_ratio",
    "cross_metaverse_enabled",
    "trace_path",
    "trace_schema",
    "trace_field_mapping",
  ];
  return Object.fromEntries(keys.map((key) => [key, topology[key as keyof typeof topology]]).filter(([, value]) => value !== undefined));
}
