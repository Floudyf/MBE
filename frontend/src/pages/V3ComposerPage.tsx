import { useEffect, useMemo, useState } from "react";

import {
  fetchV3ComposerPreview,
  fetchV3ComposerTemplates,
  runV3ControlledSmoke,
  runV3ComposerDraftSmoke,
  runV3ComposerSmoke,
  validateV3ComposerDraft,
  type V2Artifact,
  type V3ComposerPreviewResponse,
  type V3ControlledSmokeRunResponse,
  type V3DraftModuleStatus,
  type V3DraftSmokeRunResponse,
  type V3DraftValidationResponse,
  type V3TemplateSummary,
} from "../api";
import ArtifactGroups from "../components/v3/ArtifactGroups";
import DraftRunHistoryPanel from "../components/v3/DraftRunHistoryPanel";
import DraftRunResultPanel from "../components/v3/DraftRunResultPanel";
import FairnessScopePanel from "../components/v3/FairnessScopePanel";
import PluginMatrixTable from "../components/v3/PluginMatrixTable";
import RunLevelPanel from "../components/v3/RunLevelPanel";
import RuntimeTopologyPanel from "../components/v3/RuntimeTopologyPanel";
import SingleChainComposer from "../components/v3/SingleChainComposer";
import { createComposerDraft, summarizeDraft, toComposerDraftRequest, updateDraftTopology, type ComposerDraft } from "../components/v3/composerDraft";
import { labelFor, profileLabels, templateLabels, yesNo } from "../components/v3/localization";

const metatrackPrimaryMetrics = ["cross_shard_ratio", "remote_state_access_ratio", "fast_track_count", "conservative_track_count", "cache_hit_rate", "prefetch_hit_rate", "aggregation_ratio", "constraint_failed_count", "avg_execution_latency_ms", "avg_state_access_latency_ms", "avg_commit_latency_ms"];
const metatrackExpectedArtifacts = ["routing_log.csv", "execution_log.csv", "state_access_log.csv", "state_commit_log.csv", "summary.csv", "summary.json", "block_log.csv", "tx_results.csv"];
const metatrackLockedModules = { Workload: "synthetic_hotspot", TxPool: "fifo_pool", BlockProducer: "time_or_count_block_producer", Consensus: "simple_leader", CommitteeEpoch: "disabled", StateStorage: "hash_state_storage", MetricsReport: "basic_metrics" };
const metatrackTruthfulnessNote = "This ablation template compares deterministic local MetaTrack component combinations in the Go-backed Draft Smoke runtime. It is not a paper-ready benchmark sweep, not Fabric live execution, and not a full multi-node emulator.";
const metatrackPresets = [
  { preset_id: "metatrack_baseline_smoke", preset_name: "MetaTrack baseline smoke", ablation_stage: "baseline", enabled_metatrack_components: [], controlled_modules: ["Routing", "Execution", "StateAccess", "Commit"], default_plugin_selection: { Routing: "hash_sharding", Execution: "serial_execution", StateAccess: "direct_fetch", Commit: "normal_commit" }, locked_modules: metatrackLockedModules, primary_metrics: metatrackPrimaryMetrics, expected_artifacts: metatrackExpectedArtifacts, result_guide: "Compare this baseline against later presets; focus on routing/state access ratios, execution tracks, commit aggregation, and latency.", truthfulness_note: metatrackTruthfulnessNote },
  { preset_id: "metatrack_routing_only_smoke", preset_name: "MetaTrack routing-only smoke", ablation_stage: "routing_only", enabled_metatrack_components: ["routing"], controlled_modules: ["Routing", "Execution", "StateAccess", "Commit"], default_plugin_selection: { Routing: "metatrack_coaccess_routing", Execution: "serial_execution", StateAccess: "direct_fetch", Commit: "normal_commit" }, locked_modules: metatrackLockedModules, primary_metrics: metatrackPrimaryMetrics, expected_artifacts: metatrackExpectedArtifacts, result_guide: "Focus on routing_log.csv, cross_shard_ratio, touched shards, and downstream latency changes.", truthfulness_note: metatrackTruthfulnessNote },
  { preset_id: "metatrack_routing_execution_smoke", preset_name: "MetaTrack routing + execution smoke", ablation_stage: "routing_execution", enabled_metatrack_components: ["routing", "execution"], controlled_modules: ["Routing", "Execution", "StateAccess", "Commit"], default_plugin_selection: { Routing: "metatrack_coaccess_routing", Execution: "metatrack_dual_track_execution", StateAccess: "direct_fetch", Commit: "normal_commit" }, locked_modules: metatrackLockedModules, primary_metrics: metatrackPrimaryMetrics, expected_artifacts: metatrackExpectedArtifacts, result_guide: "Focus on routing_log.csv, execution_log.csv, fast/conservative tracks, dependency edges, and latency.", truthfulness_note: metatrackTruthfulnessNote },
  { preset_id: "metatrack_routing_execution_state_access_smoke", preset_name: "MetaTrack routing + execution + state access smoke", ablation_stage: "routing_execution_state_access", enabled_metatrack_components: ["routing", "execution", "state_access"], controlled_modules: ["Routing", "Execution", "StateAccess", "Commit"], default_plugin_selection: { Routing: "metatrack_coaccess_routing", Execution: "metatrack_dual_track_execution", StateAccess: "access_list_prefetch", Commit: "normal_commit" }, locked_modules: metatrackLockedModules, primary_metrics: metatrackPrimaryMetrics, expected_artifacts: metatrackExpectedArtifacts, result_guide: "Focus on routing, execution, state_access_log.csv, cache/prefetch hit rates, remote state access ratio, and latency.", truthfulness_note: metatrackTruthfulnessNote },
  { preset_id: "metatrack_full_smoke", preset_name: "MetaTrack full smoke", ablation_stage: "full", enabled_metatrack_components: ["routing", "execution", "state_access", "commit"], controlled_modules: ["Routing", "Execution", "StateAccess", "Commit"], default_plugin_selection: { Routing: "metatrack_coaccess_routing", Execution: "metatrack_dual_track_execution", StateAccess: "access_list_prefetch", Commit: "constraint_checked_aggregation" }, locked_modules: metatrackLockedModules, primary_metrics: metatrackPrimaryMetrics, expected_artifacts: metatrackExpectedArtifacts, result_guide: "Focus on the full MetaTrack component chain: routing, execution, state access, commit aggregation/constraints, and all runtime logs.", truthfulness_note: metatrackTruthfulnessNote },
];
const currentStageLabel = "V3.5.2 Launcher Preview";
const latestRuntimeLabel = "V3.5.1 Logical Topology Ready";
const runtimeBoundaryText = "Current capability generates local launcher preview artifacts from logical node topology. It is preview-only: not real TCP, not a real multi-process runtime, not real PBFT/HotStuff/Raft, not Fabric/EVM live, not BlockEmulator backend, not a real cross-shard protocol, and not a paper-grade benchmark.";
const templateStageNote = "Template stage note: metatrack_ablation was introduced in V3.4.9; the controlled runner was introduced in V3.4.10; logical node topology was introduced in V3.5.1; launcher preview artifacts are introduced in V3.5.2.";
const controlledSmokeDescription = "Runs the five MetaTrack presets in fixed order. Workload, seed, TxPool, BlockProducer, Consensus, CommitteeEpoch, StateStorage, and MetricsReport stay fixed; only Routing, Execution, StateAccess, and Commit vary. Outputs aggregate summary and realism readiness for smoke-level controlled comparison only.";

const fallbackTemplates: V3TemplateSummary[] = [
  { template_id: "metatrack_ablation", template_name: "MetaTrack ablation", stage: "V3.4.9", chain_mode: "single_chain", runnable: true, preview_only: false, description: "MetaTrack preset-controlled local Draft Smoke ablation", variable_modules: ["Routing", "Execution", "StateAccess", "Commit"], fixed_modules: ["Workload", "TxPool", "BlockProducer", "Consensus", "StateStorage"], disabled_modules: ["CommitteeEpoch"], planned_modules: [], output_modules: ["MetricsReport"], default_preset_id: "metatrack_baseline_smoke", locked_modules: metatrackLockedModules, presets: metatrackPresets, truthfulness_note: metatrackTruthfulnessNote },
  { template_id: "single_module_execution", template_name: "Single-module Execution", stage: "V3.4.6", chain_mode: "single_chain", runnable: true, preview_only: false, description: "Execution runtime hardening smoke preset", variable_module: "Execution", allowed_variable_plugins: ["serial_execution", "parallel_light_execution", "metatrack_dual_track_execution"], default_preset_id: "execution_dual_track_smoke", variable_modules: ["Execution"], fixed_modules: ["Workload", "TxPool", "BlockProducer", "Consensus", "Routing", "StateAccess", "StateStorage", "Commit"], disabled_modules: ["CommitteeEpoch"], planned_modules: [], output_modules: ["MetricsReport"], presets: [{ preset_id: "execution_dual_track_smoke", preset_name: "Execution dual-track smoke", variable_module: "Execution", primary_metrics: ["fast_track_count", "conservative_track_count", "blocked_tx_count", "dependency_edge_count"], expected_artifacts: ["execution_log.csv", "routing_log.csv", "summary.csv", "summary.json", "block_log.csv", "tx_results.csv"], result_guide: "Focus on dependency edges, tracks, logical workers, and execution_log.csv.", truthfulness_note: "This is a local deterministic light model, not real concurrent execution or rollback." }] },
  { template_id: "single_module_state_access", template_name: "Single-module StateAccess", stage: "V3.4.7", chain_mode: "single_chain", runnable: true, preview_only: false, description: "StateAccess runtime hardening smoke preset", variable_module: "StateAccess", allowed_variable_plugins: ["direct_fetch", "remote_state_access_model", "cached_state_access", "access_list_prefetch"], default_preset_id: "state_access_remote_prefetch_smoke", variable_modules: ["StateAccess"], fixed_modules: ["Workload", "TxPool", "BlockProducer", "Consensus", "Routing", "Execution", "StateStorage", "Commit"], disabled_modules: ["CommitteeEpoch"], planned_modules: [], output_modules: ["MetricsReport"], presets: [{ preset_id: "state_access_remote_prefetch_smoke", preset_name: "StateAccess remote/prefetch smoke", variable_module: "StateAccess", primary_metrics: ["remote_state_access_ratio", "cache_hit_rate", "prefetch_hit_rate", "avg_state_access_latency_ms"], expected_artifacts: ["state_access_log.csv", "execution_log.csv", "routing_log.csv", "summary.csv", "summary.json", "block_log.csv", "tx_results.csv"], result_guide: "Focus on local/remote access, cache/prefetch hit rates, latency, and state_access_log.csv.", truthfulness_note: "This is a local deterministic light model, not real proof/witness, MPT, or remote storage IO." }] },
  { template_id: "single_module_commit", template_name: "Single-module Commit", stage: "V3.4.8", chain_mode: "single_chain", runnable: true, preview_only: false, description: "Commit runtime hardening smoke preset", variable_module: "Commit", allowed_variable_plugins: ["normal_commit", "conservative_commit", "hot_update_aggregation", "constraint_checked_aggregation"], default_preset_id: "commit_hot_update_smoke", variable_modules: ["Commit"], fixed_modules: ["Workload", "TxPool", "BlockProducer", "Consensus", "Routing", "Execution", "StateAccess", "StateStorage"], disabled_modules: ["CommitteeEpoch"], planned_modules: [], output_modules: ["MetricsReport"], presets: [{ preset_id: "commit_hot_update_smoke", preset_name: "Commit hot-update smoke", variable_module: "Commit", primary_metrics: ["aggregation_ratio", "hotspot_update_count", "aggregated_update_count", "constraint_check_count", "constraint_failed_count", "avg_commit_latency_ms", "p95_commit_latency_ms"], expected_artifacts: ["state_commit_log.csv", "state_access_log.csv", "execution_log.csv", "routing_log.csv", "summary.csv", "summary.json", "block_log.csv", "tx_results.csv"], result_guide: "Focus on aggregation ratio, hotspot updates, constraint checks, commit latency, and state_commit_log.csv.", truthfulness_note: "This is a local deterministic light model, not real database locking, concurrent commit, or persistent state-tree validation." }] },
  { template_id: "committee_lifecycle_planned", stage: "V3.3.2", chain_mode: "single_chain", runnable: false, preview_only: true, description: "Committee lifecycle preview", variable_modules: ["CommitteeEpoch", "Consensus"], fixed_modules: [], disabled_modules: [], planned_modules: ["CommitteeEpoch"], output_modules: ["MetricsReport"] },
];

const profileOptions = [
  { id: "metatrack_go_backed_ablation_smoke", label: "MetaTrack Go-backed 消融 Smoke" },
  { id: "single_chain_role_separation_smoke", label: "单链角色拆分 Smoke" },
  { id: "single_chain_composer_preview", label: "单链 Composer 预览" },
];

function moduleStatusForLockedPlugin(moduleId: string, plugin: string): V3DraftModuleStatus {
  if (moduleId === "MetricsReport") return "output";
  if (moduleId === "CommitteeEpoch" || plugin === "disabled") return "disabled";
  return "fixed";
}

type Props = {
  onRunCompleted?: (runId: string) => void;
};

export default function V3ComposerPage({ onRunCompleted }: Props) {
  const [profileId, setProfileId] = useState("metatrack_go_backed_ablation_smoke");
  const [preview, setPreview] = useState<V3ComposerPreviewResponse | null>(null);
  const [templates, setTemplates] = useState<V3TemplateSummary[]>(fallbackTemplates);
  const [artifacts, setArtifacts] = useState<V2Artifact[]>([]);
  const [draftRunResult, setDraftRunResult] = useState<V3DraftSmokeRunResponse | null>(null);
  const [controlledSmokeResult, setControlledSmokeResult] = useState<V3ControlledSmokeRunResponse | null>(null);
  const [draft, setDraft] = useState<ComposerDraft | null>(null);
  const [backendValidation, setBackendValidation] = useState<V3DraftValidationResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [running, setRunning] = useState(false);
  const [validatingDraft, setValidatingDraft] = useState(false);
  const [runningDraft, setRunningDraft] = useState(false);
  const [runningControlled, setRunningControlled] = useState(false);
  const [error, setError] = useState("");
  const [draftError, setDraftError] = useState("");

  useEffect(() => { void loadPreview(profileId); }, [profileId]);

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
      setTemplates(nextTemplates);
      setError("");
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setLoading(false);
    }
  }

  async function runSmoke() {
    try {
      setRunning(true);
      const result = await runV3ComposerSmoke();
      setArtifacts(result.artifacts || []);
      setDraftRunResult(null);
      if (result.run_id) onRunCompleted?.(result.run_id);
      setError("");
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setRunning(false);
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
    const variableModule = template.variable_module || "";
    const presetId = template.default_preset_id || template.presets?.[0]?.preset_id || "";
    const preset = template.presets?.find((item) => item.preset_id === presetId);
    const allowedPlugins = template.allowed_variable_plugins || [];
    const lockedModules = preset?.locked_modules || template.locked_modules || {};
    const defaultSelection = preset?.default_plugin_selection || {};
    const controlledModules = new Set([...(template.variable_modules || []), ...(preset?.controlled_modules || [])]);
    const nextModules = Object.fromEntries(
      Object.entries(draft.modules).map(([moduleId, module]) => {
        if (defaultSelection[moduleId]) {
          const nextStatus: V3DraftModuleStatus = controlledModules.has(moduleId) ? "variable" : moduleStatusForLockedPlugin(moduleId, defaultSelection[moduleId]);
          return [moduleId, { ...module, status: nextStatus, plugin: defaultSelection[moduleId], runnable: true }];
        }
        if (moduleId === variableModule) {
          const nextPlugin = allowedPlugins.includes(module.plugin) ? module.plugin : (allowedPlugins[0] || module.plugin);
          return [moduleId, { ...module, status: "variable" as const, plugin: nextPlugin, runnable: true }];
        }
        if (lockedModules[moduleId]) {
          const nextStatus = moduleStatusForLockedPlugin(moduleId, lockedModules[moduleId]);
          return [moduleId, { ...module, status: nextStatus as typeof module.status, plugin: lockedModules[moduleId], runnable: true }];
        }
        return [moduleId, module];
      }),
    );
    updateDraft(summarizeDraft({ templateId, presetId, modules: nextModules, topology: draft.topology }));
  }

  function selectPreset(presetId: string) {
    if (!draft) return;
    const template = templates.find((item) => item.template_id === draft.templateId);
    const preset = template?.presets?.find((item) => item.preset_id === presetId);
    if (!template || !preset) {
      updateDraft(summarizeDraft({ templateId: draft.templateId, presetId, modules: draft.modules, topology: draft.topology }));
      return;
    }
    const lockedModules = preset.locked_modules || template.locked_modules || {};
    const defaultSelection = preset.default_plugin_selection || {};
    const controlledModules = new Set([...(template.variable_modules || []), ...(preset.controlled_modules || [])]);
    const nextModules = Object.fromEntries(
      Object.entries(draft.modules).map(([moduleId, module]) => {
        if (defaultSelection[moduleId]) {
          const nextStatus: V3DraftModuleStatus = controlledModules.has(moduleId) ? "variable" : moduleStatusForLockedPlugin(moduleId, defaultSelection[moduleId]);
          return [moduleId, { ...module, status: nextStatus, plugin: defaultSelection[moduleId], runnable: true }];
        }
        if (lockedModules[moduleId]) {
          return [moduleId, { ...module, status: moduleStatusForLockedPlugin(moduleId, lockedModules[moduleId]), plugin: lockedModules[moduleId], runnable: true }];
        }
        return [moduleId, module];
      }),
    );
    updateDraft(summarizeDraft({ templateId: draft.templateId, presetId, modules: nextModules, topology: draft.topology }));
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

  async function runDraftSmoke() {
    if (!draft) return;
    try {
      setRunningDraft(true);
      const validation = backendValidation?.is_runnable ? backendValidation : await validateDraftOnServer();
      if (!validation?.is_runnable) {
        setDraftError("后端 Draft 校验未通过，不能运行当前 Draft Smoke。");
        return;
      }
      const result = await runV3ComposerDraftSmoke(toComposerDraftRequest(draft));
      setBackendValidation(result.validation);
      setArtifacts(result.artifacts || []);
      setDraftRunResult(result);
      if (result.run_id) onRunCompleted?.(result.run_id);
      setDraftError("");
      setError("");
    } catch (caught) {
      setDraftError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setRunningDraft(false);
    }
  }

  async function runControlledSmoke() {
    try {
      setRunningControlled(true);
      const result = await runV3ControlledSmoke();
      setControlledSmokeResult(result);
      setArtifacts(result.artifacts || []);
      setDraftRunResult(null);
      if (result.run_id) onRunCompleted?.(result.run_id);
      setError("");
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setRunningControlled(false);
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
  const selectedLockedModules = selectedPreset?.locked_modules || selectedTemplate?.locked_modules || {};
  const selectedVariableModules = selectedPreset?.controlled_modules || (selectedTemplate?.variable_module ? [selectedTemplate.variable_module] : selectedTemplate?.variable_modules || []);
  const identitySummary = [
    labelFor(profileLabels, preview?.experiment_profile_id || profileId),
    preview?.current_stage || preview?.stage || "-",
    composer?.truth_labels?.backend_type || String(profilePreview.backend_type || "-"),
    composer?.truth_labels?.truth_label || String(profilePreview.truth_label || "-"),
    composer?.chain_mode === "single_chain" ? "单链" : composer?.chain_mode || "-",
    yesNo(preview?.runnable && composer?.runnable),
  ].join(" · ");

  return (
    <section className="page-grid v3-composer-page">
      <header className="final-card wide v3-composer-header v3-compact-header">
        <div>
          <p className="eyebrow">{currentStageLabel}</p>
          <h2>V3 Composer · {latestRuntimeLabel}</h2>
          <p>{runtimeBoundaryText}</p>
        </div>
        <div className="v3-boundary-badges">
          <span>单链</span>
          <span>Go Runtime</span>
          <span>Smoke 实验</span>
          <span>FIFO TxPool</span>
          <span>Time/Count Producer</span>
          <span>Consensus-light</span>
          <span>PoA/PBFT-light</span>
          <span>Execution-light</span>
          <span>StateAccess-light</span>
          <span>Commit-light</span>
          <span>非 Fabric</span>
          <span>非 MetaFlow</span>
          <span>非 real PBFT</span>
          <span>非 HotStuff/Raft</span>
          <span>非真实并发/rollback</span>
          <span>非 proof/witness/MPT</span>
          <span>非真实DB锁/state root</span>
          <span>非多节点网络</span>
        </div>
      </header>

      <section className="final-card wide v3-template-bar">
        <label>
          <span>实验模板</span>
          <select value={profileId} onChange={(event) => setProfileId(event.target.value)}>
            {profileOptions.map((item) => <option key={item.id} value={item.id}>{item.label}</option>)}
          </select>
        </label>
        <p>当前模板用于验证 MetaTrack 在分片/路由、交易执行、状态访问、状态提交四类模块上的消融组合。</p>
        <details className="v3-foldout">
          <summary className="v3-foldout-summary">实验身份详情</summary>
          <p className="v3-identity-summary">{identitySummary}</p>
          <dl className="v3-identity-grid">
            <div><dt>实验模板</dt><dd>{labelFor(templateLabels, composer?.template_id || preview?.experiment_template || "-")}</dd></div>
            <div><dt>current stage</dt><dd>{preview?.current_stage || preview?.stage || "-"}</dd></div>
            <div><dt>latest runtime</dt><dd>{preview?.latest_runtime_stage || "-"}</dd></div>
            <div><dt>next stage</dt><dd>{preview?.next_stage || "-"}</dd></div>
            <div><dt>后端类型</dt><dd>{composer?.truth_labels?.backend_type || String(profilePreview.backend_type || "-")}</dd></div>
            <div><dt>真实性标签</dt><dd>{composer?.truth_labels?.truth_label || String(profilePreview.truth_label || "-")}</dd></div>
            <div><dt>是否可运行</dt><dd>{yesNo(preview?.runnable && composer?.runnable)}</dd></div>
            <div><dt>链模式</dt><dd>{composer?.chain_mode === "single_chain" ? "单链" : composer?.chain_mode || "-"}</dd></div>
          </dl>
        </details>
      </section>

      {draft && (
        <section className="final-card wide v3-template-bar">
          <label>
            <span>Single-module template</span>
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
              {selectedTemplate.presets && selectedTemplate.presets.length > 0 && (
                <label>
                  <span>Smoke preset</span>
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
                <div><dt>variable module</dt><dd>{selectedTemplate.variable_module || selectedTemplate.variable_modules?.join(", ") || "-"}</dd></div>
                <div><dt>allowed plugins</dt><dd>{(selectedTemplate.allowed_variable_plugins || []).join(", ") || "-"}</dd></div>
                <div><dt>preset</dt><dd>{selectedPreset?.preset_id || selectedTemplate.default_preset_id || "legacy/default smoke"}</dd></div>
                <div><dt>ablation stage</dt><dd>{selectedPreset?.ablation_stage || "-"}</dd></div>
                <div><dt>enabled components</dt><dd>{(selectedPreset?.enabled_metatrack_components || []).join(", ") || "baseline / none"}</dd></div>
                <div><dt>fairness rule</dt><dd>{selectedTemplate.fairness_rule || "Only configured variable modules may differ."}</dd></div>
              </dl>
              {selectedPreset && (
                <details className="v3-foldout">
                  <summary className="v3-foldout-summary">Preset result guide</summary>
                  <p className="muted">{selectedPreset.result_guide || selectedPreset.description}</p>
                  <dl className="v3-identity-grid">
                    <div><dt>primary metrics</dt><dd>{(selectedPreset.primary_metrics || []).join(", ") || "-"}</dd></div>
                    <div><dt>expected artifacts</dt><dd>{(selectedPreset.expected_artifacts || []).join(", ") || "-"}</dd></div>
                  </dl>
                </details>
              )}
              {Object.keys(selectedLockedModules).length > 0 && (
                <details className="v3-foldout">
                  <summary className="v3-foldout-summary">Locked modules</summary>
                  <ul className="v3-check-list compact">
                    {Object.entries(selectedLockedModules).map(([moduleId, plugin]) => (
                      <li key={moduleId}><span>{moduleId}</span> <code>{plugin}</code></li>
                    ))}
                  </ul>
                </details>
              )}
              {selectedTemplate.truthfulness_note && <p className="muted">{selectedTemplate.truthfulness_note}</p>}
              {selectedTemplate.template_id === "metatrack_ablation" && <p className="muted">{templateStageNote}</p>}
            </div>
          )}
        </section>
      )}

      {draft && (
        <RuntimeTopologyPanel
          topology={draft.topology}
          onChange={(topology) => updateDraft(updateDraftTopology(draft, topology))}
        />
      )}

      {loading && <p className="notice">正在加载 V3 Composer 预览...</p>}
      {error && <p className="file-error">{error}</p>}
      {composer && draft && (
        <SingleChainComposer
          preview={composer}
          draft={draft}
          onDraftChange={updateDraft}
          variableModule={selectedTemplate?.variable_module}
          variableModules={selectedVariableModules}
          lockedModules={selectedLockedModules}
        />
      )}

      <div className="final-card-grid v3-post-workbench">
        <FairnessScopePanel scope={composer?.fairness_scope || preview?.fairness_scope || {}} valid={Boolean(profilePreview.valid ?? preview?.runnable)} warnings={warnings} draft={draft} />
        <RunLevelPanel
          runnable={Boolean(preview?.runnable && composer?.runnable)}
          running={running}
          onRunSmoke={runSmoke}
          draft={draft}
          backendValidation={backendValidation}
          validatingDraft={validatingDraft}
          runningDraft={runningDraft}
          draftError={draftError}
          onValidateDraft={validateDraftOnServer}
          onRunDraftSmoke={runDraftSmoke}
        />
      </div>
      <section className="final-card wide v3-controlled-smoke">
        <div className="v3-section-head">
          <div>
            <p className="eyebrow">V3.4.10 Controlled Smoke</p>
            <h3>MetaTrack controlled preset comparison</h3>
          </div>
          <button type="button" className="v3-secondary-button" disabled={runningControlled} onClick={runControlledSmoke}>
            {runningControlled ? "Running..." : "Run controlled smoke"}
          </button>
        </div>
        <p className="muted">{controlledSmokeDescription}</p>
        <p className="muted">Boundary: local Go-backed modular research chain Draft Smoke only; not Fabric live, not EVM live, not BlockEmulator backend, not real PBFT/HotStuff/Raft, not a real cross-shard protocol, and not paper-grade evidence.</p>
        {controlledSmokeResult && (
          <>
            <dl className="v3-result-grid">
              <div><dt>run_id</dt><dd>{controlledSmokeResult.run_id}</dd></div>
              <div><dt>run_mode</dt><dd>{controlledSmokeResult.run_mode}</dd></div>
              <div><dt>preset_count</dt><dd>{controlledSmokeResult.preset_order.length}</dd></div>
              <div><dt>backend_truth</dt><dd>{controlledSmokeResult.realism_readiness?.backend_truth || "local Go-backed Draft Smoke"}</dd></div>
            </dl>
            <div className="v3-summary-preview">
              {controlledSmokeResult.aggregate_summary.map((row) => (
                <div key={String(row.preset_id)}>
                  <dt>{String(row.preset_id)}</dt>
                  <dd>{String(row.ablation_stage || "-")} / cross {String(row.cross_shard_ratio ?? "-")} / exec {String(row.avg_execution_latency_ms ?? "-")} / state {String(row.avg_state_access_latency_ms ?? "-")} / commit {String(row.avg_commit_latency_ms ?? "-")}</dd>
                </div>
              ))}
            </div>
            <div className="v3-summary-preview">
              {(controlledSmokeResult.realism_readiness?.modules || []).map((module) => (
                <div key={String(module.module_id)}>
                  <dt>{String(module.module_id)}</dt>
                  <dd>{String(module.realism_level || "-")} / {String(module.runtime_status || "-")}</dd>
                </div>
              ))}
            </div>
            <ArtifactGroups
              artifacts={controlledSmokeResult.artifacts}
              title="Controlled smoke artifacts"
              expectedArtifacts={["run_index.csv", "aggregate_summary.csv", "ablation_report.md", "realism_readiness.json", "realism_readiness.md"]}
              defaultOpen
            />
          </>
        )}
      </section>
      <DraftRunResultPanel result={draftRunResult} />
      <DraftRunHistoryPanel />

      {composer && (
        <section className="v3-supporting-sections">
          <PluginMatrixTable rows={composer.plugin_matrix || preview?.plugin_matrix || []} />
          <details className="final-card wide v3-foldout">
            <summary className="v3-foldout-summary">实验模板列表</summary>
            <p className="muted">可用模板保留在此处用于核对，主流程只使用上方下拉框。</p>
            <div className="v3-template-list">
              {templates.map((template) => (
                <div key={template.template_id} className="v3-template-row">
                  <span><strong>{labelFor(templateLabels, template.template_id)}</strong><small>{template.template_id}</small></span>
                  <span className={`v3-status-badge status-${template.runnable ? "variable" : "planned"}`}>
                    {template.runnable ? "可运行" : template.preview_only ? "仅预览" : "规划中"}
                  </span>
                </div>
              ))}
            </div>
            {selectedTemplate && <p className="muted">{selectedTemplate.description}</p>}
          </details>
        </section>
      )}
      <ArtifactGroups
        artifacts={artifacts}
        title="Current run artifacts and downloads"
        expectedArtifacts={selectedPreset?.expected_artifacts}
      />
    </section>
  );
}
