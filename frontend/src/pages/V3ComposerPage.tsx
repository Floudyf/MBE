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
import HelpTip from "../components/v3/HelpTip";
import PluginMatrixTable from "../components/v3/PluginMatrixTable";
import RunLevelPanel from "../components/v3/RunLevelPanel";
import RunProgressPanel, { type RunProgressMode } from "../components/v3/RunProgressPanel";
import RuntimeTopologyPanel from "../components/v3/RuntimeTopologyPanel";
import SingleChainComposer from "../components/v3/SingleChainComposer";
import { createComposerDraft, summarizeDraft, toComposerDraftRequest, updateDraftTopology, type ComposerDraft } from "../components/v3/composerDraft";
import { labelFor, profileLabels, templateLabels, yesNo } from "../components/v3/localization";

const fallbackStageMetadata = {
  currentStage: "V3-final Fault, Observability, and Reproducibility Closure",
  latestRuntime: "deterministic fault injection MVP, observability summary, final artifact catalog, reproducibility guide, experiment manual, and paper experiment mapping",
  runtimeTruth: "v3_final_emulator_closure_not_production_system",
  nextStage: "V3 maintenance only; do not start V4 unless explicitly requested",
};
const controlledSmokeDescription = "按固定顺序运行五组 MetaTrack 快速验证预设。Workload、seed、TxPool、BlockProducer、Consensus、CommitteeEpoch、StateStorage、MetricsReport 保持固定，只改变 Routing、Execution、StateAccess、Commit。输出 aggregate summary 与 realism readiness；这是快速验证级别的受控对照。";

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

type Props = {
  onRunCompleted?: (runId: string) => void;
};

function moduleStatusForLockedPlugin(moduleId: string, plugin: string): V3DraftModuleStatus {
  if (moduleId === "MetricsReport") return "output";
  if (moduleId === "CommitteeEpoch" || plugin === "disabled") return "disabled";
  return "fixed";
}

export default function V3ComposerPage({ onRunCompleted }: Props) {
  const [profileId, setProfileId] = useState("metatrack_go_backed_ablation_smoke");
  const [preview, setPreview] = useState<V3ComposerPreviewResponse | null>(null);
  const [templates, setTemplates] = useState<V3TemplateSummary[]>(fallbackTemplates);
  const [artifacts, setArtifacts] = useState<V2Artifact[]>([]);
  const [draftRunResult, setDraftRunResult] = useState<V3DraftSmokeRunResponse | null>(null);
  const [controlledResult, setControlledResult] = useState<V3ControlledSmokeRunResponse | null>(null);
  const [draft, setDraft] = useState<ComposerDraft | null>(null);
  const [backendValidation, setBackendValidation] = useState<V3DraftValidationResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [runningBuiltin, setRunningBuiltin] = useState(false);
  const [validatingDraft, setValidatingDraft] = useState(false);
  const [runningDraft, setRunningDraft] = useState(false);
  const [runningControlled, setRunningControlled] = useState(false);
  const [progressMode, setProgressMode] = useState<RunProgressMode>("idle");
  const [progressStep, setProgressStep] = useState(0);
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
      setTemplates(nextTemplates.length ? nextTemplates : fallbackTemplates);
      setError("");
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setLoading(false);
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
    const variableModule = template.variable_module || "";
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
  const stageLabel = preview?.current_stage || preview?.stage || fallbackStageMetadata.currentStage;
  const latestRuntimeLabel = preview?.latest_runtime_stage || preview?.latest_completed_runtime_stage || fallbackStageMetadata.latestRuntime;
  const runtimeTruthLabel = preview?.runtime_truth || fallbackStageMetadata.runtimeTruth;
  const nextStageLabel = preview?.next_stage || fallbackStageMetadata.nextStage;

  return (
    <section className="page-grid v3-composer-page">
      <header className="final-card wide v3-composer-header console-hero">
        <div>
          <p className="eyebrow">当前阶段：{stageLabel}</p>
          <h2>MBE V3 实验控制台</h2>
          <p>{`运行能力：${latestRuntimeLabel}。`} <HelpTip title="真实性边界">V3-final 是本地 emulator 闭环：确定性故障注入、观测摘要和复现 bundle；不是多服务器部署、生产集群、生产共识网络或论文级性能结论。</HelpTip></p>
        </div>
        <div className="v3-boundary-badges">
          <span>本地快速验证</span>
          <span>中文控制台</span>
          <span>HelpTip 解释</span>
          <span>轻量图表</span>
          <span>V3-final closure</span>
        </div>
      </header>

      <section className="final-card wide console-section-title">
        <p className="eyebrow">1. 当前实验概览</p>
        <h3>用清晰控制台组织 V3.5-V3.10 已有能力</h3>
        <p className="muted">运行拓扑、网络通信、共识预览、跨片 skeleton、状态真实性和 Benchmark 都保留原有语义。这里提供更简洁的中文配置、运行和结果展示。</p>
      </section>

      <section className="final-card wide v3-template-bar">
        <label>
          <span>实验入口模板</span>
          <select value={profileId} onChange={(event) => setProfileId(event.target.value)}>
            {profileOptions.map((item) => <option key={item.id} value={item.id}>{item.label}</option>)}
          </select>
        </label>
        <p>入口模板决定 Composer 初始配置；Benchmark 模板、对照基线和重复次数在下方“实验控制”中选择。</p>
        <details className="v3-foldout">
          <summary className="v3-foldout-summary">实验身份详情</summary>
          <dl className="v3-identity-grid">
            <div><dt>实验模板</dt><dd>{labelFor(templateLabels, composer?.template_id || preview?.experiment_template || "-")}</dd></div>
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
          <p className="eyebrow">2. 实验配置</p>
          <h3>模板、拓扑、协议和实验控制</h3>
        </section>
      )}

      {draft && (
        <section className="final-card wide v3-template-bar">
          <label>
            <span>模块实验模板</span>
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
                  <span>快速验证预设 <HelpTip title="快速验证">小规模试运行，用于确认配置和产物是否正常，不代表论文级正式实验。</HelpTip></span>
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
              {Object.keys(selectedLockedModules).length > 0 && (
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

      {draft && <RuntimeTopologyPanel topology={draft.topology} onChange={(topology) => updateDraft(updateDraftTopology(draft, topology))} />}

      {loading && <p className="notice">正在加载 V3 Composer 预览...</p>}
      {error && <p className="file-error">{error}</p>}

      {composer && draft && (
        <>
          <section className="final-card wide console-section-title">
            <p className="eyebrow">3. 模块流程</p>
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
          />
        </>
      )}

      <section className="final-card wide console-section-title">
        <p className="eyebrow">4. 运行与结果</p>
        <h3>先看进度与核心指标，再展开详细数据</h3>
      </section>

      <div className="final-card-grid v3-post-workbench">
        <FairnessScopePanel scope={composer?.fairness_scope || preview?.fairness_scope || {}} valid={Boolean(profilePreview.valid ?? preview?.runnable)} warnings={warnings} draft={draft} />
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
      </div>

      <RunProgressPanel mode={progressMode} activeStep={progressStep} error={draftError || error} />

      <section className="final-card wide v3-controlled-smoke">
        <div className="v3-section-head">
          <div>
            <p className="eyebrow">受控对照试运行</p>
            <h3>MetaTrack 五组快速验证预设</h3>
          </div>
          <button type="button" className="v3-secondary-button" disabled={runningControlled} onClick={runControlledTrial}>
            {runningControlled ? "运行中..." : "运行受控对照试验"}
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
              <div><dt>运行模式</dt><dd>受控对照试运行</dd></div>
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

      <DraftRunResultPanel result={draftRunResult} />
      <DraftRunHistoryPanel />

      {composer && (
        <section className="v3-supporting-sections">
          <details className="final-card wide v3-foldout">
            <summary className="v3-foldout-summary">插件矩阵与开发者详情</summary>
            <PluginMatrixTable rows={composer.plugin_matrix || preview?.plugin_matrix || []} />
          </details>
          <details className="final-card wide v3-foldout">
            <summary className="v3-foldout-summary">实验模板列表</summary>
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

      <ArtifactGroups artifacts={artifacts} title="当前运行产物" expectedArtifacts={selectedPreset?.expected_artifacts} />
    </section>
  );
}
