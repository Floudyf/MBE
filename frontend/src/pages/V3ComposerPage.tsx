import { useEffect, useMemo, useState } from "react";

import {
  fetchV3ComposerPreview,
  fetchV3ComposerTemplates,
  runV3ComposerDraftSmoke,
  runV3ComposerSmoke,
  validateV3ComposerDraft,
  type V2Artifact,
  type V3ComposerPreviewResponse,
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
import SingleChainComposer from "../components/v3/SingleChainComposer";
import { createComposerDraft, toComposerDraftRequest, type ComposerDraft } from "../components/v3/composerDraft";
import { labelFor, profileLabels, templateLabels, yesNo } from "../components/v3/localization";

const fallbackTemplates: V3TemplateSummary[] = [
  { template_id: "metatrack_ablation", stage: "V3.3.2", chain_mode: "single_chain", runnable: true, preview_only: false, description: "MetaTrack single-chain ablation", variable_modules: ["Routing", "Execution", "StateAccess", "Commit"], fixed_modules: ["Workload", "TxPool", "BlockProducer", "Consensus", "StateStorage"], disabled_modules: [], planned_modules: ["CommitteeEpoch"], output_modules: ["MetricsReport"] },
  { template_id: "committee_lifecycle_planned", stage: "V3.3.2", chain_mode: "single_chain", runnable: false, preview_only: true, description: "Committee lifecycle preview", variable_modules: ["CommitteeEpoch", "Consensus"], fixed_modules: [], disabled_modules: [], planned_modules: ["CommitteeEpoch"], output_modules: ["MetricsReport"] },
];

const profileOptions = [
  { id: "metatrack_go_backed_ablation_smoke", label: "MetaTrack Go-backed 消融 Smoke" },
  { id: "single_chain_role_separation_smoke", label: "单链角色拆分 Smoke" },
  { id: "single_chain_composer_preview", label: "单链 Composer 预览" },
];

type Props = {
  onRunCompleted?: (runId: string) => void;
};

export default function V3ComposerPage({ onRunCompleted }: Props) {
  const [profileId, setProfileId] = useState("metatrack_go_backed_ablation_smoke");
  const [preview, setPreview] = useState<V3ComposerPreviewResponse | null>(null);
  const [templates, setTemplates] = useState<V3TemplateSummary[]>(fallbackTemplates);
  const [artifacts, setArtifacts] = useState<V2Artifact[]>([]);
  const [draftRunResult, setDraftRunResult] = useState<V3DraftSmokeRunResponse | null>(null);
  const [draft, setDraft] = useState<ComposerDraft | null>(null);
  const [backendValidation, setBackendValidation] = useState<V3DraftValidationResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [running, setRunning] = useState(false);
  const [validatingDraft, setValidatingDraft] = useState(false);
  const [runningDraft, setRunningDraft] = useState(false);
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

  const composer = preview?.composer_preview;
  const profilePreview = preview?.profile_preview || {};
  const warnings = useMemo(() => {
    const value = profilePreview.warnings;
    return Array.isArray(value) ? value.map(String) : [];
  }, [profilePreview]);
  const selectedTemplate = useMemo(
    () => templates.find((template) => template.template_id === (composer?.template_id || preview?.experiment_template)),
    [composer?.template_id, preview?.experiment_template, templates],
  );
  const identitySummary = [
    labelFor(profileLabels, preview?.experiment_profile_id || profileId),
    preview?.stage || "-",
    composer?.truth_labels?.backend_type || String(profilePreview.backend_type || "-"),
    composer?.truth_labels?.truth_label || String(profilePreview.truth_label || "-"),
    composer?.chain_mode === "single_chain" ? "单链" : composer?.chain_mode || "-",
    yesNo(preview?.runnable && composer?.runnable),
  ].join(" · ");

  return (
    <section className="page-grid v3-composer-page">
      <header className="final-card wide v3-composer-header v3-compact-header">
        <div>
          <p className="eyebrow">V3.3.5a Composer Draft</p>
          <h2>V3 单链 Composer</h2>
          <p>选择模板，点击模块，配置插件与实验变量，查看 Draft 校验，然后运行已有 Smoke。</p>
        </div>
        <div className="v3-boundary-badges">
          <span>单链</span>
          <span>Go Runtime</span>
          <span>Smoke 实验</span>
          <span>FIFO TxPool</span>
          <span>Time/Count Producer</span>
          <span>Consensus-light</span>
          <span>PoA/PBFT-light</span>
          <span>非 Fabric</span>
          <span>非 MetaFlow</span>
          <span>非 real PBFT</span>
          <span>非 HotStuff/Raft</span>
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
            <div><dt>阶段</dt><dd>{preview?.stage || "-"}</dd></div>
            <div><dt>后端类型</dt><dd>{composer?.truth_labels?.backend_type || String(profilePreview.backend_type || "-")}</dd></div>
            <div><dt>真实性标签</dt><dd>{composer?.truth_labels?.truth_label || String(profilePreview.truth_label || "-")}</dd></div>
            <div><dt>是否可运行</dt><dd>{yesNo(preview?.runnable && composer?.runnable)}</dd></div>
            <div><dt>链模式</dt><dd>{composer?.chain_mode === "single_chain" ? "单链" : composer?.chain_mode || "-"}</dd></div>
          </dl>
        </details>
      </section>

      {loading && <p className="notice">正在加载 V3 Composer 预览...</p>}
      {error && <p className="file-error">{error}</p>}
      {composer && draft && <SingleChainComposer preview={composer} draft={draft} onDraftChange={updateDraft} />}

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
      <ArtifactGroups artifacts={artifacts} title="Current run artifacts and downloads" />
    </section>
  );
}
