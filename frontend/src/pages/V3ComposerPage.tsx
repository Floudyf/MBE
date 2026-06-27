import { useEffect, useMemo, useState } from "react";

import {
  fetchV3ComposerPreview,
  fetchV3ComposerTemplates,
  runV3ComposerSmoke,
  type V2Artifact,
  type V3ComposerPreviewResponse,
  type V3TemplateSummary,
} from "../api";
import ArtifactGroups from "../components/v3/ArtifactGroups";
import ExperimentTemplateSelector from "../components/v3/ExperimentTemplateSelector";
import FairnessScopePanel from "../components/v3/FairnessScopePanel";
import PluginMatrixTable from "../components/v3/PluginMatrixTable";
import RunLevelPanel from "../components/v3/RunLevelPanel";
import SingleChainComposer from "../components/v3/SingleChainComposer";
import { labelFor, profileLabels, templateLabels, yesNo } from "../components/v3/localization";

const fallbackTemplates: V3TemplateSummary[] = [
  { template_id: "metatrack_ablation", stage: "V3.3.2", chain_mode: "single_chain", runnable: true, preview_only: false, description: "MetaTrack 消融实验", variable_modules: ["Routing", "Execution", "StateAccess", "Commit"], fixed_modules: ["Workload", "TxPool", "BlockProducer", "Consensus", "StateStorage"], disabled_modules: [], planned_modules: ["CommitteeEpoch"], output_modules: ["MetricsReport"] },
  { template_id: "committee_lifecycle_planned", stage: "V3.3.2", chain_mode: "single_chain", runnable: false, preview_only: true, description: "委员会生命周期预览", variable_modules: ["CommitteeEpoch", "Consensus"], fixed_modules: [], disabled_modules: [], planned_modules: ["CommitteeEpoch"], output_modules: ["MetricsReport"] },
];

type Props = {
  onRunCompleted?: (runId: string) => void;
};

export default function V3ComposerPage({ onRunCompleted }: Props) {
  const [profileId, setProfileId] = useState("metatrack_go_backed_ablation_smoke");
  const [preview, setPreview] = useState<V3ComposerPreviewResponse | null>(null);
  const [templates, setTemplates] = useState<V3TemplateSummary[]>(fallbackTemplates);
  const [artifacts, setArtifacts] = useState<V2Artifact[]>([]);
  const [loading, setLoading] = useState(false);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => { void loadPreview(profileId); }, [profileId]);

  async function loadPreview(nextProfileId: string) {
    try {
      setLoading(true);
      const [nextPreview, nextTemplates] = await Promise.all([
        fetchV3ComposerPreview(nextProfileId),
        fetchV3ComposerTemplates().catch(() => fallbackTemplates),
      ]);
      setPreview(nextPreview);
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
      if (result.run_id) onRunCompleted?.(result.run_id);
      setError("");
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setRunning(false);
    }
  }

  const composer = preview?.composer_preview;
  const profilePreview = preview?.profile_preview || {};
  const warnings = useMemo(() => {
    const value = profilePreview.warnings;
    return Array.isArray(value) ? value.map(String) : [];
  }, [profilePreview]);

  return (
    <section className="page-grid v3-composer-page">
      <header className="final-card wide v3-composer-header">
        <div>
          <p className="eyebrow">V3.3.4 Composer 中文化与布局精修</p>
          <h2>V3 单链模块化实验台</h2>
          <p>基于 V3.3.2 profile preview 数据渲染的只读 Composer 视图。</p>
        </div>
        <div className="v3-boundary-badges">
          <span>单链</span>
          <span>Go Runtime</span>
          <span>Smoke 实验</span>
          <span>非 Fabric</span>
          <span>非 MetaFlow</span>
        </div>
      </header>

      <ExperimentTemplateSelector templates={templates} selectedProfile={profileId} onProfileChange={setProfileId} />
      <section className="final-card">
        <p className="eyebrow">实验身份</p>
        <h3>{labelFor(profileLabels, preview?.experiment_profile_id || profileId)}</h3>
        <p className="v3-sub-id">{preview?.experiment_profile_id || profileId}</p>
        <dl className="v3-identity-grid">
          <div><dt>实验模板</dt><dd>{labelFor(templateLabels, composer?.template_id || preview?.experiment_template || "-")}</dd></div>
          <div><dt>阶段</dt><dd>{preview?.stage || "-"}</dd></div>
          <div><dt>后端类型</dt><dd>{composer?.truth_labels?.backend_type || String(profilePreview.backend_type || "-")}</dd></div>
          <div><dt>真实性标签</dt><dd>{composer?.truth_labels?.truth_label || String(profilePreview.truth_label || "-")}</dd></div>
          <div><dt>是否可运行</dt><dd>{yesNo(preview?.runnable && composer?.runnable)}</dd></div>
          <div><dt>链模式</dt><dd>{composer?.chain_mode === "single_chain" ? "单链" : composer?.chain_mode || "-"}</dd></div>
        </dl>
      </section>

      {loading && <p className="notice">正在加载 V3 Composer 预览...</p>}
      {error && <p className="file-error">{error}</p>}
      {composer && <SingleChainComposer preview={composer} />}
      {composer && <PluginMatrixTable rows={composer.plugin_matrix || preview?.plugin_matrix || []} />}
      <div className="final-card-grid">
        <FairnessScopePanel scope={composer?.fairness_scope || preview?.fairness_scope || {}} valid={Boolean(profilePreview.valid ?? preview?.runnable)} warnings={warnings} />
        <RunLevelPanel runnable={Boolean(preview?.runnable && composer?.runnable)} running={running} onRunSmoke={runSmoke} />
      </div>
      <ArtifactGroups artifacts={artifacts} />
    </section>
  );
}
