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

const fallbackTemplates: V3TemplateSummary[] = [
  { template_id: "metatrack_ablation", stage: "V3.3.2", chain_mode: "single_chain", runnable: true, preview_only: false, description: "MetaTrack ablation", variable_modules: ["Routing", "Execution", "StateAccess", "Commit"], fixed_modules: ["Workload", "TxPool", "BlockProducer", "Consensus", "StateStorage"], disabled_modules: [], planned_modules: ["CommitteeEpoch"], output_modules: ["MetricsReport"] },
  { template_id: "committee_lifecycle_planned", stage: "V3.3.2", chain_mode: "single_chain", runnable: false, preview_only: true, description: "Committee lifecycle preview", variable_modules: ["CommitteeEpoch", "Consensus"], fixed_modules: [], disabled_modules: [], planned_modules: ["CommitteeEpoch"], output_modules: ["MetricsReport"] },
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
          <p className="eyebrow">V3.3.3 Single-chain Modular Composer MVP</p>
          <h2>Single-chain modular research chain</h2>
          <p>Read-only Composer View backed by V3.3.2 profile preview data.</p>
        </div>
        <div className="v3-boundary-badges">
          <span>Single-chain</span>
          <span>Go-backed Runtime</span>
          <span>Smoke</span>
          <span>Not Fabric</span>
          <span>Not MetaFlow</span>
        </div>
      </header>

      <ExperimentTemplateSelector templates={templates} selectedProfile={profileId} onProfileChange={setProfileId} />
      <section className="final-card">
        <p className="eyebrow">Experiment Identity</p>
        <h3>{preview?.experiment_profile_id || profileId}</h3>
        <dl className="v3-identity-grid">
          <div><dt>Template</dt><dd>{composer?.template_id || preview?.experiment_template || "-"}</dd></div>
          <div><dt>Stage</dt><dd>{preview?.stage || "-"}</dd></div>
          <div><dt>Backend Type</dt><dd>{composer?.truth_labels?.backend_type || String(profilePreview.backend_type || "-")}</dd></div>
          <div><dt>Truth Label</dt><dd>{composer?.truth_labels?.truth_label || String(profilePreview.truth_label || "-")}</dd></div>
          <div><dt>Runnable</dt><dd>{String(Boolean(preview?.runnable && composer?.runnable))}</dd></div>
          <div><dt>Chain Mode</dt><dd>{composer?.chain_mode || "-"}</dd></div>
        </dl>
      </section>

      {loading && <p className="notice">Loading V3 composer preview...</p>}
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
