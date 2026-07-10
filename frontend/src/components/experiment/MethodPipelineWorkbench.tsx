import { useEffect, useMemo, useState, type ReactNode } from "react";

import type { V3ComposerModule, V3ComposerPreview, V3DraftValidationResponse, V3SavedConfig, V3TemplateSummary } from "../../api";
import ModuleDetailPanel from "../v3/ModuleDetailPanel";
import { type ComposerDraft, updateDraftModule } from "../v3/composerDraft";
import { moduleCatalogEntry, statusLabels } from "../v3/composerCatalog";

type Props = {
  preview: V3ComposerPreview;
  draft: ComposerDraft;
  onDraftChange: (draft: ComposerDraft) => void;
  templates: V3TemplateSummary[];
  savedConfigs: V3SavedConfig[];
  templateName: string;
  templateRole: string;
  templateDescription: string;
  validation: V3DraftValidationResponse | null;
  selectedPresetId?: string;
  onTemplateNameChange: (value: string) => void;
  onTemplateRoleChange: (value: string) => void;
  onTemplateDescriptionChange: (value: string) => void;
  onSelectTemplate: (templateId: string) => void;
  onLoadSavedConfig: (config: V3SavedConfig) => void;
};

const pipelineStages = [
  { title: "Packaging", subtitle: "TxPool / Block / Consensus", modules: ["TxPool", "BlockProducer", "Consensus"] },
  { title: "Scheduling", subtitle: "Epoch / Routing", modules: ["CommitteeEpoch", "Routing"] },
  { title: "Execution", subtitle: "Exec / Access / Storage", modules: ["Execution", "StateAccess", "StateStorage"] },
  { title: "Commit", subtitle: "Commit / Metrics", modules: ["Commit", "MetricsReport"] },
];

const defaultTemplateCards = [
  ["MetaTrack Full", "main", "routing + execution + state access + commit"],
  ["Hash Baseline", "baseline", "hash routing + serial execution"],
  ["Serial Baseline", "baseline", "serial execution reference"],
  ["No Aggregation", "ablation", "commit aggregation disabled"],
  ["Routing Only", "ablation", "routing component only"],
] as const;

const coreModules = new Set(["Consensus", "Routing", "Execution", "StateAccess", "Commit"]);
const templateRoles = ["main", "baseline", "ablation", "custom"] as const;

export default function MethodPipelineWorkbench({
  preview,
  draft,
  onDraftChange,
  templates,
  savedConfigs,
  templateName,
  templateRole,
  templateDescription,
  validation,
  selectedPresetId,
  onTemplateNameChange,
  onTemplateRoleChange,
  onTemplateDescriptionChange,
  onSelectTemplate,
  onLoadSavedConfig,
}: Props) {
  const modulesById = useMemo(() => new Map((preview.modules || []).map((module) => [module.module_id, module])), [preview.modules]);
  const [activeModuleId, setActiveModuleId] = useSafeActiveModule(modulesById);
  const activeModule = modulesById.get(activeModuleId) || modulesById.get("Routing") || (preview.modules || [])[0];
  const methodSavedConfigs = savedConfigs.filter((config) => config.config_kind === "method");
  const compatibilityConfigs = methodSavedConfigs.filter(isTestOrCompatibilityConfig);
  const userReadableConfigs = methodSavedConfigs.filter((config) => config.source === "user_saved" && !isTestOrCompatibilityConfig(config));
  const regularSavedConfigs = methodSavedConfigs.filter((config) => !isTestOrCompatibilityConfig(config));
  const recentSavedConfigs = userReadableConfigs.slice(0, 4);
  const recentCatalogTemplates = templates.filter((template) => template.runnable).slice(0, 4);
  const validationStatus = validation?.is_runnable ? "verified" : validation?.is_valid ? "valid draft" : "draft";

  function updateActiveModule(moduleId: string, patch: Parameters<typeof updateDraftModule>[2]) {
    onDraftChange(updateDraftModule(draft, moduleId, patch));
  }

  return (
    <section className="composer-workbench">
      <aside className="template-sidebar">
        <div className="template-current-card">
          <p className="eyebrow">Current Template</p>
          <h3 title={templateName}>{templateName || "Untitled method template"}</h3>
          <div className="template-meta-row">
            <span className="status-badge badge-preview">{templateRole}</span>
            <span className={`status-badge ${validation?.is_runnable ? "badge-verified" : validation?.is_valid ? "badge-runnable" : "badge-draft"}`}>{validationStatus}</span>
          </div>
          <small>unsaved draft state</small>
        </div>

        <div className="template-role-filter" aria-label="Template role">
          {templateRoles.map((role) => (
            <button key={role} type="button" className={templateRole === role ? "active" : ""} onClick={() => onTemplateRoleChange(role)}>
              {role}
            </button>
          ))}
        </div>

        <div className="template-list-block">
          <h4>Recent templates</h4>
          {recentSavedConfigs.length ? recentSavedConfigs.map((config) => (
            <button key={config.config_id} type="button" className="template-list-row compact" onClick={() => onLoadSavedConfig(config)}>
              <strong>{config.name}</strong>
              <small>{config.validation_status} / {config.config_id}</small>
            </button>
          )) : recentCatalogTemplates.map((template) => (
            <button key={template.template_id} type="button" className="template-list-row compact" onClick={() => onSelectTemplate(template.template_id)}>
              <strong>{template.template_name || template.template_id}</strong>
              <small>{template.default_preset_id || selectedPresetId || "default preset"}</small>
            </button>
          ))}
          <small className="muted">Showing up to 4 recent templates.</small>
        </div>

        <button type="button" className="v3-secondary-button" disabled>New template</button>

        <details className="template-manager">
          <summary>Manage all templates</summary>
          <div className="template-manager-body">
            <label>
              <span>Template name</span>
              <input value={templateName} onChange={(event) => onTemplateNameChange(event.target.value)} />
            </label>
            <label>
              <span>Template note</span>
              <textarea value={templateDescription} onChange={(event) => onTemplateDescriptionChange(event.target.value)} rows={3} />
            </label>
            <div className="template-role-help">
              <p><strong>main</strong> primary method</p>
              <p><strong>baseline</strong> comparison method</p>
              <p><strong>ablation</strong> ablation method</p>
              <p><strong>custom</strong> custom method</p>
            </div>
            <TemplateList title="Catalog presets">
              {defaultTemplateCards.map(([name, role, note]) => (
                <button key={name} type="button" className={`template-list-row ${templateName === name ? "active" : ""}`} onClick={() => {
                  onTemplateNameChange(name);
                  onTemplateRoleChange(role);
                  onTemplateDescriptionChange(note);
                }}>
                  <strong>{name}</strong>
                  <small>{role} / {note}</small>
                </button>
              ))}
            </TemplateList>
            <TemplateList title="Composer templates">
              {templates.filter((template) => template.runnable).map((template) => (
                <button key={template.template_id} type="button" className="template-list-row" onClick={() => onSelectTemplate(template.template_id)}>
                  <strong>{template.template_name || template.template_id}</strong>
                  <small>{template.default_preset_id || selectedPresetId || "default preset"}</small>
                </button>
              ))}
            </TemplateList>
            <TemplateList title="Saved templates">
              {regularSavedConfigs.length ? regularSavedConfigs.map((config) => (
                <button key={config.config_id} type="button" className="template-list-row" onClick={() => onLoadSavedConfig(config)}>
                  <strong>{config.name}</strong>
                  <small>{config.validation_status} / {config.config_id}</small>
                </button>
              )) : <p className="muted">No saved method config yet.</p>}
            </TemplateList>
            <TemplateList title="Test / compatibility templates">
              {compatibilityConfigs.length ? compatibilityConfigs.map((config) => (
                <button key={config.config_id} type="button" className="template-list-row" onClick={() => onLoadSavedConfig(config)}>
                  <strong>{config.name}</strong>
                  <small>{config.validation_status} / {config.config_id}</small>
                </button>
              )) : <p className="muted">No test or compatibility templates.</p>}
            </TemplateList>
          </div>
        </details>
      </aside>

      <main className="method-canvas">
        <div className="method-canvas-head">
          <div>
            <p className="eyebrow">Method Pipeline</p>
            <h3>Interactive method canvas</h3>
          </div>
          <span className="status-badge badge-preview">click module to configure</span>
        </div>
        <div className="pipeline-stage-grid">
          {pipelineStages.map((stage) => (
            <section key={stage.title} className="pipeline-stage">
              <div className="pipeline-stage-title">
                <h4>{stage.title}</h4>
                <small>{stage.subtitle}</small>
              </div>
              {stage.modules.map((moduleId) => {
                const module = modulesById.get(moduleId);
                const draftModule = draft.modules[moduleId];
                if (!module || !draftModule) return null;
                const active = activeModuleId === moduleId;
                const status = draftModule.status;
                return (
                  <button
                    key={moduleId}
                    type="button"
                    className={`module-node-card module-${status}${active ? " active" : ""}${coreModules.has(moduleId) ? " core" : " support"}`}
                    onClick={() => setActiveModuleId(moduleId)}
                  >
                    <span className={`status-badge status-${status}`}>{statusLabels[status]}</span>
                    <strong>{moduleId}</strong>
                    <code>{draftModule.plugin}</code>
                    <small>{paramSummary(draftModule.params) || "default params"}</small>
                  </button>
                );
              })}
            </section>
          ))}
        </div>
      </main>

      <aside className="module-config-panel">
        <ModuleDetailPanel
          module={activeModule as V3ComposerModule}
          draft={draft}
          onDraftModuleChange={updateActiveModule}
          variableModules={draft.variableModules}
          controlledExperimentEnabled={draft.topology.controlled_experiment_enabled}
        />
      </aside>
    </section>
  );
}

function TemplateList({ title, children }: { title: string; children: ReactNode }) {
  return (
    <div className="template-list-block">
      <h4>{title}</h4>
      {children}
    </div>
  );
}

function useSafeActiveModule(modulesById: Map<string, V3ComposerModule>): [string, (value: string) => void] {
  const [activeModuleId, setActiveModuleId] = useState("Routing");
  useEffect(() => {
    if (!modulesById.has(activeModuleId)) {
      setActiveModuleId(modulesById.has("Routing") ? "Routing" : Array.from(modulesById.keys())[0] || "");
    }
  }, [activeModuleId, modulesById]);
  return [activeModuleId, setActiveModuleId];
}

function paramSummary(params: Record<string, string | number | boolean>) {
  const entries = Object.entries(params).filter(([, value]) => value !== "" && value !== undefined);
  if (!entries.length) return "";
  return entries.slice(0, 2).map(([key, value]) => `${key}=${String(value)}`).join(", ");
}

function isTestOrCompatibilityConfig(config: V3SavedConfig) {
  const tags = new Set(config.tags.map((tag) => tag.trim().toLowerCase()));
  const normalizedName = config.name.trim().toLowerCase();
  return ["e2e", "test", "compatibility"].some((tag) => tags.has(tag))
    || /e2e[\s_-]+compatibility|compatibility[\s_-]+e2e|playwright[\s_-]+e2e/i.test(normalizedName);
}
