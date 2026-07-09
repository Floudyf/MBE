import { useEffect, useState } from "react";

import type { V3ComposerModule, V3ComposerPreview, V3SavedConfig, V3TemplateSummary, V3DraftValidationResponse } from "../../api";
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
  { title: "打包与共识", modules: ["TxPool", "BlockProducer", "Consensus"] },
  { title: "调度与分片", modules: ["CommitteeEpoch", "Routing"] },
  { title: "执行与状态", modules: ["Execution", "StateAccess", "StateStorage"] },
  { title: "提交与观测", modules: ["Commit", "MetricsReport"] },
];

const defaultTemplateCards = [
  ["MetaTrack Full", "main", "routing + execution + state access + commit"],
  ["Hash Baseline", "baseline", "hash routing + serial execution"],
  ["Serial Baseline", "baseline", "serial execution reference"],
  ["No Aggregation", "ablation", "commit aggregation disabled"],
  ["Routing Only", "ablation", "routing component only"],
] as const;

const coreModules = new Set(["Consensus", "Routing", "Execution", "StateAccess", "Commit"]);

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
  const modulesById = new Map((preview.modules || []).map((module) => [module.module_id, module]));
  const [activeModuleId, setActiveModuleId] = useSafeActiveModule(modulesById);
  const activeModule = modulesById.get(activeModuleId) || modulesById.get("Routing") || (preview.modules || [])[0];
  const methodSavedConfigs = savedConfigs.filter((config) => config.config_kind === "method").slice(0, 6);

  function updateActiveModule(moduleId: string, patch: Parameters<typeof updateDraftModule>[2]) {
    onDraftChange(updateDraftModule(draft, moduleId, patch));
  }

  return (
    <section className="composer-workbench">
      <aside className="template-sidebar">
        <p className="eyebrow">Template</p>
        <h3>模板栏</h3>
        <label>
          <span>模板名称</span>
          <input value={templateName} onChange={(event) => onTemplateNameChange(event.target.value)} />
        </label>
        <label>
          <span>模板角色</span>
          <select value={templateRole} onChange={(event) => onTemplateRoleChange(event.target.value)}>
            <option value="main">main</option>
            <option value="baseline">baseline</option>
            <option value="ablation">ablation</option>
            <option value="custom">custom</option>
          </select>
        </label>
        <label>
          <span>模板说明</span>
          <textarea value={templateDescription} onChange={(event) => onTemplateDescriptionChange(event.target.value)} rows={3} />
        </label>
        <div className="template-status-card">
          <span className={`status-badge ${validation?.is_runnable ? "badge-verified" : validation?.is_valid ? "badge-runnable" : "badge-draft"}`}>
            {validation?.is_runnable ? "verified" : validation?.is_valid ? "valid draft" : "draft"}
          </span>
          <small>当前模板状态</small>
        </div>

        <div className="template-list-block">
          <h4>默认模板 / catalog presets</h4>
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
        </div>

        <div className="template-list-block">
          <h4>Composer templates</h4>
          {templates.filter((template) => template.runnable).slice(0, 5).map((template) => (
            <button key={template.template_id} type="button" className="template-list-row" onClick={() => onSelectTemplate(template.template_id)}>
              <strong>{template.template_name || template.template_id}</strong>
              <small>{template.default_preset_id || selectedPresetId || "default preset"}</small>
            </button>
          ))}
        </div>

        <div className="template-list-block">
          <h4>已保存模板</h4>
          {methodSavedConfigs.length ? methodSavedConfigs.map((config) => (
            <button key={config.config_id} type="button" className="template-list-row" onClick={() => onLoadSavedConfig(config)}>
              <strong>{config.name}</strong>
              <small>{config.validation_status} / {config.config_id}</small>
            </button>
          )) : <p className="muted">暂无保存的 method config。</p>}
        </div>

        <div className="template-role-help">
          <p><strong>main</strong> 论文主方法</p>
          <p><strong>baseline</strong> 对比方法</p>
          <p><strong>ablation</strong> 消融方法</p>
          <p><strong>custom</strong> 自定义实验方法</p>
        </div>
        <button type="button" disabled>新建主方法模板</button>
        <button type="button" disabled>新建 baseline 模板</button>
        <button type="button" disabled>新建消融模板</button>
      </aside>

      <main className="method-canvas">
        <div className="method-canvas-head">
          <div>
            <p className="eyebrow">Method Pipeline</p>
            <h3>可交互方法流水线</h3>
          </div>
          <span className="status-badge badge-preview">click module to configure</span>
        </div>
        <div className="pipeline-stage-grid">
          {pipelineStages.map((stage) => (
            <section key={stage.title} className="pipeline-stage">
              <h4>{stage.title}</h4>
              {stage.modules.map((moduleId) => {
                const module = modulesById.get(moduleId);
                const draftModule = draft.modules[moduleId];
                if (!module || !draftModule) return null;
                const catalog = moduleCatalogEntry(moduleId);
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
                    <small>{paramSummary(draftModule.params) || catalog.description}</small>
                    <em>点击配置</em>
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
