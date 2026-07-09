import type { V3ComposerModule } from "../../api";
import {
  type DraftModuleStatus,
  type DraftPluginOption,
  moduleCatalogEntry,
  pluginStatusLabels,
  requiredModuleIds,
  statusLabels,
} from "./composerCatalog";
import {
  type ComposerDraft,
  type ComposerDraftModule,
  pluginOptionsForModule,
} from "./composerDraft";

type Props = {
  module?: V3ComposerModule | null;
  draft: ComposerDraft;
  onDraftModuleChange: (moduleId: string, patch: Partial<Pick<ComposerDraftModule, "status" | "plugin" | "params">>) => void;
  variableModule?: string;
  variableModules?: string[];
  lockedModules?: Record<string, string>;
  controlledExperimentEnabled?: boolean;
};

const statusChoices: DraftModuleStatus[] = ["default", "fixed", "variable", "disabled"];

export default function ModuleDetailPanel({
  module,
  draft,
  onDraftModuleChange,
  variableModule = "",
  variableModules = [],
  lockedModules = {},
  controlledExperimentEnabled = false,
}: Props) {
  if (!module) {
    return (
      <aside className="v3-detail-panel">
        <p className="eyebrow">Module Config</p>
        <h3>Select a module</h3>
        <p>Choose a pipeline module to edit its plugin, role, and parameters.</p>
      </aside>
    );
  }

  const selectedModule = module;
  const catalog = moduleCatalogEntry(selectedModule.module_id);
  const draftModule = draft.modules[selectedModule.module_id];
  const pluginOptions = dedupePlugins(pluginOptionsForModule(selectedModule));
  const primaryPlugins = pluginOptions.filter((plugin) => plugin.status !== "planned");
  const plannedPlugins = pluginOptions.filter((plugin) => plugin.status === "planned");
  const currentPlugin = draftModule?.plugin || catalog.defaultPlugin;
  const currentStatus = draftModule?.status || "fixed";
  const moduleStatusChoices: DraftModuleStatus[] = selectedModule.module_id === "MetricsReport" ? ["output"] : statusChoices;
  const selectedPlugin = pluginOptions.find((plugin) => plugin.id === currentPlugin);
  const lockedPlugin = lockedModules[selectedModule.module_id];
  const isVariable = selectedModule.module_id === variableModule || variableModules.includes(selectedModule.module_id);
  const isLocked = controlledExperimentEnabled && Boolean(lockedPlugin);
  const templateRole = controlledExperimentEnabled && isVariable ? "variable" : isLocked ? "locked" : "";
  const committeeEpochTopologyEnabled = selectedModule.module_id === "CommitteeEpoch" && Boolean(draft.topology.enable_committee_epoch);
  const statusLabel = committeeEpochTopologyEnabled ? "topology enabled" : statusLabels[currentStatus];
  const selectedPluginIsPreview = selectedPlugin?.status === "preview";
  const selectedPluginIsPlanned = selectedPlugin?.status === "planned";
  const visibleMessages = draft.validationMessages
    .filter((message) => message.includes(catalog.label) || message.includes(selectedModule.module_id))
    .slice(0, 3);

  function changeStatus(status: DraftModuleStatus) {
    onDraftModuleChange(selectedModule.module_id, { status });
  }

  function changePlugin(plugin: string) {
    onDraftModuleChange(selectedModule.module_id, { plugin });
  }

  function changeParam(name: string, value: string) {
    onDraftModuleChange(selectedModule.module_id, { params: { ...(draftModule?.params || {}), [name]: value } });
  }

  return (
    <aside className="v3-detail-panel v3-config-panel">
      <p className="eyebrow">Module Config</p>
      <div className="v3-config-title">
        <div>
          <h3>{catalog.label}</h3>
          <p className="v3-sub-id">{selectedModule.module_id}</p>
        </div>
        <span className={`v3-status-badge status-${committeeEpochTopologyEnabled ? "variable" : currentStatus}`}>{statusLabel}</span>
      </div>

      {selectedPluginIsPreview && (
        <div className="v3-warning-card">
          Preview plugin: visible for configuration, but not guaranteed runnable in Draft Smoke.
        </div>
      )}

      <dl className="v3-detail-list compact">
        <div>
          <dt>Current plugin</dt>
          <dd title={currentPlugin}>{selectedPlugin?.label || currentPlugin}<small>{currentPlugin}</small></dd>
        </div>
        {templateRole && (
          <div>
            <dt>Template role</dt>
            <dd>{templateRole === "variable" ? "experiment variable" : `locked: ${lockedPlugin}`}</dd>
          </div>
        )}
      </dl>

      <section className="v3-config-section">
        <h4>Status</h4>
        <div className="v3-radio-list">
          {moduleStatusChoices.map((status) => {
            const disabled = isLocked || statusDisabled(selectedModule.module_id, status);
            return (
              <label key={status} className={disabled ? "disabled" : ""}>
                <input type="radio" checked={currentStatus === status} disabled={disabled} onChange={() => changeStatus(status)} />
                <span>{statusLabels[status]}</span>
              </label>
            );
          })}
        </div>
      </section>

      <section className="v3-config-section">
        <h4>Plugin</h4>
        <PluginList plugins={primaryPlugins} currentPlugin={currentPlugin} locked={isLocked} onChange={changePlugin} />
        {plannedPlugins.length > 0 && (
          <details className="v3-foldout compact-foldout">
            <summary className="v3-foldout-summary">Planned plugins</summary>
            <PluginList plugins={plannedPlugins} currentPlugin={currentPlugin} locked onChange={changePlugin} />
          </details>
        )}
      </section>

      <section className="v3-config-section">
        <h4>Parameters</h4>
        {(catalog.params && catalog.params.length > 0) ? (
          <div className="v3-param-grid">
            {catalog.params.map((param) => (
              <label key={param}>
                <span>{param}</span>
                <input value={String(draftModule?.params?.[param] ?? "")} onChange={(event) => changeParam(param, event.target.value)} />
              </label>
            ))}
          </div>
        ) : (
          <p className="muted">This module has no frontend parameter fields yet.</p>
        )}
      </section>

      <section className="v3-config-section">
        <h4>Actions</h4>
        <div className="module-action-row primary-actions">
          <button type="button" className="v3-secondary-button" onClick={() => onDraftModuleChange(selectedModule.module_id, { status: currentStatus, plugin: currentPlugin, params: draftModule?.params || {} })}>Apply config</button>
          <button type="button" className="ghost-button" onClick={() => onDraftModuleChange(selectedModule.module_id, { status: defaultStatusForModule(selectedModule.module_id), plugin: catalog.defaultPlugin, params: Object.fromEntries((catalog.params || []).map((param) => [param, ""])) })}>Restore default</button>
        </div>
      </section>

      <details className="v3-config-section v3-foldout compact-foldout">
        <summary className="v3-foldout-summary">Guidance and validation</summary>
        <p>{catalog.description}</p>
        <p>{editStateMessage(selectedModule.module_id, controlledExperimentEnabled, isLocked, isVariable, selectedPluginIsPlanned, committeeEpochTopologyEnabled)}</p>
        <p className="muted">{moduleHint(selectedModule.module_id)}</p>
        <p className="muted">{boundaryHint(selectedModule.module_id)}</p>
        {requiredModuleIds.has(selectedModule.module_id) && selectedModule.module_id !== "MetricsReport" && <p className="muted">Required modules cannot be disabled.</p>}
        <div className="module-action-row">
          <button type="button" className="ghost-button" disabled={statusDisabled(selectedModule.module_id, "variable") || isLocked} onClick={() => onDraftModuleChange(selectedModule.module_id, { status: "variable" })}>Mark variable</button>
          <button type="button" className="ghost-button" disabled={statusDisabled(selectedModule.module_id, "disabled") || isLocked} onClick={() => onDraftModuleChange(selectedModule.module_id, { status: "disabled" })}>Disable module</button>
        </div>
        <ul className="v3-check-list compact">
          {(visibleMessages.length ? visibleMessages : draft.validationMessages.slice(0, 3)).map((message) => <li key={message}>{message}</li>)}
        </ul>
      </details>
    </aside>
  );
}

function PluginList({ plugins, currentPlugin, locked, onChange }: { plugins: DraftPluginOption[]; currentPlugin: string; locked: boolean; onChange: (plugin: string) => void }) {
  return (
    <div className="v3-plugin-option-list">
      {plugins.map((plugin) => (
        <label key={plugin.id} className={plugin.status === "planned" ? "disabled" : ""}>
          <input type="radio" checked={currentPlugin === plugin.id} disabled={locked || plugin.status === "planned"} onChange={() => onChange(plugin.id)} />
          <span>
            <strong>{plugin.label}</strong>
            <small title={plugin.id}>{plugin.id}</small>
            <small>{pluginAvailabilityText(plugin.status)}</small>
          </span>
          <b className={`v3-status-badge plugin-${plugin.status}`}>{pluginStatusLabels[plugin.status]}</b>
        </label>
      ))}
    </div>
  );
}

function dedupePlugins(plugins: DraftPluginOption[]): DraftPluginOption[] {
  const seen = new Set<string>();
  return plugins.filter((plugin) => {
    const normalized = plugin.id
      .replace("_planned", "")
      .replace("_model", "")
      .replace("_commit", "");
    if (seen.has(normalized)) return false;
    seen.add(normalized);
    return true;
  });
}

function pluginAvailabilityText(status: DraftPluginOption["status"]): string {
  if (status === "runnable") return "runnable";
  if (status === "preview") return "preview only";
  return "planned";
}

function editStateMessage(
  moduleId: string,
  controlledExperimentEnabled: boolean,
  isLocked: boolean,
  isVariable: boolean,
  selectedPluginIsPlanned: boolean,
  committeeEpochTopologyEnabled: boolean,
): string {
  if (moduleId === "MetricsReport") return "MetricsReport is an output module and is not used as an experiment variable.";
  if (selectedPluginIsPlanned) return "Planned plugins are shown for roadmap clarity and cannot be run.";
  if (moduleId === "CommitteeEpoch") {
    return committeeEpochTopologyEnabled
      ? "CommitteeEpoch is enabled through topology compatibility settings."
      : "CommitteeEpoch is disabled by default and is not a normal method variable.";
  }
  if (!controlledExperimentEnabled) return "Free configuration mode: choose among runnable or preview plugin options.";
  if (isLocked) return "Controlled comparison mode: this module is locked by the selected template.";
  if (isVariable) return "This module is a controlled experiment variable.";
  return "This module remains editable unless locked by the selected template.";
}

function moduleHint(moduleId: string): string {
  if (moduleId === "Routing") return "Routing owns sharding/routing policy selection; workload and topology stay on the Run Experiment page.";
  if (moduleId === "StateAccess") return "State proof and witness views are MVP artifacts, not full stateless execution.";
  if (moduleId === "StateStorage") return "State backend compatibility remains local emulator scope.";
  if (moduleId === "Consensus") return "Consensus options remain V3/V4 emulator semantics, not production PBFT.";
  if (moduleId === "MetricsReport") return "MetricsReport controls output reporting behavior.";
  return "This module configures the reusable method template.";
}

function boundaryHint(moduleId: string): string {
  if (moduleId === "Routing") return "Does not claim complete Relay/Broker/2PC atomic cross-shard commit.";
  if (moduleId === "Commit") return "Does not claim production DB locking, rollback, or atomic cross-shard validation.";
  if (moduleId === "StateAccess" || moduleId === "StateStorage") return "Merkle/MPT-like roots remain MVP scope, not Ethereum-compatible MPT.";
  if (moduleId === "Consensus") return "Does not claim production PBFT, HotStuff, Raft, or Byzantine security.";
  return "V3 remains a local emulator and formal experiment console baseline.";
}

function statusDisabled(moduleId: string, status: DraftModuleStatus): boolean {
  if (moduleId === "MetricsReport") return status !== "output";
  if (requiredModuleIds.has(moduleId) && status === "disabled") return true;
  if (moduleId === "CommitteeEpoch" && status === "variable") return true;
  return false;
}

function defaultStatusForModule(moduleId: string): DraftModuleStatus {
  if (moduleId === "MetricsReport") return "output";
  if (moduleId === "CommitteeEpoch") return "disabled";
  return "fixed";
}
