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
import HelpTip from "./HelpTip";

type Props = {
  module?: V3ComposerModule | null;
  draft: ComposerDraft;
  onDraftModuleChange: (moduleId: string, patch: Partial<Pick<ComposerDraftModule, "status" | "plugin" | "params">>) => void;
  variableModule?: string;
  lockedModules?: Record<string, string>;
};

const statusChoices: DraftModuleStatus[] = ["default", "fixed", "variable", "disabled"];

export default function ModuleDetailPanel({ module, draft, onDraftModuleChange, variableModule = "", lockedModules = {} }: Props) {
  if (!module) {
    return (
      <aside className="v3-detail-panel">
        <p className="eyebrow">模块配置</p>
        <h3>请选择模块</h3>
        <p>选择一个流程模块后，可以配置插件、实验变量状态和草稿参数。</p>
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
  const moduleStatusChoices: DraftModuleStatus[] = module.module_id === "MetricsReport" ? ["output"] : statusChoices;
  const selectedPlugin = pluginOptions.find((plugin) => plugin.id === currentPlugin);
  const lockedPlugin = lockedModules[selectedModule.module_id];
  const templateRole = selectedModule.module_id === variableModule ? "variable" : lockedPlugin ? "locked" : "";
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
      <p className="eyebrow">模块配置</p>
      <div className="v3-config-title">
        <div>
          <h3>{catalog.label}</h3>
          <p className="v3-sub-id">{selectedModule.module_id}</p>
        </div>
        <span className={`v3-status-badge status-${currentStatus}`}>{statusLabels[currentStatus]}</span>
      </div>

      <dl className="v3-detail-list compact">
        <div><dt>当前插件</dt><dd title={currentPlugin}>{selectedPlugin?.label || currentPlugin}<small>{currentPlugin}</small></dd></div>
        {templateRole && (
          <div>
            <dt>模板角色</dt>
            <dd>{templateRole === "variable" ? "实验变量" : `模板固定：${lockedPlugin}`}</dd>
          </div>
        )}
      </dl>

      <section className="v3-config-section">
        <h4>模块说明 <HelpTip title={catalog.label}>{moduleHint(selectedModule.module_id)}</HelpTip></h4>
        <p>{catalog.description}</p>
        <p className="muted">{boundaryHint(selectedModule.module_id)}</p>
      </section>

      <section className="v3-config-section">
        <h4>模块状态</h4>
        <div className="v3-radio-list">
          {moduleStatusChoices.map((status) => {
            const disabled = templateRole === "locked" || statusDisabled(selectedModule.module_id, status);
            return (
              <label key={status} className={disabled ? "disabled" : ""}>
                <input type="radio" checked={currentStatus === status} disabled={disabled} onChange={() => changeStatus(status)} />
                <span>{statusLabels[status]}</span>
              </label>
            );
          })}
        </div>
        {requiredModuleIds.has(selectedModule.module_id) && selectedModule.module_id !== "MetricsReport" && <p className="muted">必需模块不能关闭；模板固定项不能在当前模板中改为实验变量。</p>}
      </section>

      <section className="v3-config-section">
        <h4>插件选择 <HelpTip title="插件选择">主列表只显示可运行或有展示意义的预览项；规划中插件折叠在下方，不干扰本轮试运行。</HelpTip></h4>
        <PluginList plugins={primaryPlugins} currentPlugin={currentPlugin} locked={templateRole === "locked"} onChange={changePlugin} />
        {plannedPlugins.length > 0 && (
          <details className="v3-foldout">
            <summary className="v3-foldout-summary">规划中插件</summary>
            <PluginList plugins={plannedPlugins} currentPlugin={currentPlugin} locked onChange={changePlugin} />
          </details>
        )}
      </section>

      <details className="v3-config-section v3-foldout">
        <summary className="v3-foldout-summary">参数配置</summary>
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
          <p className="muted">该模块当前没有前端参数占位。</p>
        )}
      </details>

      <section className="v3-config-section">
        <h4>草稿校验</h4>
        <ul className="v3-check-list compact">
          {(visibleMessages.length ? visibleMessages : draft.validationMessages.slice(0, 3)).map((message) => <li key={message}>{message}</li>)}
        </ul>
      </section>
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

function moduleHint(moduleId: string): string {
  if (moduleId === "Routing") return "跨片协议是 Routing/Sharding 的子能力，不新增主流程卡片。";
  if (moduleId === "StateAccess") return "状态证明和 witness 是 MVP 产物，不是完整无状态执行。";
  if (moduleId === "StateStorage") return "状态后端通过运行拓扑面板选择，Ethereum MPT 仍是规划项。";
  if (moduleId === "Consensus") return "PBFT 网络预览是可选 runtime，不是唯一共识，也不是生产 PBFT。";
  if (moduleId === "MetricsReport") return "Benchmark 属于实验控制 / 结果层，不是新的主流程模块。";
  return "当前模块用于本地 V3 快速验证和受控实验配置。";
}

function boundaryHint(moduleId: string): string {
  if (moduleId === "Routing") return "不实现完整 Relay / Broker / 2PC，不声称原子跨片提交。";
  if (moduleId === "Commit") return "不实现真实 DB 锁、回滚或跨片原子验证提交。";
  if (moduleId === "StateAccess" || moduleId === "StateStorage") return "Proof / Witness 为 MVP，Merkle/MPT-like root 为 MVP；非 Ethereum MPT，非完整无状态执行。";
  if (moduleId === "Consensus") return "不声称 real PBFT、HotStuff、Raft 或生产网络。";
  return "V3.11 增加本地 Relay MVP 观测闭环，但不代表生产级跨片协议。";
}

function statusDisabled(moduleId: string, status: DraftModuleStatus): boolean {
  if (moduleId === "MetricsReport") return status !== "output";
  if (requiredModuleIds.has(moduleId) && status === "disabled") return true;
  if (moduleId === "CommitteeEpoch" && status === "variable") return true;
  return false;
}
