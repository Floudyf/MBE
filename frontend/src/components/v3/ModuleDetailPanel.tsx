import type { V3ComposerModule } from "../../api";
import {
  type DraftModuleStatus,
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
  lockedModules?: Record<string, string>;
};

const statusChoices: DraftModuleStatus[] = ["default", "fixed", "variable", "disabled"];

export default function ModuleDetailPanel({ module, draft, onDraftModuleChange, variableModule = "", lockedModules = {} }: Props) {
  if (!module) {
    return (
      <aside className="v3-detail-panel">
        <p className="eyebrow">模块配置</p>
        <h3>请选择模块</h3>
        <p>选择一个模块后，可以配置插件、实验变量状态和 Draft 参数。</p>
      </aside>
    );
  }

  const selectedModule = module;
  const catalog = moduleCatalogEntry(selectedModule.module_id);
  const draftModule = draft.modules[selectedModule.module_id];
  const pluginOptions = pluginOptionsForModule(selectedModule);
  const currentPlugin = draftModule?.plugin || catalog.defaultPlugin;
  const currentStatus = draftModule?.status || "fixed";
  const moduleStatusChoices: DraftModuleStatus[] = selectedModule.module_id === "MetricsReport" ? ["output"] : statusChoices;
  const selectedPlugin = pluginOptions.find((plugin) => plugin.id === currentPlugin);
  const isRequired = requiredModuleIds.has(selectedModule.module_id);
  const lockedPlugin = lockedModules[selectedModule.module_id];
  const templateRole = selectedModule.module_id === variableModule ? "variable" : lockedPlugin ? "locked" : "";
  const moduleMessages = draft.validationMessages.filter((message) => message.includes(catalog.label) || message.includes(selectedModule.module_id));
  const visibleMessages = (moduleMessages.length ? moduleMessages : draft.validationMessages).slice(0, 3);

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
            <dt>template role</dt>
            <dd>{templateRole === "variable" ? "variable module" : `locked by template (${lockedPlugin})`}</dd>
          </div>
        )}
      </dl>

      <section className="v3-config-section">
        <h4>模块说明</h4>
        <p>{catalog.description}</p>
        {catalog.notes?.slice(0, 1).map((note) => <p key={note} className="muted">{note}</p>)}
        {selectedModule.module_id === "TxPool" && (
          <p className="muted">V3.4.1 realizes FIFO pool runtime behavior for Draft Smoke. Priority, hotspot-aware, and fee-based pools remain planned and are not real runtime implementations.</p>
        )}
        {selectedModule.module_id === "BlockProducer" && (
          <p className="muted">V3.4.2 realizes the time-or-count BlockProducer for Draft Smoke. Fixed-size and adaptive block cut plugins remain planned and are not real runtime implementations.</p>
        )}
        {selectedModule.module_id === "Consensus" && (
          <p className="muted">V3.4.3 realizes simple_leader, poa_light, and pbft_light_model as local virtual-time consensus-light models. pbft_light_model models PBFT stages and quorum accounting; it is not production PBFT or real network PBFT.</p>
        )}
        {selectedModule.module_id === "Routing" && (
          <p className="muted">V3.4.5 realizes Routing/Sharding decision records for hash_sharding, metatrack_coaccess_routing, and hotspot_aware_routing. Routing estimates shard assignment, touched shards, hotspots, and co-access groups; it does not implement relay, broker, 2PC, CLPA, ShardCutter, state migration, or real cross-shard protocols.</p>
        )}
      </section>

      <section className="v3-config-section">
        <h4>模块状态</h4>
        <div className="v3-radio-list">
          {moduleStatusChoices.map((status) => {
            const disabled = templateRole === "locked" || statusDisabled(selectedModule.module_id, status);
            return (
              <label key={status} className={disabled ? "disabled" : ""}>
                <input
                  type="radio"
                  checked={currentStatus === status}
                disabled={disabled}
                  onChange={() => changeStatus(status)}
                />
                <span>{statusLabels[status]}</span>
              </label>
            );
          })}
        </div>
        {selectedModule.module_id === "MetricsReport" && <p className="muted">指标 / 报告固定为输出模块，不能作为普通实验变量。</p>}
        {isRequired && selectedModule.module_id !== "MetricsReport" && <p className="muted">必需模块不能关闭；不选表示使用默认配置或固定环境。</p>}
      </section>

      <section className="v3-config-section">
        <h4>插件选择</h4>
        <div className="v3-plugin-option-list">
          {pluginOptions.map((plugin) => (
            <label key={plugin.id} className={plugin.status === "planned" ? "disabled" : ""}>
              <input
                type="radio"
                checked={currentPlugin === plugin.id}
                disabled={templateRole === "locked" || plugin.status === "planned"}
                onChange={() => changePlugin(plugin.id)}
              />
              <span>
                <strong>{plugin.label}</strong>
                <small title={plugin.id}>{plugin.id}</small>
                {selectedModule.module_id === "TxPool" && (
                  <small>{plugin.id === "fifo_pool" ? "runtime-supported FIFO hardening" : "planned only"}</small>
                )}
                {selectedModule.module_id === "BlockProducer" && (
                  <small>{plugin.id === "time_or_count_block_producer" ? "runtime-supported time/count hardening" : "planned only"}</small>
                )}
                {selectedModule.module_id === "Consensus" && (
                  <small>{consensusPluginHint(plugin.id)}</small>
                )}
                {selectedModule.module_id === "Routing" && (
                  <small>{routingPluginHint(plugin.id)}</small>
                )}
              </span>
              <b className={`v3-status-badge plugin-${plugin.status}`}>{pluginStatusLabels[plugin.status]}</b>
            </label>
          ))}
        </div>
      </section>

      <details className="v3-config-section v3-foldout">
        <summary className="v3-foldout-summary">参数配置</summary>
        <p className="muted">当前仅用于 Draft 预览，正式自定义运行将在后续阶段支持。</p>
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
        <h4>Draft Validation</h4>
        <ul className="v3-check-list compact">
          {visibleMessages.map((message) => <li key={message}>{message}</li>)}
        </ul>
        {draft.validationMessages.length > visibleMessages.length && (
          <details className="v3-foldout">
            <summary className="v3-foldout-summary">更多校验信息</summary>
            <ul className="v3-check-list">
              {draft.validationMessages.map((message) => <li key={message}>{message}</li>)}
            </ul>
          </details>
        )}
      </section>
    </aside>
  );
}

function consensusPluginHint(pluginId: string): string {
  if (pluginId === "simple_leader") return "runtime-supported simple leader";
  if (pluginId === "poa_light") return "runtime-supported PoA-light model";
  if (pluginId === "pbft_light_model") return "PBFT-style light model only";
  return "planned or unsupported";
}

function routingPluginHint(pluginId: string): string {
  if (pluginId === "hash_sharding") return "runtime-supported hash routing";
  if (pluginId === "metatrack_coaccess_routing" || pluginId === "co_access_sharding") return "runtime-supported co-access light routing";
  if (pluginId === "hotspot_aware_routing") return "runtime-supported hotspot-aware light routing";
  return "planned/future strategy, not a real cross-shard protocol";
}

function statusDisabled(moduleId: string, status: DraftModuleStatus): boolean {
  if (moduleId === "MetricsReport") return status !== "output";
  if (requiredModuleIds.has(moduleId) && status === "disabled") return true;
  if (moduleId === "CommitteeEpoch" && status === "variable") return true;
  return false;
}
