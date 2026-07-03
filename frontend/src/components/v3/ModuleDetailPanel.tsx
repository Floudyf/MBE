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
          <p className="muted">V3.8 keeps CrossShardProtocol as a Routing/Sharding sub-capability. cross_shard_protocol supports none and relay_preview skeleton artifacts; broker_preview and two_phase_commit_preview remain planned. This is not full Relay, Broker, 2PC, atomic cross-shard commit, state proof, rollback, timeout recovery, CLPA, ShardCutter, or state migration.</p>
        )}
        {selectedModule.module_id === "Execution" && (
          <p className="muted">V3.4.6 realizes Execution records for serial_execution, parallel_light_execution, and metatrack_dual_track_execution. Execution estimates scheduling order, dependency edges, logical workers, blocking, and fast/conservative tracks; it does not implement real concurrent execution, rollback, Block-STM, Calvin, or database lock management.</p>
        )}
        {selectedModule.module_id === "StateAccess" && (
          <p className="muted">V3.4.7 realizes StateAccess records for direct_fetch, remote_state_access_model, cached_state_access, and access_list_prefetch. StateAccess estimates local/remote access, cache/prefetch hits, latency, and proof/witness sizes; it does not implement real remote storage, real proofs, witnesses, MPT, state root, persistent KV, snapshot, or state migration.</p>
        )}
        {selectedModule.module_id === "Commit" && (
          <p className="muted">V3.4.8 realizes Commit records for normal_commit, conservative_commit, hot_update_aggregation, and constraint_checked_aggregation. These are deterministic local commit light models; they do not implement real database locking, real concurrent commit, rollback, MPT/state root, persistent KV, or snapshots.</p>
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
                {selectedModule.module_id === "Execution" && (
                  <small>{executionPluginHint(plugin.id)}</small>
                )}
                {selectedModule.module_id === "StateAccess" && (
                  <small>{stateAccessPluginHint(plugin.id)}</small>
                )}
                {selectedModule.module_id === "Commit" && (
                  <small>{commitPluginHint(plugin.id)}</small>
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

function executionPluginHint(pluginId: string): string {
  if (pluginId === "serial_execution") return "runtime-supported serial execution";
  if (pluginId === "parallel_light_execution") return "runtime-supported deterministic logical parallel model";
  if (pluginId === "metatrack_dual_track_execution" || pluginId === "dual_track_execution") return "runtime-supported dual-track light model";
  return "planned/future execution strategy, not real concurrency or rollback";
}

function stateAccessPluginHint(pluginId: string): string {
  if (pluginId === "direct_fetch") return "runtime-supported direct fetch";
  if (pluginId === "remote_state_access_model") return "runtime-supported remote access light model";
  if (pluginId === "cached_state_access") return "runtime-supported cache light model";
  if (pluginId === "access_list_prefetch") return "runtime-supported prefetch light model";
  return "planned/future state access strategy, not real proof, witness, MPT, or remote storage";
}

function commitPluginHint(pluginId: string): string {
  if (pluginId === "normal_commit") return "runtime-supported default commit path";
  if (pluginId === "conservative_commit") return "runtime-supported conservative commit light model";
  if (pluginId === "hot_update_aggregation" || pluginId === "hot_update_aggregation_commit") return "runtime-supported hot-update aggregation light model";
  if (pluginId === "constraint_checked_aggregation") return "runtime-supported constraint-check aggregation light model";
  return "planned/future commit strategy, not real DB lock, rollback, MPT/state root, or persistent KV";
}

function statusDisabled(moduleId: string, status: DraftModuleStatus): boolean {
  if (moduleId === "MetricsReport") return status !== "output";
  if (requiredModuleIds.has(moduleId) && status === "disabled") return true;
  if (moduleId === "CommitteeEpoch" && status === "variable") return true;
  return false;
}
