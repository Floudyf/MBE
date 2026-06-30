import type { V3ComposerModule } from "../../api";
import { statusLabels } from "./composerCatalog";
import { labelFor, moduleNames, tagLabels } from "./localization";

type Props = {
  module: V3ComposerModule;
  selected: boolean;
  onSelect: (module: V3ComposerModule) => void;
  templateRole?: "variable" | "locked";
};

export default function ModuleCard({ module, selected, onSelect, templateRole }: Props) {
  const plugin = module.plugin && module.plugin !== "none" ? module.plugin : "无";

  const status = (module.status in statusLabels ? module.status : "fixed") as keyof typeof statusLabels;
  const supportHint = moduleSupportHint(module.module_id, module.status, plugin);

  return (
    <button
      type="button"
      className={`v3-module-card v3-module-${status}${selected ? " selected" : ""}`}
      onClick={() => onSelect(module)}
      title={`${module.display_name || module.module_id} / ${module.module_id} / ${plugin} / ${supportHint}`}
    >
      <span className="v3-module-position">{module.position}</span>
      <strong>{labelFor(moduleNames, module.module_id, module.display_name)}</strong>
      <small title={module.module_id}>{module.module_id}</small>
      <span className="v3-plugin-id" title={plugin}>插件：{plugin}</span>
      <span className={`v3-status-badge status-${status}`}>{statusLabels[status]}</span>
      {templateRole && (
        <span className={`v3-status-badge status-${templateRole === "variable" ? "variable" : "fixed"}`}>
          {templateRole === "variable" ? "variable module" : "locked by template"}
        </span>
      )}
      <small title={supportHint}>{supportHint}</small>
      <span className="v3-tag-row">
        {(module.tags || []).map((tag) => (
          <span key={tag} className="v3-tag" title={tag}>{labelFor(tagLabels, tag)}</span>
        ))}
      </span>
    </button>
  );
}

function moduleSupportHint(moduleId: string, status: string, plugin: string): string {
  if (status === "planned") return "planned";
  if (moduleId === "Consensus") {
    if (plugin === "simple_leader") return "runtime-supported simple leader";
    if (plugin === "poa_light") return "runtime-supported PoA-light";
    if (plugin === "pbft_light_model") return "runtime-supported PBFT-light model, not real PBFT";
    return "planned or unsupported consensus";
  }
  if (moduleId === "Routing") {
    if (plugin === "hash_sharding") return "runtime-supported hash routing";
    if (plugin === "metatrack_coaccess_routing" || plugin === "co_access_sharding") return "runtime-supported co-access light routing";
    if (plugin === "hotspot_aware_routing") return "runtime-supported hotspot-aware light routing";
    return "planned/future routing strategy";
  }
  if (moduleId === "Execution") {
    if (plugin === "serial_execution") return "runtime-supported serial execution";
    if (plugin === "parallel_light_execution") return "runtime-supported parallel-light model";
    if (plugin === "metatrack_dual_track_execution" || plugin === "dual_track_execution") return "runtime-supported dual-track light model";
    return "planned/future execution strategy";
  }
  if (moduleId === "StateAccess") {
    if (plugin === "direct_fetch") return "runtime-supported direct fetch";
    if (plugin === "remote_state_access_model") return "runtime-supported remote access light model";
    if (plugin === "cached_state_access") return "runtime-supported cache light model";
    if (plugin === "access_list_prefetch") return "runtime-supported prefetch light model";
    return "planned/future state access strategy";
  }
  if (status === "variable" || status === "fixed" || status === "default") return "configured runnable; runtime support depends on backend validation";
  return String(status);
}
