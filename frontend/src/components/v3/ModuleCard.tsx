import type { V3ComposerModule } from "../../api";
import { statusLabels } from "./composerCatalog";
import { labelFor, moduleNames, pluginLabels, tagLabels } from "./localization";

type Props = {
  module: V3ComposerModule;
  selected: boolean;
  onSelect: (module: V3ComposerModule) => void;
  templateRole?: "variable" | "locked";
  topologyEnabled?: boolean;
};

export default function ModuleCard({ module, selected, onSelect, templateRole, topologyEnabled = false }: Props) {
  const plugin = module.plugin && module.plugin !== "none" ? module.plugin : "none";
  const status = (module.status in statusLabels ? module.status : "fixed") as keyof typeof statusLabels;
  const supportHint = moduleSupportHint(module.module_id, module.status, plugin);

  return (
    <button
      type="button"
      className={`v3-module-card v3-module-${status}${selected ? " selected" : ""}`}
      onClick={() => onSelect(module)}
      title={`${labelFor(moduleNames, module.module_id, module.display_name)} / ${module.module_id} / ${plugin} / ${supportHint}`}
    >
      <span className="v3-module-position">{module.position}</span>
      <strong>{labelFor(moduleNames, module.module_id, module.display_name)}</strong>
      <small title={module.module_id}>{module.module_id}</small>
      <span className="v3-plugin-id" title={plugin}>{labelFor(pluginLabels, plugin)}</span>
      <small title={plugin}>{plugin}</small>
      <span className={`v3-status-badge status-${status}`}>{statusLabels[status]}</span>
      {topologyEnabled && (
        <span className="v3-status-badge status-variable">
          拓扑启用
        </span>
      )}
      {templateRole && !topologyEnabled && (
        <span className={`v3-status-badge status-${templateRole === "variable" ? "variable" : "fixed"}`}>
          {templateRole === "variable" ? "实验变量" : "模板固定"}
        </span>
      )}
      <small title={supportHint}>{supportHint}</small>
      <span className="v3-tag-row">
        {(module.tags || []).slice(0, 3).map((tag) => (
          <span key={tag} className="v3-tag" title={tag}>{labelFor(tagLabels, tag)}</span>
        ))}
      </span>
    </button>
  );
}

function moduleSupportHint(moduleId: string, status: string, plugin: string): string {
  if (status === "planned") return "规划中";
  if (moduleId === "Consensus") {
    if (plugin === "simple_leader") return "可运行：简单 Leader";
    if (plugin === "poa_light") return "可运行：PoA 轻量模型";
    if (plugin === "pbft_light_model") return "可运行：PBFT 轻量模型，非真实 PBFT";
    if (plugin === "blockemulator_aligned_pbft_preview") return "可运行：PBFT 网络预览，非生产 PBFT";
    return "规划或不支持的共识";
  }
  if (moduleId === "Routing") {
    if (plugin === "hash_sharding") return "可运行：Hash 路由";
    if (plugin === "metatrack_coaccess_routing" || plugin === "co_access_sharding") return "可运行：共访问路由";
    if (plugin === "hotspot_aware_routing") return "可运行：热点感知路由";
    return "规划中的路由策略";
  }
  if (moduleId === "Execution") {
    if (plugin === "serial_execution") return "可运行：串行执行";
    if (plugin === "parallel_light_execution") return "可运行：轻量并行模型";
    if (plugin === "metatrack_dual_track_execution" || plugin === "dual_track_execution") return "可运行：双轨轻量模型";
    return "规划中的执行策略";
  }
  if (moduleId === "StateAccess") {
    if (plugin === "direct_fetch") return "可运行：直接读取";
    if (plugin === "remote_state_access_model") return "可运行：远程访问轻量模型";
    if (plugin === "cached_state_access") return "可运行：缓存访问轻量模型";
    if (plugin === "access_list_prefetch") return "可运行：访问列表预取";
    return "规划中的状态访问策略";
  }
  if (moduleId === "Commit") {
    if (plugin === "normal_commit") return "可运行：普通提交";
    if (plugin === "conservative_commit") return "可运行：保守提交";
    if (plugin === "hot_update_aggregation" || plugin === "hot_update_aggregation_commit") return "可运行：热点聚合";
    if (plugin === "constraint_checked_aggregation") return "可运行：约束检查聚合";
    return "规划中的提交策略";
  }
  if (status === "variable" || status === "fixed" || status === "default" || status === "output") return "当前配置可用于本地试运行";
  return String(status);
}
