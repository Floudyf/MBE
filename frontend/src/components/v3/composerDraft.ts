import type { V3ComposerDraftRequest, V3ComposerModule, V3ComposerPreview, V3RuntimeTopology } from "../../api";
import {
  type DraftModuleStatus,
  type DraftPluginOption,
  composerCatalog,
  moduleCatalogEntry,
  optionalModuleIds,
  pluginOption,
  requiredModuleIds,
} from "./composerCatalog";

export type DraftParamValue = string | number | boolean;

export type ComposerDraftModule = {
  moduleId: string;
  status: DraftModuleStatus;
  plugin: string;
  runnable: boolean;
  params: Record<string, DraftParamValue>;
};

export type ComposerDraft = {
  templateId: string;
  presetId?: string;
  modules: Record<string, ComposerDraftModule>;
  variableModules: string[];
  fixedModules: string[];
  disabledModules: string[];
  plannedModules: string[];
  outputModules: string[];
  isRunnable: boolean;
  hasValidationErrors: boolean;
  validationMessages: string[];
  topology: V3RuntimeTopology;
};

export type DraftModuleView = V3ComposerModule & {
  draftStatus: DraftModuleStatus;
  draftPlugin: string;
  draftRunnable: boolean;
};

export type DraftValidation = {
  isRunnable: boolean;
  messages: string[];
  errors: string[];
};

export function createComposerDraft(preview: V3ComposerPreview): ComposerDraft {
  const modules = Object.fromEntries(
    (preview.modules || []).map((module) => {
      const entry = moduleCatalogEntry(module.module_id);
      const plugin = normalizePlugin(module, entry.defaultPlugin);
      const status = initialStatus(module);
      const option = pluginOption(module.module_id, plugin);
      return [module.module_id, {
        moduleId: module.module_id,
        status,
        plugin,
        runnable: status !== "planned" && option.status === "runnable",
        params: initialParams(module.module_id),
      }];
    }),
  );
  return summarizeDraft({ templateId: preview.template_id, modules });
}

export function summarizeDraft(input: Pick<ComposerDraft, "templateId" | "modules"> & { presetId?: string; topology?: V3RuntimeTopology }): ComposerDraft {
  const modules = input.modules;
  const values = Object.values(modules);
  const validation = validateDraft(modules);
  return {
    templateId: input.templateId,
    presetId: input.presetId,
    modules,
    topology: input.topology || defaultRuntimeTopology(),
    variableModules: values.filter((module) => module.status === "variable").map((module) => module.moduleId),
    fixedModules: values.filter((module) => module.status === "fixed" || module.status === "default").map((module) => module.moduleId),
    disabledModules: values.filter((module) => module.status === "disabled").map((module) => module.moduleId),
    plannedModules: values.filter((module) => module.status === "planned").map((module) => module.moduleId),
    outputModules: values.filter((module) => module.status === "output").map((module) => module.moduleId),
    isRunnable: validation.isRunnable,
    hasValidationErrors: validation.errors.length > 0,
    validationMessages: validation.messages,
  };
}

export function updateDraftModule(
  draft: ComposerDraft,
  moduleId: string,
  patch: Partial<Pick<ComposerDraftModule, "status" | "plugin" | "params">>,
): ComposerDraft {
  const current = draft.modules[moduleId];
  if (!current) return draft;
  const nextStatus = normalizeStatus(moduleId, patch.status ?? current.status);
  const nextPlugin = patch.plugin ?? current.plugin;
  const option = pluginOption(moduleId, nextPlugin);
  const nextModules = {
    ...draft.modules,
    [moduleId]: {
      ...current,
      ...patch,
      status: nextStatus,
      plugin: nextPlugin,
      runnable: nextStatus !== "planned" && option.status === "runnable",
      params: patch.params ?? current.params,
    },
  };
  return summarizeDraft({ templateId: draft.templateId, presetId: draft.presetId, modules: nextModules, topology: draft.topology });
}

export function toComposerDraftRequest(draft: ComposerDraft): V3ComposerDraftRequest {
  return {
    template_id: draft.templateId,
    preset_id: draft.presetId,
    topology: draft.topology,
    modules: Object.fromEntries(
      Object.values(draft.modules).map((module) => [module.moduleId, {
        module_id: module.moduleId,
        status: module.status,
        plugin: module.plugin,
        params: module.params,
      }]),
    ),
  };
}

export function defaultRuntimeTopology(): V3RuntimeTopology {
  return {
    shard_count: 4,
    validators_per_shard: 4,
    executors_per_shard: 1,
    storage_nodes_per_shard: 1,
    supervisor_enabled: true,
    node_runtime_mode: "logical_single_process",
    network_mode: "in_memory_message_bus",
  network_adapter: "in_memory_message_bus",
  cross_shard_protocol: "none",
  relay_failure_mode: "none",
  relay_force_proof_fail_every_n: 0,
  relay_force_timeout_every_n: 0,
  relay_timeout_ms: 0,
  state_backend: "memory_kv",
  benchmark_template: "full_stack_v3_template",
  baseline_profile: "baseline_simple_chain",
  repeat_count: 1,
};
}

export function updateDraftTopology(draft: ComposerDraft, topology: V3RuntimeTopology): ComposerDraft {
  return summarizeDraft({ templateId: draft.templateId, presetId: draft.presetId, modules: draft.modules, topology });
}

export function moduleView(module: V3ComposerModule, draft?: ComposerDraft): DraftModuleView {
  const draftModule = draft?.modules[module.module_id];
  if (!draftModule) {
    return {
      ...module,
      draftStatus: module.status as DraftModuleStatus,
      draftPlugin: module.plugin || "",
      draftRunnable: true,
    };
  }
  return {
    ...module,
    status: draftModule.status,
    plugin: draftModule.plugin,
    draftStatus: draftModule.status,
    draftPlugin: draftModule.plugin,
    draftRunnable: draftModule.runnable,
  };
}

export function pluginOptionsForModule(module: V3ComposerModule): DraftPluginOption[] {
  const entry = moduleCatalogEntry(module.module_id);
  const allowed = module.allowed_plugins || [];
  const merged = new Map(entry.plugins.map((plugin) => [plugin.id, plugin]));
  allowed.forEach((pluginId) => {
    if (!merged.has(pluginId)) merged.set(pluginId, pluginOption(module.module_id, pluginId));
  });
  const current = normalizePlugin(module, entry.defaultPlugin);
  if (current && !merged.has(current)) merged.set(current, pluginOption(module.module_id, current));
  return Array.from(merged.values());
}

export function validateDraft(modules: Record<string, ComposerDraftModule>): DraftValidation {
  const errors: string[] = [];
  const messages: string[] = [];
  let hasPreviewOnlyPlugin = false;
  requiredModuleIds.forEach((moduleId) => {
    const module = modules[moduleId];
    const label = moduleCatalogEntry(moduleId).label;
    if (!module) errors.push(`${label} 是必需模块，当前 Draft 缺少该模块。`);
    if (module && module.status === "disabled") errors.push(`${label} 是必需模块，不能关闭。`);
    if (module && !module.plugin) errors.push(`${label} 是必需模块，必须保留一个默认插件。`);
  });

  Object.values(modules).forEach((module) => {
    const entry = moduleCatalogEntry(module.moduleId);
    const plugin = pluginOption(module.moduleId, module.plugin);
    if (plugin.status === "planned") {
      errors.push(`${entry.label} 的 ${plugin.label} 当前为规划中插件，不能用于运行。`);
    }
    if (plugin.status === "preview") {
      hasPreviewOnlyPlugin = true;
      messages.push(`${entry.label} 的 ${plugin.label} 仅用于预览，不能进入自定义运行。`);
    }
    if (module.status === "variable" && module.moduleId === "MetricsReport") {
      errors.push("指标 / 报告是输出模块，不能作为实验变量。");
    }
    if (module.status === "variable" && module.moduleId === "CommitteeEpoch") {
      errors.push("委员会 / Epoch 当前为规划中模块，不能作为可运行实验变量。");
    }
    if (!entry.allowVariable && module.status === "variable" && module.moduleId !== "MetricsReport") {
      messages.push(`${entry.label} 当前作为实验变量仅用于 Draft 预览。`);
    }
    if (!entry.allowDisable && module.status === "disabled" && !optionalModuleIds.has(module.moduleId)) {
      errors.push(`${entry.label} 不支持关闭；不选表示使用默认配置或固定环境。`);
    }
    if (module.status === "fixed" || module.status === "default") {
      if (!module.plugin) errors.push(`${entry.label} 需要保留默认插件。`);
    }
  });

  if (errors.length === 0) {
    messages.unshift("当前 Draft 可预览。");
  }
  messages.push("当前草稿配置使用快速验证链路运行；快速验证不代表论文级正式实验。");

  return { isRunnable: errors.length === 0 && !hasPreviewOnlyPlugin, messages: [...messages, ...errors], errors };
}

function normalizePlugin(module: V3ComposerModule, fallback: string): string {
  if (module.module_id === "CommitteeEpoch") return "disabled";
  if (!module.plugin || module.plugin === "none" || module.plugin === "varies") return fallback;
  return module.plugin;
}

function initialStatus(module: V3ComposerModule): DraftModuleStatus {
  if (module.module_id === "MetricsReport") return "output";
  if (module.module_id === "CommitteeEpoch") return "disabled";
  if (module.status === "variable") return "variable";
  if (module.status === "disabled") return requiredModuleIds.has(module.module_id) ? "fixed" : "disabled";
  if (module.status === "planned") return requiredModuleIds.has(module.module_id) ? "fixed" : "disabled";
  return "fixed";
}

function normalizeStatus(moduleId: string, status: DraftModuleStatus): DraftModuleStatus {
  if (moduleId === "MetricsReport") return "output";
  if (requiredModuleIds.has(moduleId) && status === "disabled") return "fixed";
  if (moduleId === "CommitteeEpoch" && status === "variable") return "planned";
  return status;
}

function initialParams(moduleId: string): Record<string, DraftParamValue> {
  const params = composerCatalog[moduleId]?.params || [];
  return Object.fromEntries(params.map((param) => [param, ""]));
}
