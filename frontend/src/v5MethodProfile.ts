import type { V3SavedConfig, V5ExperimentSpec, V5FormalMethod, V5PluginManifest, V5PluginSelection } from "./api";

export const V5_METHOD_PROFILE_SCHEMA_VERSION = "v5_plugin_profile_v1";
export const V5_METHOD_EXCLUDED_CATEGORIES = new Set(["workload", "fault_injection"]);

export function defaultV5PluginSelections(catalog: V5PluginManifest[]): V5PluginSelection[] {
  return Array.from(new Set(catalog.map((item) => item.category))).map((category) => {
    const plugin = catalog.find((item) => item.category === category)!;
    return { category, plugin_id: plugin.plugin_id, config: { ...plugin.default_config } };
  });
}

export function v5MethodSelectionsFromCatalog(catalog: V5PluginManifest[]): V5PluginSelection[] {
  return defaultV5PluginSelections(catalog).filter((item) => !V5_METHOD_EXCLUDED_CATEGORIES.has(item.category));
}

export function parseSavedV5Method(saved: V3SavedConfig, catalog: V5PluginManifest[]): V5FormalMethod | null {
  const payload = saved.payload;
  if (saved.validation_status !== "runnable" || payload.schema_version !== V5_METHOD_PROFILE_SCHEMA_VERSION || !record(payload.compatibility_snapshot) || payload.compatibility_snapshot.valid !== true || !Array.isArray(payload.plugin_selections) || !payload.plugin_selections.length) return null;
  const overrides: Record<string, string> = {};
  for (const item of payload.plugin_selections) {
    if (!record(item) || typeof item.category !== "string" || !item.category || typeof item.plugin_id !== "string" || !item.plugin_id || V5_METHOD_EXCLUDED_CATEGORIES.has(item.category) || overrides[item.category] || !catalog.some((plugin) => plugin.category === item.category && plugin.plugin_id === item.plugin_id)) return null;
    overrides[item.category] = item.plugin_id;
  }
  const draft = record(payload.source_composer_draft) ? payload.source_composer_draft : {};
  const role = typeof draft.role === "string" && ["main", "baseline", "ablation", "custom"].includes(draft.role) ? draft.role : saved.tags.find((tag) => ["main", "baseline", "ablation", "custom"].includes(tag)) ?? "custom";
  return { method_id: saved.config_id, display_name: saved.name, plugin_overrides: overrides, role: role as V5FormalMethod["role"] };
}

export function buildV5MethodValidationSpec(catalog: V5PluginManifest[], methodSelections: V5PluginSelection[]): V5ExperimentSpec {
  const workload = catalog.find((item) => item.category === "workload" && item.plugin_id === "deterministic_signed_synthetic");
  const faults = catalog.find((item) => item.category === "fault_injection" && item.plugin_id === "faults_disabled");
  if (!workload) throw new Error("The real_cluster catalog does not provide workload/deterministic_signed_synthetic.");
  if (!faults) throw new Error("The real_cluster catalog does not provide fault_injection/faults_disabled.");
  const all = defaultV5PluginSelections(catalog).map((selection) => {
    const override = methodSelections.find((item) => item.category === selection.category);
    if (selection.category === "workload") return { category: "workload", plugin_id: workload.plugin_id, config: { ...workload.default_config, cross_shard_ratio: 0, timeout_every: 0 } };
    if (selection.category === "fault_injection") return { category: "fault_injection", plugin_id: faults.plugin_id, config: { ...faults.default_config } };
    if (override) {
      const plugin = catalog.find((item) => item.category === override.category && item.plugin_id === override.plugin_id);
      if (!plugin) throw new Error(`The real_cluster catalog does not provide ${override.category}/${override.plugin_id}.`);
      return { category: selection.category, plugin_id: plugin.plugin_id, config: { ...plugin.default_config } };
    }
    return selection;
  });
  return { name: "v5_method_profile_validation", execution_backend: "real_cluster", plugin_selections: all, topology: { nodes: 4, shards: 1, validators_per_shard: 4 }, tx_count: 20, seed: 11, duration_ms: 6000, fault_policy: { mode: "disabled" }, requested_metrics: [] };
}

export function applyV5MethodSelections(base: V5ExperimentSpec, method: V5FormalMethod): V5ExperimentSpec {
  return { ...base, topology: { ...base.topology }, fault_policy: { ...(base.fault_policy ?? {}) }, plugin_selections: base.plugin_selections.map((item) => ({ ...item, plugin_id: method.plugin_overrides[item.category] ?? item.plugin_id, config: { ...item.config } })) };
}

function record(value: unknown): value is Record<string, unknown> { return typeof value === "object" && value !== null; }
