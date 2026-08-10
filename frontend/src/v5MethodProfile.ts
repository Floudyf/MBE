import type { V3SavedConfig, V5ExperimentSpec, V5FormalMethod, V5PluginManifest, V5PluginSelection } from "./api";

export const V5_METHOD_PROFILE_SCHEMA_VERSION = "v5_plugin_profile_v1";
export const V5_METHOD_EXCLUDED_CATEGORIES = new Set(["workload", "fault_injection"]);
export const V5_DEFAULT_METHOD_IDS = ["hash_serial", "hash_block_stm", "metatrack_serial", "metatrack_block_stm"] as const;

const V5_CANONICAL_DEFAULT_PLUGIN_IDS: Record<string, string> = {
  workload: "deterministic_signed_synthetic",
  transaction_admission: "signature_nonce_admission",
  txpool: "fifo_per_node_mempool",
  sharding: "deterministic_state_key_sharding",
  routing: "hash_routing_baseline",
  block_producer: "time_or_count_block_producer",
  consensus: "pbft_style_consensus",
  network: "localhost_tcp_typed_network",
  execution: "serial_execution_baseline",
  scheduler: "fifo_serial_scheduler",
  block_executor: "serial_block_executor",
  state_access: "direct_state_access",
  state_storage: "persistent_local_state_store",
  cross_shard: "relay_certificate_protocol",
  commit: "normal_commit",
  fault_injection: "faults_disabled",
  metrics: "runtime_core_metrics",
  observability: "node_network_consensus_observer",
};

export const V5_BUILTIN_METHODS: V5FormalMethod[] = [
  { method_id: "hash_serial", display_name: "Stateful Hash + Serial Reference", role: "baseline", plugin_overrides: { routing: "hash_routing_baseline", execution: "serial_execution_baseline", scheduler: "fifo_serial_scheduler", block_executor: "serial_block_executor", commit: "normal_commit" }, plugin_config_overrides: { block_executor: { worker_count: 1 } } },
  { method_id: "hash_block_stm", display_name: "Stateful Hash + Block-STM Reference", role: "main", plugin_overrides: { routing: "hash_routing_baseline", execution: "serial_execution_baseline", scheduler: "fifo_serial_scheduler", block_executor: "block_stm_block_executor", commit: "normal_commit" }, plugin_config_overrides: { block_executor: { worker_count: 4, execution_mode: "performance", oracle_mode: "off", maximum_incarnations: 0, incarnation_limit_action: "fail" } } },
  { method_id: "hash_aria", display_name: "Aria", role: "baseline", plugin_overrides: { routing: "hash_routing_baseline", block_producer: "aria_block_producer", execution: "serial_execution_baseline", scheduler: "fifo_serial_scheduler", block_executor: "aria_block_executor", commit: "normal_commit" }, plugin_config_overrides: { block_producer: { candidate_scan_multiplier: 1, reordering: true, read_only_optimization: true, retry_nonce_gaps: true }, block_executor: { worker_count: 4, reordering: true, read_only_optimization: true, retry_nonce_gaps: true } } },
  { method_id: "hash_groundhog", display_name: "Groundhog", role: "baseline", plugin_overrides: { routing: "hash_routing_baseline", block_producer: "groundhog_block_producer", execution: "serial_execution_baseline", scheduler: "fifo_serial_scheduler", block_executor: "groundhog_block_executor", commit: "normal_commit" }, plugin_config_overrides: { block_producer: { candidate_scan_multiplier: 4, ordered_set_limit: 64 }, block_executor: { worker_count: 4, ordered_set_limit: 64 } } },
  { method_id: "hash_cg", display_name: "Conflict Graph (CG)", role: "baseline", plugin_overrides: { routing: "hash_routing_baseline", execution: "cg_execution", scheduler: "cg_scheduler", block_executor: "cg_block_executor", commit: "normal_commit" }, plugin_config_overrides: { block_executor: { worker_count: 4 } } },
  { method_id: "hash_acg", display_name: "Address Conflict Graph (ACG/Nezha)", role: "baseline", plugin_overrides: { routing: "hash_routing_baseline", execution: "acg_execution", scheduler: "acg_scheduler", block_executor: "acg_block_executor", commit: "normal_commit" }, plugin_config_overrides: { block_executor: { worker_count: 4 } } },
  { method_id: "hash_bsx", display_name: "Batch-Schedule-Execute (BSX)", role: "baseline", plugin_overrides: { routing: "hash_routing_baseline", execution: "bsx_execution", scheduler: "bsx_scheduler", block_executor: "bsx_block_executor", commit: "normal_commit" }, plugin_config_overrides: { block_executor: { worker_count: 4 } } },
  { method_id: "hash_batch_si", display_name: "Batch-SI", role: "main", plugin_overrides: { routing: "hash_routing_baseline", execution: "batch_si_execution", scheduler: "batch_si_scheduler", block_executor: "batch_si_block_executor", commit: "normal_commit" }, plugin_config_overrides: { scheduler: { partition_mode: "wrbp", ordering_mode: "ofas", priority_mode: "paper" }, block_executor: { worker_count: 4, partition_mode: "wrbp", ordering_mode: "ofas", priority_mode: "paper", execution_mode: "snapshot_parallel" } } },
  { method_id: "hash_batch_si_no_wrbp", display_name: "Batch-SI w/o WRBP", role: "ablation", plugin_overrides: { routing: "hash_routing_baseline", execution: "batch_si_execution", scheduler: "batch_si_scheduler", block_executor: "batch_si_block_executor", commit: "normal_commit" }, plugin_config_overrides: { scheduler: { partition_mode: "sequential", ordering_mode: "ofas", priority_mode: "paper" }, block_executor: { worker_count: 4, partition_mode: "sequential", ordering_mode: "ofas", priority_mode: "paper", execution_mode: "snapshot_parallel" } } },
  { method_id: "hash_batch_si_no_ofas", display_name: "Batch-SI w/o OFAS", role: "ablation", plugin_overrides: { routing: "hash_routing_baseline", execution: "batch_si_execution", scheduler: "batch_si_scheduler", block_executor: "batch_si_block_executor", commit: "normal_commit" }, plugin_config_overrides: { scheduler: { partition_mode: "wrbp", ordering_mode: "dependency_graph", priority_mode: "paper" }, block_executor: { worker_count: 4, partition_mode: "wrbp", ordering_mode: "dependency_graph", priority_mode: "paper", execution_mode: "snapshot_parallel" } } },
  { method_id: "hash_batch_si_serial_batch", display_name: "Batch-SI w/o Snapshot Parallelism", role: "ablation", plugin_overrides: { routing: "hash_routing_baseline", execution: "batch_si_execution", scheduler: "batch_si_scheduler", block_executor: "batch_si_block_executor", commit: "normal_commit" }, plugin_config_overrides: { scheduler: { partition_mode: "wrbp", ordering_mode: "ofas", priority_mode: "paper" }, block_executor: { worker_count: 4, partition_mode: "wrbp", ordering_mode: "ofas", priority_mode: "paper", execution_mode: "snapshot_serial" } } },
  { method_id: "hash_batch_si_txid_priority", display_name: "Batch-SI w/o OFAS Priority", role: "ablation", plugin_overrides: { routing: "hash_routing_baseline", execution: "batch_si_execution", scheduler: "batch_si_scheduler", block_executor: "batch_si_block_executor", commit: "normal_commit" }, plugin_config_overrides: { scheduler: { partition_mode: "wrbp", ordering_mode: "ofas", priority_mode: "txid" }, block_executor: { worker_count: 4, partition_mode: "wrbp", ordering_mode: "ofas", priority_mode: "txid", execution_mode: "snapshot_parallel" } } },
  { method_id: "stateless_hash_serial", display_name: "Stateless Hash + Serial", role: "baseline", plugin_overrides: { routing: "stateless_hash_routing", execution: "serial_execution_baseline", scheduler: "fifo_serial_scheduler", block_executor: "serial_block_executor", commit: "normal_commit" }, plugin_config_overrides: { block_executor: { worker_count: 1 } } },
  { method_id: "stateless_hash_block_stm", display_name: "Stateless Hash + Block-STM", role: "compatibility", plugin_overrides: { routing: "stateless_hash_routing", execution: "serial_execution_baseline", scheduler: "fifo_serial_scheduler", block_executor: "block_stm_block_executor", commit: "normal_commit" }, plugin_config_overrides: { block_executor: { worker_count: 4, execution_mode: "performance", oracle_mode: "off", maximum_incarnations: 0, incarnation_limit_action: "fail" } } },
  // MBE_META_TRACK_RAPID_FIX_V3
  { method_id: "metatrack_serial", display_name: "MetaTrack", role: "main", plugin_overrides: { routing: "metatrack_coaccess_routing", execution: "dual_track_execution", scheduler: "fast_first_scheduler", block_executor: "metatrack_block_executor", commit: "commutative_hot_update_aggregation" }, plugin_config_overrides: { execution: { access_size_threshold: 4 }, block_executor: { worker_count: 4 } } },
  { method_id: "metatrack_block_stm", display_name: "MetaTrack with Block-STM backend", role: "compatibility", plugin_overrides: { routing: "metatrack_coaccess_routing", execution: "dual_track_execution", scheduler: "fast_first_scheduler", block_executor: "block_stm_block_executor", commit: "commutative_hot_update_aggregation" }, plugin_config_overrides: { execution: { access_size_threshold: 4 }, block_executor: { worker_count: 4, execution_mode: "performance", oracle_mode: "off", maximum_incarnations: 0, incarnation_limit_action: "fail" } } },
];

export function defaultV5PluginSelections(catalog: V5PluginManifest[]): V5PluginSelection[] {
  return Array.from(new Set(catalog.map((item) => item.category))).map((category) => {
    const canonicalPluginId = V5_CANONICAL_DEFAULT_PLUGIN_IDS[category];
    const plugin = catalog.find((item) => item.category === category && item.plugin_id === canonicalPluginId)
      ?? catalog.find((item) => item.category === category);
    if (!plugin) throw new Error(`The V5 plugin catalog does not provide category ${category}.`);
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
  if (!overrides.block_executor) {
    const fallback = catalog.find((plugin) => plugin.category === "block_executor" && plugin.plugin_id === "serial_block_executor") ?? catalog.find((plugin) => plugin.category === "block_executor");
    if (!fallback) return null;
    overrides.block_executor = fallback.plugin_id;
  }
  const draft = record(payload.source_composer_draft) ? payload.source_composer_draft : {};
  const role = typeof draft.role === "string" && ["main", "baseline", "ablation", "compatibility", "custom"].includes(draft.role) ? draft.role : saved.tags.find((tag) => ["main", "baseline", "ablation", "compatibility", "custom"].includes(tag)) ?? "custom";
  const configOverrides = record(payload.plugin_config_overrides) ? payload.plugin_config_overrides as Record<string, Record<string, unknown>> : {};
  return { method_id: saved.config_id, display_name: saved.name, plugin_overrides: overrides, plugin_config_overrides: configOverrides, role: role as V5FormalMethod["role"] };
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

export function applyV5MethodSelections(base: V5ExperimentSpec, method: V5FormalMethod, catalog: V5PluginManifest[] = []): V5ExperimentSpec {
  return {
    ...base,
    topology: { ...base.topology },
    fault_policy: { ...(base.fault_policy ?? {}) },
    plugin_selections: base.plugin_selections.map((item) => {
      const pluginId = method.plugin_overrides[item.category] ?? item.plugin_id;
      const plugin = catalog.find((candidate) => candidate.category === item.category && candidate.plugin_id === pluginId);
      const config: Record<string, unknown> = { ...(pluginId !== item.plugin_id && plugin ? plugin.default_config : item.config) };
      if (item.category === "block_producer" && pluginId !== item.plugin_id) {
        for (const key of ["block_size", "interval_ms", "block_interval_ms"]) {
          if (key in item.config) config[key] = item.config[key];
        }
      }
      Object.assign(config, method.plugin_config_overrides?.[item.category] ?? {});
      return { ...item, plugin_id: pluginId, config };
    }),
  };
}

function record(value: unknown): value is Record<string, unknown> { return typeof value === "object" && value !== null; }
