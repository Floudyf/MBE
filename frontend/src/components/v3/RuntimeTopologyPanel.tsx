import type { V3RuntimeTopology } from "../../api";

type Props = {
  topology: V3RuntimeTopology;
  onChange: (topology: V3RuntimeTopology) => void;
};

const numericFields: Array<keyof Pick<V3RuntimeTopology, "shard_count" | "validators_per_shard" | "executors_per_shard" | "storage_nodes_per_shard">> = [
  "shard_count",
  "validators_per_shard",
  "executors_per_shard",
  "storage_nodes_per_shard",
];

export default function RuntimeTopologyPanel({ topology, onChange }: Props) {
  const summary = topologySummary(topology);
  function patch(patchValue: Partial<V3RuntimeTopology>) {
    onChange({ ...topology, ...patchValue });
  }
  return (
    <section className="final-card wide v3-template-bar">
      <div className="v3-section-head">
        <div>
          <p className="eyebrow">V3.10 experiment control / benchmark hardening</p>
          <h3>Runtime Topology / Node Topology</h3>
        </div>
      </div>
      <p className="muted">Configures logical nodes, NetworkAdapter, Routing/Sharding cross_shard_protocol, StateAccess / StateStorage / Commit state_backend, and V3.10 benchmark template controls. Benchmark is experiment control / result layer only; it is not paper-grade evidence or a large-scale distributed benchmark.</p>
      <div className="v3-identity-grid">
        {numericFields.map((field) => (
          <label key={field}>
            <span>{field}</span>
            <input
              type="number"
              min={field === "executors_per_shard" || field === "storage_nodes_per_shard" ? 0 : 1}
              max={field === "shard_count" ? 32 : 64}
              value={topology[field]}
              onChange={(event) => patch({ [field]: Number(event.target.value) } as Partial<V3RuntimeTopology>)}
            />
          </label>
        ))}
        <label>
          <span>supervisor_enabled</span>
          <input type="checkbox" checked={topology.supervisor_enabled} onChange={(event) => patch({ supervisor_enabled: event.target.checked })} />
        </label>
        <label>
          <span>node_runtime_mode</span>
          <select value={topology.node_runtime_mode} onChange={(event) => patch({ node_runtime_mode: event.target.value })}>
            <option value="logical_single_process">logical_single_process</option>
          </select>
        </label>
        <label>
          <span>network_adapter</span>
          <select value={topology.network_adapter || topology.network_mode} onChange={(event) => patch({ network_adapter: event.target.value, network_mode: event.target.value })}>
            <option value="in_memory_message_bus">in_memory_message_bus</option>
            <option value="localhost_tcp_preview">localhost_tcp_preview</option>
          </select>
        </label>
        <label>
          <span>cross_shard_protocol</span>
          <select value={topology.cross_shard_protocol || "none"} onChange={(event) => patch({ cross_shard_protocol: event.target.value })}>
            <option value="none">none</option>
            <option value="relay_preview">relay_preview</option>
            <option value="broker_preview" disabled>broker_preview planned</option>
            <option value="two_phase_commit_preview" disabled>two_phase_commit_preview planned</option>
          </select>
        </label>
        <label>
          <span>state_backend</span>
          <select value={topology.state_backend || "memory_kv"} onChange={(event) => patch({ state_backend: event.target.value })}>
            <option value="memory_kv">memory_kv</option>
            <option value="persistent_kv">persistent_kv</option>
            <option value="merkle_trie_mvp">merkle_trie_mvp</option>
            <option value="ethereum_mpt_compatible" disabled>ethereum_mpt_compatible planned</option>
          </select>
        </label>
        <label>
          <span>benchmark_template</span>
          <select value={topology.benchmark_template || "full_stack_v3_template"} onChange={(event) => patch({ benchmark_template: event.target.value })}>
            <option value="metatrack_hotspot_template">metatrack_hotspot_template</option>
            <option value="pbft_network_template">pbft_network_template</option>
            <option value="cross_shard_relay_preview_template">cross_shard_relay_preview_template</option>
            <option value="state_authenticity_template">state_authenticity_template</option>
            <option value="full_stack_v3_template">full_stack_v3_template</option>
          </select>
        </label>
        <label>
          <span>baseline_profile</span>
          <select value={topology.baseline_profile || "baseline_simple_chain"} onChange={(event) => patch({ baseline_profile: event.target.value })}>
            <option value="baseline_simple_chain">baseline_simple_chain</option>
            <option value="baseline_hash_sharding">baseline_hash_sharding</option>
            <option value="baseline_no_prefetch">baseline_no_prefetch</option>
            <option value="baseline_no_cross_shard_protocol">baseline_no_cross_shard_protocol</option>
            <option value="baseline_memory_kv">baseline_memory_kv</option>
            <option value="baseline_no_state_authenticity">baseline_no_state_authenticity</option>
          </select>
        </label>
        <label>
          <span>repeat_count</span>
          <input type="number" min={1} max={20} value={topology.repeat_count || 1} onChange={(event) => patch({ repeat_count: Number(event.target.value) })} />
        </label>
      </div>
      <dl className="v3-result-grid">
        {Object.entries(summary).map(([key, value]) => (
          <div key={key}><dt>{key}</dt><dd>{String(value)}</dd></div>
        ))}
      </dl>
    </section>
  );
}

function topologySummary(topology: V3RuntimeTopology): Record<string, number> {
  const validator_node_count = topology.shard_count * topology.validators_per_shard;
  const executor_node_count = topology.shard_count * topology.executors_per_shard;
  const storage_node_count = topology.shard_count * topology.storage_nodes_per_shard;
  const supervisor_node_count = topology.supervisor_enabled ? 1 : 0;
  return {
    total_logical_nodes: validator_node_count + executor_node_count + storage_node_count + supervisor_node_count,
    validator_node_count,
    executor_node_count,
    storage_node_count,
    supervisor_node_count,
    consensus_domain_count: topology.shard_count,
  };
}
