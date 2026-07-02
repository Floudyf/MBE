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
          <p className="eyebrow">V3.7.2 runtime support layer</p>
          <h3>Runtime Topology / Node Topology</h3>
        </div>
      </div>
      <p className="muted">Configures logical nodes and the runtime NetworkAdapter. V3.7.2 supports optional ConsensusRuntime PBFT preview over the selected NetworkAdapter path; it is not production networking, not production PBFT, and not a BlockEmulator backend.</p>
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
