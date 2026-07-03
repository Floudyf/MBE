import type { ReactNode } from "react";
import type { V3RuntimeTopology } from "../../api";
import HelpTip from "./HelpTip";

type Props = {
  topology: V3RuntimeTopology;
  onChange: (topology: V3RuntimeTopology) => void;
};

type SelectOption = [string, string, boolean?];

const benchmarkTemplates: SelectOption[] = [
  ["full_stack_v3_template", "V3 全栈快速验证模板"],
  ["state_authenticity_template", "状态真实性模板"],
  ["cross_shard_relay_preview_template", "跨片 Relay 预览模板"],
  ["pbft_network_template", "PBFT 网络预览模板"],
  ["metatrack_hotspot_template", "MetaTrack 热点负载模板"],
];

const baselineProfiles: SelectOption[] = [
  ["baseline_simple_chain", "简单链基线"],
  ["baseline_hash_sharding", "Hash 分片基线"],
  ["baseline_no_prefetch", "无预取基线"],
  ["baseline_no_cross_shard_protocol", "无跨片协议基线"],
  ["baseline_memory_kv", "内存 KV 基线"],
  ["baseline_no_state_authenticity", "无状态真实性基线"],
];

export default function RuntimeTopologyPanel({ topology, onChange }: Props) {
  const summary = topologySummary(topology);
  function patch(patchValue: Partial<V3RuntimeTopology>) {
    onChange({ ...topology, ...patchValue });
  }

  return (
    <section className="final-card wide topology-console">
      <div className="v3-section-head">
        <div>
          <p className="eyebrow">实验配置</p>
          <h3>运行拓扑与实验控制</h3>
        </div>
        <HelpTip title="运行拓扑">
          这里配置的是本地 V3 实验控制台使用的逻辑拓扑、协议预览和实验模板，不会把系统变成生产级多节点网络。
        </HelpTip>
      </div>

      <div className="topology-groups">
        <ConfigGroup title="节点拓扑">
          <NumberField label="分片数量" id="shard_count" value={topology.shard_count} min={1} max={32} onChange={(value) => patch({ shard_count: value })}>
            决定逻辑分片数量。默认 4。
          </NumberField>
          <NumberField label="每片验证节点数" id="validators_per_shard" value={topology.validators_per_shard} min={1} max={64} onChange={(value) => patch({ validators_per_shard: value })}>
            用于逻辑共识域和 PBFT preview 的验证节点数量。
          </NumberField>
          <NumberField label="每片执行节点数" id="executors_per_shard" value={topology.executors_per_shard} min={0} max={64} onChange={(value) => patch({ executors_per_shard: value })}>
            逻辑执行节点数量，不代表真实多进程执行集群。
          </NumberField>
          <NumberField label="每片存储节点数" id="storage_nodes_per_shard" value={topology.storage_nodes_per_shard} min={0} max={64} onChange={(value) => patch({ storage_nodes_per_shard: value })}>
            逻辑状态存储节点数量。
          </NumberField>
          <label className="field-card checkbox-card">
            <span>启用监督节点 <HelpTip title="监督节点">监督节点是本地逻辑拓扑的一部分，用于产物和日志表达。</HelpTip></span>
            <input type="checkbox" checked={topology.supervisor_enabled} onChange={(event) => patch({ supervisor_enabled: event.target.checked })} />
            <small>supervisor_enabled</small>
          </label>
        </ConfigGroup>

        <ConfigGroup title="协议与状态">
          <SelectField label="节点运行模式" id="node_runtime_mode" value={topology.node_runtime_mode} options={[["logical_single_process", "单进程逻辑节点"]]} onChange={(value) => patch({ node_runtime_mode: value })}>
            当前仍是单进程逻辑节点运行时。
          </SelectField>
          <SelectField label="网络通信方式" id="network_adapter" value={topology.network_adapter || topology.network_mode} options={[["in_memory_message_bus", "内存消息总线"], ["localhost_tcp_preview", "本地 TCP 预览"]]} onChange={(value) => patch({ network_adapter: value, network_mode: value })}>
            本地 TCP 预览只证明 typed message path，不是生产网络。
          </SelectField>
          <SelectField label="跨片协议" id="cross_shard_protocol" value={topology.cross_shard_protocol || "none"} options={[["none", "不启用"], ["relay_preview", "Relay 预览"], ["broker_preview", "Broker 预览（规划中）", true], ["two_phase_commit_preview", "2PC 预览（规划中）", true]]} onChange={(value) => patch({ cross_shard_protocol: value })}>
            relay_preview 是 skeleton，不是完整 Relay / Broker / 2PC。
          </SelectField>
          <SelectField label="状态存储后端" id="state_backend" value={topology.state_backend || "memory_kv"} options={[["memory_kv", "内存 KV"], ["persistent_kv", "持久化 KV"], ["merkle_trie_mvp", "Merkle Trie MVP"], ["ethereum_mpt_compatible", "Ethereum MPT 兼容（规划中）", true]]} onChange={(value) => patch({ state_backend: value })}>
            Merkle Trie MVP 不是 Ethereum-compatible MPT，也不是完整无状态执行。
          </SelectField>
        </ConfigGroup>

        <ConfigGroup title="实验控制">
          <SelectField label="实验模板" id="benchmark_template" value={topology.benchmark_template || "full_stack_v3_template"} options={benchmarkTemplates} onChange={(value) => patch({ benchmark_template: value })}>
            模板用于组织本地受控实验配置，不自动证明性能优势。
          </SelectField>
          <SelectField label="对照基线" id="baseline_profile" value={topology.baseline_profile || "baseline_simple_chain"} options={baselineProfiles} onChange={(value) => patch({ baseline_profile: value })}>
            基线用于结构化对照输出，不是论文级结论。
          </SelectField>
          <NumberField label="重复次数" id="repeat_count" value={topology.repeat_count || 1} min={1} max={20} onChange={(value) => patch({ repeat_count: value })}>
            记录 repeat_index / seed 的本地 repeatability MVP。
          </NumberField>
        </ConfigGroup>
      </div>

      <dl className="topology-summary-grid">
        {Object.entries(summary).map(([key, value]) => (
          <div key={key}><dt>{key}</dt><dd>{String(value)}</dd></div>
        ))}
      </dl>
    </section>
  );
}

function ConfigGroup({ title, children }: { title: string; children: ReactNode }) {
  return <section className="topology-group"><h4>{title}</h4><div className="topology-field-grid">{children}</div></section>;
}

function NumberField({ label, id, value, min, max, onChange, children }: { label: string; id: string; value: number; min: number; max: number; onChange: (value: number) => void; children: ReactNode }) {
  return (
    <label className="field-card">
      <span>{label} <HelpTip title={label}>{children}</HelpTip></span>
      <input type="number" min={min} max={max} value={value} onChange={(event) => onChange(Number(event.target.value))} />
      <small>{id}</small>
    </label>
  );
}

function SelectField({ label, id, value, options, onChange, children }: { label: string; id: string; value?: string; options: SelectOption[]; onChange: (value: string) => void; children: ReactNode }) {
  return (
    <label className="field-card">
      <span>{label} <HelpTip title={label}>{children}</HelpTip></span>
      <select value={value} onChange={(event) => onChange(event.target.value)}>
        {options.map(([optionValue, labelText, disabled]) => (
          <option key={optionValue} value={optionValue} disabled={disabled}>{labelText}</option>
        ))}
      </select>
      <small>{id}: {value}</small>
    </label>
  );
}

function topologySummary(topology: V3RuntimeTopology): Record<string, number> {
  const validator_node_count = topology.shard_count * topology.validators_per_shard;
  const executor_node_count = topology.shard_count * topology.executors_per_shard;
  const storage_node_count = topology.shard_count * topology.storage_nodes_per_shard;
  const supervisor_node_count = topology.supervisor_enabled ? 1 : 0;
  return {
    逻辑节点总数: validator_node_count + executor_node_count + storage_node_count + supervisor_node_count,
    验证节点数: validator_node_count,
    执行节点数: executor_node_count,
    存储节点数: storage_node_count,
    监督节点数: supervisor_node_count,
    共识域数量: topology.shard_count,
  };
}
