import type { ReactNode } from "react";
import type { V3RuntimeTopology } from "../../api";
import HelpTip from "./HelpTip";

type Props = {
  topology: V3RuntimeTopology;
  onChange: (topology: V3RuntimeTopology) => void;
};

type SelectOption = [string, string, boolean?];

const benchmarkTemplates: SelectOption[] = [
  ["metaverse_mixed_template", "元宇宙混合场景模板"],
  ["metaverse_asset_transfer_template", "虚拟资产转移模板"],
  ["metaverse_cross_scene_template", "跨场景迁移模板"],
  ["metaverse_cross_metaverse_template", "跨元宇宙转移模板"],
  ["full_stack_v3_template", "V3 全栈快速验证模板"],
  ["state_authenticity_template", "状态真实性模板"],
  ["cross_shard_relay_preview_template", "跨片 Relay 预览模板"],
  ["cross_shard_relay_mvp_template", "跨片 Relay MVP 模板"],
  ["pbft_network_template", "PBFT 网络预览模板"],
  ["metatrack_hotspot_template", "MetaTrack 热点负载模板"],
];

const metaverseScenarios: SelectOption[] = [
  ["mixed_metaverse", "混合元宇宙场景"],
  ["asset_transfer", "虚拟资产转移"],
  ["avatar_update", "Avatar 状态更新"],
  ["scene_hotspot", "场景热点访问"],
  ["item_transfer", "道具 / 装备转移"],
  ["cross_scene_migration", "跨场景迁移"],
  ["onchain_offchain_confirmation", "链上 + 链下确认"],
  ["cross_metaverse_transfer", "跨元宇宙转移 MVP"],
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
          这里配置本地 V3 实验控制台使用的逻辑拓扑、协议预览和实验模板。local_multi_process 只在本机启动小规模进程，不是多服务器部署。
        </HelpTip>
      </div>

      <div className="topology-groups">
        <ConfigGroup title="节点拓扑">
          <NumberField label="分片数量" id="shard_count" value={topology.shard_count} min={1} max={32} onChange={(value) => patch({ shard_count: value })}>
            决定逻辑分片数量，默认 4。
          </NumberField>
          <NumberField label="每片验证节点数" id="validators_per_shard" value={topology.validators_per_shard} min={1} max={64} onChange={(value) => patch({ validators_per_shard: value })}>
            用于逻辑共识域和 committee assignment。
          </NumberField>
          <NumberField label="每片执行节点数" id="executors_per_shard" value={topology.executors_per_shard} min={0} max={64} onChange={(value) => patch({ executors_per_shard: value })}>
            逻辑执行节点数；local_multi_process 会受到 max_local_processes 保护。
          </NumberField>
          <NumberField label="每片存储节点数" id="storage_nodes_per_shard" value={topology.storage_nodes_per_shard} min={0} max={64} onChange={(value) => patch({ storage_nodes_per_shard: value })}>
            逻辑状态存储节点数量。
          </NumberField>
          <label className="field-card checkbox-card">
            <span>启用监督节点 <HelpTip title="监督节点">监督节点用于本地拓扑和日志表达。</HelpTip></span>
            <input type="checkbox" checked={topology.supervisor_enabled} onChange={(event) => patch({ supervisor_enabled: event.target.checked })} />
            <small>supervisor_enabled</small>
          </label>
        </ConfigGroup>

        <ConfigGroup title="协议与状态">
          <SelectField label="节点运行模式" id="node_runtime_mode" value={topology.node_runtime_mode} options={[["logical_single_process", "单进程逻辑节点"], ["local_multi_process", "本地多进程 MVP"]]} onChange={(value) => patch({ node_runtime_mode: value })}>
            本地多进程只在本机启动小规模进程，不是多服务器部署，也不是生产集群。
          </SelectField>
          <SelectField label="进程运行模式" id="process_runtime_mode" value={topology.process_runtime_mode || "dry_run"} options={[["dry_run", "dry_run"], ["smoke", "smoke"]]} onChange={(value) => patch({ process_runtime_mode: value })}>
            dry_run 只生成计划和状态产物；smoke 会启动短生命周期本地进程并清理。
          </SelectField>
          <NumberField label="最大本地进程数" id="max_local_processes" value={topology.max_local_processes || 8} min={1} max={32} onChange={(value) => patch({ max_local_processes: value })}>
            防止本机启动过多进程；超过上限会进入 capped mode。
          </NumberField>
          <label className="field-card checkbox-card">
            <span>启用委员会 / Epoch <HelpTip title="委员会 / Epoch">生成 shard assignment、committee assignment、epoch log 和轻量 reconfiguration plan；不是安全随机重分片。</HelpTip></span>
            <input type="checkbox" checked={topology.enable_committee_epoch ?? true} onChange={(event) => patch({ enable_committee_epoch: event.target.checked })} />
            <small>enable_committee_epoch</small>
          </label>
          <NumberField label="Epoch 数" id="epoch_count" value={topology.epoch_count || 1} min={1} max={5} onChange={(value) => patch({ epoch_count: value })}>
            默认 1 个 epoch；大于 1 时生成 deterministic round-robin reconfiguration plan。
          </NumberField>
          <SelectField label="网络通信方式" id="network_adapter" value={topology.network_adapter || topology.network_mode} options={[["in_memory_message_bus", "内存消息总线"], ["localhost_tcp_preview", "本地 TCP 预览"]]} onChange={(value) => patch({ network_adapter: value, network_mode: value })}>
            本地 TCP 预览只表示 typed message path，不是生产网络。
          </SelectField>
          <SelectField label="跨片协议" id="cross_shard_protocol" value={topology.cross_shard_protocol || "none"} options={[["none", "不启用"], ["relay_preview", "Relay 预览"], ["relay_mvp", "Relay MVP"], ["broker_preview", "Broker 预览（规划中）", true], ["two_phase_commit_preview", "2PC 预览（规划中）", true]]} onChange={(value) => patch({ cross_shard_protocol: value })}>
            relay_mvp 是本地可观测 MVP，不是生产级 atomic commit / Broker / 2PC。
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

        <ConfigGroup title="元宇宙实验套件">
          <label className="field-card checkbox-card">
            <span>启用元宇宙 workload suite <HelpTip title="元宇宙 workload suite">生成受控合成场景、baseline matrix、multi-seed sweep 和 paper export scaffold；不是真实平台 trace。</HelpTip></span>
            <input type="checkbox" checked={topology.metaverse_suite_enabled ?? false} onChange={(event) => patch({ metaverse_suite_enabled: event.target.checked })} />
            <small>metaverse_suite_enabled</small>
          </label>
          <SelectField label="元宇宙场景" id="metaverse_scenario" value={topology.metaverse_scenario || "mixed_metaverse"} options={metaverseScenarios} onChange={(value) => patch({ metaverse_scenario: value })}>
            控制 trace metadata 的场景语义；默认 mixed_metaverse。
          </SelectField>
          <NumberField label="交易数量" id="tx_count" value={topology.tx_count || 10000} min={1} max={10000000} onChange={(value) => patch({ tx_count: value })}>
            用于 V3.13 metadata artifacts；Draft Smoke 的 Go 执行仍保持小规模稳定验证。
          </NumberField>
          <NumberField label="随机种子" id="seed" value={topology.seed || 42} min={0} max={2147483647} onChange={(value) => patch({ seed: value })}>
            相同配置和 seed 会生成相同场景 metadata。
          </NumberField>
          <NumberField label="用户数" id="user_count" value={topology.user_count || 100} min={1} max={100000} onChange={(value) => patch({ user_count: value })}>
            合成用户 ID 空间。
          </NumberField>
          <NumberField label="资产数" id="asset_count" value={topology.asset_count || 1000} min={1} max={1000000} onChange={(value) => patch({ asset_count: value })}>
            合成虚拟资产 ID 空间。
          </NumberField>
          <NumberField label="场景数" id="scene_count" value={topology.scene_count || 16} min={1} max={10000} onChange={(value) => patch({ scene_count: value })}>
            合成 scene ID 空间。
          </NumberField>
          <NumberField label="元宇宙数量" id="metaverse_count" value={topology.metaverse_count || 2} min={1} max={100} onChange={(value) => patch({ metaverse_count: value })}>
            用于 cross_metaverse_transfer 的 source / target metaverse。
          </NumberField>
          <NumberField label="热点比例" id="hotspot_ratio" value={topology.hotspot_ratio ?? 0.2} min={0} max={1} step={0.01} onChange={(value) => patch({ hotspot_ratio: value })}>
            控制热点 scene/key 集中程度。
          </NumberField>
          <NumberField label="跨场景比例" id="cross_scene_ratio" value={topology.cross_scene_ratio ?? 0.15} min={0} max={1} step={0.01} onChange={(value) => patch({ cross_scene_ratio: value })}>
            控制 cross_scene_migration metadata 数量。
          </NumberField>
          <NumberField label="跨片比例" id="cross_shard_ratio" value={topology.cross_shard_ratio ?? 0.2} min={0} max={1} step={0.01} onChange={(value) => patch({ cross_shard_ratio: value })}>
            控制合成跨片 metadata 数量。
          </NumberField>
          <label className="field-card checkbox-card">
            <span>链下确认 <HelpTip title="链下确认">生成 deterministic offchain_confirmation_log.csv，不调用真实外部服务。</HelpTip></span>
            <input type="checkbox" checked={topology.offchain_confirmation_enabled ?? true} onChange={(event) => patch({ offchain_confirmation_enabled: event.target.checked })} />
            <small>offchain_confirmation_enabled</small>
          </label>
          <NumberField label="链下确认延迟" id="offchain_confirm_delay_ms" value={topology.offchain_confirm_delay_ms ?? 100} min={0} max={600000} onChange={(value) => patch({ offchain_confirm_delay_ms: value })}>
            写入链下确认 metadata，不等待真实时间。
          </NumberField>
          <NumberField label="链下失败比例" id="offchain_failure_ratio" value={topology.offchain_failure_ratio ?? 0} min={0} max={1} step={0.01} onChange={(value) => patch({ offchain_failure_ratio: value })}>
            确定性生成 failed confirmation 行。
          </NumberField>
          <label className="field-card checkbox-card">
            <span>跨元宇宙转移 <HelpTip title="跨元宇宙转移">生成 Relay MVP 可衔接的 cross_metaverse_transfer_log.csv；不是生产桥。</HelpTip></span>
            <input type="checkbox" checked={topology.cross_metaverse_enabled ?? true} onChange={(event) => patch({ cross_metaverse_enabled: event.target.checked })} />
            <small>cross_metaverse_enabled</small>
          </label>
          <label className="field-card checkbox-card">
            <span>Baseline matrix</span>
            <input type="checkbox" checked={topology.baseline_matrix_enabled ?? false} onChange={(event) => patch({ baseline_matrix_enabled: event.target.checked })} />
            <small>baseline_matrix_enabled</small>
          </label>
          <label className="field-card checkbox-card">
            <span>Multi-seed / sweep</span>
            <input type="checkbox" checked={topology.multi_seed_enabled ?? false} onChange={(event) => patch({ multi_seed_enabled: event.target.checked, benchmark_suite_enabled: event.target.checked || topology.benchmark_suite_enabled })} />
            <small>multi_seed_enabled</small>
          </label>
          <label className="field-card checkbox-card">
            <span>Paper export scaffold</span>
            <input type="checkbox" checked={topology.paper_export_enabled ?? false} onChange={(event) => patch({ paper_export_enabled: event.target.checked })} />
            <small>paper_export_enabled</small>
          </label>
          <NumberField label="Sweep seed 数" id="sweep_seed_count" value={topology.sweep_seed_count || 3} min={1} max={20} onChange={(value) => patch({ sweep_seed_count: value })}>
            默认 3；用于 deterministic multi-seed scaffold。
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

function NumberField({ label, id, value, min, max, step = 1, onChange, children }: { label: string; id: string; value: number; min: number; max: number; step?: number; onChange: (value: number) => void; children: ReactNode }) {
  return (
    <label className="field-card">
      <span>{label} <HelpTip title={label}>{children}</HelpTip></span>
      <input type="number" min={min} max={max} step={step} value={value} onChange={(event) => onChange(Number(event.target.value))} />
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

function topologySummary(topology: V3RuntimeTopology): Record<string, number | string | boolean> {
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
    节点运行模式: topology.node_runtime_mode,
    进程运行模式: topology.process_runtime_mode || "dry_run",
    最大本地进程数: topology.max_local_processes || 8,
    启用委员会Epoch: topology.enable_committee_epoch ?? true,
    Epoch数量: topology.epoch_count || 1,
    元宇宙套件: topology.metaverse_suite_enabled ?? false,
    元宇宙场景: topology.metaverse_scenario || "mixed_metaverse",
    元宇宙交易数: topology.tx_count || 10000,
    Paper导出: topology.paper_export_enabled ?? false,
  };
}
