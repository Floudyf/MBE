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

const faultProfiles: SelectOption[] = [
  ["none", "不注入故障"],
  ["node_failure", "节点失败"],
  ["node_recovery", "节点恢复"],
  ["network_delay", "网络延迟"],
  ["network_drop", "网络丢包"],
  ["target_congestion", "目标分片拥塞"],
  ["relay_fault", "Relay 故障观察"],
  ["mixed_fault", "混合故障"],
];

const relayFaultModes: SelectOption[] = [
  ["none", "不指定"],
  ["proof_fail", "proof_fail"],
  ["timeout", "timeout"],
  ["target_reject", "target_reject"],
];

const observabilityLevels: SelectOption[] = [
  ["basic", "basic"],
  ["detailed", "detailed"],
];

const workloadSources: SelectOption[] = [
  ["synthetic", "可控合成"],
  ["metaverse", "元宇宙场景化"],
  ["saved_workload", "已保存负载"],
  ["existing_trace_preview", "真实 trace 预览"],
];

export default function RuntimeTopologyPanel({ topology, onChange }: Props) {
  const summary = topologySummary(topology);
  const workloadSource = topology.workload_source || "synthetic";
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
          这里配置本地 V3 实验控制台使用的逻辑拓扑、协议预览和实验模板。local_multi_process 只在本机启动受限数量的本地进程，不是多服务器部署。
        </HelpTip>
      </div>

      <div className="topology-groups">
        <ConfigGroup title="节点拓扑细节" summary={`${topology.shard_count} 分片 / ${topology.validators_per_shard} 验证节点每片 / ${topology.node_runtime_mode}`}>
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
          {topology.node_runtime_mode === "local_multi_process" && (
            <NumberField label="最大本地进程数" id="max_local_processes" value={topology.max_local_processes || 8} min={1} max={32} onChange={(value) => patch({ max_local_processes: value })}>
              防止本机启动过多进程；超过上限会进入 capped mode。
            </NumberField>
          )}
          <label className="field-card checkbox-card">
            <span>启用委员会 / Epoch <HelpTip title="委员会 / Epoch">生成 shard assignment、committee assignment、epoch log 和轻量 reconfiguration plan；不是安全随机重分片。</HelpTip></span>
            <input type="checkbox" checked={topology.enable_committee_epoch ?? true} onChange={(event) => patch({ enable_committee_epoch: event.target.checked })} />
            <small>enable_committee_epoch</small>
          </label>
          <NumberField label="Epoch 数" id="epoch_count" value={topology.epoch_count || 1} min={1} max={5} onChange={(value) => patch({ epoch_count: value })}>
            默认 1 个 epoch；大于 1 时生成 deterministic round-robin reconfiguration plan。
          </NumberField>
        </ConfigGroup>

        <ConfigGroup title="核心运行配置" defaultOpen summary="决定当前运行路径和负载语义；正式性能实验矩阵在论文实验设计中配置。">
          <label className="field-card checkbox-card">
            <span>受控对照模式 <HelpTip title="受控对照模式">关闭时，模板只作为起始配置，模块插件可以自由切换；开启时，模板会锁定固定模块，只允许实验变量模块变化，用于公平 baseline / 消融实验。</HelpTip></span>
            <input type="checkbox" checked={topology.controlled_experiment_enabled ?? false} onChange={(event) => patch({ controlled_experiment_enabled: event.target.checked })} />
            <small>controlled_experiment_enabled</small>
          </label>
          <SelectField label="节点运行模式" id="node_runtime_mode" value={topology.node_runtime_mode} options={[["logical_single_process", "单进程逻辑节点"], ["local_multi_process", "本地多进程 MVP"]]} onChange={(value) => patch({ node_runtime_mode: value })}>
            本地多进程只在本机启动受限数量的本地进程，不是多服务器部署，也不是生产集群。
          </SelectField>
          <SelectField label="进程运行模式" id="process_runtime_mode" value={topology.process_runtime_mode || "dry_run"} options={[["dry_run", "dry_run"], ["smoke", "smoke"]]} onChange={(value) => patch({ process_runtime_mode: value })}>
            dry_run 只生成计划和状态产物；smoke 会启动短生命周期本地进程并清理。
          </SelectField>
          <SelectField label="网络通信方式" id="network_adapter" value={topology.network_adapter || topology.network_mode} options={[["in_memory_message_bus", "内存消息总线"], ["localhost_tcp_preview", "本地 TCP 预览"]]} onChange={(value) => patch({ network_adapter: value, network_mode: value })}>
            本地 TCP 预览只表示 typed message path，不是生产网络。
          </SelectField>
          <SelectField label="跨片协议" id="cross_shard_protocol" value={topology.cross_shard_protocol || "none"} options={[["none", "不启用"], ["relay_preview", "Relay 预览"], ["relay_mvp", "Relay MVP"], ["broker_preview", "Broker 预览（规划中）", true], ["two_phase_commit_preview", "2PC 预览（规划中）", true]]} onChange={(value) => patch({ cross_shard_protocol: value })}>
            relay_mvp 是本地可观测 MVP，不是生产级 atomic commit / Broker / 2PC。
          </SelectField>
          <SelectField label="状态存储后端" id="state_backend" value={topology.state_backend || "memory_kv"} options={[["memory_kv", "内存 KV"], ["persistent_kv", "持久化 KV"], ["merkle_trie_mvp", "Merkle Trie MVP"], ["ethereum_mpt_compatible", "Ethereum MPT 兼容（规划中）", true]]} onChange={(value) => patch({ state_backend: value })}>
            Merkle Trie MVP 不是 Ethereum-compatible MPT，也不是完整无状态执行。
          </SelectField>
          <label className="field-card checkbox-card">
            <span>启用元宇宙 workload suite <HelpTip title="元宇宙 workload suite">生成受控合成场景、baseline matrix、multi-seed sweep 和 paper export scaffold；不是真实平台 trace。</HelpTip></span>
            <input type="checkbox" checked={topology.metaverse_suite_enabled ?? false} onChange={(event) => patch({ metaverse_suite_enabled: event.target.checked })} />
            <small>metaverse_suite_enabled</small>
          </label>
          <SelectField label="元宇宙场景" id="metaverse_scenario" value={topology.metaverse_scenario || "mixed_metaverse"} options={metaverseScenarios} onChange={(value) => patch({ metaverse_scenario: value })}>
            控制 trace metadata 的场景语义；默认 mixed_metaverse。
          </SelectField>
          <NumberField label="交易数量" id="tx_count" value={topology.tx_count || 10000} min={1} max={10000000} onChange={(value) => patch({ tx_count: value })}>
            用于 V3.13 metadata artifacts；Draft Smoke 的 Go 执行仍保持受限交易数的稳定验证。
          </NumberField>
          <SelectField label="故障 profile" id="fault_profile" value={topology.fault_profile || "none"} options={faultProfiles} onChange={(value) => patch({ fault_profile: value })}>
            主性能实验默认不启用故障注入；故障注入属于鲁棒性或真实性验证。
          </SelectField>
        </ConfigGroup>

        <ConfigGroup title="兼容旧 Benchmark 配置" summary="V3.10/V3.13 兼容配置；新的 MetaTrack 正式性能实验以论文实验设计面板为准。">
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

        <ConfigGroup title="负载配置" summary={`${workloadSource} / tx=${topology.tx_count || 10000} / hotspot=${topology.hotspot_ratio ?? 0.2} / cross_shard=${topology.cross_shard_ratio ?? 0.2}`}>
          <SelectField label="负载来源" id="workload_source" value={workloadSource} options={workloadSources} onChange={(value) => patch({ workload_source: value, metaverse_suite_enabled: value === "metaverse" })}>
            synthetic 使用可控合成参数；metaverse 展开场景化字段；saved_workload 从配置库加载；existing_trace_preview 只做预览，不默认进入正式性能实验。
          </SelectField>
          {workloadSource === "synthetic" && (
            <>
              <NumberField label="交易数量" id="tx_count" value={topology.tx_count || 10000} min={1} max={10000000} onChange={(value) => patch({ tx_count: value })}>合成负载交易数量。</NumberField>
              <NumberField label="随机种子" id="seed" value={topology.seed || 42} min={0} max={2147483647} onChange={(value) => patch({ seed: value })}>相同配置和 seed 会生成相同合成负载。</NumberField>
              <NumberField label="热点比例" id="hotspot_ratio" value={topology.hotspot_ratio ?? 0.2} min={0} max={1} step={0.01} onChange={(value) => patch({ hotspot_ratio: value })}>控制热点 key 集中程度。</NumberField>
              <NumberField label="跨片比例" id="cross_shard_ratio" value={topology.cross_shard_ratio ?? 0.2} min={0} max={1} step={0.01} onChange={(value) => patch({ cross_shard_ratio: value })}>控制合成跨片交易比例。</NumberField>
              <NumberField label="读写比例" id="read_write_ratio" value={topology.read_write_ratio ?? 0.3} min={0} max={1} step={0.01} onChange={(value) => patch({ read_write_ratio: value })}>合成读写混合比例。</NumberField>
              <NumberField label="Zipf 偏斜" id="zipf_alpha" value={topology.zipf_alpha ?? 0.8} min={0} max={2} step={0.05} onChange={(value) => patch({ zipf_alpha: value })}>合成热点偏斜参数。</NumberField>
              <NumberField label="提交速率" id="submit_rate" value={topology.submit_rate ?? topology.arrival_rate ?? 120} min={0} max={1000000} onChange={(value) => patch({ submit_rate: value, arrival_rate: value })}>本地 profile 元数据，不模拟真实墙钟压测。</NumberField>
              <NumberField label="Key 空间" id="key_space_size" value={topology.key_space_size ?? 10000} min={1} max={100000000} onChange={(value) => patch({ key_space_size: value })}>合成负载 key 空间大小。</NumberField>
            </>
          )}
          {workloadSource === "metaverse" && (
            <>
              <NumberField label="随机种子" id="seed" value={topology.seed || 42} min={0} max={2147483647} onChange={(value) => patch({ seed: value })}>相同配置和 seed 会生成相同场景 metadata。</NumberField>
              <SelectField label="元宇宙场景" id="metaverse_scenario" value={topology.metaverse_scenario || "mixed_metaverse"} options={metaverseScenarios} onChange={(value) => patch({ metaverse_scenario: value })}>控制 trace metadata 的场景语义。</SelectField>
              <NumberField label="交易数量" id="tx_count" value={topology.tx_count || 10000} min={1} max={10000000} onChange={(value) => patch({ tx_count: value })}>场景化负载交易数量。</NumberField>
              <NumberField label="用户数" id="user_count" value={topology.user_count || 100} min={1} max={100000} onChange={(value) => patch({ user_count: value })}>合成用户 ID 空间。</NumberField>
              <NumberField label="资产数" id="asset_count" value={topology.asset_count || 1000} min={1} max={1000000} onChange={(value) => patch({ asset_count: value })}>合成虚拟资产 ID 空间。</NumberField>
              <NumberField label="道具数" id="item_count" value={topology.item_count || 1000} min={0} max={1000000} onChange={(value) => patch({ item_count: value })}>合成 item ID 空间。</NumberField>
              <NumberField label="Avatar 数" id="avatar_count" value={topology.avatar_count || 100} min={1} max={100000} onChange={(value) => patch({ avatar_count: value })}>合成 avatar ID 空间。</NumberField>
              <NumberField label="场景数" id="scene_count" value={topology.scene_count || 16} min={1} max={10000} onChange={(value) => patch({ scene_count: value })}>合成 scene ID 空间。</NumberField>
              <NumberField label="元宇宙数量" id="metaverse_count" value={topology.metaverse_count || 2} min={1} max={100} onChange={(value) => patch({ metaverse_count: value })}>用于 cross_metaverse_transfer。</NumberField>
              <NumberField label="热点比例" id="hotspot_ratio" value={topology.hotspot_ratio ?? 0.2} min={0} max={1} step={0.01} onChange={(value) => patch({ hotspot_ratio: value })}>控制热点 scene/key 集中程度。</NumberField>
              <NumberField label="跨场景比例" id="cross_scene_ratio" value={topology.cross_scene_ratio ?? 0.15} min={0} max={1} step={0.01} onChange={(value) => patch({ cross_scene_ratio: value })}>控制跨场景迁移比例。</NumberField>
              <NumberField label="跨片比例" id="cross_shard_ratio" value={topology.cross_shard_ratio ?? 0.2} min={0} max={1} step={0.01} onChange={(value) => patch({ cross_shard_ratio: value })}>控制合成跨片 metadata 数量。</NumberField>
              <NumberField label="burst_rate" id="burst_rate" value={topology.burst_rate ?? 0} min={0} max={1} step={0.01} onChange={(value) => patch({ burst_rate: value })}>突发负载比例。</NumberField>
              <NumberField label="读写比例" id="read_write_ratio" value={topology.read_write_ratio ?? 0.3} min={0} max={1} step={0.01} onChange={(value) => patch({ read_write_ratio: value })}>读写混合比例。</NumberField>
              <NumberField label="资产偏斜" id="asset_skew" value={topology.asset_skew ?? 0.2} min={0} max={1} step={0.01} onChange={(value) => patch({ asset_skew: value })}>资产访问偏斜。</NumberField>
              <NumberField label="场景偏斜" id="scene_skew" value={topology.scene_skew ?? 0.2} min={0} max={1} step={0.01} onChange={(value) => patch({ scene_skew: value })}>场景访问偏斜。</NumberField>
              <label className="field-card checkbox-card"><span>链下确认</span><input type="checkbox" checked={topology.offchain_confirmation_enabled ?? true} onChange={(event) => patch({ offchain_confirmation_enabled: event.target.checked })} /><small>offchain_confirmation_enabled</small></label>
              <NumberField label="链下确认延迟" id="offchain_confirm_delay_ms" value={topology.offchain_confirm_delay_ms ?? 100} min={0} max={600000} onChange={(value) => patch({ offchain_confirm_delay_ms: value })}>写入链下确认 metadata，不等待真实时间。</NumberField>
              <NumberField label="链下失败比例" id="offchain_failure_ratio" value={topology.offchain_failure_ratio ?? 0} min={0} max={1} step={0.01} onChange={(value) => patch({ offchain_failure_ratio: value })}>确定性生成 failed confirmation 行。</NumberField>
              <label className="field-card checkbox-card"><span>跨元宇宙转移</span><input type="checkbox" checked={topology.cross_metaverse_enabled ?? true} onChange={(event) => patch({ cross_metaverse_enabled: event.target.checked })} /><small>cross_metaverse_enabled</small></label>
            </>
          )}
          {workloadSource === "saved_workload" && <div className="field-card"><span>已保存负载选择器</span><p className="muted">在“保存 / 加载配置”区域加载 workload 配置；这里保留只读摘要位置。</p><small>saved_workload</small></div>}
          {workloadSource === "existing_trace_preview" && (
            <>
              <label className="field-card"><span>trace_path</span><input value={topology.trace_path || ""} onChange={(event) => patch({ trace_path: event.target.value })} /><small>existing_trace_preview</small></label>
              <label className="field-card"><span>trace_schema</span><input value={topology.trace_schema || ""} onChange={(event) => patch({ trace_schema: event.target.value })} /><small>字段映射摘要</small></label>
              <div className="v3-warning-card">当前 trace 回放仍是 preview，不默认进入正式性能实验主路径。</div>
            </>
          )}
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

        <ConfigGroup title="故障、观测与复现" summary="主性能实验默认不启用故障注入；故障注入属于鲁棒性或真实性验证。">
          <label className="field-card checkbox-card">
            <span>启用故障注入 <HelpTip title="故障注入">生成确定性的本地故障事件和观察日志；不是生产级 Byzantine adversary、不是生产容错协议。</HelpTip></span>
            <input type="checkbox" checked={topology.fault_injection_enabled ?? false} onChange={(event) => patch({ fault_injection_enabled: event.target.checked })} />
            <small>fault_injection_enabled</small>
          </label>
          <NumberField label="故障 seed" id="fault_seed" value={topology.fault_seed ?? 42} min={0} max={2147483647} onChange={(value) => patch({ fault_seed: value })}>
            相同配置和 seed 会生成相同故障事件。
          </NumberField>
          <NumberField label="起始 round" id="fault_start_round" value={topology.fault_start_round ?? 1} min={0} max={1000000} onChange={(value) => patch({ fault_start_round: value })}>
            故障事件开始的逻辑 round。
          </NumberField>
          <NumberField label="持续 rounds" id="fault_duration_rounds" value={topology.fault_duration_rounds ?? 1} min={0} max={1000000} onChange={(value) => patch({ fault_duration_rounds: value })}>
            用于 node_recovery 和 mixed_fault 的恢复计划。
          </NumberField>
          <NumberField label="失败节点数" id="failed_node_count" value={topology.failed_node_count ?? 1} min={0} max={10000} onChange={(value) => patch({ failed_node_count: value })}>
            只标记本地逻辑/进程节点，不模拟生产容错。
          </NumberField>
          <NumberField label="消息延迟 ms" id="message_delay_ms" value={topology.message_delay_ms ?? 0} min={0} max={600000} onChange={(value) => patch({ message_delay_ms: value })}>
            写入 network_fault_log.csv 的确定性延迟元数据。
          </NumberField>
          <NumberField label="消息丢弃比例" id="message_drop_ratio" value={topology.message_drop_ratio ?? 0} min={0} max={1} step={0.01} onChange={(value) => patch({ message_drop_ratio: value })}>
            生成 delivered=false 的确定性 drop 事件。
          </NumberField>
          <NumberField label="目标拥塞比例" id="target_congestion_ratio" value={topology.target_congestion_ratio ?? 0} min={0} max={1} step={0.01} onChange={(value) => patch({ target_congestion_ratio: value })}>
            生成 target_congestion_log.csv 的本地拥塞观察。
          </NumberField>
          <SelectField label="Relay 故障模式" id="relay_fault_mode" value={topology.relay_fault_mode || "none"} options={relayFaultModes} onChange={(value) => patch({ relay_fault_mode: value })}>
            使用 proof_fail / timeout / target_reject 语义观察 Relay MVP，不实现生产级跨链桥。
          </SelectField>
          <label className="field-card checkbox-card">
            <span>启用观测摘要</span>
            <input type="checkbox" checked={topology.observability_enabled ?? true} onChange={(event) => patch({ observability_enabled: event.target.checked })} />
            <small>observability_enabled</small>
          </label>
          <SelectField label="观测级别" id="observability_level" value={topology.observability_level || "basic"} options={observabilityLevels} onChange={(value) => patch({ observability_level: value })}>
            detailed 会额外写入组件级 timeline 行；仍不是生产监控系统。
          </SelectField>
          <label className="field-card checkbox-card">
            <span>复现 bundle</span>
            <input type="checkbox" checked={topology.reproducibility_bundle_enabled ?? true} onChange={(event) => patch({ reproducibility_bundle_enabled: event.target.checked })} />
            <small>reproducibility_bundle_enabled</small>
          </label>
          <label className="field-card checkbox-card">
            <span>Paper mapping</span>
            <input type="checkbox" checked={topology.paper_mapping_enabled ?? true} onChange={(event) => patch({ paper_mapping_enabled: event.target.checked })} />
            <small>paper_mapping_enabled</small>
          </label>
          <label className="field-card checkbox-card">
            <span>最终 artifact catalog</span>
            <input type="checkbox" checked={topology.final_artifact_catalog_enabled ?? true} onChange={(event) => patch({ final_artifact_catalog_enabled: event.target.checked })} />
            <small>final_artifact_catalog_enabled</small>
          </label>
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

function ConfigGroup({ title, summary = "", defaultOpen = false, children }: { title: string; summary?: string; defaultOpen?: boolean; children: ReactNode }) {
  return (
    <details className="topology-group v3-foldout" open={defaultOpen}>
      <summary className="v3-foldout-summary">
        <span>{title}</span>
        {summary && <small>{summary}</small>}
      </summary>
      <div className="topology-field-grid">{children}</div>
    </details>
  );
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
    故障注入: topology.fault_injection_enabled ?? false,
    故障Profile: topology.fault_profile || "none",
    观测级别: topology.observability_level || "basic",
    复现Bundle: topology.reproducibility_bundle_enabled ?? true,
    逻辑节点总数: validator_node_count + executor_node_count + storage_node_count + supervisor_node_count,
    验证节点数: validator_node_count,
    执行节点数: executor_node_count,
    存储节点数: storage_node_count,
    监督节点数: supervisor_node_count,
    共识域数量: topology.shard_count,
    节点运行模式: topology.node_runtime_mode,
    插件选择模式: topology.controlled_experiment_enabled ? "受控对照" : "自由配置",
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
