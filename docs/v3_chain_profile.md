# V3 ChainProfile

## 1. ChainProfile 目的

ChainProfile 描述一条 V3 modular research chain 的身份、部署、节点、区块、共识、finality、交易池、执行、状态、提交、分片、网络、跨链、应用、安全和指标配置。

ChainProfile 是实验输入，不是实验结果。`max_tps`、`stable_tps`、`peak_tps` 是实验结果，不是固定链属性。

## 2. Identity

- `chain_id`: 链唯一 ID。
- `chain_name`: 展示名称。
- `role`: `single_chain` / `source` / `target` / `observer`。
- `chain_type`: `modular_research_chain` / `fabric_validation` / planned backend type。
- `truth_label`: expected result truth label。

## 3. Deployment

- `mode`: `local_process` / `single_process_logical_nodes` / future distributed mode。
- `node_count`: logical node count。
- `validator_count`: validator count。
- `executor_count`: executor worker count。
- `hardware_profile`: hardware/environment identifier for fairness checks。

## 4. Node

- `node_id_prefix`: prefix for generated node ids。
- `node_roles`: validator/executor/observer/client。
- `clock_mode`: virtual/logical/wall-clock observation mode。
- `storage_root`: local runtime storage root, not committed to git。

## 5. Block

- `block_interval_ms`: 配置项，控制出块间隔。
- `max_tx_per_block`: 配置项，控制每块最大交易数。
- `block_cut_policy`: `time` / `count` / `time_or_count`。
- `empty_block_enabled`: 是否允许空块。

`max_tps`、`stable_tps`、`peak_tps` 是实验结果，不应写成 ChainProfile 固定属性。

## 6. Consensus

- `plugin`: consensus plugin id。
- `leader_policy`: leader selection policy。
- `ordering_policy`: ordering rule。
- `fault_model`: none / crash planned / byzantine planned。
- `config`: plugin-specific settings。

V3.2 第一版不要求真实 Byzantine consensus。

## 7. Finality

- `finality_type`: deterministic / probabilistic planned。
- `finality_depth`: finality depth。
- `finality_timeout_ms`: timeout guard。
- `commit_rule`: when block/tx is considered final。

## 8. TxPool

- `plugin`: tx pool plugin id。
- `max_pool_size`: queue capacity。
- `batch_selection_policy`: by_arrival / priority planned。
- `dedup_enabled`: duplicate tx guard。
- `backpressure_policy`: reject / delay / mark_failed。

## 9. Execution

- `plugin`: execution scheduler plugin。
- `parallelism`: worker count。
- `access_list_enabled`: whether workload carries access metadata。
- `dependency_check_enabled`: whether dependencies are enforced。
- `dual_track_enabled`: MetaTrack-specific execution path toggle。

## 10. State

- `model`: key_value / object planned。
- `backend`: memory / file planned。
- `key_count`: state key cardinality。
- `read_set_required`: whether read sets are required。
- `write_set_required`: whether write sets are required。
- `remote_fetch_cost_ms`: calibrated or configured remote access cost。

## 11. Commit

- `plugin`: commit plugin id。
- `aggregation_enabled`: hot update aggregation flag。
- `commit_batch_size`: commit group size。
- `state_commit_log_enabled`: output state commit log。

## 12. Sharding

- `enabled`: true/false。
- `shard_count`: number of shards。
- `plugin`: sharding plugin id。
- `routing_policy`: hash / co_access / future plugin。
- `cross_shard_policy`: local_only / planned protocol。

## 13. Network

- `delay_ms`: configured network delay。
- `jitter_ms`: optional jitter。
- `loss_rate`: configured loss rate; default 0 for deterministic evaluation。
- `bandwidth_limit`: optional planned field。

Network configuration must be identical across fair baselines unless network behavior is the studied variable.

## 14. Cross-chain

- `cross_chain_enabled`: whether this chain participates in dual-chain runtime。
- `peer_chain_ids`: linked chains。
- `finality_export_enabled`: whether finality observations can be exported。
- `bridge_adapter`: planned protocol adapter id。

## 15. Application

- `chaincode_or_contract`: application model id。
- `operation_types`: supported operation types。
- `asset_model`: optional asset semantics。
- `hotspot_model`: workload semantic model。

## 16. Fault / Safety

- `fault_injection_enabled`: false by default。
- `crash_faults`: planned。
- `byzantine_faults`: planned。
- `safety_assertions_enabled`: runtime invariant checks。

## 17. Metrics

- `trace_enabled`: output runtime event trace。
- `report_enabled`: generate report。
- `mechanism_metrics_enabled`: output plugin-level metrics。
- `block_log_enabled`: output block log。
- `tx_results_enabled`: output transaction results。

## 18. YAML 示例

```yaml
chain:
  chain_id: chain_x
  chain_name: MetaChain-X
  role: single_chain
  chain_type: modular_research_chain

deployment:
  mode: local_process
  node_count: 4
  validator_count: 4
  executor_count: 4

block:
  block_interval_ms: 100
  max_tx_per_block: 500
  block_cut_policy: time_or_count

consensus:
  plugin: simple_leader
  finality_type: deterministic
  finality_depth: 1

tx_pool:
  plugin: fifo_pool
  max_pool_size: 100000
  batch_selection_policy: by_arrival

sharding:
  enabled: true
  shard_count: 4
  plugin: hash_sharding

execution:
  plugin: serial_execution
  parallelism: 4
  access_list_enabled: true

state:
  model: key_value
  backend: memory
  key_count: 100000
  remote_fetch_cost_ms: 1

commit:
  plugin: normal_commit
  aggregation_enabled: false

network:
  delay_ms: 5
  loss_rate: 0

metrics:
  trace_enabled: true
  report_enabled: true
```
