# V3 PluginProfile

## 1. PluginProfile 目的

PluginProfile 定义 V3 modular research chain runtime 中可替换插件的职责、输入、输出、baseline 对应关系、阶段边界和禁止行为。V3.0 只定义文档；V3.1 才实现 schema / loader / validator / preview。

## 2. WorkloadPlugin

- 职责：生成或导入 workload arrival sequence。
- 输入：WorkloadProfile、seed、tx_count、hotspot/skew/cross-shard parameters。
- 输出：transactions, access_list/read_set/write_set metadata where available。
- 可替换 baseline：synthetic_hotspot、reward_burst、chain_backed_import。
- MetaTrack / MetaFlow 对应：提供同一 workload for fair comparison。
- V3 阶段：V3.1 profile declaration; V3.2+ runtime integration。
- 不能做什么：不能为 proposed method 和 baseline 生成不同 workload 来制造优势。

## 3. TxPoolPlugin

- 职责：交易接收、排队、去重、batch selection。
- 输入：submitted transactions, pool config。
- 输出：candidate batch for BlockProducer。
- 可替换 baseline：fifo_pool, priority_pool planned。
- 对应插件：MetaTrack/MetaFlow 都应共享同一 TxPoolPlugin unless explicitly studied。
- V3 阶段：V3.2。
- 不能做什么：不能根据方法标签给 proposed method 特权排序。

## 4. BlockProducer

- 职责：按 block interval / count policy 生成 block proposal。
- 输入：TxPool batch, block config。
- 输出：block proposal。
- baseline：time_or_count producer。
- V3 阶段：V3.2。
- 不能做什么：不能为不同 baseline 使用不同 block interval or max tx per block。

## 5. ConsensusPlugin

- 职责：排序并给出 finality decision。
- 输入：block proposal, consensus config。
- 输出：ordered/finalized block。
- baseline：simple_leader deterministic finality。
- V3 阶段：V3.2 minimal; richer plugins future。
- 不能做什么：V3.2 不声称生产级 PBFT/HotStuff。

## 6. ShardingPlugin

- 职责：state/execution shard assignment and routing metadata。
- 输入：transaction access metadata, state config。
- 输出：shard assignment, remote access estimate。
- baseline：hash_sharding。
- MetaTrack 对应：`hash_sharding`, `co_access_sharding`。
- V3 阶段：V3.3。
- 不能做什么：不能改变 workload 或 chain profile。

## 7. ExecutionSchedulerPlugin

- 职责：决定 serial/dual-track/parallel execution plan。
- 输入：ordered txs, shard metadata, dependencies。
- 输出：execution schedule, fast/conservative track assignment。
- baseline：serial_execution。
- MetaTrack 对应：`serial_execution`, `dual_track_execution`。
- V3 阶段：V3.3。
- 不能做什么：不能跳过 correctness checks 来换取 throughput。

## 8. StateAccessPlugin

- 职责：state read/write, remote fetch, access-list prefetch。
- 输入：execution plan, read/write sets。
- 输出：state access results and metrics。
- baseline：direct_fetch。
- MetaTrack 对应：`direct_fetch`, `access_list_prefetch`。
- V3 阶段：V3.3。
- 不能做什么：不能假设 public-chain imported trace has reliable access lists by default。

## 9. CommitPlugin

- 职责：commit state deltas and write state_commit_log。
- 输入：execution results, state deltas。
- 输出：commit result, aggregated update metrics。
- baseline：normal_commit。
- MetaTrack 对应：`normal_commit`, `hot_update_aggregation_commit`。
- V3 阶段：V3.3。
- 不能做什么：不能 drop updates or change application semantics。

## 10. CrossChainProtocolPlugin

- 职责：source/target chain protocol state transitions。
- 输入：cross-chain txs, chain finality events, timeout config。
- 输出：protocol events, final statuses, control decisions。
- baseline：lock_mint_serial, lock_mint_pipeline, fixed_window_baseline, committee_bridge_basic。
- MetaFlow 对应：metaflow_basic, metaflow_afs_fda。
- V3 阶段：V3.6。
- 不能做什么：V3.6 不要求 production bridge security; must not claim real threshold signatures unless implemented and validated。

## 11. MetricsPlugin

- 职责：collect and export metrics。
- 输入：runtime/plugin events。
- 输出：summary, latency, mechanism metrics, reports。
- baseline：basic_metrics。
- V3 阶段：V3.2+。
- 不能做什么：不能 rewrite or smooth results to make methods look better。

## 12. MetaTrack 插件组合

```text
baseline_hash_only:
  ShardingPlugin = hash_sharding
  ExecutionSchedulerPlugin = serial_execution
  StateAccessPlugin = direct_fetch
  CommitPlugin = normal_commit

co_access_only:
  ShardingPlugin = co_access_sharding
  ExecutionSchedulerPlugin = serial_execution
  StateAccessPlugin = direct_fetch
  CommitPlugin = normal_commit

co_access_dual_track:
  ShardingPlugin = co_access_sharding
  ExecutionSchedulerPlugin = dual_track_execution
  StateAccessPlugin = access_list_prefetch
  CommitPlugin = normal_commit

full_MetaTrack:
  ShardingPlugin = co_access_sharding
  ExecutionSchedulerPlugin = dual_track_execution
  StateAccessPlugin = access_list_prefetch
  CommitPlugin = hot_update_aggregation_commit
```

## 13. MetaFlow 插件组合

```text
serial_baseline:
  CrossChainProtocolPlugin = lock_mint_serial

pipeline_baseline:
  CrossChainProtocolPlugin = lock_mint_pipeline

fixed_window_baseline:
  CrossChainProtocolPlugin = fixed_window_baseline

committee_baseline:
  CrossChainProtocolPlugin = committee_bridge_basic

metaflow_basic:
  CrossChainProtocolPlugin = metaflow_basic

metaflow_afs_fda:
  CrossChainProtocolPlugin = metaflow_afs_fda
```

## 14. V3.3.1 Role Separation Semantics

V3.3.1 separates persistent state placement from execution routing:

```text
StatePlacement:
  phi(key) -> state_storage_unit_id

ExecutionRouting:
  M_t(tx/key) -> execution_shard_id
```

For MetaTrack, `hash_sharding` and `co_access_sharding` are execution-side routing plugins in the V3.3.1 Go-backed runtime. They do not migrate persistent state placement. The persistent location of state keys remains controlled by the chain profile `state.placement_policy`, currently `hash_state_storage`.

`shard_id` in old artifacts is retained as a compatibility alias for `execution_shard_id` and should not be used as the precise role-separated identifier.

## 15. V3.3.2 Plugin Matrix

V3.3.2 maps plugin profiles onto composer modules so the frontend can later show which blocks differ across methods:

```yaml
method_id: baseline_hash_only
label: Baseline Hash Only
module_plugins:
  Routing: hash_sharding
  Execution: serial_execution
  StateAccess: direct_fetch
  Commit: normal_commit
tags:
  - baseline
```

The current MetaTrack plugin profiles store this information as `module_plugins`, `label`, and `tags` on each method profile. This is a preview/fairness metadata layer. It does not add new runtime mechanisms.

Template-driven fairness allows differences only in modules listed by `variable_modules`.
