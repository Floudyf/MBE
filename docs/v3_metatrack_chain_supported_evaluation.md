# V3 MetaTrack Chain-supported Evaluation

## 0. V3.3 Go-backed Evaluation Note

V3.3 uses the Go-backed runtime as the execution path while preserving the V3.2 Python reference runtime as a semantic oracle. The first V3.3 profile is a smoke / controlled evaluation profile, not final paper-scale evidence.

## 1. MetaTrack 在 V3 中的位置

MetaTrack 是 V3.3 的单链插件评估主线。它在 V3.2 modular research chain runtime 上评估 co-access sharding、dual-track execution、access-list prefetch、hot-update aggregation commit 等机制。

## 2. 为什么不直接改 Fabric peer

不直接修改 Fabric peer 的原因：

- Fabric peer 内部路径复杂，难以公平替换每个研究插件。
- 修改 Fabric 内核会把实验问题变成 Fabric fork 维护问题。
- 共识、分片、执行、状态访问、提交路径不容易在 Fabric 中逐项隔离比较。
- V3 目标是研究链插件公平比较，而不是 Fabric 内核工程。

## 3. 为什么采用 modular runtime + Fabric-backed validation

Modular runtime 用于主实验：可控、可替换、可保存 profile、可公平比较。

Fabric-backed validation 用于真实性补充：采集真实 commit latency、tx status、block number、chaincode state updates，用于 calibration and small-scale validation，防止结果被理解为纯 simulation。

## 4. MetaTrack 插件组合

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

## 5. MetaTrack baseline 组合

所有 baseline 与 full_MetaTrack 必须使用同一 workload、seed、ChainProfile、block config、consensus config、hardware profile、submit rate、calibration profile。

只允许 `ShardingPlugin`、`ExecutionSchedulerPlugin`、`StateAccessPlugin`、`CommitPlugin` 不同。

## 6. Workload 要求

最低论文级 workload 要求：

- 不少于 10k / 50k / 100k 级 workload。
- 至少三种 hotspot / skew 场景。
- 至少包含可控 co-access pattern。
- 至少包含 aggregation candidate operations。
- 保存 seed 和 workload profile。

Tiny smoke workload 只能验证链路，不得作为最终性能结论。

## 7. Chain-backed trace 要求

Chain-backed trace 应包含：

- `tx_id`
- submit/commit timestamp where available。
- status。
- block number where available。
- operation type。
- state key / asset id。
- access metadata if collector can provide it。

如果 access-list 或 commutative semantics 不可靠，必须标记 semantic limitation。

## 8. Calibration 要求

至少一次 Fabric chain-backed calibration。Calibration profile 应记录：

- Fabric trace source。
- commit latency distribution。
- block interval/finality observation。
- suggested runtime parameters。
- error metrics。

同一 calibration profile 必须应用到 proposed method and baselines。

## 9. Metrics

MetaTrack metrics:

- `throughput_tps`
- `avg_latency_ms`
- `p95_latency_ms`
- `p99_latency_ms`
- `remote_fetch_count`
- `cross_shard_ratio`
- `fast_track_count`
- `conservative_track_count`
- `aggregated_update_count`
- `aggregation_ratio`
- `conflict_count`
- `queue_wait_ms`
- `block_commit_latency_ms`
- `calibration_error_ms`

## 10. Artifacts

MetaTrack artifacts:

- `used_chain_profile.yaml/json`
- `used_plugin_profile.yaml/json`
- `used_experiment_profile.yaml/json`
- `metatrack_summary.csv/json`
- `metatrack_latency.csv`
- `metatrack_mechanism_metrics.csv`
- `block_log.csv`
- `tx_results.csv`
- `state_commit_log.csv`
- `runtime.log`
- `report.md`

## 11. 实验阶段

- V3.1: profile schema/validator/preview。
- V3.2: minimal runtime。
- V3.3: MetaTrack plugin evaluation。
- V3.4: Fabric-backed validation。
- V3-final: frontend acceptance and report。

## 12. 论文表述口径

必须表述：

```text
MetaTrack full mechanism is evaluated in V3 modular research chain runtime.
Fabric is used for chain-backed observation, calibration, and small-scale real-chain validation.
V3 does not claim MetaTrack is implemented inside Fabric peer.
```

不得表述：

- MetaTrack 已修改 Fabric peer。
- Fabric validation 等于主实验 runtime。
- smoke-level V1 结果证明最终性能。
- local runtime result 是 Fabric live execution。

## 13. 最低论文级实验要求

- 不少于 10k / 50k / 100k 级 workload。
- 至少三种 hotspot / skew 场景。
- 至少一次 Fabric chain-backed calibration。
- 至少一次 Fabric real-chain validation。
- 所有 baseline 保存 used profiles。
