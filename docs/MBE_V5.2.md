# MBE V5 指标真值、产物契约与论文结果页闭合方案

> 文档定位：V5 后续修复与实现的正式执行说明  
> 适用仓库：`https://github.com/Floudyf/MBE`  
> 建议分支：`feat/v5-paper-console-and-metric-truth`  
> 建议阶段名称：`V5 Metric Truth, Artifact Contract, and Paper Result UX Closure`

---

## 1. 背景与本轮目标

MBE V5 已经完成以下主链闭合：

- 四种方法可通过前端创建正式 RunGroup 并运行完成：
  - Baseline / Hash + Serial
  - Hash + Block-STM
  - MetaTrack + Serial
  - MetaTrack + Block-STM
- 真实数据集可以完成物化、签名、nonce 检查与真实多进程回放。
- 真实 TCP、多节点、PBFT 风格消息、跨片协议、状态持久化和最终确认链路已形成完整运行证据。
- Block-STM 的执行正确性、活性、重执行与串行等价性已完成阶段验收。
- MetaTrack 的批量路由、双轨执行、远程状态获取、远程写回与热点更新聚合已进入真实运行主链。
- 前端可以展示 RunGroup、子实验、机制指标和原始产物。

当前剩余问题集中在三个方面：

1. **指标真值不够清晰**  
   部分指标存在分类错误、副本放大、含义混合、节点级数据未聚合、缺失指标未被识别等问题。

2. **产物契约不统一**  
   编译器声明、运行时实际写入、后端提取和前端展示使用的路径与文件语义存在差异。

3. **最终结果页可读性较差**  
   现有页面把大量子实验详情和原始产物平铺展示，核心实验结论不突出。最终页面需要收敛为两张经过分析的论文风格图：
   - End-to-End TPS
   - P99 Finality Latency

本轮目标是：

> 在不改动 Block-STM、MetaTrack、PBFT、跨片协议和状态提交主机制的前提下，完成指标真值、产物契约、结果分析和最终页面的统一闭合。

---

## 2. 本轮边界

### 2.1 本轮必须完成

1. 远程读写分类修复。
2. 副本级与逻辑级指标拆分。
3. MetaTrack 与 Block-STM 节点指标向根目录汇总。
4. 方法特定缺失指标检查。
5. `logical_update_count` 语义统一。
6. 预览与物化规则统一。
7. 产物路径与前端展示统一。
8. TPS、延迟与调度指标口径明确。
9. 增加 RunGroup 结果分析产物。
10. 重做最终结果页，仅保留 TPS 和 P99 两张论文风格图。
11. 清理平台中积累的旧运行记录、无用日志、孤儿运行目录和过期缓存。
12. 删除旧的用户实验方案与过时模板，重新建立简洁的正式实验方案。
13. 将区块大小与出块间隔接入正式实验配置、预览、编译、运行和结果证据。
14. 将默认区块大小从 10 提高到 100，将默认出块间隔从 150 ms 降低到 75 ms，并允许用户调整。

### 2.2 本轮不做

- 不重写 Block-STM 调度器。
- 不更换 MetaTrack 路由算法。
- 不修改 PBFT 消息状态机。
- 不修改跨片协议语义。
- 不接入新的链或虚拟机。
- 不增加十几种论文图。
- 不构建通用 BI 平台。
- 不对单样本数据生成虚假置信区间。
- 不把现有烟雾测试结果包装成正式论文结论。
- 不改变已经通过验收的正确性边界。

---

# 3. 问题一：远程读写分类修复

## 3.1 当前问题

运行时远程写回日志可能包含：

```text
write_apply
write_apply:commutative_delta
```

当前 Go 汇总和 Python 指标提取只把完全等于 `write_apply` 的记录识别为写回。

因此：

```text
write_apply:commutative_delta
```

被错误归入远程读取。

这会导致：

- `remote_state_read_count` 被高估；
- `remote_state_write_apply_count` 被低估；
- 前端展示与论文统计失真；
- 后续副本去重结果无法正确分类。

在已经完成的 1K MetaTrack + Serial 实验中，原始节点级物理日志为：

```text
read_write                         7,112
read                               2,628
commutative_delta                  1,596
write_apply:commutative_delta      6,876
write_apply                          904
总计                              19,116
```

正确分类应为：

```text
物理远程获取：
7,112 + 2,628 + 1,596 = 11,336

物理远程写回：
6,876 + 904 = 7,780

物理远程操作总数：
11,336 + 7,780 = 19,116
```

## 3.2 修改要求

### Go

所有远程状态分类逻辑改为：

```go
func isRemoteWritebackKind(kind string) bool {
    return strings.HasPrefix(strings.TrimSpace(kind), "write_apply")
}
```

禁止在多个文件中重复写字符串判断。

### Python

统一增加：

```python
def is_remote_writeback_kind(value: object) -> bool:
    return str(value or "").strip().startswith("write_apply")
```

所有指标提取、分析和 CSV 聚合统一调用该函数。

## 3.3 分类标准

| 原始 `access_kind` | 归一化类别 |
|---|---|
| `read` | `fetch` |
| `read_write` | `fetch` |
| `commutative_delta` | `fetch` |
| `write_apply` | `writeback` |
| `write_apply:commutative_delta` | `writeback` |
| 其他 `write_apply:*` | `writeback` |
| 未知空值 | `unknown` |

未知类型不能静默归入读取，应计入：

```text
remote_operation_unknown_kind_count
```

若未知类型大于 0：

- 在 child 指标中加入 warning；
- 在论文分析中标记样本不可直接使用；
- 保留原始记录供审计。

## 3.4 需要新增的测试

1. Go：
   - `write_apply` 被识别为写回；
   - `write_apply:commutative_delta` 被识别为写回；
   - `read_write` 被识别为获取；
   - 未知值进入 unknown。
2. Python：
   - 与 Go 使用相同测试样例；
   - 同一 CSV 输入得到一致分类。
3. 集成测试：
   - 使用包含五种 `access_kind` 的临时文件；
   - 验证总数守恒：

```text
fetch + writeback + unknown = total
```

## 3.5 验收条件

- 1K 旧实验重新分析后输出：
  - physical fetch = 11,336
  - physical writeback = 7,780
  - physical total = 19,116
- 不再出现 `write_apply:commutative_delta` 被归入读取。
- Go、Python、前端展示结果一致。

---

# 4. 问题二：副本级与逻辑级指标拆分

## 4.1 当前问题

当前：

```text
remote_state_access_count = 19,116
```

表示所有节点的成功远程操作日志总行数。

该值同时包含：

- 多验证者副本重复执行产生的远程获取；
- 执行片 leader 向 home shard 多副本发送写回并收集 ACK；
- 多区块重复访问同一状态键；
- 获取与写回两种不同语义。

该指标容易被误解成“1,000 笔交易发起了 19,116 次逻辑远程访问”。

## 4.2 指标层级

远程状态指标必须分成三层。

### 第一层：节点级物理操作

表示日志真实记录的网络操作。

```text
physical_remote_operation_count
physical_remote_fetch_count
physical_remote_writeback_count
physical_remote_failed_count
physical_remote_avg_latency_ms
```

### 第二层：副本去重操作

按照统一去重键合并多验证副本记录：

```text
replica_deduplicated_remote_operation_count
replica_deduplicated_remote_fetch_count
replica_deduplicated_remote_writeback_count
```

建议去重主键：

```text
normalized_kind
block_hash
execution_shard
home_shard
state_key
```

对于写回，还应加入能区分不同逻辑增量的字段：

```text
source_height
source_block_hash
delta_id 或稳定写回摘要
```

如果 CSV 当前缺少这些字段，应在写回日志中补充：

```text
delta_id
source_height
source_block_hash
update_semantics
```

读取去重键和写回去重键可以不同：

### 获取去重键

```text
block_hash
execution_shard
home_shard
state_key
normalized_kind=fetch
```

### 写回去重键

```text
source_block_hash
execution_shard
home_shard
state_key
delta_id
normalized_kind=writeback
```

### 第三层：按逻辑交易归一化

```text
remote_fetches_per_logical_tx
remote_writebacks_per_logical_tx
remote_operations_per_logical_tx
```

计算分母使用：

```text
submitted_unique_logical_tx_count
```

若提交数与终态数不一致，必须同时保存：

```text
per_submitted_logical_tx
per_terminal_logical_tx
```

论文默认使用完成样本中的：

```text
per_terminal_logical_tx
```

## 4.3 副本放大指标

新增：

```text
replica_amplification_factor
```

定义：

```text
physical_remote_operation_count
/
replica_deduplicated_remote_operation_count
```

同时分别输出：

```text
remote_fetch_replica_amplification_factor
remote_writeback_replica_amplification_factor
```

该值不能预设等于 `validators_per_shard`。必须从实际日志计算。

## 4.4 本轮 1K 实验的已知参考结果

按当前去重口径：

```text
物理操作总数：19,116
去副本操作总数：4,779

去副本远程获取：2,834
去副本远程写回：1,945

每笔逻辑交易远程获取：2.834
每笔逻辑交易远程写回：1.945

副本放大倍数：
19,116 / 4,779 = 4.0
```

这些值只作为回归参考。正式代码应根据 CSV 计算，禁止硬编码。

## 4.5 建议根目录产物

```text
aggregate/remote_state_metrics_summary.json
aggregate/replica_deduplicated_remote_operations.csv
```

`remote_state_metrics_summary.json` 示例：

```json
{
  "schema_version": "mbe_remote_state_metrics_v2",
  "physical": {
    "operation_count": 19116,
    "fetch_count": 11336,
    "writeback_count": 7780,
    "failed_count": 0
  },
  "replica_deduplicated": {
    "operation_count": 4779,
    "fetch_count": 2834,
    "writeback_count": 1945
  },
  "normalized": {
    "logical_tx_count": 1000,
    "fetches_per_logical_tx": 2.834,
    "writebacks_per_logical_tx": 1.945,
    "operations_per_logical_tx": 4.779
  },
  "replica_amplification": {
    "overall": 4.0,
    "fetch": 4.0,
    "writeback": 4.0
  },
  "unknown_kind_count": 0,
  "truth_scope": "node_physical_and_replica_deduplicated"
}
```

## 4.6 验收条件

- 总数守恒。
- 去重结果可由 CSV 重算。
- 副本放大来自真实比值。
- 获取与写回分别去重。
- 前端不再用模糊的 `remote_state_access_count` 作为唯一显示值。
- 旧字段可保留一段兼容期，但必须标记为 deprecated。

---

# 5. 问题三：MetaTrack / Block-STM 指标向根目录汇总

## 5.1 当前问题

部分机制指标仅写在：

```text
nodes/n0/
nodes/n1/
...
```

后端指标提取器和前端结果页主要读取根目录或 child summary，导致：

- 根结果缺少机制指标；
- `null` 被误当成“没有发生”；
- 多节点值可能被简单相加；
- Block-STM 节点产物存在，但正式 RunGroup 看不到；
- MetaTrack 快轨、保守轨和聚合指标可能无法用于分析。

## 5.2 聚合原则

不同指标使用不同聚合方式。

禁止将所有节点值直接求和。

### 可以求和的指标

适用于节点级物理事件总量：

```text
physical_remote_operation_count
physical_network_message_count
physical_scheduler_event_count
```

### 应做副本去重的指标

适用于逻辑事件：

```text
unique_logical_scheduling_decision_count
replica_deduplicated_remote_operation_count
unique_final_logical_completion_count
```

### 应取最大值的指标

```text
maximum_parallel_width
scheduler_ready_queue_max_depth
scheduler_fast_queue_max_depth
scheduler_conservative_queue_max_depth
```

### 应验证副本一致的指标

```text
worker_count
block_executor_id
block_executor_version
plan_digest_consistent
serial_equivalent
```

### 应仅使用 leader 的指标

若某些控制面计划只由 leader 生成，应输出：

```text
leader_value
```

并记录 leader 节点 ID。

### 应输出分布而非单值的指标

```text
abort_count_by_node
reexecution_count_by_node
execution_ms_by_node
```

根汇总可同时给：

```text
sum
mean
max
min
leader_value
replica_consistent
```

## 5.3 MetaTrack 根汇总

建议生成：

```text
aggregate/metatrack_aggregate_summary.json
```

至少包括：

```text
fast_track_logical_tx_count
conservative_track_logical_tx_count
fast_track_ratio
conservative_track_ratio

planning_scheduler_event_count
runtime_scheduler_event_count
unique_logical_scheduling_decision_count
blocked_logical_tx_count
wakeup_logical_tx_count

aggregation_group_count
aggregated_key_count
aggregated_logical_delta_count
pre_aggregation_physical_op_count
post_aggregation_physical_op_count
physical_ops_saved_count
aggregation_reduction_ratio

physical_remote_fetch_count
physical_remote_writeback_count
replica_deduplicated_remote_fetch_count
replica_deduplicated_remote_writeback_count
```

## 5.4 Block-STM 根汇总

建议生成：

```text
aggregate/block_stm_aggregate_summary.json
```

至少包括：

```text
worker_count
maximum_parallel_width
abort_count
reexecution_count
validation_failure_count
dependency_wait_count
estimate_publish_count
scheduler_event_count
serial_equivalent
state_root_equal
receipt_root_equal
```

对 abort、reexecution 等副本执行指标，需要同时给：

```text
physical_replica_sum
per_validator_mean
leader_value
```

论文中使用哪一种必须由指标定义明确指定。

## 5.5 根 summary 接入

`real_cluster_summary.json` 或 child 的统一 summary 中应嵌入：

```json
{
  "mechanism_metrics": {
    "metatrack": {},
    "block_stm": {},
    "remote_state": {}
  }
}
```

同时保留独立 JSON，便于审计和下载。

## 5.6 验收条件

- Block-STM 方法运行后根目录可读到完整机制汇总。
- MetaTrack 方法运行后根目录可读到完整机制汇总。
- 非 MetaTrack 方法的 MetaTrack 指标标记为 `not_applicable`。
- 非 Block-STM 方法的 Block-STM 指标标记为 `not_applicable`。
- 不再用空值混淆“0、缺失、不适用”。

---

# 6. 问题四：方法特定缺失指标检查

## 6.1 当前问题

部分关键字段为 `null` 时，child 仍可能显示：

```text
missing = []
```

这会导致缺少机制证据的实验被错误纳入论文分析。

## 6.2 三种状态必须区分

每个指标需要支持：

```text
available
missing
not_applicable
```

禁止只使用 `null`。

### `available`

产物存在且值通过校验。

### `missing`

该方法必须提供，但产物缺失或无法解析。

### `not_applicable`

该方法没有该机制。

## 6.3 方法必需指标矩阵

### Baseline / Hash + Serial

必须有：

```text
end_to_end_tps
logical_finality_tps
p95_finality_ms
p99_finality_ms
submitted_unique_tx_count
terminal_unique_tx_count
state_root_consistent
receipt_root_consistent
plan_digest_consistent
no_fallback
```

### Hash + Block-STM

除通用指标外，必须有：

```text
worker_count
maximum_parallel_width
abort_count
reexecution_count
validation_failure_count
serial_equivalent
```

### MetaTrack + Serial

除通用指标外，必须有：

```text
fast_track_logical_tx_count
conservative_track_logical_tx_count
replica_deduplicated_remote_fetch_count
replica_deduplicated_remote_writeback_count
aggregation_group_count
pre_aggregation_physical_op_count
post_aggregation_physical_op_count
```

### MetaTrack + Block-STM

必须同时满足 MetaTrack 和 Block-STM 的必需指标。

## 6.4 缺失处理

当关键指标缺失时：

1. child summary：
   ```text
   metric_completeness = incomplete
   ```
2. `missing` 中列出具体字段。
3. RunGroup 汇总列出：
   ```text
   incomplete_metric_children
   ```
4. 结果分析不纳入该样本。
5. 前端两张主图显示：
   ```text
   数据不完整
   ```
6. 允许用户下载原始产物检查。
7. RunGroup 运行状态可以仍是 `completed`，但：
   ```text
   paper_analysis_status = incomplete
   ```

## 6.5 验收条件

- 人为删除一个 Block-STM summary 后，Block-STM child 必须报告缺失。
- 人为删除一个 MetaTrack remote summary 后，MetaTrack child 必须报告缺失。
- Baseline 不因缺少 Block-STM 指标而失败。
- 缺失样本不会进入 TPS/P99 图。

---

# 7. 问题五：`logical_update_count` 统一语义

## 7.1 当前问题

普通提交插件和聚合提交插件对：

```text
logical_update_count
```

使用了不同定义。

可能分别表示：

- 区块交易数；
- 聚合前物理操作数；
- 交易数与物理操作数的最大值。

该字段无法进行跨方法公平比较。

## 7.2 新指标定义

废弃模糊字段，新增：

```text
executed_logical_transaction_count
executed_transaction_instance_count

pre_aggregation_physical_op_count
post_aggregation_physical_op_count

aggregated_logical_delta_count
aggregated_key_count
physical_ops_saved_count
aggregation_reduction_ratio
```

## 7.3 精确定义

### `executed_logical_transaction_count`

按 logical tx ID 去重后的成功执行交易数。

### `executed_transaction_instance_count`

所有执行副本和跨片阶段的实际执行实例数。

### `pre_aggregation_physical_op_count`

提交插件执行聚合前，待应用的物理状态操作数。

### `post_aggregation_physical_op_count`

聚合后实际进入状态提交或远程写回的物理操作数。

### `aggregated_logical_delta_count`

被合并进聚合操作的逻辑增量条数。

### `aggregated_key_count`

发生聚合的唯一状态键数。

### `physical_ops_saved_count`

```text
pre_aggregation_physical_op_count
-
post_aggregation_physical_op_count
```

### `aggregation_reduction_ratio`

```text
physical_ops_saved_count
/
pre_aggregation_physical_op_count
```

分母为 0 时返回 0，并标明无聚合输入。

## 7.4 非聚合方法

对于普通提交：

```text
pre_aggregation_physical_op_count
=
post_aggregation_physical_op_count
```

```text
aggregated_logical_delta_count = 0
aggregated_key_count = 0
physical_ops_saved_count = 0
aggregation_reduction_ratio = 0
```

## 7.5 兼容策略

原字段：

```text
logical_update_count
physical_update_count
```

可暂时保留一个版本，增加：

```text
deprecated = true
```

前端和论文分析停止使用旧字段。

## 7.6 验收条件

- 四种方法的更新指标语义一致。
- 普通提交的 pre/post 相等。
- 聚合提交的 post 不大于 pre。
- `physical_ops_saved_count` 与差值一致。
- reduction ratio 范围为 `[0, 1]`。

---

# 8. 问题六：预览与物化规则统一

## 8.1 当前问题一：1K 支持不一致

前端可以选择 1K，预览阶段显示可运行，物化阶段默认只允许：

```text
10K
50K
100K
250K
```

导致四个方法在物化阶段全部失败。

临时环境变量：

```text
MBE_V5_LOCAL_SMOKE_COUNTS=1000
```

可以绕过，但平台规则仍不统一。

## 8.2 正式支持规模

建议正式支持：

```text
1K
10K
50K
100K
250K
全量数据
```

1K 用于：

- 前端快速功能验证；
- 小规模机制检查；
- 结果页测试；
- 教学与演示。

正式论文数据仍要求更大规模和多次重复。

## 8.3 单一支持规模来源

后端增加统一函数或配置：

```python
def supported_workload_counts() -> list[int]:
    ...
```

所有位置调用同一来源：

```text
数据集列表 API
前端规模选择器
Workload Preview
Experiment Matrix Preview
materialize_request
Experiment Compiler
Formal RunGroup
```

环境变量可以继续作为开发扩展，但不应成为前端正常使用的必需步骤。

## 8.4 当前问题二：预览百分比错误

当前预览把完整数据集类别数放入 1K 窗口，并以 1,000 为分母，造成百分比超过 100%。

## 8.5 预览统计分层

预览接口明确返回两个对象：

```json
{
  "dataset_summary": {},
  "selected_window_preview": {}
}
```

### `dataset_summary`

表示完整数据集：

```text
total_tx_count
full_time_range
full_category_counts
full_operation_counts
full_natural_skew
```

### `selected_window_preview`

表示当前请求实际会使用的窗口：

```text
requested_tx_count
actual_selected_count
selected_time_range
category_counts
operation_counts
realized_skew
cross_shard_count
cross_shard_ratio
shard_distribution
selection_digest
```

## 8.6 预览与物化共用选择函数

必须让 preview 和 materialize 调用同一核心选择逻辑：

```text
select_workload_rows(request)
```

预览只是不写物化文件。

禁止两套独立实现。

## 8.7 百分比规则

类别百分比：

```text
selected_category_count
/
actual_selected_count
```

全量类别百分比：

```text
full_category_count
/
total_tx_count
```

前端必须清楚标注“选中窗口”或“全数据集”。

## 8.8 验收条件

- 1K 无需环境变量可正常运行。
- 预览和物化得到相同 selection digest。
- 类别百分比总和约等于 100%。
- selected count 不超过请求数和数据集总量。
- 预览显示的偏斜、时间范围和分片分布来自实际选中窗口。

---

# 9. 问题七：产物路径与前端展示统一

## 9.1 当前问题

同名产物可能存在于：

```text
client/
nodes/n*/
run root/
```

并具有不同语义。

例如：

```text
client/remote_state_access.csv
```

可能是计划或预测数据。

```text
nodes/n*/remote_state_access.csv
```

是节点级物理运行数据。

后端和前端若只按文件名判断，容易拿错。

## 9.2 建议目录结构

```text
run_root/
├── compiled/
│   ├── compiled_run_plan.json
│   └── compiled_workload_plan.json
├── workload/
│   ├── workload_manifest_snapshot.json
│   ├── workload_materialization_summary.json
│   └── workload_replay_summary.json
├── client/
│   ├── client_submission_log.csv
│   ├── predicted_remote_access.csv
│   └── resolved_access_lists.jsonl.gz
├── nodes/
│   ├── n0/
│   ├── n1/
│   └── ...
├── aggregate/
│   ├── real_cluster_summary.json
│   ├── finality_summary.json
│   ├── remote_state_metrics_summary.json
│   ├── replica_deduplicated_remote_operations.csv
│   ├── metatrack_aggregate_summary.json
│   ├── block_stm_aggregate_summary.json
│   ├── mechanism_metrics_summary.json
│   ├── paper_result_analysis.json
│   └── paper_result_analysis.csv
├── logs/
│   ├── supervisor_stdout.log
│   └── supervisor_stderr.log
└── artifact_catalog.json
```

如暂时不迁移旧目录，也应通过 artifact catalog 为每个文件明确：

```text
artifact_role
truth_scope
producer
schema_version
```

## 9.3 产物命名

建议区分：

```text
predicted_remote_access.csv
physical_remote_state_operations.csv
replica_deduplicated_remote_operations.csv
```

避免三个阶段使用同一个 `remote_state_access.csv`。

## 9.4 Artifact Catalog

每个产物至少记录：

```json
{
  "name": "aggregate/remote_state_metrics_summary.json",
  "artifact_role": "aggregate_metric",
  "truth_scope": "cluster_replica_deduplicated",
  "producer": "v5_metric_aggregator",
  "schema_version": "mbe_remote_state_metrics_v2",
  "size_bytes": 1234,
  "sha256": "...",
  "download_url": "..."
}
```

## 9.5 前端展示

结果页按以下分组：

1. 核心分析结果
2. 聚合指标
3. 子实验
4. 节点级机制证据
5. 客户端与工作负载证据
6. 日志与审计

默认只展开：

```text
核心分析结果
```

其余折叠。

## 9.6 预期产物校验

编译器的 `expected_artifacts` 必须与真实路径一致。

运行结束后生成：

```text
artifact_contract_status
missing_expected_artifacts
unexpected_artifacts
```

## 9.7 验收条件

- 前端能下载节点级 MetaTrack 产物。
- 前端能下载节点级 Block-STM 产物。
- 聚合产物优先展示。
- 预测、物理和去副本远程访问产物名称不同。
- 编译器预期路径与实际路径一致。
- 缺失预期产物会明确显示。

---

# 10. 问题八：TPS、延迟与调度指标口径明确

## 10.1 TPS 主口径

正式结果页主图使用：

```text
end_to_end_tps
```

定义：

```text
terminal_unique_logical_tx_count
/
completion_duration_seconds
```

起点和终点必须由 `finality_summary.json` 明确记录。

## 10.2 辅助 TPS

继续保留：

```text
logical_finality_tps
```

定义：

```text
terminal_unique_logical_tx_count
/
logical_finality_duration_seconds
```

最终页面主图只画 End-to-End TPS。

“查看数据”中同时展示两个值。

## 10.3 时间窗口

需要明确输出：

```text
first_submission_at_ms
logical_finality_completed_at_ms
drain_completed_at_ms

logical_finality_duration_ms
completion_duration_ms
tail_completion_overhead_ms
```

关系：

```text
completion_duration_ms
=
logical_finality_duration_ms
+
tail_completion_overhead_ms
```

允许少量时间戳舍入误差。

## 10.4 延迟主口径

最终页面默认：

```text
p99_finality_ms
```

延迟定义：

```text
client submitted
→
logical transaction terminal
```

### 片内终态

```text
durable_committed
```

### 跨片终态

```text
source_finalize
refund
failed
```

目标片提交不能被视为完整跨片终态。

## 10.5 P95/P99

页面默认 P99。

允许在延迟图右上角切换：

```text
P95 / P99
```

仍保持一张延迟图。

## 10.6 调度指标拆分

当前 `scheduler_event_count` 混合多种事件。

新增：

```text
planning_scheduler_event_count
runtime_scheduler_event_count

leader_scheduler_event_count
replica_scheduler_event_count

unique_logical_scheduling_decision_count
blocked_logical_tx_count
wakeup_logical_tx_count
dependency_wait_event_count
work_steal_attempt_count
work_steal_success_count
```

每个事件必须带：

```text
event_scope
event_phase
node_role
logical_tx_id
```

### `event_scope`

```text
physical_node_event
logical_decision
```

### `event_phase`

```text
planning
runtime
```

## 10.7 前端展示规则

最终页面不画调度图。

调度指标进入：

```text
机制指标（折叠）
```

这样既保留研究证据，也不会让主结果页过载。

## 10.8 验收条件

- 主图 TPS 明确使用 end-to-end。
- 数据抽屉同时显示 logical finality TPS。
- P99 延迟起止事件明确。
- 调度事件不再使用一个混合总数字解释机制开销。
- 旧字段标记 deprecated。

---

# 11. RunGroup 结果分析层

## 11.1 目标

实验完成后不能直接把 child 的单次数字塞进图表。

需要先执行：

```text
完整性检查
→ 公平性检查
→ 样本筛选
→ 分组统计
→ 生成分析结果
→ 前端绘图
```

## 11.2 新增产物

```text
aggregate/paper_result_analysis.json
aggregate/paper_result_analysis.csv
```

## 11.3 样本纳入条件

只有同时满足以下条件的 child 才能进入主图：

```text
status = completed
terminal_unique_tx_count = submitted_unique_tx_count
incomplete_unique_tx_count = 0
no_fallback = true
state_root_consistent = true
receipt_root_consistent = true
plan_digest_consistent = true
metric_completeness = complete
```

Block-STM 方法还需：

```text
serial_equivalent = true
```

## 11.4 公平性条件

同一比较组必须一致：

```text
dataset_id
materialized_sha256
source_sha256
requested_tx_count
actual_tx_count
seed
topology
validators_per_shard
fault_policy
block_size
worker_count（若不是扫描变量）
```

不一致时：

```text
fairness_status = failed
```

主图不绘制。

## 11.5 分组

按以下维度分组：

```text
method_config_id
method_display_name
scan_variable
scan_value
```

当前单点四方法实验：

```text
scan_variable = none
```

## 11.6 统计

每组计算：

```text
valid_sample_count
excluded_sample_count
raw_values
mean
median
std
min
max
ci95_low
ci95_high
```

## 11.7 单样本

当：

```text
valid_sample_count = 1
```

输出：

```text
mean = 当前值
std = null
ci95_low = null
ci95_high = null
statistical_note = "single_sample_no_variance_or_ci"
```

前端显示：

```text
n = 1，暂无标准差和 95% CI
```

不画误差棒。

## 11.8 异常值处理

本轮禁止自动删除异常值。

若未来增加异常值规则，需要：

- 预先定义规则；
- 保存被排除样本；
- 给出排除原因；
- 允许用户查看原始值。

## 11.9 JSON 示例

```json
{
  "schema_version": "mbe_paper_result_analysis_v1",
  "run_group_id": "v5grp_xxx",
  "analysis_status": "complete",
  "fairness_status": "passed",
  "metrics": {
    "end_to_end_tps": [
      {
        "method_id": "hash_serial",
        "method_name": "Baseline",
        "valid_sample_count": 1,
        "excluded_sample_count": 0,
        "raw_values": [80.12],
        "mean": 80.12,
        "std": null,
        "ci95_low": null,
        "ci95_high": null,
        "statistical_note": "single_sample_no_variance_or_ci"
      }
    ],
    "p99_finality_ms": []
  },
  "excluded_samples": []
}
```

## 11.10 CSV 字段

```text
run_group_id
metric_name
metric_unit
method_id
method_name
valid_sample_count
excluded_sample_count
mean
median
std
min
max
ci95_low
ci95_high
statistical_note
source_child_ids
```

---

# 12. 最终结果页改造

## 12.1 页面目标

主结果页只突出两个问题：

1. 哪个方法吞吐量更高？
2. 哪个方法尾延迟更低？

## 12.2 页面结构

```text
RunGroup 紧凑摘要
────────────────────────────────────────────
End-to-End TPS       | P99 Finality Latency
────────────────────────────────────────────
分析说明
子实验详情（折叠）
机制指标（折叠）
原始产物（折叠）
日志与审计（折叠）
```

## 12.3 顶部摘要

仅显示：

```text
RunGroup ID
数据集
实际交易数
拓扑
方法数
有效样本数
状态
```

详细配置放入折叠面板。

## 12.4 图一：End-to-End TPS

- 图类型：竖向柱状图。
- 横轴：方法。
- 纵轴：End-to-End TPS。
- 柱顶显示数值。
- 多样本时显示 95% CI 误差棒。
- 单样本显示 `n=1`。
- Tooltip 显示：
  - mean
  - std
  - 95% CI
  - sample count
  - excluded count

## 12.5 图二：P99 Finality Latency

- 图类型：竖向柱状图。
- 横轴：方法。
- 纵轴：P99 Finality Latency（ms）。
- 数值越低越好。
- 支持 P95/P99 小型切换。
- 默认 P99。
- 多样本时显示 95% CI。
- 单样本显示 `n=1`。

## 12.6 方法顺序

固定顺序：

```text
Baseline
Block-STM
MetaTrack
MetaTrack + Block-STM
```

不能按数值动态重新排序，避免不同图中方法位置变化。

## 12.7 方法配色

参考用户提供的论文绘图控制台风格：

```text
Baseline                 蓝灰色
Block-STM                棕黄色
MetaTrack                粉红色
MetaTrack + Block-STM    绿色
```

建议默认值：

```text
Baseline                 #6382AA
Block-STM                #BE9A62
MetaTrack                #FA8095
MetaTrack + Block-STM    #56B76A
```

配色集中放入：

```text
figureStyles.ts
```

禁止散落在组件中。

## 12.8 样式

- 白色背景。
- 浅灰网格线。
- 字体清晰。
- 图例简洁。
- 适当留白。
- 柱体宽度统一。
- 图卡片带轻边框和轻阴影。
- 桌面端一行两张。
- 小屏上下排列。
- 不使用纯黑粗柱。
- 不显示大量控制项。
- 不把用户提供的完整控制台照搬进页面。

## 12.9 每张图底部功能

只保留：

```text
查看分析数据
下载 CSV
下载 PNG
下载 SVG
下载 PDF
```

### 查看分析数据

展开表格：

```text
方法
有效样本数
排除样本数
原始值
均值
标准差
95% CI
数据来源
统计说明
```

## 12.10 导出

### CSV

由后端分析产物生成，保证与图一致。

### PNG / SVG

由 ECharts 导出。

### PDF

将当前图导出为单页 PDF。

文件名示例：

```text
v5grp_xxx_end_to_end_tps.png
v5grp_xxx_p99_finality_latency.csv
```

## 12.11 前端组件建议

```text
frontend/src/components/v5/results/
├── V5RunGroupResultPage.tsx
├── V5ResultSummaryBar.tsx
├── V5CoreResultCharts.tsx
├── V5PaperBarChart.tsx
├── V5ChartDataDrawer.tsx
├── V5ChartExportButtons.tsx
├── V5AdvancedResultSections.tsx
├── figureStyles.ts
└── types.ts
```

只需两种图，不建立庞大的通用图表系统。

## 12.12 API

建议新增：

```text
GET /api/v5/formal-runs/{run_group_id}/result-analysis
```

返回：

```text
run_group summary
analysis status
fairness status
TPS analysis
P95 analysis
P99 analysis
download URLs
```

---

# 13. 后端服务建议

建议新增：

```text
backend/app/services/v5_metric_truth.py
backend/app/services/v5_mechanism_metric_aggregator.py
backend/app/services/v5_result_analyzer.py
backend/app/services/v5_artifact_contract.py
```

## 13.1 `v5_metric_truth.py`

负责：

- 远程操作类型归一化；
- 副本去重键；
- TPS/延迟定义；
- 指标状态 `available/missing/not_applicable`；
- 更新指标定义。

## 13.2 `v5_mechanism_metric_aggregator.py`

负责：

- 扫描 `nodes/n*/`；
- 聚合 MetaTrack；
- 聚合 Block-STM；
- 生成根目录机制汇总；
- 生成远程状态去重数据。

## 13.3 `v5_result_analyzer.py`

负责：

- child 完整性检查；
- 公平性检查；
- 样本筛选；
- TPS/P95/P99 分析；
- 多种子统计；
- 生成 JSON/CSV。

## 13.4 `v5_artifact_contract.py`

负责：

- 定义预期产物；
- 检查真实路径；
- 建立 artifact catalog；
- 标记缺失与额外产物；
- 提供前端分类信息。

---

# 14. 可能修改的现有文件

根据当前仓库结构，预计涉及：

```text
executor/v5/runtime.go
executor/v5/runtime_test.go

backend/app/services/v5_metric_extractor.py
backend/app/services/v5_real_cluster_runner.py
backend/app/services/v5_experiment_compiler.py
backend/app/services/v5_workload_data_plane.py

backend/app/models/v5_compiled_run_plan.py
backend/app/models/v5_experiment_spec.py

backend/tests/test_v5_plugin_platform.py
backend/tests/test_v5_drain_timing_metrics.py
backend/tests/test_v5_workload_data_plane.py

frontend/src/api.ts
frontend/src/components/v5/V5ChildDetail.tsx
frontend/src/components/v5/*
frontend/src/pages/*
```

具体文件以本地搜索结果为准。

---


# 15. 平台历史运行、日志与缓存清理

## 15.1 当前问题

平台经过 V0、V1、V2、V3、V4 和 V5 多轮开发后，本地运行目录中已经积累了大量：

```text
旧 RunGroup 元数据
旧 child experiment 元数据
失败尝试记录
Supervisor stdout/stderr
节点级 network / consensus / execution 日志
旧 real-cluster 输出目录
已失去父 RunGroup 引用的孤儿运行目录
前端最近访问记录
过期的临时预览与导出文件
旧版 saved config / method profile
```

当前 V5 的主要运行目录包括：

```text
.cache/v5_formal_runs/
.cache/v5_real_cluster_runs/
```

工作负载缓存位于：

```text
.cache/workloads/
```

本轮需要清理平台历史垃圾，同时保留仍有价值的实验证据。

## 15.2 清理边界

清理功能只能处理 MBE 明确管理的运行目录。

允许处理：

```text
FORMAL_RUN_ROOT
V5_REAL_CLUSTER_RUNS_ROOT
平台生成的临时导出目录
平台生成的旧 saved config 记录
```

默认不处理：

```text
Git 跟踪源码
docs/
configs/ 中仍被代码引用的正式配置
用户上传的原始数据集
workload canonical source
materialized workload cache
用户手工放置的文件
当前正在运行的 RunGroup
被标记为保留或论文候选的 RunGroup
```

严禁使用宽泛删除命令：

```text
git clean
git reset --hard
对仓库根目录递归删除
按文件扩展名全盘删除
```

所有删除必须经过路径白名单与对象状态检查。

## 15.3 RunGroup 清理功能

后端增加明确的删除服务：

```text
delete_run_group(run_group_id)
delete_selected_run_groups(run_group_ids)
delete_failed_run_groups()
delete_old_unpinned_run_groups(before_time)
```

删除一个 RunGroup 时，需要同时清理：

```text
.cache/v5_formal_runs/<run_group_id>/
该 RunGroup child.result.output_dir 指向的 real-cluster 目录
该 RunGroup 的临时 ZIP / PDF / 图表导出缓存
浏览器最近访问记录中的对应 ID
```

删除前必须检查：

```text
status 不属于 queued / starting / running
路径位于允许的根目录下
real-cluster output_dir 确实属于 MBE 管理目录
没有其他 RunGroup 引用同一 output_dir
```

## 15.4 孤儿运行目录清理

新增扫描：

```text
referenced_real_cluster_dirs
actual_real_cluster_dirs
orphan_real_cluster_dirs
```

其中：

```text
orphan_real_cluster_dirs
=
actual_real_cluster_dirs
-
referenced_real_cluster_dirs
```

孤儿目录只能在以下条件下删除：

```text
目录位于 V5_REAL_CLUSTER_RUNS_ROOT
目录名满足 v5_* 受控格式
目录中不存在 active_process 标志
目录修改时间早于安全窗口
```

默认安全窗口建议为：

```text
24 小时
```

避免误删刚结束但尚未完成聚合的运行。

## 15.5 日志保留规则

日志分成三类。

### 核心证据

必须保留：

```text
finality_summary.json
real_cluster_summary.json
paper_result_analysis.json
paper_result_analysis.csv
fairness artifacts
state / receipt / plan digest consistency evidence
aggregate mechanism summaries
artifact_catalog.json
```

### 可压缩节点证据

可以保留在 ZIP 中，页面默认不展开：

```text
network_log.csv
consensus_message_log.csv
transaction_lifecycle.csv/jsonl
execution traces
node summaries
Block-STM traces
MetaTrack traces
```

### 临时或重复日志

可在分析完成并生成归档包后删除：

```text
重复 stdout/stderr 临时副本
中间 preview 临时文件
已被 artifact bundle 收录的临时导出文件
原子写入残留 .tmp
失败启动且未产生有效 child 的空目录
```

任何日志删除都必须在：

```text
artifact_catalog 已生成
核心分析产物已生成
归档包校验成功
```

之后执行。

## 15.6 前端清理入口

在结果中心或设置页增加：

```text
删除当前实验组
批量删除选中实验组
清理失败实验
清理孤儿运行目录
查看可释放空间
```

删除必须二次确认，并显示：

```text
将删除的 RunGroup 数
将删除的 child 数
将删除的 real-cluster 目录数
预计释放空间
保留的核心归档
```

不增加模糊的“一键清空整个仓库”。

## 15.7 一次性迁移清理

本轮实现完成后执行一次受控迁移：

1. 扫描全部 V5 RunGroup。
2. 标记当前仍有效的完成实验。
3. 保留最新一次成功四方法实验。
4. 保留明确标记为 paper candidate / pinned 的实验。
5. 删除失败、阻塞、空结果、重复 smoke run。
6. 删除无引用的 real-cluster 孤儿目录。
7. 删除旧前端本地 recent group ID。
8. 输出清理报告：

```text
cleanup_report.json
cleanup_report.csv
```

报告至少包括：

```text
deleted_run_group_ids
deleted_output_dirs
preserved_run_group_ids
skipped_active_runs
released_bytes
errors
```

## 15.8 验收条件

- 正在运行的 RunGroup 无法删除。
- 删除一个完成 RunGroup 后，其 formal 元数据和专属 real-cluster 目录同时消失。
- 共享目录不会被重复删除。
- 工作负载源文件与物化数据默认保留。
- 清理前后都生成可审计报告。
- 前端列表不再显示已删除方案和运行。
- 清理功能不会触碰 Git 跟踪文件。

---

# 16. 删除旧实验方案并重新建立正式方案

## 16.1 当前问题

当前正式实验页同时加载：

```text
目录默认基线
四个内置方法
V3 saved config 中解析出的历史方法
多种 suite
旧 workload/topology/fault 扫描点
```

长期使用后会出现：

- 旧方法方案堆积；
- 方法名称和角色混乱；
- 不再兼容当前 V5 插件目录的方案仍出现在平台；
- 用户难以判断哪个方案才是当前正式实验；
- 区块生产参数被隐藏在插件默认配置中；
- 旧模板与正式四方法对比混在一起。

## 16.2 删除范围

删除：

```text
旧用户 saved method configs
旧 Formal Plan configs
旧 RunGroup 创建方案
已废弃的 V5 前端实验模板
不再兼容当前插件目录的历史方案
仅用于旧 smoke 阶段且不再需要展示的方案
```

保留：

```text
四种内置执行方法的真实插件定义
插件 manifest
算法复现文档
测试夹具中明确需要的最小配置
历史 docs 中用于审计的说明
```

“删除旧方案”指清理平台可选方案和保存记录，不删除四种方法的实现代码。

## 16.3 新方案结构

重新建立一个主要方案：

```text
正式四方法对比实验
```

固定包含：

```text
Baseline
Block-STM
MetaTrack
MetaTrack + Block-STM
```

用户仍可取消某个方法，但默认四个全部选中。

方案配置分成四组：

### 负载

```text
数据来源
数据集
变体
交易数量
Seed
偏斜度
```

### 区块生产

```text
每块交易数 block_size
出块间隔 block_interval_ms
```

### 拓扑与执行

```text
节点数
分片数
每片验证者
重复次数
Block-STM worker_count
```

### 故障

默认折叠，正常实验中保持关闭。

## 16.4 新建方案的唯一默认值

建议新正式方案默认：

```text
方法：
Baseline
Block-STM
MetaTrack
MetaTrack + Block-STM

拓扑：
nodes = 8
shards = 2
validators_per_shard = 4

block_size = 100
block_interval_ms = 75

repeats = 1
faults = disabled
```

1K 用于功能回归。

正式性能实验再扫描：

```text
block_size = 100 / 250 / 500 / 1000
worker_count = 1 / 2 / 4 / 8
```

## 16.5 保存新方案

若继续支持保存方案，保存对象必须使用新 schema：

```text
v5_formal_experiment_profile_v2
```

保存内容必须包括：

```text
method_ids
workload settings
topology
block_size
block_interval_ms
worker settings
repeat settings
schema version
created_at
updated_at
compatibility snapshot
```

旧 schema 不再直接加载到正式实验页。

可以提供一次只读迁移预览，但本轮按用户要求，旧方案直接从可选列表清除。

## 16.6 验收条件

- 正式实验页默认只显示新的正式四方法方案。
- 旧 saved methods 不再混入方法列表。
- 四个方法仍由当前内置插件组合构造。
- 新方案包含区块大小和出块间隔。
- 新方案预览、保存、重新加载后参数不丢失。
- 旧方案删除后前端和 API 均不可再选。

---

# 17. 区块大小与出块速度可调

## 17.1 当前问题

当前 block producer manifest 默认：

```text
block_size = 10
interval_ms = 150
```

正式实验页没有提供区块生产参数输入框，因此用户无法直接调整。

当前四种方法都从插件目录默认配置构造实验 spec，导致每块只有 10 笔交易。

1K 交易在该配置下会形成大量小区块，使以下固定成本被重复支付：

```text
区块提议
PBFT 消息
状态持久化
远程状态准备
提交与快照
调度器初始化
Block-STM 块级结构建立
MetaTrack 批级共现分析
```

这会显著放大固定开销，并限制每个区块内可利用的并行度。

## 17.2 参数命名

前端显示：

```text
每块交易数
出块间隔（毫秒）
```

内部字段统一为：

```text
block_size
block_interval_ms
```

插件 config 仍可映射为：

```text
interval_ms
```

在 ExperimentSpec 和 Formal Plan 中建议使用更清楚的：

```text
block_interval_ms
```

编译时写入 block producer config：

```json
{
  "category": "block_producer",
  "plugin_id": "time_or_count_block_producer",
  "config": {
    "block_size": 100,
    "interval_ms": 75
  }
}
```

## 17.3 默认值调整

正式默认值修改为：

```text
block_size：10 → 100
interval_ms：150 → 75
```

含义：

```text
默认每块容量扩大 10 倍
默认出块间隔缩短一半
```

运行时 Go fallback 也必须同步，禁止 manifest 默认 100、Go fallback 仍为 10。

所有默认来源统一：

```text
后端 plugin manifest
前端正式方案初始值
Go block producer fallback
接受测试默认参数
文档示例
E2E fixture
```

## 17.4 前端控件

在“拓扑与重复”前增加“区块生产”卡片：

```text
每块交易数：[100]
出块间隔：[75] ms
```

建议约束：

### block_size

```text
minimum = 10
maximum = 5000
step = 10
default = 100
```

提供快捷值：

```text
100
250
500
1000
```

### block_interval_ms

```text
minimum = 25
maximum = 5000
step = 25
default = 75
```

提供快捷值：

```text
50
75
100
200
500
```

区块间隔越小，出块越快。前端应直接显示说明，避免用户把数值调大误认为速度更快。

## 17.5 参数进入完整主链

参数必须依次进入：

```text
V5FormalRunPage state
→ V5FormalRunRequest
→ base_spec.plugin_selections
→ Formal preview row
→ immutable Child ExperimentSpec
→ compiled_run_plan.json
→ node plugin profile
→ Go block producer
→ node summary
→ real_cluster_summary
→ paper_result_analysis source configuration
```

任何环节不得回退到隐藏默认值。

## 17.6 预览展示

Formal Matrix Preview 增加：

```text
block_size
block_interval_ms
estimated_block_count
```

估算：

```text
estimated_block_count
=
ceil(estimated_transactions / block_size)
```

该值只作为估算，不当作实际出块数量。

## 17.7 公平性

四种方法比较时，以下参数必须相同：

```text
block_size
block_interval_ms
```

公平性快照和 fairness key 必须包含这两个字段。

如果不同：

```text
fairness_status = failed
```

不得进入 TPS/P99 主图。

## 17.8 运行结果证据

每个 child 根 summary 输出：

```text
configured_block_size
configured_block_interval_ms
actual_committed_block_count
actual_average_tx_per_block
actual_min_tx_per_block
actual_max_tx_per_block
actual_block_interval_mean_ms
actual_block_interval_p95_ms
```

其中实际区块间隔来自提交区块时间戳，不能用配置值冒充。

## 17.9 参数扫描

本轮前端只需支持手动设置单个值。

后续正式实验可增加独立扫描：

```text
block_size_sensitivity
```

扫描点：

```text
100
250
500
1000
```

出块间隔暂不与 block size 同时做笛卡尔积，避免实验规模失控。

## 17.10 与吞吐量的关系

提高 block size 通常可以：

- 减少相同交易量下的区块数量；
- 摊薄 PBFT 和持久化固定开销；
- 为 Block-STM 提供更多块内并行交易；
- 为 MetaTrack 提供更大的批级共现分析窗口。

缩短 interval 可以让未填满区块更快提议，但过短会产生更多未满小块。

因此运行时仍采用：

```text
达到 block_size
或
达到 block_interval_ms
```

任一条件满足即出块。

默认 `100 / 75 ms` 用于替换当前明显偏小的 `10 / 150 ms`。正式论文结果必须继续做区块大小扫描，不能只凭一个默认值下结论。

## 17.11 需要修改的主要文件

预计包括：

```text
backend/app/services/v5_plugin_manifest_store.py
frontend/src/pages/V5FormalRunPage.tsx
frontend/src/api.ts
frontend/src/v5MethodProfile.ts
backend/app/models/v5_experiment_spec.py
backend/app/models/v5_formal_experiment.py
backend/app/services/v5_formal_scheduler.py
backend/app/services/v5_experiment_compiler.py
executor/v5/registry.go
executor/v5/runtime.go
相关 Go / Python / Playwright 测试
```

## 17.12 验收条件

- 前端可以修改 block size。
- 前端可以修改 block interval。
- 默认值为 `100 / 75 ms`。
- 四种方法的 child spec 中参数一致。
- 编译产物包含参数。
- Go 运行时实际读取参数。
- 结果摘要显示配置值与实际区块统计。
- 修改参数后 preview 自动失效，必须重新预览。
- 1K 实验实际区块数明显少于原先约 100 个小区块。
- TPS/P99 图的数据分析记录所使用的 block 参数。

---

# 18. 更新后的完整任务清单

本轮完整任务调整为：

```text
1. 远程读写分类
2. 副本级与逻辑级指标拆分
3. MetaTrack / Block-STM 指标向根汇总
4. 方法特定缺失指标检查
5. logical_update_count 统一语义
6. 预览与物化规则统一
7. 产物路径与前端展示统一
8. TPS、延迟和调度指标口径明确
9. 清理历史 RunGroup、无用日志和孤儿运行目录
10. 删除旧用户实验方案与过时模板
11. 重建正式四方法实验方案
12. 增加 block_size 与 block_interval_ms 前端配置
13. 将默认值改为 block_size=100、block_interval_ms=75
14. 增加 RunGroup TPS/P99 结果分析
15. 将最终结果页改成两张论文风格图
```


# 19. 实施顺序

## 阶段 A：指标真值

1. 修复远程读写分类。
2. 增加 unknown 类型检查。
3. 增加副本去重逻辑。
4. 输出远程状态聚合 JSON/CSV。
5. 增加测试。

## 阶段 B：机制指标汇总

1. 聚合 MetaTrack 节点指标。
2. 聚合 Block-STM 节点指标。
3. 定义每个指标的 sum/max/leader/dedup 规则。
4. 生成根目录汇总。
5. 增加方法特定缺失检查。

## 阶段 C：更新指标统一

1. 新增统一字段。
2. 修改 normal commit。
3. 修改 aggregation commit。
4. 保留 deprecated 字段兼容。
5. 修改前端与分析层使用新字段。

## 阶段 D：预览与物化统一

1. 正式支持 1K。
2. 提取统一支持规模来源。
3. preview 与 materialize 共用选择函数。
4. 修正类别比例、时间范围、偏斜和分片统计。
5. 增加 selection digest 一致性测试。

## 阶段 E：产物契约

1. 统一 artifact role 和 truth scope。
2. 修正 expected artifacts。
3. 区分预测、物理、去副本文件。
4. 修复前端产物分组。
5. 增加产物缺失检查。

## 阶段 F：平台清理与方案重建

1. 增加 RunGroup 删除与批量清理 API。
2. 增加孤儿 real-cluster 目录扫描。
3. 增加路径白名单、运行状态和共享引用检查。
4. 清理失败、空结果和重复 smoke run。
5. 删除旧 saved method / formal plan 配置。
6. 重建唯一的正式四方法实验方案。
7. 生成 cleanup report。

## 阶段 G：区块生产参数闭合

1. 将 block size 和 block interval 加入前端。
2. 将默认值调整为 100 / 75 ms。
3. 参数进入 FormalRequest、ChildSpec、Compiler 和 Go runtime。
4. fairness key 加入两个参数。
5. 结果摘要输出配置值和实际区块统计。
6. 增加 Python、Go 和 E2E 测试。

## 阶段 H：TPS、延迟和调度口径

1. 固化 End-to-End TPS 主口径。
2. 固化 P99 finality 延迟。
3. 保存时间窗口。
4. 拆分调度事件。
5. 标记旧字段 deprecated。

## 阶段 I：结果分析

1. 生成 `paper_result_analysis.json`。
2. 生成 `paper_result_analysis.csv`。
3. 单样本不画误差棒。
4. 多样本计算 std 和 95% CI。
5. 缺失或不公平样本禁止进入图。

## 阶段 J：页面美化

1. 新增紧凑摘要。
2. 增加 TPS 图。
3. 增加 P99 图。
4. 增加 P95/P99 切换。
5. 增加数据查看与下载。
6. 折叠子实验和原始产物。
7. 使用论文控制台配色和样式。

---

# 20. 测试与验收

## 16.1 单元测试

### Go

```text
远程操作类型归一化
写回后缀识别
总数守恒
副本去重键稳定
更新指标计算
```

### Python

```text
物理获取/写回统计
副本去重
方法必需指标
TPS 分析
P99 分析
单样本处理
多样本 CI
预览与物化一致
产物契约校验
```

### 前端

```text
两张图正常渲染
方法顺序固定
n=1 不显示误差棒
P95/P99 切换
CSV 下载
PNG/SVG/PDF 导出
缺失数据提示
高级区域默认折叠
```

## 16.2 1K 回归

配置：

```text
数据集：dcl_sales_polygon_271868
变体：key_zipf
交易数：1K
方法：四种
拓扑：2 shards × 4 validators
seed：73
```

验收：

- 四个 child completed。
- 0 incomplete tx。
- fairness passed。
- 状态根一致。
- receipt root 一致。
- plan digest 一致。
- no fallback。
- MetaTrack 远程指标正确分类。
- 1K 无需环境变量。
- 页面自动出现 TPS 和 P99 图。
- 图下数据与 JSON/CSV 一致。

## 16.3 远程指标参考验收

对已知 1K 产物重新分析：

```text
physical_remote_operation_count = 19116
physical_remote_fetch_count = 11336
physical_remote_writeback_count = 7780

replica_deduplicated_remote_operation_count = 4779
replica_deduplicated_remote_fetch_count = 2834
replica_deduplicated_remote_writeback_count = 1945

replica_amplification_factor = 4.0
```

## 16.4 10K 回归

- 四种方法全部完成。
- 页面自动生成两张图。
- 分析层不会因规模增加而超时。
- 产物路径一致。
- 根汇总完整。
- 节点级产物仍可下载。

## 16.5 多样本回归

至少：

```text
3 seeds × 2 repeats
```

验收：

- 每个方法 n = 6。
- mean、std、CI 正确。
- 误差棒显示。
- 原始样本可查看。
- 被排除样本有原因。
- 不静默删除异常值。

---

# 21. 最终验收清单

## 指标真值

- [ ] `write_apply:*` 全部归入写回。
- [ ] unknown 类型可检测。
- [ ] 物理级与去副本级指标分开。
- [ ] 获取与写回分别去重。
- [ ] 每笔逻辑交易归一化指标可计算。
- [ ] 副本放大来自真实日志。

## 机制指标

- [ ] MetaTrack 根汇总存在。
- [ ] Block-STM 根汇总存在。
- [ ] 聚合方式按指标定义执行。
- [ ] 缺失、不适用、0 三种状态区分。
- [ ] 方法必需指标检查生效。

## 更新指标

- [ ] `logical_update_count` 停止作为正式跨方法指标。
- [ ] pre/post aggregation 语义一致。
- [ ] reduction ratio 正确。
- [ ] 非聚合方法 pre=post。

## 负载

- [ ] 1K 正式支持。
- [ ] 前端、预览、物化、编译共用支持规模。
- [ ] 预览与物化 selection digest 一致。
- [ ] 类别百分比正确。
- [ ] 预览统计来自实际选中窗口。

## 产物

- [ ] 预测、物理、去副本产物分开。
- [ ] expected artifacts 与真实路径一致。
- [ ] artifact catalog 含 role 与 truth scope。
- [ ] 节点级 MetaTrack/Block-STM 产物可下载。
- [ ] 缺失预期产物有告警。

## TPS 与延迟

- [ ] 主图使用 End-to-End TPS。
- [ ] 辅助展示 Logical-Finality TPS。
- [ ] P99 延迟起止事件明确。
- [ ] 跨片终态使用 source finalize/refund/failed。
- [ ] 时间窗口和 tail drain 可追溯。

## 调度

- [ ] planning/runtime 拆分。
- [ ] leader/replica 拆分。
- [ ] physical event/logical decision 拆分。
- [ ] 旧混合总数标记 deprecated。

## 平台清理

- [ ] 正在运行的实验不可删除。
- [ ] 删除 RunGroup 时同步删除其专属 real-cluster 目录。
- [ ] 孤儿目录扫描受路径白名单保护。
- [ ] 原始工作负载和物化缓存默认保留。
- [ ] 清理报告可审计。
- [ ] 旧 saved methods 和旧 Formal Plans 已从平台清除。

## 区块生产

- [ ] 前端可调 block size。
- [ ] 前端可调 block interval。
- [ ] 默认值为 100 tx/block 与 75 ms。
- [ ] Go fallback 与 manifest 默认一致。
- [ ] fairness key 包含两个参数。
- [ ] 结果产物同时记录配置值与实际出块统计。

## 结果分析

- [ ] 公平性检查通过后才画图。
- [ ] 缺失样本不进入主图。
- [ ] n=1 不显示虚假 CI。
- [ ] 多样本计算 mean/std/CI。
- [ ] JSON、CSV 和图一致。

## 页面

- [ ] 只显示 TPS 与 P99 两张主图。
- [ ] 一行两图，小屏上下布局。
- [ ] 使用论文控制台风格。
- [ ] 每图可查看分析数据。
- [ ] 每图可下载 CSV/PNG/SVG/PDF。
- [ ] 子实验、机制指标、产物和日志默认折叠。

---

# 22. 建议提交拆分

建议分成多个可审计提交：

```text
1. Fix V5 remote-state metric classification
2. Add replica-deduplicated remote-state metrics
3. Aggregate MetaTrack and Block-STM metrics
4. Enforce method-specific metric completeness
5. Unify V5 update metric semantics
6. Align workload preview and materialization
7. Normalize V5 artifact paths and catalog
8. Add safe V5 run cleanup and orphan artifact pruning
9. Replace legacy experiment schemes with formal four-method profile
10. Expose block size and block interval with 100/75 defaults
11. Clarify TPS latency and scheduler metric scopes
12. Add V5 paper result analysis
13. Redesign V5 result page with TPS and P99 charts
```

每个提交都应通过：

```text
git diff --check
Python targeted tests
Go targeted tests
frontend typecheck/build
```

最终再运行全量 CI。

---

# 23. 最终交付物

代码交付：

```text
指标分类修复
副本去重聚合器
MetaTrack/Block-STM 根汇总
方法必需指标检查
更新指标统一
预览/物化统一
产物契约
结果分析 API
TPS/P99 页面
```

新增正式产物：

```text
aggregate/remote_state_metrics_summary.json
aggregate/replica_deduplicated_remote_operations.csv
aggregate/metatrack_aggregate_summary.json
aggregate/block_stm_aggregate_summary.json
aggregate/mechanism_metrics_summary.json
aggregate/paper_result_analysis.json
aggregate/paper_result_analysis.csv
```

验证交付：

```text
1K 四方法回归
10K 四方法回归
多种子/多重复统计回归
前端 E2E
GitHub Actions
```

---

# 24. 完成后的平台效果

完成本轮后，用户从前端启动一次正式 RunGroup，系统自动完成：

```text
真实实验运行
→ 节点产物生成
→ 指标分类与副本去重
→ MetaTrack/Block-STM 根汇总
→ 方法指标完整性检查
→ 公平性和正确性分析
→ TPS/P99 统计
→ 两张论文风格图
→ CSV/PNG/SVG/PDF 下载
```

最终结果页直接回答：

```text
哪种方法吞吐量更高？
哪种方法尾延迟更低？
```

同时保留完整机制指标、节点证据和原始产物，满足实验复现、论文分析和审计追溯需求。
