# V0 平台骨架闭环实施方案

## 1. V0 定位

V0 是平台骨架闭环阶段，不是命令行 demo，也不是完整课题组平台。目标是搭好完整工程框架：每类核心模块只实现一个最基础默认插件，并通过基础前端完成实验创建、组件预览、实验运行、日志查看和结果展示。

```text
前端创建实验
  -> 后端生成 config.yaml
  -> Experiment Composer 自动补齐默认组件
  -> Synthetic Workload 生成 trace.jsonl.gz
  -> Go Executor 读取 trace 并使用 virtual_clock replay
  -> Metrics 输出 summary.csv、latency.csv、runtime.log
  -> 前端展示 TPS、P95、P99、成功数和失败数
```

## 1.1 V0 技术栈版本约束

V0 的依赖与开发工具必须遵循以下版本范围；除非获得明确批准，不得自行升级、降级或替换：

| 技术 | V0 版本约束 | 用途 |
| --- | --- | --- |
| Python | 3.12.x | workload、trace、后端与实验编排脚本 |
| Go | 1.24.x | executor 与虚拟时钟回放 |
| Node.js | 22 LTS 或 24 LTS | 前端构建与开发工具链 |
| React | 18.x | V0 基础前端 |
| TypeScript | 5.x | 前端类型与应用代码 |
| FastAPI | 0.115.x | 后端实验与 Composer API |
| Uvicorn | 0.30.x | FastAPI 本地服务进程 |

仓库根目录的 `.python-version` 固定为 `3.12`。后续创建 Python、Go 或前端依赖清单时，必须与本表保持一致。

## 2. V0 默认插件包

V0 只能实现以下默认插件：

| 插件类型 | 默认插件 |
| --- | --- |
| chain_backend | mockchain |
| workload | asset_hotspot |
| trace | jsonl_gzip |
| consensus_protocol | simple_ordering |
| consensus_sharding | single_group |
| state_sharding | hash_state_sharding |
| execution_sharding | hash_execution_sharding |
| routing | hash_routing |
| cross_shard_protocol | local_only |
| cross_chain_protocol | disabled |
| execution | serial_execution |
| commit | normal_commit |
| clock | virtual_clock |
| network_model | fixed_latency_model |
| metrics | basic_metrics |
| composer | default_composer |
| frontend_template | default_single_chain_experiment |

## 3. V0 禁止实现内容

V0 不实现 Fabric 网络、EVM 合约、PBFT、HotStuff、DAG 共识、co-access routing、dual-track execution、hot update aggregation、Block-STM-like baseline、Calvin-like baseline、MetaFlow、committee bridge、Pending Pool、Grafana、多用户权限系统、分布式部署或复杂拓扑拖拽设计器。这些内容只允许创建接口或占位，不允许在 V0 中实现具体逻辑。

## 4. V0 基础前端页面

前端只实现四个页面，且颜色仅使用黑、白、灰。

### 4.1 Experiments

创建默认单链实验，配置 `tx_count`、`zipf_theta`、`hot_key_ratio`、`cross_shard_ratio`、`shard_count`，并选择 `full_pipeline` 或 `replay_only` 运行模式。

### 4.2 Composer Preview

展示 `default_composer` 补齐后的组件组合、默认插件包、配置合法性和非法配置原因。

### 4.3 Run Console

启动实验，显示实验状态、`runtime.log` 和失败原因。

### 4.4 Results

显示 TPS、`avg_latency_ms`、`p95_latency_ms`、`p99_latency_ms`、`success_count`、`failed_count`，并提供 `config.yaml`、`summary.csv`、`latency.csv`、`runtime.log` 下载入口。

## 5. V0 后端功能

后端使用 FastAPI，实现创建实验、生成 `config.yaml`、预览 Composer 组件组合、启动实验、查询状态、WebSocket 推送日志、获取 summary 结果及下载指定产物。V0 只使用本地进程运行 workload 和 executor，不控制 Fabric、EVM 或 Docker 集群。

## 6. V0 Trace 要求

正式实验使用 `trace.jsonl.gz`。Python workload 流式写入，Go executor 流式读取；不允许一次性加载完整 trace。小规模调试可保留 `trace.jsonl`，且必须生成 `trace_meta.json`。

单链 trace 字段包括 `tx_id`、`tx_type`、`timestamp`、`chain_id`、`contract`、`function`、`args`、`read_set`、`write_set`、`access_list`、`commutative`、`update_type`、`status`、`chain_latency_ms`。

## 7. V0 Workload 要求

V0 只实现 `asset_hotspot` synthetic workload，支持 `asset_transfer`、`asset_trade`、`scene_join`、`reward_claim` 四种交易，并支持 `tx_count`、`zipf_theta`、`hot_key_ratio`、`cross_shard_ratio`、`seed` 参数。每笔交易必须生成 `read_set`、`write_set`、`access_list`、`commutative`、`update_type`。

## 8. V0 Executor 与虚拟时钟

Go executor 默认链路为：

```text
simple_ordering
  -> single_group
  -> hash_state_sharding
  -> hash_execution_sharding
  -> hash_routing
  -> local_only
  -> serial_execution
  -> normal_commit
  -> basic_metrics
```

定义稳定的 `consensus`、`consensus_sharding`、`state_sharding`、`execution_sharding`、`routing`、`cross_shard`、`execution`、`commit`、`metrics` 接口，每类仅实现一个默认插件。

replay mode 必须使用 `virtual_clock`。严禁使用 `time.Sleep` 模拟网络、远程状态访问、执行、提交或 finality 等待；所有模拟延迟通过虚拟时间推进。必须区分 `virtual_latency_ms` 与 `wall_clock_runtime_ms`，交易延迟为：

```text
commit_done_time - tx_arrival_time
```

## 9. V0 输出文件

每次成功实验必须输出：

* `config.yaml`
* `trace.jsonl.gz`
* `trace_meta.json`
* `summary.csv`
* `latency.csv`
* `runtime.log`

`summary.csv` 至少包含 `tx_count`、`success_count`、`failed_count`、`throughput_tps`、`avg_latency_ms`、`p95_latency_ms`、`p99_latency_ms`、`cross_shard_ratio`、`remote_fetch_count`、`wall_clock_runtime_ms`。

## 10. V0 默认配置

默认配置路径为：

```text
configs/experiments/v0_default_asset_hotspot.yaml
```

配置应包含实验名称、`full_pipeline` 模式和 seed；使用 mockchain、asset_hotspot、jsonl_gzip、virtual_clock、simple_ordering、single_group、哈希状态/执行分片、hash_routing、local_only、disabled 跨链、serial_execution、normal_commit 和 basic_metrics。负载配置包括 10000 笔交易、`zipf_theta: 1.2`、`hot_key_ratio: 0.05`、`cross_shard_ratio: 0.2`；区块大小为 500、区块间隔 100ms、finality 延迟 100ms、分片数为 4。

## 11. V0 推荐目录结构

至少创建：

```text
frontend/
backend/
workload/
trace/
chain/mockchain/
executor/
configs/
experiments/
tests/
docs/
```

其中必须存在 `configs/experiments/v0_default_asset_hotspot.yaml`。

## 12. V0 验收标准

1. 前端可以创建 `default_single_chain_experiment`。
2. 后端可以生成完整 `config.yaml`。
3. `default_composer` 可以自动补齐所有默认插件。
4. workload 可以生成 `asset_hotspot trace.jsonl.gz`。
5. executor 可以加载 `simple_ordering`、`hash_state_sharding`、`hash_routing`、`serial_execution`、`normal_commit`。
6. `virtual_clock` 可以统计虚拟延迟。
7. metrics 可以输出 `summary.csv`、`latency.csv`、`runtime.log`。
8. Results 可以显示 TPS、P95、P99、成功数、失败数和文件下载。
9. 同一个 seed 运行两次，核心结果一致。
10. 替换其中一个插件后，其他默认组件仍能自动补齐。

## 13. 当前阶段优先级

当前只实现 V0。本文与整体方案冲突时以本文为准；与项目约束冲突时遵循：

```text
AGENTS.md > SKILL.md > docs/v0_implementation_plan.md > docs/platform_plan_overview.md
```
