# V1 论文实验闭环实施方案

## 1. V1 定位

V1 是论文实验可用阶段，不是完整课题组通用平台，也不是单纯的 Fabric 上链 Demo。V1 的目标是在 V0 平台骨架闭环基础上，进一步形成“分片执行原型 + 论文机制插件 + baseline 对比 + 上链 trace 校验 + 实验报告输出”的完整实验能力。

V1 要解决的问题是：

```text
V0：平台能跑
V1：论文实验能用
V2：课题组能复用
V3：多服务器真实部署
```

V1 的最终目标是：

```text
统一 workload / trace
  -> MBE 分片执行原型
  -> 多种 baseline replay
  -> ours 机制 replay
  -> 消融实验
  -> 参数扫描
  -> 指标文件
  -> 图表和 report
  -> 小规模 Fabric chain-backed trace 校验
```

V1 完成后，平台应能支撑当前无状态分片 / 元宇宙高偏斜交易流论文的主实验、baseline 对比、消融实验和上链 trace 真实性校验。

---

## 2. V1 总体目标

V1 不是简单扩展页面，而是把 V0 的默认链路升级为论文实验链路：

```text
前端选择 V1 实验模板
  -> 后端 Composer 生成 baseline / ours 配置
  -> workload 生成 synthetic trace 或 Fabric chain-backed trace
  -> Go executor 加载不同策略插件
  -> 跑 hash_serial / blockstm_like / calvin_like / porygon_like / ours / ablation
  -> 输出 summary、latency、remote_wait、track_stats、aggregation、report
  -> 前端展示多策略对比结果
```

V1 必须同时保留两类实验模式：

1. **MBE 原型主实验**
   在 MBE 自研 executor 中实现分片、路由、执行调度、提交和指标统计，用于主实验和大规模参数扫描。

2. **Fabric chain-backed trace 校验实验**
   在 Fabric 最小网络中真实执行元宇宙业务交易，采集链上执行结果并转成统一 trace，用于证明业务负载具有链上来源。

---

## 3. V1 技术栈约束

V1 继续继承 V0 技术栈，不随意升级：

| 技术         | V1 版本约束        | 用途                            |
| ---------- | -------------- | ----------------------------- |
| Python     | 3.12.x         | workload、trace、后端、实验脚本、report |
| Go         | 1.26.1         | executor、分片原型、虚拟时钟 replay     |
| Node.js    | 22 LTS         | 前端构建                          |
| React      | 18.x           | 前端                            |
| TypeScript | 5.x            | 前端类型                          |
| FastAPI    | 0.115.x        | 后端 API                        |
| Uvicorn    | 0.30.x         | 本地后端服务                        |
| Fabric     | V1 固定一个最小可运行版本 | chain-backed trace 校验         |

V1 不允许因为接入 Fabric 或 baseline 而破坏 V0 环境。所有 V0 sanity、Go test、pytest、frontend build 必须继续通过。

---

## 4. V1 范围

V1 只做以下内容：

1. V1 实验模板和多策略 replay 框架。
2. V1 workload 参数扩展。
3. MBE 分片执行原型增强。
4. co-access routing / MetaTrack routing。
5. dual-track execution。
6. hot update aggregation。
7. baseline：

   * hash_serial
   * blockstm_like
   * calvin_like
   * porygon_like
8. 消融实验：

   * ours_full
   * ours_no_routing
   * ours_no_dual_track
   * ours_no_hot_aggregation
9. sweep / report / figures / tables。
10. Fabric 最小 chain-backed trace 校验：

    * Asset 链码
    * Scene 链码
    * Reward 链码
    * access_schema.yaml
    * Fabric workload runner
    * Fabric trace collector
    * Fabric trace replay

---

## 5. V1 禁止实现内容

V1 不做以下内容：

```text
MetaFlow
committee bridge
Pending Pool
跨链多通道
完整 EVM
Grafana 大屏
Prometheus 监控体系
多用户权限
分布式部署
多服务器 Fabric
Parquet / Arrow
复杂拖拽拓扑
完整课题组插件市场
```

这些内容属于 V2 或 V3。V1 的目标是“当前论文实验可用”，不是一次性完成整个最终平台。

---

## 6. V1 实验模式

V1 支持三类模式。

### 6.1 Synthetic Prototype Mode

使用 synthetic workload 生成 trace，并在 MBE executor 中 replay 多种策略。

```text
asset_hotspot_v1 / reward_burst
  -> trace.jsonl.gz
  -> MBE executor
  -> baseline / ours
  -> metrics / report
```

用途：

* 大规模压力实验；
* 参数扫描；
* 消融实验；
* 机制趋势分析。

### 6.2 Chain-backed Trace Mode

在 Fabric 上真实执行元宇宙业务交易，采集链上结果并生成统一 trace，再进入 MBE executor replay。

```text
workload
  -> Fabric chaincode calls
  -> raw_chain_log.jsonl
  -> fabric_to_unified_trace
  -> trace.jsonl.gz
  -> MBE executor replay
```

用途：

* 证明负载具有链上业务来源；
* 证明 trace 不是凭空生成；
* 给论文增加真实性实验。

### 6.3 Replay-only Comparison Mode

复用已有 trace，只切换 routing / execution / commit / baseline 插件。

```text
existing trace.jsonl.gz
  -> hash_serial
  -> blockstm_like
  -> calvin_like
  -> porygon_like
  -> ours
  -> report
```

用途：

* 公平对比；
* 快速复现实验；
* 批量跑 baseline。

---

## 7. V1 新增插件

### 7.1 Routing 插件

| 插件                | 状态    | 说明           |
| ----------------- | ----- | ------------ |
| hash_routing      | V0 已有 | 基础 baseline  |
| coaccess_routing  | V1 新增 | 基于状态共现的批次级路由 |
| metatrack_routing | V1 新增 | 论文完整控制面路由    |

### 7.2 Execution 插件

| 插件                   | 状态    | 说明                  |
| -------------------- | ----- | ------------------- |
| serial_execution     | V0 已有 | 基础串行执行              |
| blockstm_like        | V1 新增 | 乐观并发与冲突回滚 baseline  |
| calvin_like          | V1 新增 | 确定性排序与保守执行 baseline |
| porygon_like         | V1 新增 | 无状态分片相关 baseline    |
| dual_track_execution | V1 新增 | 论文双轨执行机制            |

### 7.3 Commit 插件

| 插件                     | 状态    | 说明          |
| ---------------------- | ----- | ----------- |
| normal_commit          | V0 已有 | 基础提交        |
| hot_update_aggregation | V1 新增 | 可交换增量热点更新聚合 |

### 7.4 Dependency 插件

| 插件             | 状态    | 说明            |
| -------------- | ----- | ------------- |
| conflict_graph | V1 新增 | 构建读写冲突图       |
| topo_safe      | V1 新增 | 判断快速通道可安全推进条件 |

### 7.5 Workload 插件

| 插件                    | 状态    | 说明                         |
| --------------------- | ----- | -------------------------- |
| asset_hotspot         | V0 已有 | 基础资产热点负载                   |
| asset_hotspot_v1      | V1 新增 | 增强冲突、跨片、访问集合、可交换更新参数       |
| reward_burst          | V1 新增 | 奖励集中领取和热点增量更新负载            |
| fabric_asset_workload | V1 新增 | Fabric chain-backed 资产交易负载 |

---

## 8. V1 新增配置

### 8.1 实验配置文件

新增：

```text
configs/experiments/v1_baseline_hash_serial.yaml
configs/experiments/v1_baseline_blockstm_like.yaml
configs/experiments/v1_baseline_calvin_like.yaml
configs/experiments/v1_baseline_porygon_like.yaml
configs/experiments/v1_ours_metatrack.yaml
configs/experiments/v1_ablation_no_routing.yaml
configs/experiments/v1_ablation_no_dual_track.yaml
configs/experiments/v1_ablation_no_hot_aggregation.yaml
configs/experiments/v1_fabric_chain_backed_asset.yaml
```

### 8.2 实验模板文件

新增：

```text
configs/templates/v1_execution_comparison.yaml
configs/templates/v1_routing_comparison.yaml
configs/templates/v1_commit_comparison.yaml
configs/templates/v1_ablation.yaml
configs/templates/v1_metatrack_main.yaml
configs/templates/v1_fabric_chain_backed.yaml
```

### 8.3 实验套件文件

新增：

```text
experiments/suites/v1_metatrack_main.yaml
experiments/suites/v1_ablation.yaml
experiments/suites/v1_stress_zipf.yaml
experiments/suites/v1_stress_cross_shard.yaml
experiments/suites/v1_stress_conflict.yaml
experiments/suites/v1_fabric_trace_validation.yaml
```

---

## 9. V1 输出文件

V1 每次实验至少输出：

```text
config.yaml
trace.jsonl.gz
trace_meta.json
summary.csv
latency.csv
runtime.log
```

在论文实验中额外输出：

```text
throughput.csv
remote_wait.csv
track_stats.csv
aggregation.csv
dependency.csv
aborts.csv
control_overhead.csv
report.md
figures/
tables/
```

### 9.1 summary.csv 新增字段

V1 的 summary.csv 至少包含：

```text
tx_count
success_count
failed_count
throughput_tps
avg_latency_ms
p95_latency_ms
p99_latency_ms
cross_shard_ratio
remote_fetch_count
remote_wait_ms
fast_track_count
conservative_track_count
rollback_count
abort_count
aggregation_count
aggregation_ratio
lock_wait_ms
control_overhead_ms
wall_clock_runtime_ms
```

---

## 10. V1 分阶段实施路线

V1 按 8 个小阶段实施。每个阶段都必须保持 V0 通过。

---

# V1.1：实验模板与多策略配置框架

## 目标

让平台知道 V1 有哪些实验模板、baseline、ours 和消融配置，但暂时不实现复杂算法。

## 新增内容

```text
docs/v1_implementation_plan.md
configs/experiments/v1_baseline_hash_serial.yaml
configs/experiments/v1_baseline_blockstm_like.yaml
configs/experiments/v1_baseline_calvin_like.yaml
configs/experiments/v1_baseline_porygon_like.yaml
configs/experiments/v1_ours_metatrack.yaml
configs/experiments/v1_ablation_no_routing.yaml
configs/experiments/v1_ablation_no_dual_track.yaml
configs/experiments/v1_ablation_no_hot_aggregation.yaml
configs/templates/v1_*.yaml
```

## 后端新增 API

```text
GET /api/v1/composer/templates
GET /api/v1/composer/experiments
GET /api/v1/composer/preview
```

## 前端新增最小展示

在现有前端增加 V1 区域：

```text
V1 实验模板列表
V1 实验配置列表
runnable / planned 状态
```

不重构前端，不引入复杂路由。

## 验收标准

1. V0 sanity 继续通过。
2. V1 配置文件可被解析。
3. v1_baseline_hash_serial 可运行。
4. planned 配置不会被误认为已实现。
5. 前端能显示 V1 实验配置。
6. CI 继续通过。

---

# V1.2：MBE 分片执行原型增强

## 目标

把 V0 executor 从基础 replay 升级为更完整的分片执行原型。

## 新增或整理模块

```text
executor/core/experiment.go
executor/core/config.go
executor/core/transaction.go
executor/state_sharding/
executor/execution_sharding/
executor/routing/
executor/execution/
executor/commit/
executor/dependency/
executor/metrics/
executor/time/
```

## 必须明确的映射

```text
phi: state_key -> state_shard
M_t: state_key -> execution_shard
psi_t: tx -> execution_shard
```

## 验收标准

1. hash_state_sharding 可定位状态分片。
2. hash_execution_sharding 可分配执行分片。
3. hash_routing 可生成默认 M_t。
4. executor 输出 remote_fetch_count 和 cross_shard_ratio。
5. 不使用 time.Sleep。
6. V0 replay 结果不被破坏。

---

# V1.3：workload 与 trace 增强

## 目标

让 workload 支持论文实验压力维度。

## asset_hotspot_v1 新增参数

```yaml
conflict_injection_ratio: 0.2
commutative_update_ratio: 0.5
access_set_size: 4
arrival_rate: 1000
burst_rate: 3000
hot_tx_ratio: 0.6
multi_hotspot_count: 4
read_write_ratio: 0.7
```

## 新增 reward_burst workload

用于测试热点更新聚合：

```text
reward_claim
add_reward
batch_reward
balance_delta
reward_pool_delta
```

## trace 新增可选字段

```text
primary_key
access_size
is_cross_shard
hot_key_tag
conflict_group
dependency_hint
delta_value
```

## trace_meta.json 新增统计

```text
actual_conflict_ratio
actual_commutative_update_ratio
avg_access_set_size
hot_tx_ratio
multi_hotspot_count
```

## 验收标准

1. 同 seed 可复现。
2. trace.jsonl.gz 仍可流式读取。
3. V0 trace 字段兼容。
4. trace_meta 统计正确。
5. 高冲突、高热点、高可交换更新比例可配置。

---

# V1.4：Fabric 最小 chain-backed trace 闭环

## 目标

完成最小上链业务闭环，用于证明元宇宙业务交易可以真实上链执行，并转成统一 trace。

## 新增目录

```text
chain/fabric/
chain/fabric/network/
chain/fabric/chaincode/asset/
chain/fabric/chaincode/scene/
chain/fabric/chaincode/reward/
chain/fabric/clients/
workload/chain_backed/
trace/collector/
trace/converter/
```

## 最小链码

### AssetContract

```text
TransferAsset(asset_id, from, to)
TradeAsset(asset_id, seller, buyer, price)
```

### SceneContract

```text
JoinScene(user_id, scene_id)
```

### RewardContract

```text
AddReward(pool_id, amount)
ClaimReward(user_id, pool_id, amount)
```

## access_schema.yaml

每个链码函数必须声明：

```text
read_set
write_set
access_list
commutative
update_type
```

## Fabric runner 输出

```text
raw_chain_log.jsonl
```

字段包括：

```text
tx_id
tx_type
submit_time
commit_time
status
contract
function
args
block_number
event
chain_latency_ms
```

## trace converter 输出

```text
trace.jsonl.gz
trace_meta.json
```

## 验收标准

1. 本地最小 Fabric 网络可启动。
2. Asset / Scene / Reward 链码可部署。
3. 至少 1000 笔业务交易可上链执行。
4. raw_chain_log.jsonl 可生成。
5. raw_chain_log 可转统一 trace.jsonl.gz。
6. Go executor 可 replay Fabric chain-backed trace。
7. Fabric 失败时不影响 V0 和 synthetic V1。

---

# V1.5：co-access routing / MetaTrack routing

## 目标

实现论文控制面机制，根据访问列表构建状态共现关系并生成批次级执行侧路由。

## 新增模块

```text
executor/routing/coaccess.go
executor/routing/metatrack_routing.go
executor/metrics/remote_wait.go
```

## 核心流程

```text
access_list
  -> X_t
  -> F_t
  -> W_t
  -> 状态共现亲和度
  -> M_t
  -> psi_t
  -> remote_wait / remote_fetch 统计
```

## 输出指标

```text
remote_fetch_count
remote_wait_ms
routing_overhead_ms
state_imbalance
```

## 验收标准

1. 同一 trace 下 hash_routing 与 coaccess_routing 可对比。
2. coaccess_routing 可减少 remote_fetch 或 remote_wait。
3. routing_overhead_ms 有统计。
4. 不改变 phi 的持久状态位置。
5. M_t 只表示执行侧路由，不写成状态迁移。

---

# V1.6：dual-track execution

## 目标

实现论文执行面机制，把交易分为快速通道和保守通道，并通过非阻塞调度减少队头阻塞和级联回滚。

## 新增模块

```text
executor/dependency/conflict.go
executor/dependency/topo_safe.go
executor/execution/dual_track.go
executor/metrics/track_stats.go
```

## 核心流程

```text
read_set / write_set / access_list
  -> conflict graph
  -> dependency risk
  -> fast track / conservative track
  -> non-blocking scheduling
  -> execution stats
```

## 输出指标

```text
fast_track_count
conservative_track_count
suspended_count
dependency_edges
rollback_count
queue_wait_ms
```

## 验收标准

1. dual_track_execution 可作为 execution 插件启用。
2. 高冲突 workload 下 fast / conservative 分流比例变化合理。
3. 不使用 time.Sleep。
4. 延迟仍由 virtual clock 计算。
5. 可与 serial_execution、blockstm_like、calvin_like 对比。

---

# V1.7：hot update aggregation

## 目标

实现提交面热点更新聚合，对可交换增量更新进行按 key 聚合，降低热点写入争用。

## 新增模块

```text
executor/commit/hot_update_aggregation.go
executor/metrics/aggregation.go
```

## 聚合条件

只允许聚合同时满足以下条件的更新：

```text
commutative = true
update_type = delta
目标 key 属于热点 key
约束检查通过
```

## fallback 条件

以下情况必须回退 normal_commit：

```text
commutative = false
update_type != delta
约束检查失败
更新语义不明确
```

## 输出指标

```text
hot_key_count
aggregation_count
aggregation_ratio
lock_wait_ms
fallback_count
constraint_failed_count
```

## 验收标准

1. reward_burst 下 aggregation_ratio > 0。
2. 非可交换更新不会被错误聚合。
3. 约束失败可 fallback。
4. 输出 aggregation.csv。
5. 对 P99 或 lock_wait 有可解释影响。

---

# V1.8：baseline、sweep 与 report

## 目标

形成论文实验完整输出能力。

## baseline

实现或补齐：

```text
hash_serial
blockstm_like
calvin_like
porygon_like
ours_full
ours_no_routing
ours_no_dual_track
ours_no_hot_aggregation
```

## 新增脚本

```text
experiments/sweep.py
experiments/report.py
experiments/plot.py
```

## 主实验 suite

```text
experiments/suites/v1_metatrack_main.yaml
```

## 参数扫描

```yaml
zipf_theta: [0.8, 1.0, 1.2, 1.4]
cross_shard_ratio: [0.1, 0.2, 0.4, 0.6]
conflict_injection_ratio: [0.0, 0.1, 0.2, 0.4]
commutative_update_ratio: [0.0, 0.25, 0.5, 0.75]
shard_count: [2, 4, 8]
tx_count: [10000, 50000, 100000]
```

## 自动生成图表

```text
TPS vs zipf_theta
P99 vs cross_shard_ratio
rollback_count vs conflict_injection_ratio
aggregation_ratio vs commutative_update_ratio
control_overhead_ms vs tx_count
```

图表配色使用黑白灰。

## 输出目录

```text
experiments/runs/v1_metatrack_main/
  hash_serial/
  blockstm_like/
  calvin_like/
  porygon_like/
  ours_full/
  ours_no_routing/
  ours_no_dual_track/
  ours_no_hot_aggregation/
  report.md
  figures/
  tables/
```

## 验收标准

1. 一条命令可运行小规模 V1 suite。
2. 同一 trace 可 replay 多种策略。
3. 所有策略输出统一 summary.csv。
4. report.md 自动生成。
5. figures 和 tables 自动生成。
6. V1 小规模 sanity 加入 CI。
7. V0 sanity 继续通过。

---

## 11. V1 前端要求

V1 前端不要一次性重构成复杂平台，只在 V0 前端基础上逐步增强。

### V1.1 前端

展示：

```text
V1 实验模板列表
V1 实验配置列表
runnable / planned 状态
```

### V1.4 前端

展示：

```text
Synthetic trace
Fabric chain-backed trace
trace 来源
trace_meta 摘要
```

### V1.8 前端

展示：

```text
baseline 对比表
TPS / P99 / remote_wait / rollback / aggregation
report.md 下载
figures 下载
tables 下载
```

暂不做复杂拖拽拓扑，不做 Grafana，不做多用户。

---

## 12. V1 后端要求

V1 后端新增 API：

```text
GET  /api/v1/composer/templates
GET  /api/v1/composer/experiments
POST /api/v1/composer/preview
POST /api/v1/experiments/{experiment_id}/run
POST /api/v1/experiments/{experiment_id}/sweep
GET  /api/v1/experiments/{experiment_id}/summary
GET  /api/v1/experiments/{experiment_id}/compare
GET  /api/v1/experiments/{experiment_id}/files
```

V1 后端服务新增：

```text
ExperimentSuiteRunner
SweepRunner
ReportService
V1Composer
FabricTraceService
```

后端必须明确：

1. runnable 配置才能运行。
2. planned 配置只能展示，不能执行。
3. Fabric 失败不能影响 synthetic replay。
4. 所有实验必须写 config.yaml。
5. 所有实验必须保留 trace_meta.json。
6. 所有实验必须记录插件版本。

---

## 13. V1 测试要求

### 13.1 V0 回归测试

每次 V1 修改后必须运行：

```text
python scripts/v0_sanity.py
python -m pytest backend/tests -q
python -m pytest tests -q
cd executor && go test ./...
cd frontend && npm run build
git diff --check
```

### 13.2 V1 单元测试

新增：

```text
tests/test_v1_configs.py
tests/test_v1_templates.py
tests/test_v1_workload.py
tests/test_v1_trace_meta.py
tests/test_v1_report.py
backend/tests/test_v1_composer.py
executor/routing/coaccess_test.go
executor/execution/dual_track_test.go
executor/commit/hot_update_aggregation_test.go
```

### 13.3 V1 sanity

新增：

```text
scripts/v1_sanity.py
```

小规模 sanity 默认参数：

```yaml
tx_count: 1000
shard_count: 4
zipf_theta: 1.2
cross_shard_ratio: 0.2
conflict_injection_ratio: 0.1
commutative_update_ratio: 0.5
```

V1 sanity 至少检查：

1. V1 baseline hash_serial 可运行。
2. ours_full 可运行。
3. 输出 summary.csv。
4. 输出 remote_wait.csv。
5. 输出 track_stats.csv。
6. 输出 aggregation.csv。
7. report.md 可生成。
8. V0 sanity 仍通过。

### 13.4 Fabric smoke test

新增：

```text
scripts/v1_fabric_smoke.py
```

只做小规模：

```text
10-100 笔交易
Asset / Scene / Reward 各至少一类
生成 raw_chain_log.jsonl
转换 trace.jsonl.gz
executor replay 成功
```

Fabric smoke test 可以不放入默认 CI，但必须有手动验证命令。

---

## 14. V1 CI 要求

在 `.github/workflows` 中保留 V0 CI，并新增轻量 V1 sanity：

```text
python scripts/v0_sanity.py
python scripts/v1_sanity.py --small
go test ./...
python -m pytest backend/tests -q
python -m pytest tests -q
npm run build
```

Fabric smoke test 不进入默认 CI，避免 GitHub Actions 上 Docker/Fabric 环境不稳定。Fabric 作为本地或服务器手动验证。

---

## 15. V1 验收标准

V1-Core 完成标准：

1. V0 全部功能继续通过。
2. V1 实验模板可展示。
3. V1 多策略配置可被 Composer 识别。
4. 同一份 trace 可以 replay 多种策略。
5. 实现并可运行：

   * coaccess_routing
   * dual_track_execution
   * hot_update_aggregation
6. 至少支持：

   * hash_serial
   * blockstm_like
   * calvin_like
   * porygon_like
7. 支持消融实验：

   * ours_no_routing
   * ours_no_dual_track
   * ours_no_hot_aggregation
8. 支持参数扫描。
9. 输出论文级指标：

   * remote_wait
   * track_stats
   * aggregation
   * rollback
   * control_overhead
10. 自动生成 report.md、figures、tables。
11. CI 中包含 V1 小规模 sanity。
12. 不破坏 V0。

V1-Chain 完成标准：

1. Fabric 最小网络可启动。
2. Asset / Scene / Reward 链码可部署。
3. workload 可转 Fabric 链码调用。
4. raw_chain_log.jsonl 可生成。
5. Fabric trace collector 可生成统一 trace.jsonl.gz。
6. Go executor 可 replay Fabric chain-backed trace。
7. 同一 Fabric trace 可跑 baseline 和 ours。
8. Fabric smoke test 通过。

V1 全部完成标准：

```text
V1-Core + V1-Chain 均完成
```

完成后可以说：

```text
V1 已具备链上 trace 支撑的论文实验能力。
```

---

## 16. V1 不同阶段交付物

| 阶段   | 交付物                       | 是否可提交 |
| ---- | ------------------------- | ----- |
| V1.1 | V1 文档、模板、配置、Composer 展示   | 可单独提交 |
| V1.2 | 分片执行原型结构增强                | 可单独提交 |
| V1.3 | workload / trace 增强       | 可单独提交 |
| V1.4 | Fabric 最小链上 trace 闭环      | 可单独提交 |
| V1.5 | co-access routing         | 可单独提交 |
| V1.6 | dual-track execution      | 可单独提交 |
| V1.7 | hot update aggregation    | 可单独提交 |
| V1.8 | baseline / sweep / report | 可单独提交 |

每个阶段都必须做到：

```text
不破坏 V0
测试通过
不提交运行产物
不提交 node_modules / dist / runs
```

---

## 17. V1 推荐开发顺序

实际开发时按以下顺序推进：

```text
1. 给 V0 打 tag：v0.1.0
2. 创建 v1-dev 分支
3. V1.1：实验模板与多策略配置框架
4. V1.2：MBE 分片执行原型增强
5. V1.3：workload 与 trace 增强
6. V1.4：Fabric 最小 chain-backed trace 闭环
7. V1.5：co-access routing
8. V1.6：dual-track execution
9. V1.7：hot update aggregation
10. V1.8：baseline、sweep、report
11. V1 全量验收
12. 打 tag：v1.0.0
```

---

## 18. V1 与后续版本边界

V1 完成的是：

```text
论文实验可用
```

V2 才做：

```text
课题组多插件通用平台
跨链 MetaFlow
Pending Pool
committee bridge
更多共识和跨片协议
Grafana
数据保留策略
实验归档
```

V3 才做：

```text
多服务器部署
多组织 Fabric
多链真实跨链
对象存储
长期实验基础设施
```

---

## 19. V1 最终效果

V1 完成后，用户应能做到：

```text
打开前端
  -> 选择 V1 MetaTrack Main Suite
  -> 选择 synthetic trace 或 Fabric chain-backed trace
  -> 选择 baseline / ours / ablation
  -> 点击运行
  -> 查看多策略对比结果
  -> 下载 CSV
  -> 下载 report.md
  -> 下载 figures 和 tables
```

命令行应能做到：

```text
python experiments/sweep.py --suite experiments/suites/v1_metatrack_main.yaml
python experiments/report.py --run-dir experiments/runs/v1_metatrack_main
```

最终输出：

```text
summary.csv
latency.csv
remote_wait.csv
track_stats.csv
aggregation.csv
dependency.csv
control_overhead.csv
report.md
figures/
tables/
```

---

## 20. V1 总结

V1 的核心不是“大而全”，而是围绕论文实验形成可信闭环：

```text
MBE 分片执行原型
  + 论文机制插件
  + baseline 对比
  + 参数扫描
  + 上链 trace 校验
  + 自动报告输出
```

V1 完成后，平台就从“V0 可运行骨架”升级为“可支撑论文实验的研究原型平台”。
