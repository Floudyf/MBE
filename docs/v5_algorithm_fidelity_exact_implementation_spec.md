# MBE V5 三项核心改造精确实施规范（Codex 执行版 V2）

> 文档状态：强约束实施规范
> 适用仓库：`https://github.com/Floudyf/MBE`
> 基线分支：`feat/v5-core-hardening`
> 基线提交：`28dd1e5110d8e24b19744c6c670006781daf42a2`
> 后续建议分支：`feat/v5-algorithm-fidelity`
> 三项唯一主任务：
>
> 1. 语义推导静态访问列表；
> 2. 完整 Block-STM 乐观并发；
> 3. MetaTrack 无状态共现执行主链。
>
> 本文是 Codex 后续修改代码时的最高优先级项目规范。任何实现与本文冲突时，先停止并报告冲突，禁止自行简化算法、替换语义或扩大范围。

---

# 0. 为什么需要这份精确规范

当前 V5 已经完成真实节点、TCP 网络、PBFT、区块、状态持久化、WAL、跨片生命周期、四方法运行入口和 10K 正确性闭环。现有结果证明系统能够稳定运行，但算法真实性仍有三处核心缺口：

```text
真实 Decentraland CSV
→ 当前运行时临时补充访问键
→ 共现输入语义重复且不可独立审计

Block-STM
→ 一轮并行推测
→ 一轮统一验证
→ 顺序串行修复与物化
→ 缺少连续 Execute/Validate/Abort/Resume 调度闭环

MetaTrack
→ 客户端生成 X/F/W、M_t、ψ_t
→ 计划主要写入 CSV
→ 实际区块仍按 source shard 和传统跨片流程执行
→ 逐交易逐键远程拉取
→ 缺少批量预取、缓存复用、StateReady、真实双轨和工作窃取
```

后续所有改动围绕这三处缺口展开。

---

# 1. 当前源码事实基线

Codex 开始工作前必须重新读取当前分支源码，并确认以下事实仍成立。源码变化导致事实失效时，先报告差异。

## 1.1 当前真实负载访问列表链路

当前调用链：

```text
DecentralandSalesAdapter.iter_canonical_records
    ↓
生成 state_keys:
    account:sender:<buyer>
    account:receiver:<seller>
    contract:<contract>
    ↓
CanonicalTraceIterator.Next
    ↓
canonicalAccessList(wire)
    将 state_keys、routing_source_key、routing_target_key
    全部转换为 AccessReadWrite
    ↓
CanonicalTraceIterator.SignedTransaction
    ↓
canonicalRuntimeAccessList(plan, record)
    ↓
DefaultTransferAccessList(runtime_sender, runtime_receiver)
    + record.AccessList
    ↓
SignedTransaction.AccessList
```

关键文件与函数：

```text
backend/app/services/workload_adapters/decentraland_sales_v1.py
    DecentralandSalesAdapter.iter_canonical_records

executor/v5/workload_iterator.go
    canonicalWireRecord
    CanonicalTraceIterator.Next
    CanonicalTraceIterator.SignedTransaction
    canonicalRuntimeAccessList
    canonicalAccessList

executor/realism/tx/generator.go
    DefaultTransferAccessList

executor/v5/client.go
    submitBatch
    submitRecord
```

当前结果包含两套账户语义：

```text
account:sender:<buyer>
account:receiver:<seller>

balance:<runtime_sender>
nonce:<runtime_sender>
balance:<runtime_receiver>
nonce:<runtime_receiver>
```

这会扩大状态键数量和共现边，并使原始 buyer/seller 与实际执行账户出现两套命名空间。

## 1.2 当前 Block-STM 链路

关键函数：

```text
executor/realism/execution/blockstm_executor.go
    BlockSTMExecutor.ExecuteBlock
    BlockSTMExecutor.executeSpeculative
    BlockSTMExecutor.validateSpeculative
    BlockSTMExecutor.executeTx
    BlockSTMExecutor.serialFallbackResult
    BlockSTMExecutor.shouldRunSerialOracle

executor/realism/execution/blockstm/scheduler.go
    NewScheduler
    Next
    Abort
    Wait
    Resume
    Commit

executor/realism/execution/blockstm/mvmemory.go
    Read
    Write
    MarkEstimate
    Validate
```

当前 `ExecuteBlock` 大致流程：

```text
executeSpeculative(...)
validateSpeculative(...)

scheduler := NewScheduler(...)
serialWorking := copySnapshot(base)

for tx index 0..N-1:
    检查 readSetMatchesSnapshot(serialWorking)
    失败时在当前 goroutine 重执行
    validationResults[index] 失败时:
        Wait
        MarkEstimate
        Abort
        当前 goroutine 再执行一次
        Resume 只入 scheduler 队列
    将 writeSet 顺序写入 serialWorking
    Commit
```

已确认的核心问题：

```text
Abort 入队的新 incarnation 没有被并行 worker 消费
Resume 入队任务没有被 worker 消费
ESTIMATE 产生时初始并行执行阶段已经结束
重执行后缺少再次 Validate 循环
performance 路径仍依赖 serialWorking 顺序修复
正式结果来自顺序物化循环
```

## 1.3 当前 MetaTrack 链路

关键类型和函数：

```text
executor/v5/registry.go
    BatchRoutingPlugin
    BatchRoutingPlan
    AccessMatrixRow
    StateFrequencyRow
    CoaccessEdge
    StatePlacement
    TransactionPlacement
    metaTrackRouting.PlanBatch
    chooseStatePlacement
    transactionExecutionShard
    BatchClassificationInput
    BatchClassificationResult
    ScheduleResult
    ScheduleEvent
    CommitDecision

executor/v5/client.go
    submitBatch
    submitRecord
    workloadIngressShard

executor/v5/runtime.go
    prepareMetaTrackStateSnapshot
    fetchRemoteState
    executeMetaTrackSchedule
    commitOnce
```

当前控制面：

```text
客户端按 blockSize 切批
→ PlanBatch
→ X/F/W
→ StatePlacements
→ TransactionPlacements
→ artifacts CSV
```

当前实际提交：

```text
record.CrossShard == true
→ workloadIngressShard 优先使用 record.SourceShard
→ ψ_t 只进入 reason / artifact
→ 交易仍进入 source shard
```

当前远程状态：

```text
for tx in block.TxList:
    for access in tx.AccessList:
        rowKey = txID + key + homeShard
        fetchRemoteState(...)
```

去重粒度含 `txID`，多笔交易访问同一个键仍会重复拉取。

当前分类：

```text
hasRemoteExecutionBoundary(tx)
→ Conservative

存在 RAW/WAR/WAW/nonce 依赖
→ 大量交易进入 Conservative
```

当前执行：

```text
fastReady / conservativeReady
→ 一个共享 jobs channel
→ worker 并发领取
→ 每个任务使用单交易 SerialExecutor
```

当前工作窃取：

```text
没有每 worker 本地队列
没有同执行分片内 steal
StolenWork 主要作为保留字段
```

---

# 2. 全局强约束

## 2.1 只允许三项主任务

允许：

```text
A. 语义推导静态访问列表
B. Block-STM 乐观并发闭环
C. MetaTrack 无状态共现执行闭环
```

禁止顺手处理：

```text
前端重构
UI 配色
无关 API 重命名
目录大迁移
依赖升级
统一格式化
V6 规划
Fabric/EVM 新接入
新共识协议
新跨链协议
监控平台
Docker 集群重构
```

## 2.2 每次只做一个子阶段

禁止一次让 Codex 同时修改 A、B、C。

执行顺序：

```text
A0 只读审计
A1 数据结构与持久化
A2 运行时解析统一
A3 执行语义对齐
A4 摘要与独立重算
A5 100/1K 验收

B0 只读审计
B1 Scheduler 状态机
B2 MVMemory 执行 overlay
B3 Execute/Validate worker loop
B4 Abort/Estimate/Wait/Resume
B5 有序物化与指标
B6 冲突测试与 100/1K 验收

C0 只读审计
C1 区块级计划结构
C2 计划绑定 PBFT 区块
C3 φ 与 M_t 分离
C4 批量预取与 BatchStateCache
C5 StateReady 调度
C6 DAG 双轨
C7 每 worker 双队列与 steal
C8 Fast→Conservative 回退
C9 热点聚合
C10 100/1K 验收
```

一个子阶段未通过时不得进入下一子阶段。

## 2.3 不允许“日志式实现”

以下情况均视为未实现：

```text
只增加字段，没有进入控制流
只输出 CSV，没有驱动执行
只增加指标，指标始终为 0
只创建 SchedulerTask，没有 worker 消费
只设置 StolenWork 标签，没有真实队列窃取
只构造 prefetch plan，执行仍逐键同步 fetch
只计算 M_t/ψ_t，交易仍按 source shard 执行
只计算聚合 metadata，物理写入数量没有减少
```

## 2.4 不允许静默回退

正式配置中：

```text
serial_fallback_count = 0
no_fallback = true
oracle_mode = off
incarnation_limit_action = fail
```

算法无法完成时必须返回明确错误。

## 2.5 正确性不变量

任何阶段都不能破坏：

```text
签名验证
nonce 连续性
余额守恒
交易终态唯一
失败交易不得泄漏 WriteSet
PBFT 验证节点状态根一致
receipt root 一致
plan digest 一致
WAL 可重放
pending delta / commit 清零
proposal_in_flight = false
```

---

# 3. 分支、提交和回退策略

从当前稳定点创建：

```powershell
git checkout feat/v5-core-hardening
git pull --ff-only
git checkout -b feat/v5-algorithm-fidelity
```

禁止在 `main` 直接开发。

建议 checkpoint：

```text
A:
Implement semantics-derived access-list pipeline

B:
Implement faithful optimistic Block-STM scheduler

C:
Implement MetaTrack stateless coaccess execution
```

每个 checkpoint 前：

```powershell
git status --short
git diff --check
go test ./...
go vet ./...
.\.venv\Scripts\python.exe -m pytest backend/tests -q
```

不得自动 commit/push。用户明确要求后再执行。

---

# 4. Stage A：语义推导静态访问列表

# 4.1 目标定义

原始 Decentraland CSV 没有 EVM storage slot 和真实 ReadSet/WriteSet。

本阶段构造：

```text
semantics-derived static access list
```

含义：

```text
从 buyer、seller、contract、category
确定性推导交易可能访问的抽象状态
在交易进入路由和执行前固定
四种方法使用完全相同的解析结果
```

论文和实验标签：

```text
real observed Decentraland transaction stream
+
semantics-derived static access lists
```

不得标记为：

```text
real EVM storage trace
real Polygon read/write set
```

---

## 4.2 解决身份映射冲突

当前原始 buyer 地址无法直接作为 `SignedTransaction.Sender`，因为运行时使用确定性密钥生成可验证签名地址。

因此访问列表分两层：

### 第一层：持久化语义模板

写入 canonical/materialized workload：

```json
{
  "access_list_schema": "dcl_sale_access_template_v1",
  "access_list_source": "semantics_derived",
  "access_template": [
    {
      "role": "sender_balance",
      "mode": "read_write",
      "semantics": "buyer_balance",
      "delta": 0
    },
    {
      "role": "sender_nonce",
      "mode": "read_write",
      "semantics": "buyer_nonce",
      "delta": 0
    },
    {
      "role": "receiver_balance",
      "mode": "read_write",
      "semantics": "seller_balance",
      "delta": 0
    },
    {
      "role": "receiver_nonce",
      "mode": "read_write",
      "semantics": "seller_nonce_state",
      "delta": 0
    },
    {
      "role": "market_contract",
      "mode": "commutative_delta",
      "semantics": "market_sale_counter",
      "delta": 1
    },
    {
      "role": "category_metadata",
      "mode": "read",
      "semantics": "category_metadata",
      "delta": 0
    }
  ]
}
```

### 第二层：运行前确定性解析

对每条记录解析为：

```text
sender_balance
→ balance:<canonicalRuntimeSenderAddress(plan, sender_id)>

sender_nonce
→ nonce:<canonicalRuntimeSenderAddress(plan, sender_id)>

receiver_balance
→ balance:receiver_<receiver_id>

receiver_nonce
→ nonce:receiver_<receiver_id>

market_contract
→ market:<contract>

category_metadata
→ category:<category>
```

解析完成的列表必须在任何路由、共现、冲突分析和区块执行之前产生。

该解析列表称为：

```text
ResolvedAccessList
```

四种方法必须得到相同的 `ResolvedAccessListDigest`。

---

## 4.3 最终访问列表实例

原始记录：

```text
buyer   = 0xaaa
seller  = 0xbbb
contract = 0xccc
category = wearable
```

假设 buyer 的确定性运行地址为：

```text
mbe_sender_123
```

最终访问列表：

```text
balance:mbe_sender_123        read_write  buyer_balance
nonce:mbe_sender_123          read_write  buyer_nonce
balance:receiver_0xbbb        read_write  seller_balance
nonce:receiver_0xbbb          read_write  seller_nonce_state
market:0xccc                  commutative_delta(+1) market_sale_counter
category:wearable             read        category_metadata
```

禁止同时出现：

```text
account:sender:0xaaa
account:receiver:0xbbb
```

---

## 4.4 Python 侧精确修改

### 文件

```text
backend/app/services/workload_adapters/decentraland_sales_v1.py
```

### 函数

```text
DecentralandSalesAdapter.iter_canonical_records
```

### 当前行为

输出：

```text
state_keys:
    account:sender:<buyer>
    account:receiver:<seller>
    contract:<contract>

routing_source_key:
    account:sender:<buyer>

routing_target_key:
    contract:<contract>
```

### 新行为

输出：

```text
access_list_schema
access_list_source
access_template
state_keys
routing_source_key
routing_target_key
```

建议：

```text
state_keys:
    market:<contract>
    category:<category>

routing_source_key:
    sender_identity:<buyer>

routing_target_key:
    market:<contract>
```

`state_keys` 只保留兼容和预览用途。正式共现输入必须来自解析后的 AccessList。

### 新增校验

每条记录必须满足：

```text
模板 role 完整
role 不重复
mode 在 read/write/read_write/commutative_delta 中
market_sale_counter.delta = 1
category_metadata.mode = read
buyer/seller/contract/category 非空
```

### 禁止

```text
Python 直接伪造 Go 的签名地址
Python 复制一套不确定的 Go 密钥算法
把 raw buyer 直接写成最终 balance key
```

Go 运行时负责把 sender role 解析成实际签名地址。

---

## 4.5 canonical schema 精确修改

### 文件

```text
executor/v5/workload_iterator.go
```

### 新增类型

```go
type canonicalWireAccessTemplate struct {
    Role      string `json:"role"`
    Mode      string `json:"mode"`
    Semantics string `json:"semantics"`
    Delta     int64  `json:"delta,omitempty"`
}
```

### 修改 `canonicalWireRecord`

新增：

```go
AccessListSchema string                        `json:"access_list_schema"`
AccessListSource string                        `json:"access_list_source"`
AccessTemplate   []canonicalWireAccessTemplate `json:"access_template"`
Category         string                        `json:"category,omitempty"`
Contract         string                        `json:"contract,omitempty"`
```

如果 category 和 contract 已存在于 metadata，需要在 canonical record 中保留可直接解析的稳定字段，禁止运行时重新解析非结构化 metadata 字符串。

### schema 版本

推荐升级：

```text
mbe_workload_record_v2
```

保留 v1 只读兼容时，必须：

```text
明确标记 legacy_access_inference
禁止用于 formal algorithm-fidelity run
```

正式 A/B/C 实验只接受 v2。

---

## 4.6 运行时解析函数

### 删除或改造

```text
canonicalAccessList
canonicalRuntimeAccessList
```

### 新增

```go
func resolveCanonicalAccessList(
    plan WorkloadPlan,
    wire canonicalWireRecord,
) ([]tx.AccessItem, error)
```

职责：

```text
验证 schema/source/template
根据 role 解析最终 key
转换 mode
保留 semantics 和 delta
按 key/mode/semantics 排序
检测重复 key
同 key 多种 mode 时拒绝，禁止静默升级
返回唯一、确定性的 AccessList
```

### role 解析表

```go
switch template.Role {
case "sender_balance":
    key = "balance:" + canonicalRuntimeSenderAddress(plan, wire.SenderID)
case "sender_nonce":
    key = "nonce:" + canonicalRuntimeSenderAddress(plan, wire.SenderID)
case "receiver_balance":
    key = "balance:receiver_" + strings.ToLower(wire.ReceiverID)
case "receiver_nonce":
    key = "nonce:receiver_" + strings.ToLower(wire.ReceiverID)
case "market_contract":
    key = "market:" + strings.ToLower(wire.Contract)
case "category_metadata":
    key = "category:" + strings.ToLower(wire.Category)
default:
    return error
}
```

### 必须删除的行为

```go
append(
    tx.DefaultTransferAccessList(sender, receiver),
    record.AccessList...,
)
```

解析后的列表已经包含真实执行账户键，不得再次 append。

---

## 4.7 `CanonicalTraceIterator.Next` 修改

当前：

```text
wire → canonicalAccessList(wire)
```

改为：

```text
wire → resolveCanonicalAccessList(plan, wire)
```

失败时错误必须含：

```text
source_row_index
source_event_id
role
schema
```

`WorkloadRecord.AccessList` 保存最终解析列表。

新增：

```go
AccessListSchema string
AccessListSource string
AccessListDigest string
```

到 `WorkloadRecord`，或新增独立结构，禁止把 digest 只保存在日志字符串中。

---

## 4.8 `SignedTransaction` 修改

### 文件

```text
executor/v5/workload_iterator.go
```

### 函数

```text
CanonicalTraceIterator.SignedTransaction
```

必须直接使用：

```go
record.AccessList
```

禁止再次调用访问列表生成函数。

增加一致性断言：

```text
record.AccessList 非空
record.AccessListDigest 与重新计算结果一致
sender balance/nonce key 与 item.Sender 一致
receiver balance/nonce key 与 item.Receiver 一致
```

`SignedTransaction` 需要携带：

```text
AccessList
AccessListDigest
AccessListSchema
AccessListSource
```

若 `SignedTransaction` 当前没有后三个字段，可将 digest 纳入可签名 metadata，或确保完整 AccessList 本身已参与 TxID/签名摘要。Codex 必须先检查当前签名摘要是否包含 AccessList。

若 AccessList 未参与签名或 TxID，必须先停止并报告；不得让节点可以在签名后修改访问列表。

---

## 4.9 `client.go` 修改

### 当前问题

`submitBatch` 为 dataset 再次调用：

```go
canonicalRuntimeAccessList(datasetIterator.plan, record)
```

### 新行为

直接使用：

```go
record.AccessList
```

调用 `PlanBatch` 前断言：

```text
所有 record.AccessListDigest 已存在
同一 record 的 AccessList 未被修改
```

`submitRecord` 构造交易后断言：

```text
item.AccessListDigest == record.AccessListDigest
```

新增运行产物：

```text
resolved_access_lists.jsonl
access_list_summary.json
access_list_digest.txt
```

---

## 4.10 执行语义对齐

### 文件

```text
executor/realism/execution/serial.go
```

### 当前实际状态访问

```text
sender balance
sender nonce
receiver balance
receiver nonce
commutative deltas
```

### 新增语义读取

增加一个完整函数：

```go
func applyDeclaredSemanticReads(
    overlay *txOverlay,
    accesses []tx.AccessItem,
)
```

仅处理：

```text
AccessRead
category_metadata
```

执行：

```go
overlay.get(access.Key)
```

### market counter

保留：

```text
AccessCommutativeDelta
market:<contract>
delta=1
```

由现有 `applyCommutativeDeltas` 写入，使合约热点成为真实状态写，而不只是共现标签。

### 顺序

建议：

```text
applyDeclaredSemanticReads
ensureAccount
nonce check
balance transfer
sender nonce update
applyCommutativeDeltas
```

所有执行后端必须复用同一交易语义函数，避免 Serial 与 Block-STM 分叉。

---

## 4.11 AccessList digest

新增统一 Go 函数：

```go
func CanonicalAccessListDigest(
    accesses []tx.AccessItem,
) string
```

序列化字段：

```text
key
mode
update_semantics
delta
```

排序：

```text
key ASC
mode ASC
update_semantics ASC
delta ASC
```

哈希：

```text
SHA-256(lowercase hex)
```

全局 digest：

```text
按 materialized_index 排序
拼接:
    materialized_index
    logical_event_id
    per_tx_access_digest
再 SHA-256
```

Python 独立审计脚本必须按完全相同的规范重算。

---

## 4.12 Stage A 新测试文件

建议新增：

```text
backend/tests/test_v5_semantics_access_template.py
executor/v5/access_list_resolution_test.go
executor/v5/access_list_digest_test.go
executor/realism/execution/semantic_access_test.go
scripts/v5_access_list_audit.py
```

### 测试 A-1：模板生成

输入一条 wearable sale。

断言：

```text
6 个 role
无重复 role
market delta = 1
category mode = read
```

### 测试 A-2：解析到真实执行账户

断言：

```text
balance:<item.Sender> 在 AccessList
nonce:<item.Sender> 在 AccessList
balance:<item.Receiver> 在 AccessList
nonce:<item.Receiver> 在 AccessList
```

### 测试 A-3：无重复账户命名空间

断言：

```text
不存在 account:sender:
不存在 account:receiver:
```

### 测试 A-4：四方法 digest

同一物化文件、同一 seed：

```text
hash_serial
hash_block_stm
metatrack_serial
metatrack_block_stm
```

断言全局 digest 完全相同。

### 测试 A-5：语义键真的被执行

断言：

```text
market:<contract> 最终值增加
category:<category> 出现在 ReadSet
```

### 测试 A-6：独立重算 X/F/W

从 `resolved_access_lists.jsonl` 重算：

```text
X_t
F_t
W_t
```

和 `PlanBatch` 产物逐行一致。

---

## 4.13 Stage A 验收产物

必须输出：

```text
stage_a_access_list_acceptance.json
resolved_access_lists.jsonl.gz
access_list_summary.json
access_list_digest.txt
coaccess_reconstruction.json
```

`stage_a_access_list_acceptance.json`：

```json
{
  "accepted": true,
  "schema": "dcl_sale_access_template_v1",
  "source": "semantics_derived",
  "transaction_count": 1000,
  "empty_access_list_count": 0,
  "duplicate_key_count": 0,
  "legacy_account_alias_count": 0,
  "cross_method_digest_equal": true,
  "coaccess_reconstruction_equal": true
}
```

---

# 5. Stage B：完整 Block-STM 乐观并发

# 5.1 目标语义

实现持续运行的任务状态机：

```text
Execute(tx_i, incarnation_j)
→ Validate(tx_i, incarnation_j)
→ success: Validated
→ failure: Abort + MarkEstimate
→ Execute(tx_i, incarnation_j+1)
→ Validate(...)
→ 直到成功或达到上限
```

多个 worker 同时消费 Execute、Validate、Resume 任务。

最终写集和回执来自每笔交易最后一个通过验证的 incarnation。

---

## 5.2 推荐文件边界

保留现有目录，避免过度拆分。

建议：

```text
executor/realism/execution/blockstm/types.go
executor/realism/execution/blockstm/mvmemory.go
executor/realism/execution/blockstm/scheduler.go
executor/realism/execution/blockstm/dependency.go
executor/realism/execution/blockstm_executor.go
executor/realism/execution/blockstm_executor_test.go
```

最多新增：

```text
executor/realism/execution/blockstm/worker.go
```

不要创建十几个小文件。

---

## 5.3 SchedulerTask 精确结构

```go
type TaskKind string

const (
    TaskExecute  TaskKind = "execute"
    TaskValidate TaskKind = "validate"
)

type SchedulerTask struct {
    Kind    TaskKind
    Version Version
    Reason  string
}
```

Resume 不需要独立任务类型时，可将原 Execute/Validate 任务重新入队，并标记 reason=`dependency_resolved`。

---

## 5.4 每笔交易运行状态

新增或扩展：

```go
type TxnStatus string

const (
    TxnReady      TxnStatus = "ready"
    TxnExecuting  TxnStatus = "executing"
    TxnExecuted   TxnStatus = "executed"
    TxnValidating TxnStatus = "validating"
    TxnWaiting    TxnStatus = "waiting"
    TxnValidated  TxnStatus = "validated"
    TxnAborted    TxnStatus = "aborted"
    TxnCommitted  TxnStatus = "committed"
    TxnFailed     TxnStatus = "failed"
)

type TxnRuntime struct {
    mu sync.Mutex

    CurrentIncarnation Incarnation
    Status             TxnStatus

    CapturedReads CapturedReads
    ReadSet       []ReadObservation
    WriteSet      map[string]string
    Receipt       Receipt

    WaitingOn      map[Version]struct{}
    FinalValidated Version
    LastError      string
}
```

所有读写必须通过锁或原子状态完成。

禁止多个 worker 同时执行同一 `(tx, incarnation)`。

---

## 5.5 Scheduler 全局状态

```go
type Scheduler struct {
    mu   sync.Mutex
    cond *sync.Cond

    queue []SchedulerTask

    activeTasks int
    waitingTasks int
    validatedCount int
    failed bool
    failure error
    closed bool

    queued map[taskIdentity]bool
    running map[taskIdentity]bool
}
```

`Next(ctx)` 行为：

```text
队列非空 → 取任务并 activeTasks++
队列为空但仍有 active/waiting → cond.Wait
全部交易 validated 且 active=0 且 queue=0 → closed
failed → 返回 failure
context canceled → 返回 ctx.Err
```

任务完成必须调用：

```go
scheduler.Finish(task)
```

禁止 worker 取任务后不减少 activeTasks。

---

## 5.6 去重规则

同一任务标识：

```text
(kind, tx_index, incarnation)
```

最多同时存在一份 queued/running。

旧 incarnation 任务从队列取出时：

```text
task.incarnation < CurrentIncarnation
→ stale task
→ 丢弃并计 stale_task_count
```

---

## 5.7 Block-STM 执行 overlay

当前 `txOverlay` 从完整 snapshot 读取。

Block-STM 需要新的 overlay：

```go
type stmOverlay struct {
    txnIndex   TxnIndex
    incarnation Incarnation
    base       map[string]string
    memory     *MVMemory

    reads  []ReadObservation
    captured CapturedReads
    writes map[string]string
    dependency *Version
}
```

### get(key)

```text
1. 查询当前事务本地 writes；
2. 调用 MVMemory.Read(key, txnIndex)；
3. 返回最高 tx_index < 当前 tx_index 的稳定版本；
4. 读到 ESTIMATE：
      记录 dependency
      返回 dependency signal
5. 未找到前序版本：
      读取 base snapshot；
6. 记录实际读取版本和 value digest。
```

### set(key, value)

只写入本地 `writes`。

Execute 完成后一次性发布：

```text
memory.Write(key, version, value)
```

发布前检测 incarnation 仍为当前版本。

---

## 5.8 Execute 任务完整流程

```go
func executeTask(
    ctx context.Context,
    task SchedulerTask,
    runtimes []TxnRuntime,
    memory *MVMemory,
    dependencies *DependencyRegistry,
    scheduler *Scheduler,
)
```

步骤：

```text
1. 校验 task.Kind == Execute；
2. 校验 incarnation 仍是当前版本；
3. 状态 CAS: Ready/Aborted → Executing；
4. 创建 stmOverlay；
5. 调用共享交易语义执行；
6. 遇到 ESTIMATE dependency：
      状态 → Waiting
      dependencies.Register(waiter, dependency)
      scheduler.RegisterWaiting
      不发布 WriteSet
      返回；
7. 执行完成：
      保存 CapturedReads / ReadSet / WriteSet / Receipt
      发布 WriteSet 到 MVMemory
      状态 → Executed
      enqueue Validate(same version)
8. Finish task。
```

失败交易也必须形成可验证结果。业务失败的 WriteSet 必须为空。

---

## 5.9 Validate 任务完整流程

步骤：

```text
1. 校验 incarnation 仍为当前版本；
2. 状态 Executed → Validating；
3. 对 CapturedReads 逐项验证：
      当前可见版本是否仍等于执行时版本
      当前 value digest 是否一致
      是否出现 ESTIMATE
4. Valid:
      状态 → Validated
      FinalValidated = version
      scheduler.MarkValidated(tx)
      dependencies.Resolve(version)
      恢复 waiters
5. Invalid:
      状态 → Aborted
      对当前 WriteSet 调用 MarkEstimate
      incarnation++
      检查 MaximumIncarnations
      清理旧 CapturedReads/WriteSet/Receipt
      enqueue Execute(next version)
      对受影响的高索引 reader enqueue Validate
6. Finish task。
```

---

## 5.10 反向读取索引

为了让低索引交易的新版本使高索引交易重新验证，维护：

```go
type ReaderIndex struct {
    key → set(tx_index, incarnation)
}
```

Execute 捕获 read 后登记。

某版本 abort / overwrite 后：

```text
查找读过相关 key 的更高 tx_index
→ 对其当前已执行/已验证 incarnation
→ enqueue Validate
```

禁止简单地对所有后续交易全量验证，除非作为 correctness fallback 且不进入 performance 配置。

---

## 5.11 ESTIMATE 依赖闭环

当 `MVMemory.Read` 读到：

```text
key
lower Version
Estimate=true
```

返回：

```go
ReadResult{
    Dependency: &lowerVersion,
}
```

waiter 注册：

```text
waiter version → dependency version
```

当 dependency 新 incarnation 验证成功：

```text
Resolve(dependency tx index 的稳定版本)
→ 取所有 waiter
→ 将 waiter 当前 incarnation 的 Execute 重新入队
```

必须验证恢复任务被某 worker 实际消费。

---

## 5.12 Incarnation 上限

定义：

```text
incarnation 0 = 第一次执行
incarnation 1 = 第一次重执行
```

当：

```text
next incarnation >= MaximumIncarnations
```

返回错误：

```text
block-stm maximum incarnations exceeded
tx_id
tx_index
last_incarnation
conflicting_keys
```

formal 配置：

```text
MaximumIncarnations = 16
IncarnationLimitAction = fail
```

禁止正式运行进入 serial fallback。

---

## 5.13 有序物化

所有交易 Validated 后：

```text
for tx index 0..N-1:
    读取 FinalValidated incarnation
    获取该 incarnation 的 Receipt/ReadSet/WriteSet
    将 WriteSet 应用到 materialized state
    计算 StateRootAfterTx
    构造 TxDelta
```

物化阶段禁止：

```text
调用 executeTx
重新读取业务状态并重跑交易
修改 incarnation
执行验证
```

物化只应用已验证结果。

注意：

```text
StateRootAfterTx 可在物化时补写到 receipt
```

这不属于业务重执行。

---

## 5.14 Serial oracle

配置：

```text
execution_mode = correctness
oracle_mode = full
```

才允许运行。

计时边界：

```text
BlockSTMMetrics.SerialOracleMS 单独记录
正式 throughput/execution_ms 不包含 oracle
```

performance：

```text
oracle_mode = off
SerialOracleMS = 0
```

---

## 5.15 Block-STM 指标精确定义

```text
execution_task_count:
    被 worker 实际开始执行的 Execute 任务数

validation_task_count:
    被 worker 实际开始执行的 Validate 任务数

abort_count:
    Validate 失败并生成 next incarnation 的次数

reexecution_count:
    incarnation > 0 的 Execute 实际执行次数

maximum_incarnation:
    所有最终/中间 incarnation 最大值

estimate_mark_count:
    写版本被标记 ESTIMATE 的数量

estimate_read_count:
    Execute 读到 ESTIMATE 的次数

dependency_wait_count:
    事务因 ESTIMATE 进入 Waiting 的次数

dependency_resume_count:
    waiter 被重新入队的次数

validated_speculative_result_count:
    最终拥有 FinalValidated 的交易数

maximum_concurrent_executions:
    同时处于 Executing 的峰值

scheduler_queue_peak:
    队列长度峰值

stale_task_count:
    被丢弃的旧 incarnation 任务数

serial_fallback_count:
    正式运行必须为 0
```

不变量：

```text
validated_speculative_result_count = tx_count
committed_transaction_count = tx_count
reexecution_count = Σ 每个实际执行 incarnation>0 的次数
dependency_resume_count <= dependency_wait_count
serial_fallback_count = 0
```

---

## 5.16 Block-STM 测试矩阵

新增或重写：

```text
executor/realism/execution/blockstm_scheduler_test.go
executor/realism/execution/blockstm_executor_test.go
executor/realism/execution/blockstm_fidelity_test.go
```

### B-1 无冲突

4 worker，至少 16 笔不同账户交易。

断言：

```text
maximum_concurrent_executions > 1
abort_count = 0
reexecution_count = 0
all FinalValidated incarnation = 0
state root = Serial
receipt root = Serial
```

### B-2 RAW 冲突

构造：

```text
tx0 写 K
tx1 读 K 后写 L
```

强制 tx1 先推测读取旧值。

断言：

```text
tx1 validation failure
tx1 incarnation = 1
tx1 Execute(1) 被 worker 消费
tx1 Validate(1) 成功
最终状态 = Serial
```

### B-3 ESTIMATE

构造三笔：

```text
tx0 写 A
tx1 读 A 写 B，首次验证失败并 MarkEstimate
tx2 读 B
```

断言：

```text
tx2 读到 ESTIMATE
tx2 Waiting
tx1 新 incarnation 验证成功
tx2 Resume
tx2 被重新执行
最终状态 = Serial
```

### B-4 多 incarnation

故意制造至少两次失败。

断言：

```text
maximum_incarnation >= 2
每次 incarnation 都经历 Execute→Validate
```

### B-5 上限

设置：

```text
MaximumIncarnations = 2
```

持续冲突。

断言：

```text
返回明确错误
serial_fallback_count = 0
```

### B-6 调度随机性确定性

同一块重复 20 次，改变 goroutine 调度。

断言：

```text
state root 唯一
receipt root 唯一
TxDelta 顺序唯一
```

---

## 5.17 Block-STM trace 产物

100 笔 correctness run 输出：

```text
blockstm_task_trace.jsonl
blockstm_incarnation_summary.json
blockstm_dependency_trace.jsonl
blockstm_metrics.json
```

任务 trace 每行：

```json
{
  "seq": 1,
  "worker": 2,
  "task": "execute",
  "tx_index": 7,
  "incarnation": 1,
  "status_before": "aborted",
  "status_after": "executed",
  "reason": "validation_failed",
  "timestamp_ns": 0
}
```

验收脚本必须证明：

```text
至少一个 Execute 与 Validate 在时间轴交错
Abort 生成的新 incarnation 被消费
Resume 后任务被消费
最终每笔交易有 FinalValidated
```

---

# 6. Stage C：MetaTrack 无状态共现执行

# 6.1 目标主链

```text
实际候选区块 TxList
→ ResolvedAccessList
→ X_t
→ F_t
→ W_t
→ M_t
→ ψ_t
→ c_t
→ z_t
→ BatchExecutionPlan
→ PBFT 绑定
→ 批量状态预取
→ BatchStateCache
→ StateReady 双轨调度
→ 执行
→ 热点聚合
→ 按 home shard 写回
```

---

## 6.2 客户端职责收缩

当前客户端计算 `PlanBatch`。

修改后客户端只负责：

```text
读取物化负载
构造签名交易
提交到入口节点
记录 submitted lifecycle
```

客户端可以生成预览计划用于 UI，但该计划不能作为正式执行计划。

正式计划必须针对：

```text
BlockProducer 实际选中的 TxList
```

生成。

---

## 6.3 区块级计划结构

为避免 `block` 包依赖 `v5` 包，在：

```text
executor/realism/block/block.go
```

新增通用 envelope：

```go
type ExecutionPlanEnvelope struct {
    SchemaVersion string          `json:"schema_version"`
    AlgorithmID   string          `json:"algorithm_id"`
    Payload       json.RawMessage `json:"payload"`
    PayloadDigest string          `json:"payload_digest"`
}
```

`Block` 新增：

```go
ExecutionPlan *ExecutionPlanEnvelope `json:"execution_plan,omitempty"`
```

MetaTrack payload 在 v5 包定义：

```go
type MetaTrackBatchExecutionPlan struct {
    SchemaVersion string `json:"schema_version"`
    BatchID string `json:"batch_id"`

    BlockShardID string `json:"block_shard_id"`
    BlockHeight uint64 `json:"block_height"`

    OrderedTransactionIDs []string `json:"ordered_transaction_ids"`

    AccessListDigest string `json:"access_list_digest"`
    AccessMatrixDigest string `json:"access_matrix_digest"`
    FrequencyDigest string `json:"frequency_digest"`
    CoaccessDigest string `json:"coaccess_digest"`

    StatePlacements []StatePlacement `json:"state_placements"`
    TransactionPlacements []TransactionPlacement `json:"transaction_placements"`

    TrackAssignments []TrackAssignment `json:"track_assignments"`
    AggregationAssignments []AggregationAssignment `json:"aggregation_assignments"`
    PrefetchGroups []PrefetchGroup `json:"prefetch_groups"`

    DependencyGraphDigest string `json:"dependency_graph_digest"`
    PlanDigest string `json:"plan_digest"`
}
```

---

## 6.4 区块哈希绑定

检查：

```text
executor/realism/block/hash.go
```

修改区块规范化哈希输入，至少纳入：

```text
ExecutionPlan.AlgorithmID
ExecutionPlan.PayloadDigest
```

完整 Payload 独立计算 SHA-256。

验证规则：

```text
SHA256(canonical payload) == PayloadDigest
payload.PlanDigest == PayloadDigest 或明确嵌套规则
```

所有 PBFT validator 收到 proposal 后：

```text
验证 TxRoot
验证 ExecutionPlan payload digest
根据 TxList 独立重算 MetaTrack plan
比较重算 PlanDigest
不一致则拒绝 proposal
```

禁止只信 proposer 提供的 plan。

---

## 6.5 正式计划生成位置

建议在候选块形成后、PBFT proposal 广播前执行：

```go
func (r *runtimeNode) attachExecutionPlan(
    candidate *block.Block,
) error
```

逻辑：

```text
BlockProducer.BuildCandidate
→ candidate.TxList 已固定
→ MetaTrack enabled?
→ buildMetaTrackBatchExecutionPlan(candidate)
→ canonical serialize
→ attach ExecutionPlan
→ block.AssignHash(candidate)
→ PBFT proposal
```

Hash/Serial 方法：

```text
ExecutionPlan = nil
或 algorithm_id = hash_baseline_v1
```

四方法 plan digest 语义要清楚，禁止 Hash 方法伪装 MetaTrack。

---

## 6.6 X_t、F_t、W_t 精确定义

输入：

```text
candidate.TxList[i].AccessList
```

访问项先按 key 去重。

```text
X_t[i,k] = 1
F_t[k] = Σ_i X_t[i,k]
W_t[k,l] = Σ_i X_t[i,k]X_t[i,l], k != l
```

默认不存对角线。

读写计数：

```text
ReadCount:
    AccessRead
    AccessReadWrite
    AccessCommutativeDelta

WriteCount:
    AccessWrite
    AccessReadWrite
    AccessCommutativeDelta
```

同一交易重复声明同一 key：

```text
计划构建直接报错
```

禁止静默去重掩盖上游错误。

---

## 6.7 M_t 默认算法

现有 cost model 暂时保留为扩展配置：

```text
placement_policy = cost_model_v1
```

新增论文默认：

```text
placement_policy = frequency_coaccess_v1
```

formal MetaTrack 使用后者。

### 输入

```text
K_t
F_t
W_t
执行分片 P
home mapping φ(k)
placement budget b_t
```

### 顺序

状态键排序：

```text
F_t 降序
write_count 降序
key 字典序
```

### 候选分片得分

```text
Affinity(k,p)
=
Σ_{u 已放置且 M_t(u)=p} W_t[k,u]
```

负载约束：

```text
state_load[p]
tx_load_estimate[p]
```

默认选择：

```text
最大 Affinity
→ state_load 更小
→ shard ID 更小
```

当所有 Affinity=0：

```text
优先 home shard
```

### ordered nonce

nonce 键保持 home shard 可作为明确约束：

```text
ordered_nonce_home_constraint = true
```

必须写入 plan config 和 reason。

禁止使用隐藏的一百万惩罚而不记录约束。

### placement budget

定义：

```text
b_t = max(
    b_min,
    floor(|K_t| * μ / |P|)
)
```

具体 `b_min`、`μ` 来自 formal spec。

每个执行分片超过预算时，选择下一候选。

---

## 6.8 ψ_t 默认算法

对交易 `tx_i`：

```text
coverage[p]
=
访问键中 M_t(k)=p 的键数量
```

选择：

```text
coverage 最大
→ remote key count 最小
→ 当前 tx load 最小
→ shard ID 最小
```

不要使用 `placement.Frequency` 作为默认交易覆盖权重。

频率加权版本可以保留为：

```text
transaction_placement_policy = frequency_weighted_extension_v1
```

正式论文默认使用 majority coverage。

---

## 6.9 φ(k) 与 M_t(k) 分离

```text
φ(k) = shardFor(key)
M_t(k) = plan.StatePlacements[key].ExecutionShard
```

状态始终持久化在 `φ(k)`。

执行临时发生在 `M_t(k)` 或 `ψ_t(tx)`。

禁止：

```text
因为 φ(sender) != φ(contract)
→ 自动设置 v5_cross payload
→ SourceLock/TargetCommit/SourceFinalize
```

MetaTrack formal path 增加明确标记：

```text
execution_mode = metatrack_stateless
```

该模式绕过 legacy cross-shard transaction protocol，进入状态预取/写回路径。

legacy protocol 继续供 hash baseline 或兼容实验使用。

---

## 6.10 PrefetchGroup

```go
type PrefetchGroup struct {
    ExecutionShard string `json:"execution_shard"`
    HomeShard string `json:"home_shard"`
    Keys []string `json:"keys"`
    KeyDigest string `json:"key_digest"`
}
```

生成：

```text
for each transaction placement:
    execution = ψ_t(tx)
    for access key:
        home = φ(key)
        if home != execution:
            add key to group(execution, home)
```

按 key 去重、排序。

去重键：

```text
(batch_id, execution_shard, home_shard, state_key, state_version)
```

禁止包含 `tx_id`。

---

## 6.11 批量状态接口

当前：

```text
BuildFetchRequest(StateFetchInput)
单 key
```

新增批量接口，保留旧接口兼容：

```go
type BatchStateFetchInput struct {
    RequestID string
    BatchID string
    ExecutionShard string
    HomeShard string
    Keys []string
    ExpectedStateRoot string
}

type StateWitnessItem struct {
    Key string
    Value string
    Version uint64
    HomeShard string
    StateRoot string
    Proof []byte
    ProofDigest string
}

type BatchStateFetchResponse struct {
    RequestID string
    Items []StateWitnessItem
}
```

StateAccessPlugin 增加可选扩展接口：

```go
type BatchStateAccessPlugin interface {
    StateAccessPlugin
    BuildBatchFetchRequest(BatchStateFetchInput) BatchStateFetchRequest
}
```

若底层暂无可验证 proof：

```text
先停止并报告现有 state.DB 能否生成 proof
禁止生成伪 proof
```

允许第一步使用明确 truth label：

```text
state_root_bound_value_bundle
```

但论文正式“state proof/witness”声明需要真实验证机制。

---

## 6.12 BatchStateCache

新增建议文件：

```text
executor/v5/metatrack_state_cache.go
```

结构：

```go
type BatchStateCacheKey struct {
    BatchID string
    ExecutionShard string
    HomeShard string
    StateKey string
    Version uint64
}

type BatchStateCacheEntry struct {
    Value string
    Version uint64
    StateRoot string
    ProofDigest string
    Verified bool
    Ready bool
    Error string
}

type BatchStateCache struct {
    mu sync.RWMutex
    entries map[BatchStateCacheKey]BatchStateCacheEntry
    waiters map[string][]chan struct{}
}
```

方法：

```text
Reserve
PutVerified
Get
ReadyForTransaction
Subscribe
CloseWithError
Metrics
```

执行器不能直接读取远端 DB。

远程状态只能从 BatchStateCache 获取。

---

## 6.13 异步预取

当前 `prepareMetaTrackStateSnapshot` 同步循环。

替换为：

```go
func startMetaTrackPrefetch(
    ctx context.Context,
    plan MetaTrackBatchExecutionPlan,
    cache *BatchStateCache,
) *PrefetchHandle
```

每个 `(executionShard, homeShard)` 一个批量请求，可限制并发。

状态返回：

```text
验证 root/proof
PutVerified
发布 StateReady 事件
```

预取和执行允许重叠：

```text
本地状态已 ready 的交易先执行
远程状态等待中的交易挂起
```

禁止所有远程状态全部完成后才启动整个区块执行。

---

## 6.14 StateReady

每笔交易维护：

```go
type TxReadiness struct {
    DependencyRemaining int
    MissingStateKeys int
    Ready bool
    Suspended bool
}
```

Ready 条件：

```text
DependencyRemaining == 0
MissingStateKeys == 0
```

状态返回时：

```text
MissingStateKeys--
达到 0 且依赖为 0
→ enqueue 到原 track 的 ready queue
```

等待期间不占 worker。

指标：

```text
state_ready_wait_count
state_ready_resume_count
state_prefetch_wait_ms
critical_path_remote_miss_count
prefetch_cache_hit_count
prefetch_cache_miss_count
unique_remote_key_fetch_count
remote_fetch_request_count
```

---

## 6.15 DAG 与双轨

构建有向依赖：

```text
RAW: earlier writer → later reader
WAW: earlier writer → later writer
WAR: earlier reader → later writer
nonce: lower nonce → higher nonce
```

方向按区块原始顺序确定。

运行 Tarjan SCC。

### Fast Track

```text
静态访问列表完整
SCC size = 1
无 self-loop
操作类型受支持
依赖方向确定
```

Fast 中允许 DAG 边。

### Conservative Track

```text
SCC size > 1
动态/未知访问
access-list violation
unsupported operation
复杂约束
```

远程状态来源不参与 Track 分类。

---

## 6.16 每 worker 双队列

建议新增：

```text
executor/v5/metatrack_scheduler.go
```

结构：

```go
type WorkerQueues struct {
    mu sync.Mutex
    Fast deque[TxTask]
    Conservative deque[TxTask]
}

type ShardScheduler struct {
    Workers []WorkerQueues
}
```

worker 顺序：

```text
1. pop local Fast front
2. pop local Conservative front
3. steal another worker Fast back
4. steal another worker Conservative back
5. 无任务则等待 cond/event
```

steal 仅限：

```text
同 execution shard
同 track
```

禁止跨执行分片窃取，因为不同分片缓存和状态就绪条件不同。

指标：

```text
steal_attempt_count
steal_success_count
stolen_task_count
per_worker_executed_count
queue_imbalance_peak
```

---

## 6.17 Fast→Conservative 回退

Fast 执行时对实际 ReadSet/WriteSet 与声明 AccessList 比较。

触发：

```text
读取未声明 key
写入未声明 key
声明 Read，实际 Write
动态调用扩展
版本约束失败
不支持操作
```

流程：

```text
丢弃 tentative WriteSet
记录 fallback reason
从 Fast 状态移除
加入 Conservative queue
Attempt++
重新执行
```

必须保证：

```text
tentative writes 未进入全局 state
同一交易只有一个 FinalCompletion
```

指标：

```text
fast_fallback_count
fallback_reason_counts
discarded_tentative_result_count
conservative_reexecution_count
```

---

## 6.18 热点更新聚合

可聚合条件：

```text
AccessCommutativeDelta
同一 state key
相同 home shard
相同约束域
所有交易业务执行成功
```

聚合：

```text
Δ_k = Σ delta_i
```

AtomicReserve：

```text
读取 base value/version
检查:
    base + Δ >= lower_bound
    inventory constraint
    version 未变化
```

成功：

```text
生成一个 physical StateDelta
保留全部 TxIDs
```

失败：

```text
整个 group 回退 regular commit
逐笔按原序提交
```

指标：

```text
logical_update_count
physical_update_count
aggregation_ratio
aggregated_key_count
atomic_reserve_success_count
atomic_reserve_failure_count
fallback_group_count
```

本阶段 `market:<contract>` 的 sale counter 提供真实可交换热点。

---

## 6.19 MetaTrack 写回

执行结果按 `φ(k)` 分组：

```text
home shard
→ key-sorted StateDelta batch
```

每组包含：

```text
batch_id
plan_digest
source execution shard
home shard
ordered key deltas
base version/root
```

home shard 验证：

```text
plan digest
base version
AtomicReserve/constraint
重复 apply
```

成功后写入 WAL。

drain block 仅用于必要的异步最终写回，不应由逐键请求放大。

---

## 6.20 MetaTrack 测试矩阵

建议新增：

```text
executor/v5/metatrack_plan_test.go
executor/v5/metatrack_prefetch_test.go
executor/v5/metatrack_scheduler_test.go
executor/v5/metatrack_fallback_test.go
executor/v5/metatrack_aggregation_test.go
executor/v5/metatrack_runtime_integration_test.go
```

### C-1 实际区块计划

客户端预览批次与最终区块顺序不同。

断言：

```text
正式 plan 的 OrderedTransactionIDs 等于 block.TxList
不等于客户端预览批次时不报错
```

### C-2 PBFT 计划篡改

修改一个 StatePlacement。

断言：

```text
PayloadDigest 失败
validator 拒绝 proposal
```

### C-3 φ/M_t 分离

一个交易访问两个 home shard。

断言：

```text
一个 execution shard
两个 PrefetchGroup
home mapping 不变
不进入 legacy SourceLock
```

### C-4 共享键去重

10 笔交易访问同一 market key。

断言：

```text
unique_remote_key_fetch_count = 1
cache hit >= 9
remote request count 取决于 home group，不能为 10
```

### C-5 StateReady

一个交易远程状态延迟，另一个交易本地 ready。

断言：

```text
本地交易先完成
远程交易 wait/resume
等待不占 worker
```

### C-6 DAG Fast

```text
tx0 → tx1 → tx2
```

断言三笔均为 Fast，按依赖释放。

### C-7 SCC Conservative

构造 cycle。

断言 cycle 交易进入 Conservative。

### C-8 Work stealing

worker0 队列 20 项，worker1 空。

断言：

```text
steal_success_count > 0
worker1 executed_count > 0
```

### C-9 Fast fallback

实际写入未声明 key。

断言：

```text
tentative result discarded
进入 Conservative
仅一个 final completion
```

### C-10 聚合

20 笔 market delta。

断言：

```text
logical_update_count = 20
physical_update_count = 1
最终值增加 20
```

### C-11 聚合失败

构造下界/版本冲突。

断言：

```text
整个组 regular fallback
最终状态与 Serial 相同
```

---

# 7. 三阶段最终四方法含义

完成后：

| 配置 | 路由/状态获取 | 执行后端 |
|---|---|---|
| `hash_serial` | Hash home placement；legacy/local state path | Serial |
| `hash_block_stm` | Hash home placement；legacy/local state path | 完整 Block-STM |
| `metatrack_serial` | 区块级共现执行分片；批量预取；StateReady | Serial |
| `metatrack_block_stm` | 区块级共现执行分片；批量预取；StateReady | 完整 Block-STM |

四方法共享：

```text
相同 materialized workload hash
相同 resolved access-list digest
相同交易集合和顺序
相同初始状态
相同节点/分片/PBFT 配置
相同 block size
相同计时边界
```

---

# 8. 计时边界

## 8.1 End-to-end

```text
第一笔 submitted
→ 最后一笔 finalized
→ pending delta/commit/proposal 清零
```

## 8.2 MetaTrack 子指标

```text
plan_build_ms
plan_verify_ms
prefetch_manifest_ms
prefetch_network_ms
proof_verify_ms
state_ready_wait_ms
execution_ms
aggregation_ms
writeback_ms
drain_ms
```

## 8.3 Block-STM 子指标

```text
scheduler_ms
speculative_execution_ms
validation_ms
dependency_wait_ms
ordered_materialization_ms
oracle_ms
```

禁止把 oracle 加入 performance execution_ms。

---

# 9. 100 笔、1K、10K 顺序

## 9.1 每个子阶段

先跑 unit tests。

## 9.2 每个完整 Stage

跑 100 笔：

```text
检查 trace 和 artifact
```

再跑 1K：

```text
正确性和状态审计
```

三阶段全部通过后，才允许 10K。

禁止：

```text
一边改算法一边反复跑 10K
混用失败 run
复用 child runtime
自动 retry
```

---

# 10. Codex 每轮固定输出

## 10.1 只读审计阶段

必须输出：

```text
当前调用链
每个关键函数的真实职责
与本文规范的差异
预计修改文件白名单
预计新增文件白名单
每个函数修改理由
测试矩阵
风险和 stop condition
```

禁止修改代码。

## 10.2 实现阶段

报告格式：

```text
阶段:
子阶段:
基线 commit:

读取文件:
修改文件:
新增文件:
明确未修改文件:

当前行为:
目标行为:

逐函数变更:
- file
- function
- 完整新职责
- 调用者
- 不变量
- 测试

执行命令:
执行结果:
每个测试证明的语义:

算法证据:
git diff --check:
git diff --stat:
git status --short:

剩余缺口:
```

## 10.3 必须提供完整函数

每个修改函数在最终报告中给出完整函数，或给出可直接审查的 patch 文件。

禁止只贴几行关键代码。

---

# 11. Codex 停止条件

出现以下任一情况立即停止：

```text
AccessList 未参与签名或 TxID，且需要改变签名协议
state.DB 无法产生论文要求的 proof/witness
修改 Block hash 会破坏已有持久化兼容
PBFT proposal 校验路径无法访问插件配置
Block-STM 交易语义无法从 MVMemory overlay 执行
MetaTrack 写回需要新的跨片原子协议
任何测试出现 state root divergence
任何实现需要 silent serial fallback
```

报告：

```text
冲突位置
现有代码证据
可选方案
每个方案影响
推荐方案
```

等待用户确认。

---

# 12. 真值标签升级条件

当前保持：

```text
Block-STM:
staged_block_stm_prototype

MetaTrack:
metatrack_correctness_framework_prototype
```

Block-STM 标签升级必须同时满足：

```text
连续 Execute/Validate 调度
Abort 新 incarnation 被 worker 消费
ESTIMATE Wait/Resume 真实发生
多 incarnation 可收敛
正式结果来自 validated incarnation
performance 无全量 serial replay
```

MetaTrack 标签升级必须同时满足：

```text
正式计划基于 block.TxList
计划绑定 PBFT 区块
M_t/ψ_t 驱动真实执行
φ 与 M_t 分离
批量预取和缓存复用
StateReady suspend/resume
DAG 保留 Fast
真实 worker steal
Fast→Conservative fallback
聚合产生真实物理写入减少
```

---

# 13. 第一轮只读审查任务的精确边界

Codex 收到本文后，第一轮只做 Stage A0。

必须读取：

```text
backend/app/services/workload_adapters/decentraland_sales_v1.py
backend/app/services/v5_workload_data_plane.py
backend/app/services/workload_adapters/base.py
executor/v5/workload_iterator.go
executor/v5/client.go
executor/v5/registry.go
executor/realism/tx/*
executor/realism/execution/serial.go
相关测试
```

必须回答：

```text
1. canonical JSONL 的实际生成函数和序列化边界在哪里；
2. materialization seed 是否可用于 Go 侧身份解析；
3. AccessList 是否参与 TxID 和签名；
4. SignedTransaction 是否能增加 digest/source/schema 字段；
5. 当前 SerialExecutor 实际访问哪些键；
6. market/category 键如何进入真实 ReadSet/WriteSet；
7. 删除 DefaultTransferAccessList append 后是否影响执行；
8. v1 workload 兼容方式；
9. Stage A 最小修改白名单；
10. 测试先失败的具体断言。
```

第一轮不得修改代码、不得创建分支、不得 commit/push。

---

# 14. 最终 Definition of Done

整个三项改造完成时必须满足：

```text
[Access List]
语义模板持久化
运行前确定性解析
四方法 digest 相同
无重复账户命名空间
市场热点和类别键进入真实执行读写
X/F/W 可独立重算

[Block-STM]
worker 持续消费 Execute/Validate
Abort/Resume 任务真实执行
多 incarnation 真实存在
ESTIMATE 等待闭环
最终结果来自 validated incarnation
performance 无串行业务重放
状态与 Serial 等价

[MetaTrack]
计划针对真实区块
计划被 PBFT 验证
M_t/ψ_t 驱动执行
存储 home 与执行位置分离
远程状态按组批量获取
共享状态缓存复用
StateReady 非阻塞推进
DAG Fast、cycle Conservative
真实双队列和 steal
Fast 回退可审计
热点聚合减少物理写入
状态与 baseline 等价

[Formal]
100 笔通过
1K 通过
再运行单次新鲜 10K
```

在上述条件全部满足前，禁止声称已经完成论文级 MetaTrack 或 Block-STM 性能复现。
