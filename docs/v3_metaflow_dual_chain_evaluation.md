# V3 MetaFlow Dual-chain Evaluation

## 1. MetaFlow 在 V3 中的位置

MetaFlow 是 V3.6 的双链协议实验主线。它建立在 V3.5 minimal dual-chain runtime 上，并使用 `CrossChainProtocolPlugin` 与 baselines 公平比较。

V2.6 的 protocol baselines 是接口和指标种子，不是 MetaFlow 实现。

## 2. 双链 runtime 结构

V3.5 需要 source chain 和 target chain：

```text
source_chain X
  -> source lock
  -> source finality
  -> protocol decision / certificate / control
target_chain Y
  -> target mint
  -> target finality
source_chain X
  -> source complete or refund
```

## 3. Source chain / target chain 属性

每条链必须有 ChainProfile：

- chain id / role。
- node count。
- block interval。
- finality depth/profile。
- TxPool config。
- consensus config。
- execution/commit config。
- network profile。
- backend/runtime type。

Fair comparisons must keep source and target profiles identical across protocols.

## 4. CrossChainProtocolPlugin

`CrossChainProtocolPlugin` 职责：

- 维护跨链交易状态。
- 处理 source/target finality events。
- 生成 protocol actions。
- 处理 timeout/refund/complete。
- 输出 protocol events and control decisions。

它不负责改变底层 chain profile 或 workload。

## 5. Baselines

必须比较：

- `lock_mint_serial`
- `lock_mint_pipeline`
- `fixed_window_baseline`
- `committee_bridge_basic`
- `metaflow_basic`
- `metaflow_afs_fda`

`committee_bridge_basic` 是 baseline，不能写成真实生产委员会桥。

## 6. MetaFlow basic

`metaflow_basic` 是 MetaFlow 的最小协议插件。它应表达 batch / pending / timeout / finality wait 的基本控制，但不要求完整 AFS/FDA。

## 7. AFS / FDA

`metaflow_afs_fda` 包含：

- AFS: adaptive batch-size control。
- FDA: finality/pending-window control。

AFS/FDA 必须输出 `control_decisions.csv`，记录每次 B/D/T 调整的原因、输入指标和结果。

## 8. B / D / T 参数

- `B`: batch size。
- `D`: pending/finality window。
- `T`: timeout。

每次实验必须保存 initial/min/max/static/default values。Adaptation logic 只能在 MetaFlow variants 中变化。

## 9. Pending Finality Pool

Pending Finality Pool 是 MetaFlow 的研究结构。V3.6 可以先实现 research artifact 级别的 pending/finality queue，不需要生产级安全或真实桥托管资产。

## 10. 三种工况

必须覆盖：

```text
steady_load
source_burst
slow_target_finality
```

每种工况都必须保持 source/target ChainProfile and workload arrival sequence fairness。

## 11. Metrics

MetaFlow metrics:

- `throughput_tps`
- `avg_e2e_latency_ms`
- `p95_e2e_latency_ms`
- `p99_e2e_latency_ms`
- `pending_count`
- `max_pending_count`
- `finality_wait_ms`
- `timeout_count`
- `refund_count`
- `success_count`
- `B(t)`
- `D(t)`
- `control_adjustment_count`
- `source_target_imbalance`

## 12. Artifacts

MetaFlow artifacts:

- `used_chain_profile.yaml/json`
- `used_plugin_profile.yaml/json`
- `used_experiment_profile.yaml/json`
- `metaflow_summary.csv/json`
- `metaflow_events.csv`
- `protocol_results.csv`
- `control_decisions.csv`
- `metaflow_vs_baselines_report.md`
- `runtime.log`

## 13. 论文表述口径

必须写清楚：

```text
V3.6 does not need production bridge security.
MintCert / RefundCert / FinalityProof may be structured research artifacts at first.
Real threshold signatures are optional future work.
```

不得声称：

- V3.6 是生产跨链桥。
- `committee_bridge_basic` 是真实委员会桥。
- MetaFlow 已提供经济安全证明。
- V3.6 已连接公网链或多公链部署。
