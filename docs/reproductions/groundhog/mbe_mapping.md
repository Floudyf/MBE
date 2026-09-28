# Groundhog mapping to MBE

<!-- MBE_V37_GROUNDHOG_TRUTH -->

Groundhog 在 MBE 中保持独立的类型化可交换状态语义：所有固定区块交易读取同一个区块开始快照，产生类型化修改，然后进行交易原子的预留/回退检查和确定性物化。

正式组合：

```text
routing        = hash_routing_baseline
block_producer = groundhog_block_producer
execution      = serial_execution_baseline   # 仅作 MBE 分类接口
scheduler      = fifo_serial_scheduler       # 不承载 Groundhog 业务执行
block_executor = groundhog_block_executor
commit         = normal_commit
```

`groundhog_block_producer` 当前扫描**交易池中整个已就绪候选集合**，直到从该有限候选快照中选择满足区块容量和类型化约束的交易。旧的 `candidate_scan_multiplier` 不再控制正式算法，v37 不再在正式方法配置中生成该参数；清单仍接受旧配置以兼容历史保存方案，但运行时忽略它。

候选阶段的共识证据保证负载身份、区块身份和选中交易集合完整性；真正的 Groundhog 类型化状态约束仍由验证节点的固定区块执行重新检查。不得把该证据描述成“已经在共识前完整证明所有状态执行结果”。

Groundhog 当前正式比较边界仍是分片本地交易（`cross_shard_ratio = 0`）。它的类型化可交换状态语义与按顺序更新状态的 Serial/Block-STM 并不要求产生相同逐交易中间状态，因此不能伪造串行等价结论。
