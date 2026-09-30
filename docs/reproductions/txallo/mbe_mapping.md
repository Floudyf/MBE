# TxAllo to MBE mapping

`txallo_account_sharding` owns one allocator and the frozen account-to-shard mapping. `txallo_routing` and `stateless_txallo_routing` consume that mapping; they do not implement a second allocator.

For canonical datasets, MBE reads the first evaluation record's source-row index and then reads only preceding records from the canonical full trace. The oldest available historical segment initializes G-TxAllo and later historical chunks feed A-TxAllo. The mapping is frozen before the first measured transaction is submitted. Evidence records `history_cutoff_source_row_index` and `future_evaluation_transactions_used=0`.

Current canonical workloads expose sender/receiver identities; those identities are TxAllo accounts. `SchedulingAccessList`, MetaTrack co-access matrices, exact-version producer edges and MetaTrack frontier information are never used by TxAllo.

Stateful TxAllo uses the shared MBE execution/consensus stack. Stateless-TxAllo uses the same frozen mapping but opts into the generic MBE stateless remote projection/writeback substrate. This is a state-substrate adaptation, not a change to G/A-TxAllo.


## 2026-09-28 v8 multi-shard/version contract closure

MBE now emits standard `placement_plan.csv` state-home evidence for this method family. The Stateless adaptation binds an algorithm-agnostic exact predecessor/producer version chain to remote state transport, while the method-specific scheduler/executor remains authoritative. The generic MetaTrack/Stateless-Hash versioned-wave executor is explicitly not used for these methods. This is an MBE stateless/multi-shard integration boundary, not a claim about the original paper.
