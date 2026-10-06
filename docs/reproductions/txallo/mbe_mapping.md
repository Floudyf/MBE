# TxAllo to MBE mapping

`txallo_account_sharding` owns one allocator and the frozen account-to-shard mapping. `txallo_routing` and `stateless_txallo_routing` consume that mapping; they do not implement a second allocator.

For canonical datasets, MBE reads the first evaluation record's source-row index and then reads only preceding records from the canonical full trace. In the measured `paper_g_snapshot` mode, the complete selected pre-evaluation history window feeds one G-TxAllo run; it is not split into arbitrary record chunks and mislabeled as A-TxAllo. The mapping is frozen before the first measured transaction is submitted. Evidence records `history_cutoff_source_row_index`, `allocation_mode`, `louvain_level_count`, `mapping_complete`, and `future_evaluation_transactions_used=0`.

Current canonical workloads expose sender/receiver identities; those identities are TxAllo accounts. `SchedulingAccessList`, MetaTrack co-access matrices, exact-version producer edges and MetaTrack frontier information are never used by TxAllo.

Stateful TxAllo uses the shared MBE execution/consensus stack. Stateless-TxAllo uses the same frozen mapping but opts into the generic MBE stateless remote projection/writeback substrate. This is a state-substrate adaptation, not a change to G/A-TxAllo.


## 2026-09-28 v8 multi-shard/version contract closure

MBE now emits standard `placement_plan.csv` state-home evidence for this method family. The Stateless adaptation binds an algorithm-agnostic exact predecessor/producer version chain to remote state transport, while the method-specific scheduler/executor remains authoritative. The generic MetaTrack/Stateless-Hash versioned-wave executor is explicitly not used for these methods. This is an MBE stateless/multi-shard integration boundary, not a claim about the original paper.

### Node/runtime mapping identity

The client and every `mbe-node` instantiate the same `txallo_account_sharding` plugin and independently bootstrap it from the same pre-evaluation canonical history and sorted execution-shard set. This is deterministic reconstruction, not a new TxAllo mapping-consensus protocol. It is required because runtime StateHome resolution also consults the sharding plugin. PBFT is unchanged.


### v20.2 placement/relay closure

- Workload `v5_cross:*` wrappers are annotations only; TxAllo strips them and rebuilds physical cross-shard wrappers solely from its own frozen placement.
- Stateful TxAllo keeps `relay_certificate_protocol` only for TxAllo-cross transactions.
- Stateless-TxAllo composes `cross_shard:txallo_no_relay`: one business execution owner, remote shards serve exact-version state only.
- Placement evidence exports canonical logical accounts plus `history_mapping`/`fallback_hash`, per-account shards, involved shards and mapping digest.
- Evaluation-only accounts may use deterministic fallback without becoming a fidelity failure; they still never train the same measured window.


## v20.3 evidence-only closure

Adds logical-id execution join evidence, post-oracle v18 recomputation, a fail-closed Stateless-TxAllo multi-shard exact-version serial replay, and preservation of exact-version writeback identity fields. TxAllo core, PBFT, routing/allocation, worker count, block production and exact-version admission behavior are unchanged.


## v20.4 adapted-baseline evidence completion

Stateless-TxAllo now captures the actual business-state projection immediately after StateDB.Open and before runtime work begins; the multi-shard replay starts from that evidence instead of requiring the whole physical state root to be empty. Stateless-TxAllo also writes its existing remote_state_access rows with full exact-version identity so the supervisor/v18 five-tuple dedup can be proved. These are evidence-only adaptations: TxAllo allocation/routing, PBFT, serial execution, worker count, exact-version admission/readiness/fetch/writeback behavior, and block production remain unchanged.
