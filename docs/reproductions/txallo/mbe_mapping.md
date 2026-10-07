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


## v22.0 dynamic G/A lifecycle

The initial `paper_g_ratio_snapshot` name is retained as the initial-G history-selection contract for archived compatibility. It no longer means that the evaluation mapping is frozen for the whole run. The client groups the ordered canonical replay by one-hour source timestamps, submits one source-time epoch under a single mapping, waits for real successful terminal commit, and only then invokes A-TxAllo with those committed transactions. Pending/submitted transactions are never training data.

The paper defines periodic A/G gaps `tau1 < tau2` and its hybrid running-time case study uses adaptive/global gaps 1/20 time steps, with A-TxAllo updated hourly. MBE therefore defaults to `a_epoch_seconds=3600` and `g_epoch_multiple=20`; every twentieth closed source-time epoch runs a complete G-TxAllo rebuild over the initial history plus all successfully committed evaluation history so far. Both values are explicit runtime evidence.

The client is the deterministic mapping publisher. Validators independently reconstruct epoch 0 from the immutable pre-evaluation sidecar, then install only monotonic, digest-verified mapping snapshots before admitting the next epoch's transactions. This is an MBE lifecycle transport for the paper allocation result, not a new mapping consensus protocol. PBFT is unchanged.


## v22.2 source-block dynamic closure

v22.2 corrects the v22.0 epoch clock. Alien Worlds Layered V2 intentionally stores `timestamp_ms=source_order` with `timestamp_semantics=deterministic_source_order_only`; that field is not wall-clock time and must not drive TxAllo. The compiler now derives a small immutable `txallo_dynamic_blocks.jsonl.gz` from the reviewed raw Alien Worlds source and binds each evaluation record to its original WAX `block_num`.

A-TxAllo uses exact source-height windows `[B0,B0+299]`, `[B0+300,B0+599]`, ... . A non-empty epoch is fed to Algorithm 2 only after every logical transaction in that epoch reaches the required successful terminal state. Empty source epochs advance the paper time-step clock without fabricating A input. The final partial epoch is not fed back because no later measured transaction may learn from it.

The paper case-study global cadence remains one G-TxAllo refresh every 20 source-block epochs. Client publication uses a monotonically increasing mapping epoch and digest-bound snapshot. Every node runs an out-of-band TxAllo mapping watcher and writes `txallo_mapping_ack.json`; the client will not submit the next epoch until every node acknowledges the exact state digest. PBFT is unchanged.

Stateless-TxAllo can consume a changed execution mapping because its persistent state home remains the existing remote-state substrate. TxAllo Section VII does not require the TxAllo algorithm itself to add a migration protocol: fully replicated-state systems can apply it directly, and the paper's sharded-state integration assumes a virtual global ledger from periodic reshuffling/state dissemination. MBE Stateful-TxAllo, however, currently uses `persistent_local_state_store` rather than either of those paper substrates. Therefore v22.2 conservatively fail-closes before publication if a committed account's effective home would change (including a newly learned account that already committed under fallback placement). Stateless-TxAllo can change execution mapping because its persistent home remains the existing remote-state substrate. This guard is an MBE substrate correctness boundary, not a claim that TxAllo itself mandates migration.


## v22.6 MBE-adapted dynamic cadence

The TxAllo paper evaluation used `tau1=300` source blocks. MBE formal comparison runs use an explicit adapted profile with `tau1=15` Alien Worlds source blocks. The original paper value is retained as machine-readable evidence (`paper_reference_a_epoch_blocks=300`), while the effective runtime value is `a_epoch_blocks=15` and `a_epoch_parameterization=mbe_adapted_fixed_15_source_blocks`. This is an experimental adaptation, not a claim that the TxAllo paper used 15 blocks.

The external source-block clock is retained so Stateful-TxAllo and Stateless-TxAllo see identical update boundaries even when their physical MBE committed-block counts differ. The paper case-study ratio `tau2/tau1=20` is preserved, so periodic G-TxAllo occurs every 20 closed A epochs, i.e. every 300 source blocks under this profile. Commit-only A input, future-leakage prevention, mapping ACK, Stateful migration fail-closed behavior, PBFT, and the frozen TxAllo G/A core are unchanged.
