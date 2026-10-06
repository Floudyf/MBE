# OptME MBE deviations / adaptations

1. The author implementation targets Ethereum/EVM effects. MBE reuses its deterministic business executor to obtain observed read/write sets and deltas; the OptME scheduling algorithm is kept separate from business semantics.
2. Concurrent effects within one OptME sequence are materialized in deterministic original-index order for MBE receipt/state-root reproducibility. The schedule guarantees those accepted effects do not need a conflicting write order.
3. The author's commented-out third fallback is not enabled. Remaining second-pass invalid transactions are surfaced explicitly as failed receipts rather than silently retried by another algorithm.
4. Stateless-OptME changes only state materialization: AccessList-bounded remote state projection and remote-home writeback. It does not change OptME's conflict graph inputs or scheduling rules.

5. The `OptME` card preserves the author-source OptME scheduling/execution core but is integrated into the common MBE topology: each physical PBFT shard applies that core independently to its own post-consensus block while retaining persistent local state and the existing stateful MBE cross-shard lifecycle. Multi-shard topology is an MBE integration and is not claimed as an OptME paper contribution. `Stateless-OptME` uses the same OptME core but changes the state substrate to AccessList-bounded remote projection/writeback.


## 2026-09-28 v8 multi-shard/version contract closure

MBE now emits standard `placement_plan.csv` state-home evidence for this method family. The Stateless adaptation binds an algorithm-agnostic exact predecessor/producer version chain to remote state transport, while the method-specific scheduler/executor remains authoritative. The generic MetaTrack/Stateless-Hash versioned-wave executor is explicitly not used for these methods. This is an MBE stateless/multi-shard integration boundary, not a claim about the original paper.

## 2026-09-28 v10 method-preserving write-behind closure

The v8/v9 exact-version safety layer exposed one MBE-only latency source that is not part of OptME: when an execution shard had already durably committed an exact predecessor that it produced itself, its next block still waited for the persistent Home shard to materialize the same version before admission/execution could continue. v10 removes only that self-wait. The producing execution shard keeps a reconstructible source-local exact-version cache after durable commit and may reuse that exact version locally while the authoritative Home write proceeds through the existing state-home path.

This does **not** remove cross-execution-shard Home readiness, does not change OptME pre-execution/KDG/early-abort/reordering, and does not route OptME through MetaTrack StateReady or the generic versioned-wave executor. Home persistence remains PBFT-bound. The v10-specific Home queue additionally requires the declared `PreviousVersion` to be materialized before a later `ProducedVersion` can enter a Home PBFT block; legacy/MetaTrack `versioned_remote_home` behavior is unchanged.

## 2026-09-29 v14 receipt-visible exact-version closure

v13 still imposed one MBE-specific barrier on Stateless-OptME: a remote Home replica could receive a valid immutable exact version but withheld that version from exact-version readers until a later Home PBFT block durably materialized it. The original OptME algorithm does not define this extra Home durability barrier. v14 therefore separates **exact-version visibility** from **persistent Home materialization**.

For the OptME/TxAllo-specific stateless transport only, Home now publishes a received version as soon as its declared `PreviousVersion` is already the contiguous visible predecessor. A future version remains queued and invisible until the missing predecessor is received. Home PBFT materialization is unchanged and still requires the `PreviousVersion -> ProducedVersion` chain to be continuous against the durable materialized frontier. Thus v14 removes only the MBE-added durable-before-visible delay; it does not relax exact-version identity, does not read `latest`, and does not change OptME pre-execution/KDG/early-abort/reordering.

## 2026-10-02 v22 global-order projection closure

The formal `OptME` and `Stateless-OptME` cards now use one `optme-global` PBFT ordering domain. `topology.shards` denotes logical state partitions. The author-source OptME core is unchanged.

For Stateless-OptME, client/workload source order is no longer encoded as transaction-level `RequiredVersion` / `ProducedVersion`; OptME-specific pre-consensus version admission and the v8/v10/v14/v21 transaction-version publication path are superseded. Every block executes from the unique H-1 state: each Home partition serves one immutable block-start snapshot/root and every signed AccessList key in that block is projected exactly once. After OptME produces the deterministic final block delta, every validator materializes only keys owned by its logical Home partition; there is no second transaction-level remote writeback protocol.

The older v8/v10/v14/v21 sections below/above remain historical documentation for the superseded MBE multi-PBFT adaptation and for the still-frozen TxAllo transport where applicable. They are not the v22 OptME execution contract.
