# TxAllo MBE deviations / adaptations

1. The paper specifies transaction/account allocation, not a new smart-contract executor. MBE therefore keeps the common Serial execution, PBFT, network and commit modules.
2. The paper does not define a separate TxAllo state-migration protocol. MBE does not invent one. Account mapping changes and physical state-home traffic are reported separately.
3. For measured-window accounts absent from the pre-window history, MBE uses a deterministic account-hash cold-start shard until future committed history could inform a later mapping. The measured transaction itself is never used to choose its own allocation.
4. If no pre-evaluation history exists (for example a trace starts at source row 0 or a synthetic run), evidence marks `synthetic_cold_start_no_future_history`; the mapping starts empty and deterministic cold-start routing is used. No future evaluation rows are sampled.
5. The public TxAllo repository contained no usable Algorithms 1/2 implementation at review time; this is a paper reimplementation, not an official-source port.


## 2026-09-28 v8 multi-shard/version contract closure

MBE now emits standard `placement_plan.csv` state-home evidence for this method family. The Stateless adaptation binds an algorithm-agnostic exact predecessor/producer version chain to remote state transport, while the method-specific scheduler/executor remains authoritative. The generic MetaTrack/Stateless-Hash versioned-wave executor is explicitly not used for these methods. This is an MBE stateless/multi-shard integration boundary, not a claim about the original paper.

## 2026-09-28 v10 method-preserving write-behind closure

The v8/v9 exact-version safety layer exposed one MBE-only latency source that is not part of TxAllo: a shard that had already durably committed an exact predecessor it produced itself still waited for the persistent Home shard to materialize the same version before its own next block could proceed. v10 removes only that self-wait by keeping a reconstructible source-local exact-version cache after durable commit while authoritative Home persistence continues asynchronously through the existing state-home protocol.

TxAllo's historical account graph, allocation objective, frozen measured-window mapping, FIFO/Serial execution choice, and cross-shard behavior are unchanged. Other execution shards still wait for Home. The v10-specific Home queue releases versioned writes only when `PreviousVersion` matches the materialized Home version; legacy/MetaTrack `versioned_remote_home` behavior is unchanged.

## 2026-09-29 v14 receipt-visible exact-version closure

v13 still imposed an MBE-specific durable-before-visible barrier on Stateless-TxAllo: a valid exact version received by Home remained unreadable until Home PBFT later materialized it. TxAllo itself specifies allocation/routing rather than such a state-version durability barrier. v14 separates exact-version visibility from persistent Home materialization while preserving the TxAllo allocation and FIFO/Serial execution choices.

For the OptME/TxAllo-specific stateless transport only, Home publishes a received version once its declared `PreviousVersion` is the contiguous visible predecessor. Out-of-order future versions stay queued until their predecessor arrives. Durable Home state still advances only through the existing PBFT-bound materialization path and still checks continuity against the materialized frontier. No `latest` substitution, no MetaTrack scheduler, and no change to the historical TxAllo account-allocation algorithm is introduced.
