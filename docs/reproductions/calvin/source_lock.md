# Calvin dual reproduction source lock — V2

## Normative literature source

Alexander Thomson, Thaddeus Diamond, Shu-Chun Weng, Kun Ren, Philip Shao, Daniel J. Abadi. **Calvin: Fast Distributed Transactions for Partitioned Database Systems.** SIGMOD 2012.

The paper semantics are normative for the MBE `stateful_calvin` literature baseline.

## Public author implementation used as implementation evidence

Repository: `https://github.com/yaledb/calvin`
Pinned public commit inspected: `c1fb1a51584bb150a1c1e493469bd2d7ac0ff40a`.

The public repository README says most code corresponds to the later VLDB 2014 deterministic-database evaluation. MBE therefore uses the public source to corroborate implementation behavior, while the SIGMOD 2012 paper controls the literature baseline semantics.

Inspected public paths include `src_calvin/sequencer`, `src_calvin/scheduler/deterministic_lock_manager.*`, `src_calvin/scheduler/deterministic_scheduler.cc`, `src_calvin/backend/storage_manager.cc`, `src_calvin/proto/txn.proto` and `src_calvin/proto/message.proto`.

## Stateful Calvin fidelity boundary

`stateful_calvin` retains:

- one deterministic global transaction order;
- complete declared read/write access before execution;
- per-key FIFO shared/exclusive lock queues;
- all local lock requests submitted in global transaction order;
- a transaction executes only after all required local locks are granted;
- concurrent worker execution after lock acquisition;
- passive/read participants performing local reads and forwarding `READ_RESULT` values to execution/write participants;
- writer/active participants waiting for all required read values;
- deterministic transaction logic on every active/write partition and local-write-only materialization;
- no ordinary conflict abort/retry;
- no traditional two-phase commit.

Pure `write` is implemented as an X lock even though the public research code comments out part of the benchmark-specific pure-write release path. This follows the paper's general deterministic-locking semantics rather than copying that benchmark specialization.

## MBE adaptations (must not be described as original Calvin deployment)

1. **Common consensus layer.** MBE PBFT replaces Calvin's original replicated sequencer/Paxos path. PBFT determines one global serial order; Calvin owns deterministic locking and participant execution after that order is committed.
2. **Consensus identity vs state identity.** All Calvin validators use one `calvin-global` PBFT ordering domain, while persistent state is partitioned by `ExecutionShardID` (`s0`, `s1`, ...).
3. **Client ingress mapping.** Routing preserves the workload/logical execution shard (`s0`, `s1`, ...) instead of collapsing everything to the first shard. Only at submission is that logical route mapped to the single `calvin-global` consensus-domain leader.
4. **Replica-deduplicated READ_RESULT transport.** The deterministic leader of a state/execution partition sends the logical `READ_RESULT`; all replicas of destination execution partitions receive it and deterministically execute. This prevents PBFT replication from multiplying algorithmic message counts.
5. **Terminal-outcome harmonization.** Original Calvin's active participants deterministically compute the same result, while passive participants do not execute the stored procedure. MBE requires one terminal receipt per transaction on every validator. V2 therefore broadcasts a small `CALVIN_TX_OUTCOME_V2` from the deterministic outcome-partition leader so passive/nonparticipant replicas record the same success/error. This is an **MBE receipt/lifecycle adaptation**, not a Calvin paper protocol message, and is measured separately from `READ_RESULT` traffic.
6. **Read-only lifecycle ownership.** A pure read transaction has no writer/active participant in original terminology. For MBE receipt production, V2 chooses the first deterministic read-home partition as its execution/outcome home. Locks and state remain read-only.
7. **Block boundary.** The public Calvin scheduler can lock ahead over incoming batches. MBE's committed-block runtime executes PBFT blocks sequentially, so `calvin_cross_block_lock_ahead=false` and `calvin_mbe_batch_boundary_serialization=true`. This is a conservative performance limitation, not a semantic optimization.
8. **Fault experiments.** Multi-partition node kill/restart/drop experiments are blocked in V2 until participant-result replay and deterministic leader failover are reproduced. V2 does not fake failure recovery with arbitrary transaction aborts.
9. **Global PBFT payload dissemination.** To preserve the common MBE PBFT substrate, the globally ordered block body is replicated to all Calvin validators before participant filtering. Original Calvin can disseminate participant transaction batches more selectively after sequencing. Therefore MBE network totals include this common-PBFT adaptation overhead and must not be presented as the original Calvin sequencer/network cost.

## Stateless Calvin adaptation

`stateless_calvin` is a compatibility/adaptation baseline, not an original-paper reproduction. It retains the same global order, FIFO S/X lock manager, participant mapping, `READ_RESULT` exchange and deterministic worker semantics as `stateful_calvin`.

Only transaction-visible state is reduced to the signed AccessList projection. The MBE state-home processes still persist their partition state. Therefore V2 **does not claim a zero-state-node architecture** or separate physical storage-server processes. Its precise description is: **stateless transaction execution over persistent partition-home storage**.

It must not use MetaTrack-specific `SchedulingAccessList`, TopoSafe, StateReady, SemanticSafe, version-frontier/CAS, dual-track scheduling or commutative aggregation.

## V2 correctness closures over V1

V2 fixes the following V1 defects:

- initial lock grants no longer mark transactions `enqueued` before `initialReady()` actually places them in the ready queue (the V1 behavior could stall the first runnable transactions);
- AccessList canonicalization is fail-closed: empty keys, duplicate keys and unknown access modes are rejected even if admission is bypassed by recovery/direct paths;
- `READ_RESULT` carries an explicit key list and is accepted only when its key/value set exactly matches the declared read keys for that source partition;
- pure read-only transactions have a deterministic execution/outcome participant instead of becoming synthetic successes;
- passive/nonparticipant synthetic receipts are replaced by the canonical active outcome, so a business failure cannot be recorded as success on another partition;
- the previous fixed 2-second remote-read timeout is removed from algorithm semantics; `read_result_timeout_ms=0` and `outcome_timeout_ms=0` mean context/liveness-driven waiting. Finite values are opt-in diagnostics only;
- Calvin's finality mode is wired through the supervisor's direct-durable-commit path instead of accidentally falling back to legacy cross-shard finality classification;
- state-root consistency is grouped by `ExecutionShardID`, not by the shared `calvin-global` PBFT domain;
- `real_cross_shard_network` is based on actual Calvin `READ_RESULT`/outcome transport rather than merely having multiple logical partitions;
- the plan digest binds a deterministic state-home mapping digest;
- multi-partition transaction counts are owned by one deterministic outcome partition instead of being multiplied by every participant partition;
- lock-wait metrics distinguish waiting transactions from blocked lock requests;
- ready-queue depth measures actual queued work rather than the size of a single wakeup batch;
- the legacy `v5_cross:` Relay/Finalize payload shortcut is suppressed during Calvin business execution so the legacy MBE cross-shard protocol cannot silently leak back into Calvin;
- client ingress maps the preserved logical route to `calvin-global` only at submission, fixing the V1 `no leader for s0` failure and preventing workload collapse to the first execution shard;
- explicit qualified keys such as `s1::asset:7` retain their declared execution/state-home prefix when that prefix names a configured Calvin execution shard; they are not hashed a second time into another partition;
- remote `READ_RESULT` waiting is separated from bounded business-execution worker slots. A transaction waiting for remote reads may suspend without consuming the scarce `worker_count` execution capacity, preventing cross-partition thread-pool starvation that can otherwise mimic a Calvin deadlock;
- algorithmic and physical message counts are separated: `calvin_read_result_message_count` counts logical reader-partition to writer-partition messages, while `calvin_read_result_physical_message_count` records actual TCP fan-out to PBFT replicas. The same distinction is retained for the MBE-only outcome broadcast.

## Access-mode mapping

- `read` -> shared/S lock
- `write` -> exclusive/X lock
- `read_write` -> exclusive/X lock
- `commutative_delta` -> exclusive/X lock; the delta may still read its previous value as required by MBE business semantics, but Calvin performs no commutative aggregation.

## Metric truth notes

Paper-protocol metrics and MBE-adaptation metrics are kept separate:

- `calvin_read_result_message_count`: logical Calvin reader-partition -> writer-partition `READ_RESULT` messages;
- `calvin_read_result_physical_message_count`: actual `READ_RESULT` TCP unicasts after PBFT replica fan-out;
- `calvin_outcome_message_count`: logical MBE receipt/lifecycle harmonization broadcasts;
- `calvin_outcome_physical_message_count`: actual outcome TCP unicasts after replica fan-out;
- `calvin_waiting_transaction_count`: transactions blocked on at least one local lock;
- `calvin_blocked_lock_request_count`: number of individual lock requests initially unavailable;
- `calvin_multi_partition_tx_count`: logical multi-partition transactions, owned once by the outcome partition;
- `maximum_parallel_width`: simultaneous **business transaction executions** subject to `worker_count`; remote-read coordination waits do not consume those business worker slots;
- `calvin_state_home_mapping_digest`: binding of declared keys to execution/state partitions;
- `calvin_stateless_full_partition_visible_to_transaction=false`: explicit truth boundary for the stateless adaptation.

## MBE method IDs

- `stateful_calvin` — formal literature baseline, frontend title **Calvin**
- `stateless_calvin` — compatibility baseline, frontend title **Stateless Calvin**


## MBE v33 Stateless Calvin remote-state truth boundary

`stateless_calvin` remains an explicit MBE compatibility/adaptation baseline, not original Calvin. It retains Calvin global deterministic ordering and FIFO S/X locking, but the transaction execution shard no longer receives a resident partition snapshot. After all Calvin locks for a transaction are granted, it fetches each required signed-AccessList value from the deterministic state-home partition through the generic physical state-access transport. Remote writes are sent by the deterministic execution-shard leader to all replicas of the state-home partition and are materialized in the same already globally ordered block.

This path deliberately does **not** enable MetaTrack native `StateReady`, TopoSafe, SemanticSafe, multi-frontier, dual-track classification, or MetaTrack stateless-version admission. Exact predecessor/produced version metadata is used only to preserve Calvin's deterministic per-key order across physical state-home fetch/writeback. Partition-home storage remains persistent; the adaptation is not claimed to be a storage-free network.

## MBE v34 consensus-bound Stateless Calvin version plan

V34 closes a correctness defect in the v33 compatibility adaptation. V33 generated exact predecessor/producer versions at client submission time from workload/source order. That order is not the authoritative Calvin serialization order once transactions have entered the mempool and a PBFT block has been formed. A transaction could therefore acquire locks according to the globally ordered block while fetching a predecessor value derived from a different client-side order.

V34 moves that version dependency construction into `calvinScheduler.PlanBlock`, which implements MBE's existing `ConsensusExecutionPlanner` interface. The primary deterministically derives a `calvin_consensus_version_plan_v1` from the final candidate block transaction order before PRE-PREPARE. The execution-plan envelope is part of the block hash and every backup recomputes and verifies the same plan before accepting the proposal. This is an **MBE stateless-state transport adaptation**; original Calvin does not define these remote state-home version tokens.

The version semantics are deliberately block-scoped:

- the first access to a key in a committed Calvin block has `RequiredVersion=0`, meaning **the current durable value at that state home's block boundary**, not a genesis value;
- a later access in the same block depends on the exact `ProducedVersion` of the preceding writer in the PBFT-bound Calvin order;
- writer versions are `(blockHeight << 32) | (blockLocalIndex + 1)`, so produced-version identifiers are deterministic and monotonic across blocks without maintaining a second long-lived client-side frontier;
- a declared writer that fails or produces no physical value still publishes an ordering no-op version carrying its predecessor value, so a later transaction cannot wait forever on an unpublished version;
- client `ExecutionRouting.StateVersions` is empty for Stateless Calvin. The signed AccessList remains the state declaration; exact version metadata is internal consensus-bound execution evidence.

V34 does not add MetaTrack StateReady, TopoSafe, SemanticSafe, multi-frontier, dual-track classification, commutative aggregation, traditional 2PC, or a second ordering protocol. Calvin's deterministic FIFO S/X locking remains unchanged.
