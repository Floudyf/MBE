# V5 Block Execution Foundation and Serial Equivalence Closure

Status: design and implementation stage for an internal V5.2 block-execution extension point.

This is not V5.3, V5.2.1, V6, Block-STM, CG, ACG, BSX, Batch-SI, SmallBank, Geth TxPool, OptChain, HotStuff, or S-BAC. The scope is a generic block executor interface plus a legacy-faithful Serial implementation that can become the oracle for later block execution mechanisms.

## Start Point

- Branch at stage start: `main`
- Start commit: `84f28f43f2cfe93864fd4e4b68377fcfd31a5595`
- Reference label: `legacy_faithful_reference_baseline`

## Current Execution Path Audit

The V5 `real_cluster` path currently commits a block through `NodeRuntime.commitOnce`:

1. Validate commit height and previous block hash.
2. Take storage and state checkpoints.
3. Call the fixed legacy `execution.Engine.ExecuteBlock(block, db)`.
4. Durable-commit the returned `execution.Result`.
5. Save state DB.
6. Record plugin decision logs.
7. Commit reserved mempool transactions and confirm the proposer.

The legacy execution engine mutates `state.DB` directly while iterating transactions. It does not execute against a transaction-local overlay and it does not return a minimal write set. The returned `StateUpdates` is the full state snapshot after execution.

The current V5 `execution` plugin category is not a block execution engine. It only classifies a transaction into the existing MetaTrack-style track semantics such as serial, fast, or conservative. The current V5 `scheduler` plugin category only orders transactions. Both categories are retained with their historical meaning.

Plugin decision logs are currently emitted after state execution and durable commit. They are observational evidence for existing plugin categories, not the execution path used to mutate state.

## State DB Capability Audit

The current state DB already provides:

- `Snapshot`
- `Restore`
- `ApplyBatch`
- `Root`
- `Checkpoint`
- `Rollback`
- persistent `Save`

It does not yet expose a typed immutable snapshot, transaction-local overlay, deterministic ordered batch apply helper, or root-of-snapshot helper as a first-class block execution model. This stage adds those concepts without changing PBFT-style consensus, TCP transport, cross-shard protocol, durable-commit semantics, or transaction signature/ID rules.

## Legacy Receipt and Root Semantics

Legacy receipt semantics:

- Receipts are emitted in block transaction order.
- `ReceiptRoot` is the SHA-256 digest of JSON-marshaled ordered receipts.
- `StateRootAfterTx` is calculated after each transaction's legacy state effects.
- Failed transactions can still leave account initialization side effects because `ensureAccount` runs before validation.
- Successful transfer updates `balance:sender`, `balance:receiver`, and `nonce:sender`.
- The receiver nonce is not incremented by a successful transfer, but a missing receiver nonce key can be initialized by `ensureAccount`.

Block hash coverage is unchanged in this stage. The current block hash covers shard, height, previous hash, proposer, timestamp, tx IDs, tx root, state root before, state root after, and receipt root as populated by the current proposer path. The new execution plan digest is not added to the block hash in this stage.

Signed transaction public JSON, TxID, and signature semantics are unchanged. TxID and signature continue to cover the transaction core fields, including sender, receiver, nonce, value, state keys, payload, timestamp, public key, source kind, and trace source ID.

## New Extension Point

This stage introduces a new independent plugin category:

```text
block_executor
```

The first implementation is:

```text
plugin_id: serial_block_executor
category: block_executor
version: 1.0.0
truth_boundary: legacy_faithful_reference_baseline
supported_backends:
- real_cluster
```

The runtime must instantiate this through the V5 plugin factory registry and dependency injection. The runtime main path must not dispatch behavior through a large plugin-ID branch.

## Access Sets

`StateKeys` remain a transaction payload field used by workload generation, routing evidence, and legacy receipt compatibility. They are not treated as the complete execution read/write set.

`DeclaredAccessSet` is a conservative set available before execution. It can be used by later schedulers or analyzers but must not be presented as observed execution.

`ObservedAccessSet` is produced by the execution overlay while a transaction actually reads or writes state. It is the source of per-transaction execution trace evidence.

## Serial Execution Model

The serial block executor follows:

```text
immutable base snapshot
-> deterministic working state
-> block-order transaction loop
-> transaction-local overlay
-> observed reads and writes
-> TxDelta and receipt
-> apply legacy-compatible state effects to working state
-> final deterministic state delta
-> deterministic batch apply to state.DB
-> DurableCommit
-> db.Save
```

Serial uses one worker. The public model still includes worker count and plan metadata so future mechanisms can reuse the same input/result contract.

## Execution Plan Digest

The serial execution plan contains:

- engine id
- engine version
- block hash
- block height
- ordered transaction IDs
- original transaction indexes
- declared access-set digest
- worker count
- plan digest

The digest is computed from canonical, stable, sorted JSON content. It is written to runtime artifacts and summaries for consistency checks. It is not part of consensus or block hash semantics in this stage.

## Equivalence Rule

The new Serial executor must match the legacy execution engine for the same genesis state, block, and ordered transaction list:

- per-transaction success
- per-transaction error
- execution cost
- receipt state keys
- state root after each transaction
- receipt order
- receipt root
- success and failure counts
- final state map
- final state root
- state updates
- nonce and balance state

If a legacy semantic is surprising, this stage preserves it and records it. It does not repair old semantics under the cover of introducing the block executor interface.

## Real-Cluster Drain Budget

The synthetic 10K Serial acceptance exposed runtime closure issues rather than
Serial equivalence issues. Both failing runs submitted and admitted all 10,000
transactions, but the original fixed 5 second proposal timeout could release a
still-progressing proposal under 100-transaction blocks. Later diagnosis showed
two additional closure hazards:

- node processes still used the original experiment `duration_ms` as their own
  shutdown deadline even after the supervisor switched to a workload-aware
  drain budget;
- status export held the runtime mutex while reading mempool state, creating a
  possible lock-order inversion with admission/lifecycle recording and commit
  artifact recording.

The closure rule remains strict: `submitted == terminal`, `incomplete == 0`,
`pending_commit == 0`, `pending_relay == 0`, `reserved == 0`,
`proposal_in_flight == false`, root consistency must pass, and no fallback is
allowed. The fix is a workload-aware budget, not a lowered acceptance bar:

- proposal timeout is derived from block size and block interval, with a lower
  bound and an absolute cap;
- supervisor drain timeout is estimated from transaction count, block size,
  block interval, and cross-shard lifecycle work;
- node process runtime duration is extended from the same drain budget so a
  node cannot self-stop before drain closure;
- a no-progress watchdog still fails stalled runs, but it uses a multi-block
  workload window so an in-flight quorum commit is not killed by a single tight
  constant;
- an absolute hard cap prevents unbounded waiting.
- runtime status export snapshots runtime fields before reading mempool fields,
  avoiding runtime-mutex -> mempool-lock inversion.

On the current 10K stress profile, the formal block producer configuration is
`block_size=100` and `interval_ms=75`. The expected timeout model therefore
waits long enough for valid delayed quorum messages and durable commits while
still treating a true progress stall as failure.

## Non-Goals

This stage does not implement any external paper algorithm, parallel validation, speculative execution, MVMemory, incarnation, estimates, re-execution, or consensus-bound execution plans.
