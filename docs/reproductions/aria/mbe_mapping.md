# Aria MBE Mapping

## Plugin boundary

Aria is implemented as one V5 `block_executor` plugin:

- plugin ID: `aria_block_executor`;
- input: consensus block, immutable base state snapshot, worker count;
- output: receipts, observed read/write sets, deterministic state delta, plan
  digest, epoch trace, and Aria metrics.

Routing, PBFT, TCP networking, state storage, WAL, durable commit, and workload
replay remain the common MBE implementations.

## Reused transaction semantics

The executor reuses the existing `SerialExecutor.executeTx` and transaction
overlay from the same Go package. Serial and Aria therefore execute the same MBE
business operations; only visibility, validation, and retry policy differ.

## Blockchain adaptation

Original Aria places aborted transactions at the beginning of a later external
batch. A committed MBE block must produce exactly one final receipt per included
transaction. Therefore an MBE block is the initial Aria batch and conflict-aborted
transactions enter an internal next epoch before the block returns its final
state delta.

A future sender nonce is treated as a retryable snapshot dependency when a lower
nonce transaction in the block can advance the account. A permanently missing
predecessor causes deterministic no-progress failure.

This adaptation is reported as
`aria_rule2_core_reimplementation_with_mbe_internal_epoch_retry`; it is not a
claim that MBE reproduces Aria's distributed database partitioning protocol.
