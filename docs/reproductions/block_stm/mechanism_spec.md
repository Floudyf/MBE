# Block-STM Mechanism Spec

Status: mechanism specification before implementation.

Block-STM executes a block with a preset transaction order while using speculative parallel execution. The committed result must be equivalent to serial execution in that preset order.

## Required Mechanisms

- Preset order: every transaction has a stable transaction index in the block.
- Transaction version: each execution attempt is identified by `(transaction_index, incarnation)`.
- MVMemory: each key stores versioned writes by transaction version plus estimate markers for invalidated writes.
- Speculative execution: workers execute transactions before all lower-index dependencies are finalized.
- Captured reads: each transaction records every key and version observed during execution.
- Write registration: successful speculative output registers a write set under the transaction version.
- Validation: captured reads are checked against the current latest visible versions from lower transaction indexes.
- Abort: validation failure discards the speculative output for that transaction version.
- Re-execution: aborted transactions increment incarnation and are rescheduled.
- ESTIMATE: invalidated writes remain visible as estimates so dependent transactions can wait instead of reading an impossible stable value.
- Dependency registration: a transaction that reads an estimate or unresolved dependency records the dependency and waits.
- Execution task: performs transaction execution, captures reads, and registers writes.
- Validation task: validates captured reads and either commits the output as valid or aborts it.
- Worker scheduler: workers collaborate over execution and validation tasks until all transaction indexes are finalized.
- Ordered output: final receipts, deltas, and state writes are emitted in preset transaction order.
- Deterministic apply: final write sets are applied to MBE state in transaction-index order through the existing deterministic apply path.

## Completion Rule

The executor completes only when every transaction index has a final status. Final state, receipt root, and state root must equal `serial_block_executor` for the same base snapshot and ordered block.

## Non-Mechanisms

The following are not Block-STM and must not be used as substitutes:

- static conflict graph execution;
- pre-grouping only by declared access sets;
- StateKeys-only conflict detection;
- graph coloring;
- wave batching without MVMemory and validation;
- ordinary optimistic locking over the global DB;
- worker goroutines that directly mutate `state.DB`.
