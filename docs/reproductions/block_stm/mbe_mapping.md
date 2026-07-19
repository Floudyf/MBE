# Block-STM MBE Mapping

Status: mapping before implementation.

## Existing MBE Interfaces

- `v5.BlockExecutorPlugin` is the runtime extension point.
- `BlockExecutionInput` provides the block, immutable base snapshot, node ID, shard ID, and worker count.
- `serial_block_executor` is the legacy-faithful oracle.
- `execution.TxDelta` records read observations, writes, receipt, success, and error.
- Deterministic state apply happens after block execution and before durable commit.

## MBE Transaction Semantics

The first Block-STM implementation must use the same transfer semantics as Serial:

- ensure sender account;
- ensure receiver account;
- read sender nonce;
- reject nonce mismatch;
- reject invalid value;
- read sender balance;
- reject insufficient balance;
- write sender balance;
- write receiver balance;
- write sender nonce.

Failed transactions must preserve legacy Serial side effects, including account initialization, unless a later separate semantic-change stage explicitly changes the oracle.

## State View

Block-STM workers must read from:

```text
immutable base snapshot
+ visible lower-index committed/speculative MVMemory versions
```

Workers must not read or write global `state.DB` directly.

## Output

The final Block-STM output must fit the existing block executor result contract:

- ordered receipts;
- ordered `TxDelta`;
- final state delta;
- receipt root;
- state root before and after;
- plan digest;
- Block-STM metrics and artifacts.
