# Block-STM Source Code Mapping

Status: initial source mapping. Update this file when MBE implementation types land.

| Paper mechanism | Aptos source anchor | MBE target |
| --- | --- | --- |
| Preset transaction order | `block-executor/src/executor.rs` block execution over transaction indexes | `BlockExecutionInput.Block.TxList` |
| Transaction index | `aptos_mvhashmap::types::TxnIndex` | `blockstm.TxnIndex` |
| Incarnation | `aptos_mvhashmap::types::Incarnation` | `blockstm.Incarnation` |
| Transaction version | `TxnIndex + Incarnation` | `blockstm.Version` |
| MVMemory | `aptos-move/mvhashmap/src/lib.rs` and `versioned_data.rs` | `blockstm.MVMemory` |
| Registered dependencies | `aptos-move/mvhashmap/src/registered_dependencies.rs` | `blockstm.DependencyRegistry` |
| Captured reads | `block-executor/src/captured_reads.rs` | `blockstm.CapturedReads` |
| Latest speculative view | `block-executor/src/view.rs` | `blockstm.View` over MBE snapshot plus MVMemory |
| Scheduler tasks | `block-executor/src/scheduler.rs` | `blockstm.Scheduler`, `ExecutionTask`, `ValidationTask` |
| Last input/output status | `block-executor/src/txn_last_input_output.rs` | `blockstm.TxnStatus`, `TxnOutput` |
| Execution worker loop | `block-executor/src/executor.rs` | `blockstm.Executor.Run` |
| Validation failure | `scheduler.rs`, `txn_last_input_output.rs` abort paths | `blockstm.Abort` and requeue |
| ESTIMATE marker | `mvhashmap` versioned value semantics | `blockstm.Estimate` |
| Ordered final output | `executor.rs` final materialization | `execution.Result` in block order |

## Review Notes

Aptos integrates Block-STM with the Move VM, module cache, resource groups, aggregators, and delayed fields. MBE does not have these execution domains. The implementation target is the core Block-STM scheduler, MVMemory, validation, abort, dependency, and deterministic-output semantics over MBE transfer transactions.
