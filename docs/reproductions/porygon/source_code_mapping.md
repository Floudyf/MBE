# Porygon Source Code Mapping

| Paper mechanism | MBE implementation |
| --- | --- |
| Transaction Block | `porygonTransactionBlockEvidence` + `porygon_transaction_block_producer` |
| Witness / data availability | proposal body/access roots + Porygon `ProposalEvidenceVerifier` validator recomputation before PBFT |
| Ordering Committee global order | `porygon_pipeline_scheduler` serialization order |
| Execution Committee | `execution_committee_count` configuration + pipeline committee assignment |
| Execution Sub-Committee | deterministic `porygonExecutionShard` mapping |
| W/O/E/M | `porygonPipelineStage` logical protocol schedule |
| Cross-Batch Witness | `cross_batch_witness` pipeline stage |
| Single-Shard Execution | one `ExecutionShard` per `porygonTxAssignment` |
| Multi-Shard Update | involved ESC list + deterministic multi-key materialization |
| State locking | `LockedKeys` evidence for cross-shard writes |
| Stateless state access | real transaction `AccessList` + `porygon_remote_state_access` + shared remote-state transport; MetaTrack `SchedulingAccessList` and version-chain control are excluded |
| Deterministic execution | `porygon_block_executor` conflict-safe parallel waves |
| Correctness oracle | serial state-root / receipt-root regression test |
