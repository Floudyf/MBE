# V4.2 State / Cross-shard / Recovery

## 1. Stage Goal

V4.2 completes the first V4 realism runtime candidate by adding real state execution, durable commit, cross-shard state machine, recovery, fault injection, and frontend/backend Realism Mode.

Target segment:

```text
committed block -> deterministic execution -> persistent state -> cross-shard state machine -> recovery/faults -> frontend Realism Mode
```

## 2. Allowed Scope

V4.2 may add or modify:

```text
executor/realism/execution/
executor/realism/state/
executor/realism/storage/
executor/realism/xshard/
executor/realism/faults/
executor/realism/recovery/
backend/app/services/v4_*
backend/app/api/v4_*
frontend/src/components/v4/
frontend/src/pages/V4RealismPage.tsx
configs/v4/
```

## 3. Required State Execution

V4.2 must implement deterministic execution of committed blocks:

- read block tx list;
- validate account/nonce/state preconditions;
- apply state transition;
- generate receipt;
- update state root;
- write block/state/receipt/tx-index records;
- report failed transactions with reasons.

## 4. Required Storage

Each node/shard must have durable local storage for:

```text
block_db
state_db
receipt_db
tx_index
event_log
consensus_certificate
```

The first implementation may use a simple embedded store or file-backed storage, but it must support restart recovery tests.

## 5. Required Cross-shard State Machine

Cross-shard transactions must follow:

```text
SourceLock
-> RelayCertificate
-> TargetVerify
-> TargetCommit
-> SourceFinalize
```

Failure path:

```text
Timeout
-> Refund
-> Abort
```

Each state transition must produce an event and a log entry.

## 6. Fault And Recovery

V4.2 should support:

- node crash;
- node restart;
- network delay;
- message drop;
- bandwidth throttling MVP;
- leader timeout;
- target shard congestion;
- relay/proof failure.

Recovery acceptance requires that a restarted node can reload committed height and state root from local storage.

## 7. Required Artifacts

V4.2 should output:

```text
v4_execution_log.csv
v4_state_update_log.csv
v4_state_root_log.csv
v4_receipt_log.csv
v4_state_consistency_log.csv
v4_storage_recovery_log.csv
v4_fault_injection_log.csv
v4_cross_shard_state_machine_log.csv
v4_relay_certificate_log.csv
v4_refund_abort_log.csv
v4_blockemulator_bridge_log.csv
v4_realism_summary.json
```

## 8. Frontend / Backend Integration

V4.2 may introduce:

```text
/api/v4/realism/preview
/api/v4/realism/run-smoke
/api/v4/realism/runs/{run_id}
/api/v4/realism/artifacts/{run_id}/{filename}
```

Frontend must clearly separate:

```text
V3 Light Mode
V4 Realism Mode
```

V4 Realism Mode must show truth labels and must not call V4 results production-chain evidence.

## 9. Acceptance Criteria

A V4.2 run passes only if:

1. committed blocks are deterministically executed;
2. state roots come from real state transitions;
3. receipts are written;
4. block/state/receipt/tx-index storage exists;
5. state root consistency is checked across honest nodes;
6. a restarted node can recover committed height and state root;
7. cross-shard transactions change real state through SourceLock / TargetCommit / SourceFinalize or Refund;
8. fault injection affects real network/node behavior;
9. frontend can run or display V4 Realism Mode results;
10. V3 remains runnable.

## 10. Stage Truth Label

```text
v4_real_state_cross_shard_recovery
```

This label means V4 has a first real node/network/consensus/state/cross-shard candidate. It does not imply production security or paper-final superiority without comparative experiments.

## 11. Implementation Verification Status

V4.2 is implemented and verified in the current repository as a research-grade real multi-node sharded blockchain emulator path.

Implemented and verified:

- deterministic execution of committed block transactions;
- persistent file-backed state database with deterministic state root;
- durable block, receipt, and tx-index commit artifacts;
- state-root consistency checking across honest nodes;
- node recovery from committed height and state root in `data_dir`;
- cross-shard state machine MVP with SourceLock, RelayCertificate, TargetCommit, SourceFinalize, Timeout, Refund, and Abort evidence;
- fault-injection configuration/logging MVP;
- BlockEmulator trace/comparison bridge MVP;
- backend `/api/v4/realism/*` status, smoke, summary, and artifact endpoints;
- frontend Realism Mode panel and artifact display.

Verified smoke truth:

```text
runtime_stage = v4_2_state_cross_shard_recovery_frontend
runtime_truth = v4_real_state_cross_shard_recovery
research_grade_real_emulator = true
production_blockchain = false
production_pbft = false
full_byzantine_security = false
ethereum_mpt_compatible = false
fabric_execution = false
evm_execution = false
```

The final smoke writes `v4_2_realism_final_summary.json` and `v4_2_acceptance_report.json`, with at least three honest nodes agreeing on committed height, block hash, receipt root, and state root.
