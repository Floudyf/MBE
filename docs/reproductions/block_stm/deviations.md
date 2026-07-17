# Block-STM Deviations

Status: planned deviations before implementation.

## Intentional Adaptations

- The implementation language is Go, not Rust.
- MBE executes its own transfer transaction semantics, not Move VM bytecode.
- MBE does not implement Aptos module cache, resource groups, aggregators, delayed fields, gas accounting, or Move storage layouts in this stage.
- MBE uses the existing V5 `BlockExecutorPlugin` and deterministic apply path.
- MBE keeps PBFT-style consensus, block hash, finality, durable commit, and cross-shard protocol unchanged.

## Non-Claims

- Not Aptos production execution.
- Not Move VM compatibility.
- Not Aptos state storage compatibility.
- Not an implementation of all Aptos production optimizations.
- Not a performance claim until real-cluster acceptance passes.

## Required Preservation

These deviations must not alter the core Block-STM semantics:

- preset order serial equivalence;
- transaction version and incarnation;
- multi-version memory;
- captured reads;
- validation;
- abort and re-execution;
- ESTIMATE and dependency waiting;
- deterministic ordered output.
