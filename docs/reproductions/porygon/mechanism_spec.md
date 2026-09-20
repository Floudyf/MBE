# Porygon Mechanism Specification

The MBE Porygon baseline models the paper's three parallel dimensions:

1. **Storage / stateless separation.** Transaction/state homes remain authoritative at the storage side; Porygon execution uses the existing stateless remote-state path rather than MetaTrack placement.
2. **Cross-batch protocol pipeline.** Every transaction block has Witness, Ordering, Execution and Commit stages. The plan records committee assignment and Cross-Batch Witness overlap in deterministic logical protocol slots.
3. **Intra-block execution sharding.** Declared state keys are deterministically assigned to execution sub-committees (ESCs). Transactions touching one ESC are intra-shard; transactions touching multiple ESCs are cross-shard.

## Transaction block and witness

The proposer binds the complete transaction body, transaction IDs and execution `AccessList` values into proposal evidence (never the MetaTrack-only `SchedulingAccessList`). Every PBFT validator recomputes these roots before accepting the deterministic execution plan. The MBE witness truth boundary is therefore `full_body_validator_recompute_before_pbft_vote`, not a fabricated signature count.

## Ordering

The scheduler preserves the consensus transaction order as the global Porygon order. A deterministic plan commits ESC assignments, cross-shard classification, lock keys and conflict-safe execution waves before PBFT.

## Execution

Transactions in a wave have no conflicting declared accesses and execute in parallel against the same immutable wave snapshot. Conflicting transactions advance to later waves. Cross-shard transactions are barriers: they execute exactly once on one deterministic ESC and their multi-key delta is materialized atomically after the wave.

## Commit

Execution results are materialized in deterministic consensus order and committed through MBE's shared durable commit path. Final state/receipt roots must match the serial oracle for the same ordered block.
