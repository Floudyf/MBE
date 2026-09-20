# Porygon Correctness Invariants

1. A transaction block is invalid if its transaction body, transaction root or access root differs from proposal evidence.
2. Every voting validator can recompute the complete transaction block before accepting the Porygon plan.
3. The execution plan is consensus-bound and validator-recomputable.
4. The global transaction serialization order equals the consensus block order.
5. Every transaction has exactly one deterministic execution ESC.
6. Every declared state key maps to exactly one deterministic ESC.
7. A cross-shard transaction executes exactly once.
8. Cross-shard transactions record every involved ESC and every declared write lock key.
9. Conflicting transactions never execute in the same parallel wave.
10. Cross-shard transactions act as wave barriers.
11. A wave reads one immutable wave-start snapshot.
12. Wave outputs are materialized deterministically.
13. Every transaction produces exactly one final receipt and TxDelta.
14. Repeated execution with the same block/config produces the same Porygon plan digest.
15. Final Porygon state root and receipt root must equal the serial oracle for the same ordered block.
