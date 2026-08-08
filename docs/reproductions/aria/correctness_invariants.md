# Aria Correctness Invariants

1. Every transaction in an epoch reads the same immutable epoch snapshot.
2. Speculative transaction writes remain private until the commit decision.
3. Read and write reservations retain the minimum TID for each key.
4. No committed epoch contains WAW dependencies.
5. Rule 2 rejects every transaction that has both RAW and WAR.
6. Deferred transactions preserve relative order.
7. Every block transaction produces exactly one final receipt and one `TxDelta`.
8. State materialization and plan digest are independent of worker scheduling.
9. Repeated execution with the same block, state, configuration, and worker
   count produces the same state root, receipt root, commit order, and plan
   digest.
10. Aria does not advertise exact block-input-order equivalence; it advertises
    deterministic serializability under the locked Aria rule.
