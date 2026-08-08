# Batch-SI correctness invariants

1. **No intra-batch WW:** a logical key has at most one writer in a batch.
2. **Single batch assignment:** a transaction writing multiple keys belongs to exactly one batch.
3. **Read-only first batch:** read-only transactions are placed in batch 1.
4. **Reader before writer:** every accepted intra-batch RW/WR relation is serialized reader first.
5. **Cycle handling:** an unserializable dependency cycle defers at least one transaction; planning converges.
6. **Accepted-set closure:** rebuilding the plan from the proposed block produces no further deferred transactions.
7. **Plan binding:** payload digest, plan digest, block transaction order, and semantic recomputation agree on every validator.
8. **Common batch snapshot:** all transactions in a batch observe the same immutable starting state.
9. **Batch succession:** batch `k+1` reads the state committed by batch `k`.
10. **Declared writes only:** execution cannot materialize an undeclared logical key.
11. **Complete results:** each accepted block transaction yields exactly one receipt and one `TxDelta`.
12. **Worker determinism:** workers 1, 2, 4, and 8 produce identical final state and receipt order.
13. **Algorithm isolation:** Batch-SI contains no calls to another scheme's algorithm implementation.
