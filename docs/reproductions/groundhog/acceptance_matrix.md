# Groundhog acceptance matrix

| Area | Required evidence |
|---|---|
| Source lock | paper, official repository and locked commit recorded |
| Typed integer | common-base merge, separate credit/debit aggregates, same-block-credit isolation, overflow and aggregate nonnegative conflict tests |
| Byte string | same-value merge and different-value conflict tests |
| Ordered set | insert, duplicate, clear threshold and capacity tests |
| Atomic rollback | failed transaction leaves no partial reservation |
| Candidate assembly | conflicting candidate released; selected candidate stays reserved |
| Fixed-block validation | conflict rejects complete block with no state delta |
| Determinism | worker counts 1/2/4/8 produce equal state and receipt roots |
| Lifecycle | terminal application failure produces one failed no-op receipt |
| Integration | producer and executor register through existing V5 interfaces |
| Compatibility | Groundhog pair, single-shard and normal-commit constraints enforced |
| Frontend | `hash_groundhog` appears in the method selector |
| Regression | existing execution, V5, backend and frontend tests remain green |
| Fallback | `groundhog_fallback_mode = disabled` and reexecution count is zero |
