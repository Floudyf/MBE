# Block-STM Correctness Invariants

Status: acceptance invariants before implementation.

1. For the same block and base snapshot, final state equals `serial_block_executor`.
2. Ordered receipt success and error values equal Serial.
3. Ordered receipt roots equal Serial.
4. State root after deterministic apply equals Serial.
5. Worker counts 1, 2, 4, and 8 produce identical final state roots.
6. Repeated runs produce identical state root, receipt root, and plan digest.
7. No worker directly mutates global `state.DB`.
8. Every read observation records the version or base snapshot it observed.
9. Every write is registered under a transaction version.
10. Validation failure causes abort, incarnation increment, and re-execution.
11. ESTIMATE prevents dependents from consuming invalidated writes as stable values.
12. Dependency waits eventually resume when the dependency is resolved or the run halts.
13. Hotspot conflicts produce real validation failures and re-executions.
14. No-conflict blocks produce no or near-zero aborts.
15. Nonce mismatch, invalid value, insufficient balance, sender equals receiver, new account initialization, and continuous nonces match Serial.
