# OptME v24.2 compatibility closure

This closure does not change OptME or PBFT semantics. It updates the package-owned v24 regression contract so v23 stale-blocker supersession and the v24.1 runtime-fidelity fail-closed gate are tested as independent preconditions.

A method may retire `stateless_global_logical_business_state_not_proven` only when the method-specific OptME oracle and transaction-level evidence pass. Paper eligibility still additionally requires runtime-fidelity evidence from the active Go executor. Missing runtime evidence remains a blocker.

The installer accepts the exact v24.0 package-owned test SHA as a known predecessor, upgrades it to the v24.2 contract, and rejects unknown edits.
