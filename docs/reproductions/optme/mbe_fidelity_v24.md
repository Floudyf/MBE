# OptME MBE Fidelity Boundary v24

This closure is a platform-adaptation/evidence correction only. The OptME
paper/source mechanisms are frozen at author repository commit
`4cac103bd98440670d71219dfa185b8516ea6512`.

## Frozen algorithm semantics

- post-consensus simulation and observed runtime read/write sets;
- author-source parallel AddressBasedConflictGraph construction and pairwise merge;
- First-Updater-Wins and Early Detection;
- hierarchical address ranking/sort;
- author reorder eligibility;
- aborted-transaction reschedule epochs;
- second-pass re-execution and validation;
- no third serial fallback (the source fallback is commented out);
- MBE `worker_count` remains the fixed experiment CPU-resource budget used by
  simulation/parallel ACG work. v24 does not rewrite the OptME scheduler core.

## v24 platform/evidence corrections

1. Historical Alien Worlds `UNKNOWN` declarations remain conservatively
   projected as RMW for safe replay, but the executor now exports that boundary
   explicitly. Projected historical evidence is not relabeled as native
   runtime-RW truth.
2. Canonical OptME algorithm/stage metrics use one compiled global-PBFT leader
   replica instead of summing the same deterministic execution across validators.
   All-replica sums remain available under explicit `optme_replica_physical_*`
   fields, while transaction event counts are additionally cross-checked and
   canonicalized from transaction-level OptME evidence.
3. The OptME-specific v23 apply-order replay oracle supersedes only the obsolete
   v17 `stateless_global_logical_business_state_not_proven` blocker when current
   method correctness and transaction evidence both pass. The v17 group wrapper
   blocker is retired only when no other unresolved v17 child blocker remains.
4. Formal worker truth uses the topology/runtime value. Registry default 4 is a
   default only and is not evidence for an 8-worker run.
5. MBE records the execution-window mapping explicitly as one committed PBFT
   block to one OptME execution window.

## Explicit remaining MBE materialization adaptation

The author source commits transactions inside each OptME sequence concurrently.
MBE currently materializes the already-simulated effects in deterministic index
order because its mutable `map[string]string` working state and incremental
`state.Commitment` are not a thread-safe concurrent storage engine, and MBE also
requires deterministic per-transaction receipt roots.

v24 deliberately does **not** fake unsafe concurrency or alter state-root
semantics. It records this as
`mbe_deterministic_index_order_platform_adapter`. The OptME ACG/scheduling core,
PBFT, and correctness oracle remain unchanged. A future concurrent storage
adapter may remove this platform cost only if it preserves the same OptME plan,
final business state, and deterministic evidence contract.
