# MBE V5 literature runtime/truth final closure — 2026-08-10

Baseline commit: `953b6f1e444b05293de1f13e74e0b579b72ed997`.

This closure removes implementation costs that are not required by the reviewed algorithm semantics while preserving all conflict, abort/defer, ordering, PBFT, state-access and finality rules.

## Runtime-cost corrections

- Batch-SI uses the barrier-stable current working state as the logical batch snapshot `BS_k`; snapshot creation is O(1). Per-transaction overlays share `BS_k` read-only and keep only private writes. No full state is copied per batch or per transaction.
- The shared speculative transaction overlay used by CG/ACG/BSX and Aria shares its caller-owned immutable snapshot and keeps transaction-local writes. CG/ACG/BSX wave snapshots also share the barrier-stable working state instead of deep-copying it.
- Aria candidate and internal-epoch snapshots share immutable state, and tentative attempts do not compute a durable `StateRootAfterTx`; final committed receipt roots are still computed during deterministic materialization.
- Groundhog reservation tables share the caller-owned immutable block-start snapshot; materialization still creates a private working state. Typed conflict/constraint rules are unchanged.
- Serial and Block-STM core semantics are not modified by this package.

## Result-truth corrections

- BSX receives its own deterministic-coloring/serializable semantics class. Its correctness oracle is its consensus-bound deterministic schedule, not equality to the consensus-order Serial final digest.
- A valid one-member semantic cohort can be a paper-eligible sample. Multiple implementations in the same cohort must still agree on required digests.
- Cross-semantic direct uplift remains explicitly invalid when semantic classes differ; observed/paper-eligible samples remain available for transparent reporting.
- CG/ACG/BSX mechanism metrics are extracted from one leader per shard instead of being reported as not-applicable.

## Protected cores

No package change is made to Groundhog typed conflict/reservation rules, Block-STM core, CG graph construction, ACG graph construction, BSX coloring, Batch-SI WRBP/OFAS, or PBFT. Groundhog changes are limited to immutable snapshot representation/copy elision.
