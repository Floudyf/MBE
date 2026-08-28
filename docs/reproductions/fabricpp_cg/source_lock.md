# Fabric++ Traditional CG — source lock

## Source of truth

- Ankur Sharma, Felix Martin Schuhknecht, Divya Agrawal, Jens Dittrich.
- **Blurring the Lines between Blockchains and Database Systems: the Case of Hyperledger Fabric**.
- SIGMOD 2019.
- DOI: `10.1145/3299869.3319883`.
- Reproduced scope: Section 5.1 transaction reordering, especially Algorithm 1.

## Locked algorithm semantics

1. Each transaction is one graph vertex.
2. A paper conflict `Ti -> Tj` exists iff `WS(Ti) ∩ RS(Tj) != ∅`.
3. No standalone WW edge is added by this Fabric++ graph definition.
4. Tarjan partitions the conflict graph into strongly connected components.
5. Johnson-style exact elementary-cycle enumeration materializes all cycles; there is no cycle cap or heuristic fallback.
6. Cycle participation is counted for each transaction.
7. Repeatedly remove the transaction participating in the largest number of still-active cycles.
8. If several transactions have the same maximum count, remove the one with the smaller original transaction ordinal/subscript.
9. Removing a victim removes all cycles containing it and decrements the remaining membership counts; Johnson is not rerun after each victim.
10. Rebuild the residual acyclic conflict graph and compute Algorithm 1's serializable order. The paper's conflict `Ti -> Tj` means `Tj` must precede `Ti`, and Algorithm 1 returns `order.invert()`.
11. Where Algorithm 1 leaves `getNextNode()` and parent/child iteration order unspecified, MBE deterministically chooses the lower original transaction ordinal. This is a determinism adapter, not a performance heuristic.

## MBE plugin mapping

- execution: `fabricpp_cg_execution`
- scheduler: `fabricpp_cg_scheduler`
- block executor: `fabricpp_cg_block_executor`
- formal method: `hash_fabricpp_cg`
- plan algorithm: `fabricpp_sharma_sigmod2019_traditional_cg_v1`

The Fabric++ algorithm itself lives in the scheduler plugin. The execution plugin is only a classifier required by MBE's plugin contract. The block executor is only the minimal order-execute adaptation needed by MBE: it materializes the Algorithm-1 total order **serially** with one worker. Fabric++ does not define a post-reordering DAG-frontier parallel executor, so this reproduction deliberately does not add one.

The three plugins are implemented in Fabric++-owned files and do not call the existing Nezha `cg_*` planner/executor implementation.

## Lifecycle mapping

Fabric++ Algorithm 1 removes cycle-victim transactions from the reorderable set `S'`. For MBE fixed-workload accounting, a removed logical transaction is emitted as a deterministic failed no-op terminal outcome (`fabricpp_cycle_aborted`). This closes one terminal lifecycle per submitted logical transaction without applying state for the removed transaction.

## Architecture truth boundary

Fabric++ was designed for Hyperledger Fabric's execute-order-validate pipeline, where transaction simulations/read-write sets already exist before the ordering service reorders a block. MBE is an order/consensus/execute experimental framework. Reproducing Fabric's entire EOV pipeline would require changing common execution/runtime semantics, which is explicitly outside this additive baseline package. Therefore:

- the **conflict graph, Tarjan, Johnson, cycle table, victim rule, and final Algorithm-1 order** are reproduced from Fabric++;
- MBE's existing transaction/access-list contract supplies the read/write sets;
- the final order is materialized serially by a Fabric++-owned adapter;
- this truth boundary is recorded as `fabricpp_sigmod2019_transaction_reordering_v1` and must not be described as a native Hyperledger Fabric deployment.

## Deliberate non-optimizations

This reproduction does not add WW edges, Johnson cycle caps, SCC heuristics, clique shortcuts, approximate cycle counting, alternative victim selection, runtime-feedback scheduling, planner parallelism, or post-order execution parallelism. A high-RMW workload can therefore legitimately expose Johnson's `O((N+E)(C+1))` cycle-output cost.
