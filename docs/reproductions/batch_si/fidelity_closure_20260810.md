# Batch-SI paper-fidelity closure (2026-08-10)

Normative source: user-supplied **Batch-SI: Semi-Parallel Concurrency Scheme for Permissioned Blockchains Exploiting Batch Snapshot Isolation**.

This closure does not change the main Batch-SI algorithm merely to raise TPS. It locks the existing implementation against the paper's worked examples and execution invariants:

1. AWRT construction from declared write sets.
2. WRBP as Algorithm 1, including write-opportunity reuse.
3. OFAS state (`tRNum`, `kRNum`, `kMaxR`), paper priority, Rule 1/Rule 2 behavior.
4. The deterministic reader-before-writer topological realization currently used after the Algorithm-2 state pass is retained. The paper's prose/pseudocode does not fully specify tie realization, and this pass is what makes the worked serial order explicit; it is not removed without author source or a stronger proof.
5. The separate `hash_batch_si_no_ofas` path remains the full dependency-graph ablation.
6. One immutable snapshot per batch; batches advance sequentially; transactions in one batch execute in parallel.
7. OFAS-rejected transactions are proposal-level deferrals returned to the transaction pool, not committed execution failures.

Golden regressions lock the worked examples:

- Figure 7 WRBP: `B1={T1,T3,T8}`, `B2={T2,T4,T7}`, `B3={T5,T6}`.
- Figure 9 OFAS: abort `T4`, serializable order `T5,T1,T2,T3`.

The paper contains internal wording ambiguity about WRBP ID direction and OFAS tie realization. MBE follows the worked examples and records the ambiguity instead of changing semantics for performance.

No official Batch-SI source repository is used; this remains a paper-level independent reimplementation.
