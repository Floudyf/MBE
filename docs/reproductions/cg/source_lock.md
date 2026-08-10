# Conflict Graph (CG) source lock

Purpose: reproduce the CG comparison family used by the Batch-SI paper.

Primary semantic source:
- M. Piduguralla, S. Chakraborty, P. S. Anjana, S. Peri,
  **DAG-based Efficient Parallel Scheduler for Blockchains: Hyperledger Sawtooth as a Case Study**,
  Euro-Par 2023.
- The source algorithm assigns transaction IDs from the original block order. For every `Ti`, it
  checks each later `Tj` and inserts `Ti -> Tj` for `RW`, `WR`, or `WW` conflict. This construction
  is acyclic by original-order direction. Zero-indegree transactions are eligible to execute concurrently.

Locked MBE semantics:
- For every pair `i < j`, add `Ti -> Tj` iff
  `Ri ∩ Wj != ∅`, `Wi ∩ Rj != ∅`, or `Wi ∩ Wj != ∅`.
- Preserve the full `n(n-1)/2` pairwise conflict-detection work as the CG baseline.
- Execute the current zero-indegree frontier in parallel, then advance to the next frontier.
- No serial fallback and no calls into ACG, BSX, or Batch-SI scheduling code.

Important scope disclosure:
- The primary paper parallelizes DAG construction and integrates the DAG with Sawtooth validators.
- MBE currently reproduces the **dependency-DAG semantics and frontier execution**, while its
  consensus-bound plan construction is deterministic Go code and is not claimed to have the same
  graph-construction constant factors or Sawtooth integration cost.
- The Batch-SI paper describes CG as having cycle-detection/abort overhead, whereas the cited
  original-order DAG construction is acyclic by construction. MBE follows the primary CG source
  for this semantic point instead of inventing cycle aborts merely to match the secondary description.
