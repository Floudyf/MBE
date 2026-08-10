# BSX source lock

Primary source:
- Yaron Hay, Roy Friedman.
- **Batch-Schedule-Execute: On Optimizing Concurrent Deterministic Scheduling for Blockchains**.
- SRDS 2024, DOI `10.1109/SRDS64841.2024.00025`.
- Extended version: arXiv `2402.05535`.

Locked semantics for the homogeneous-transaction comparison path:
- construct an undirected transaction conflict graph;
- a color class contains no conflicting pair and can execute concurrently;
- graph coloring induces a deterministic schedule;
- the minimum-color/minimum-latency problem is NP-hard.

Because the paper does not provide an author source implementation or mandate one polynomial-time coloring heuristic, MBE uses deterministic **DSATUR** and labels the truth boundary `bsx_homogeneous_conflict_graph_coloring_dsatur_v1`. It does not claim optimal/minimum coloring.

The DSATUR tie order is deterministic: saturation degree, graph degree, then original transaction ordinal. Same-color transactions execute in parallel; color classes advance deterministically.
