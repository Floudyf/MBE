# V5.2 Implementation And Acceptance Report

## Scope

V5.2 closes the formal-experiment software path on top of the V5.1 local
real-cluster runtime. It does not claim production PBFT, production-chain
security, or production atomic cross-shard commit.

## Gates And Evidence

- Gate A: `scripts/v5_2_plugin_behavior_gate.py` passed. Factory-created
  routing, execution, and commit behavior differs in real runs.
- Gate B: `scripts/v5_2_finality_metric_acceptance.py` passed. Finality is
  derived from raw lifecycle and durable artifacts.
- 8-child RunGroup: 2 methods x 2 seeds x 2 repeats passed sequentially with
  persistence, reload, aggregate output, CI, ZIP, no fallback, and no orphan.
- Final Child: 16 nodes, 4 shards, 4 validators per shard, MetaTrack Full,
  seed 71, and 10000 signed transactions passed with `drain_quiescent`.

## Final Child Measurements

The accepted run recorded 10000 submitted and 10000 terminal transactions,
251 blocks per shard, `real_cross_shard_network=true`,
`state_root_consistent=true`, `no_fallback=true`, and zero orphan processes.
Observed throughput was 84.600937 TPS; P50/P95/P99 finality were 28419,
98987, and 109563 ms. These are measurements, not performance thresholds.

## Matrix Boundary

The 3 methods x 2 seeds x 2 repeats matrix compiles to 12 persisted rows with
fairness and resource estimates. It is deliberately not executed as twelve
long-running children in this software-validation round. Unexecuted rows are
not completed and are not Paper Candidates.

## Truth Boundary

V3 remains the Simulation logical runtime. V4 realism smoke remains historical
regression evidence. V5 Real Cluster is a local independent-process research
runtime. None of these claims production Byzantine security or production
performance.
