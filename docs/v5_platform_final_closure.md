# V5 Platform Final Closure

V5 has two outward stages only:

1. V5.1 Real Plugin-Driven Multi-Process Multi-Shard Runtime
2. V5.2 Real Formal Experiment and Result Closure

V5.1 supplies the plugin catalog, compiler, Go factory registry, independent
node processes, persistent runtime artifacts, continuous shard execution, and
real cross-shard network path. V5.2 supplies formal plans, suite-aware matrix
semantics, fairness, persistent RunGroups/ChildRuns/Attempts, scheduler
execution, cancellation/retry, metrics, statistics, artifacts, and Paper
Candidate checks.

The software closure acceptance is: all Python and backend tests, full Go
tests, frontend build, Playwright, V0 sanity, Gate A/B, V5.1 regressions,
8-child RunGroup acceptance, one completed 16-node/10000-transaction Child,
and persisted 12-child matrix compilation. The long-running 12-child paper
experiment remains queued work and is not represented as completed evidence.

Simulation is never promoted to paper evidence. Real-cluster startup failure
never falls back to Simulation or V4 smoke. Decentraland dataset integration
is `false`.
