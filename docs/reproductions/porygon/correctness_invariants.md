# Porygon v8 correctness invariants

- `porygon_stateless_routing` implements `RoutingPlugin` only; it must not implement `BatchRoutingPlugin` or `RoutingRuntimeCapabilities`.
- Formal runs require exactly one physical MBE shard/PBFT ordering domain.
- Porygon never consumes `SchedulingAccessList` or MetaTrack state-version metadata.
- Porygon logical cross-shard classification never enters MBE Relay/Finalize.
- Every transaction has exactly one execution ESC and one business execution attempt.
- Same-ESC transactions are ordered; declared state conflicts preserve global OC order.
- Disjoint transactions on different ESCs may execute in the same wave.
- Actual read/write keys must be covered by the signed AccessList; violations fail closed as deterministic execution errors.
- Multi-state writes are materialized deterministically and atomically at the MBE commit boundary.
- All validators recompute and verify proposal/plan evidence.
- Final state must match the deterministic serialization oracle for supported workload semantics.
- Pipeline evidence must retain `wall_clock_overlap_not_claimed`.
