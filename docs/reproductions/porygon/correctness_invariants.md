# Porygon v8 correctness invariants

- `porygon_stateless_routing` implements `RoutingPlugin` only; it must not implement `BatchRoutingPlugin` or `RoutingRuntimeCapabilities`.
- Formal runs expose frontend `topology.shards` as the logical ESC count while compiling all validators into exactly one physical `porygon-global` MBE/PBFT ordering domain.
- Porygon never consumes `SchedulingAccessList` or MetaTrack state-version metadata.
- Porygon logical cross-shard classification never enters MBE Relay/Finalize.
- Every transaction has exactly one owning execution ESC. Replicas inside that ESC independently execute the transaction and attest the same result digest; validators outside the owning ESC never re-execute it.
- Production ESC result certificates must contain a strict-majority set of independently verifiable Ed25519 attestations bound to the block/wave/ESC/result digest.
- Missing certificate delivery is recoverable through bounded leader-side certificate caching and explicit request/retransmission.
- Same-ESC transactions are ordered; declared state conflicts preserve global OC order.
- Disjoint transactions on different ESCs may execute in the same wave.
- Actual read/write keys must be covered by the signed AccessList; violations fail closed as deterministic execution errors.
- Multi-state writes are materialized deterministically and atomically at the MBE commit boundary.
- All validators recompute and verify proposal/plan evidence.
- Final state must match the deterministic serialization oracle for supported workload semantics.
- Pipeline evidence must retain `wall_clock_overlap_not_claimed`.
