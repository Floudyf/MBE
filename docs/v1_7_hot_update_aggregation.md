# V1.7 hot update aggregation

V1.7 is a replay-only commit-plane prototype. It aggregates deltas by primary state key only when V1.6 classified the update as fast, the update is commutative, a delta is present, and no conflict hint is present. Non-commutative, missing-delta, conservative-track, ambiguous, and constraint-failed updates use conservative commit. A negative aggregate is a deterministic minimal constraint failure.

The module does not change input traces, execute Fabric transactions, provide database concurrency control, implement cross-chain behaviour, or implement V2/V3 mechanisms. Its metrics describe reduced simulated commit count only.
