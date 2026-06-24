# V1.5 co-access routing

V1.5 adds a deterministic control-plane routing prototype for each replay batch. `M_t` maps a state key to an **execution** shard; `psi_t` maps a transaction to the execution shard selected from its access set. Neither changes persistent state placement `phi`: this is not state migration or re-sharding.

`routing.policy: hash` preserves the V0-compatible FNV baseline. `routing.policy: co_access` greedily groups keys that co-occur at least `routing.co_access_min_weight` times, up to `routing.co_access_max_group_size`, then places groups on the least-loaded execution shard. `routing.co_access_balance_weight` is retained as an explicit balancing knob. Summary output reports routing policy, cross-shard transaction count/ratio, remote key count, group count, and routing time.

This phase is execution-side routing only. It does not implement dual-track execution, hot-update aggregation, state migration, MetaFlow, or cross-chain behaviour.
