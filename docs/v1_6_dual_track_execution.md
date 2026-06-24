# V1.6 dual-track execution

V1.6 is a virtual-time replay execution prototype. It reuses V1.5 `psi_t` execution-shard assignment, then classifies small, explicit, low-risk access sets as `fast`; missing, oversized, or conflict-hinted transactions are `conservative`. Each shard serves fast work before its conservative queue and records idle/deferred metrics. It is not Fabric, cross-chain, MetaFlow, state migration, or V1.7 hot-update aggregation.

Configuration under `execution` is `dual_track_enabled`, `fast_track_max_access_size`, `conservative_on_conflict_hint`, `conservative_on_missing_access_set`, and `scheduler_policy`. Missing fields preserve the conservative V0-compatible path.
