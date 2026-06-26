# V3.3.1 Research-chain Role Separation

## Scope

V3.3.1 is a platform abstraction correction stage. It upgrades the Go-backed single-chain runtime from a MetaTrack-oriented simplified chain into a lightweight, role-separated modular research chain base.

It is not V3.4 Fabric-backed validation, not frontend integration, not MetaFlow, not dual-chain or cross-chain runtime, not AFS/FDA, not production PBFT/HotStuff, and not a real multi-machine network.

## Role Model

The current single-chain runtime is modeled as:

```text
Workload
  -> TxPool
  -> BlockProducer
  -> ConsensusDomain
  -> Committee / Epoch placeholder
  -> ExecutionRouting
  -> ExecutionShard
  -> StateAccess
  -> StateStorageUnit
  -> Commit
  -> Metrics / Report
```

Current implementation status:

- `ConsensusDomain`: fixed single domain, `consensus_0`, using `simple_leader`.
- `Committee / Epoch`: disabled / planned placeholder only.
- `ExecutionShard`: fixed logical shards in a single-process runtime.
- `StateStorageUnit`: fixed logical memory-backed units.
- `StatePlacement`: fixed `phi(key) -> state_storage_unit_id`.
- `ExecutionRouting`: variable `M_t(tx/key) -> execution_shard_id`.
- `RemoteStateAccess`: deterministic logical access model; no real network or RPC.

Each module can later be marked as `disabled`, `fixed`, `variable`, or `planned`.

## State Placement vs Execution Routing

V3.3.1 explicitly separates persistent state placement from execution routing:

```text
StatePlacement:
  phi(key) -> state_storage_unit_id

ExecutionRouting:
  M_t(tx/key) -> execution_shard_id
```

`hash_state_storage` controls where a state key lives persistently. `hash_sharding` and `co_access_sharding` control the execution-side route. MetaTrack co-access routing changes `M_t`; it does not migrate persistent state placement `phi`.

The legacy `shard_id` artifact field remains as a compatibility alias for `execution_shard_id`. It is no longer the precise role-separated identifier.

## ChainProfile Additions

Role-separated profiles may include:

```yaml
consensus:
  domain_count: 1
  plugin: simple_leader
  validator_count: 4

committee:
  enabled: false
  status: planned
  epoch_enabled: false
  lifecycle_plugin: none

execution:
  shard_count: 4
  executor_count: 4

state:
  storage_unit_count: 4
  placement_policy: hash_state_storage
  backend: memory_kv
  remote_fetch_cost_ms: 1

routing:
  plugin: hash_sharding
  routing_scope: execution_shard

network:
  plugin: fixed_delay
  base_delay_ms: 1
```

Older V3.2 / V3.3 profiles are normalized with defaults so existing tests and smoke paths remain valid.

## Runnable Smoke Profiles

V3.3.1 adds:

```text
configs/v3/chains/single_chain_research_default.yaml
configs/v3/experiments/single_chain_role_separation_smoke.yaml
```

The V3.3 MetaTrack smoke profile continues to run on the Go-backed runtime and now uses the role-separated chain profile. All four combinations still share the same workload, seed, ChainProfile, block config, consensus config, hardware profile, submit rate, and network profile.

## Artifact Fields

`block_log.csv` adds:

- `consensus_domain_id`
- `validator_count`
- `execution_shard_count`
- `state_storage_unit_count`

`tx_results.csv` adds:

- `consensus_domain_id`
- `execution_shard_id`
- `home_state_unit_ids`
- `accessed_state_unit_ids`
- `remote_state_unit_count`
- `cross_state_unit_access`
- `state_locality_hit`

`state_commit_log.csv` adds:

- `state_storage_unit_id`
- `execution_shard_id`
- `is_remote_commit`
- `placement_policy`
- `routing_plugin`

`metatrack_mechanism_metrics.csv` adds:

- `execution_shard_count`
- `state_storage_unit_count`
- `cross_state_unit_access_count`
- `remote_state_fetch_count`
- `state_locality_ratio`
- `execution_shard_load_balance`
- `state_unit_load_balance`

## Non-goals

V3.3.1 does not start Docker, Fabric, `network.sh`, public-chain clients, frontend builds, dual-chain services, or MetaFlow services. It does not implement Fabric validation, PBFT, HotStuff, production networking, committee lifecycle, state migration, or paper-scale performance claims.
