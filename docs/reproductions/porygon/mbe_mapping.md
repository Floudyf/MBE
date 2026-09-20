# Porygon Mapping to MBE

## Plugin composition

```text
routing        = porygon_stateless_routing
block_producer = porygon_transaction_block_producer
consensus      = pbft_style_consensus
execution      = porygon_execution
scheduler      = porygon_pipeline_scheduler
block_executor = porygon_block_executor
state_access   = porygon_remote_state_access
state_storage  = persistent_local_state_store
cross_shard    = porygon_cross_shard_coordinator
commit         = normal_commit
```

Other categories remain the common V5 workload, admission, txpool, network, fault, metrics and observability implementations.

## Access lists

MBE transactions already carry structured execution `AccessList` values. Porygon uses that execution list as the paper's involved-state declaration. The newer MetaTrack-only `SchedulingAccessList` overlay is deliberately ignored, so the Porygon baseline cannot inherit MetaTrack planning information. No future execution result is consulted during planning.

## Stateless routing

`porygon_stateless_routing` reuses the generic stateless remote-state fetch/writeback transport, but deliberately does **not** bind MetaTrack/stateless exact-version control metadata. It does not use MetaTrack co-access placement, logical domains, fast/conservative tracks, StateReady scheduling, or the generic versioned-wave execution controller.

## Execution sharding

The paper's account/state suffix partition is represented by a deterministic hash-to-ESC mapping because MBE canonical keys are not guaranteed to expose the same numeric suffix representation. This changes representation, not the partitioning principle.
