# OptME to MBE mapping

| OptME concept | MBE module |
| --- | --- |
| Consensus output | shared `pbft_style_consensus` |
| Simulation | `optme_block_executor` / `stateless_optme_block_executor` using `SerialExecutor.ExecuteTransaction` on one block-start snapshot |
| AddressBasedConflictGraph + hierarchical sort | `optme_scheduler` + shared `optme_core.go` |
| Early abort / reorder / sequence extraction | shared `optme_core.go` |
| Main sequence commit | OptME block executor |
| Aborted-group re-execution and validation | OptME block executor |
| Stateful state | shared `direct_state_access` + `persistent_local_state_store` |
| Stateless projection | `stateless_optme_routing` activates generic MBE remote projection/writeback; signed AccessList is a projection boundary only |

Both method cards share the same scheduling core. PBFT, block production, network, transaction admission, txpool, commit and observability stay on the normal MBE modules.


Topology boundary: both cards accept the common MBE physical-shard setting. `OptME` applies the unchanged author-source OptME core independently to each shard's post-consensus block over persistent local state and the normal stateful MBE cross-shard lifecycle. `Stateless-OptME` applies the same core per shard but obtains/materializes state through the generic stateless substrate. The multi-shard topology is an MBE integration rather than a contribution attributed to the OptME paper.


## 2026-09-28 v8 multi-shard/version contract closure

MBE now emits standard `placement_plan.csv` state-home evidence for this method family. The Stateless adaptation binds an algorithm-agnostic exact predecessor/producer version chain to remote state transport, while the method-specific scheduler/executor remains authoritative. The generic MetaTrack/Stateless-Hash versioned-wave executor is explicitly not used for these methods. This is an MBE stateless/multi-shard integration boundary, not a claim about the original paper.

## 2026-10-02 v22 global-order projection closure

The formal `OptME` and `Stateless-OptME` cards now use one `optme-global` PBFT ordering domain. `topology.shards` denotes logical state partitions. The author-source OptME core is unchanged.

For Stateless-OptME, client/workload source order is no longer encoded as transaction-level `RequiredVersion` / `ProducedVersion`; OptME-specific pre-consensus version admission and the v8/v10/v14/v21 transaction-version publication path are superseded. Every block executes from the unique H-1 state: each Home partition serves one immutable block-start snapshot/root and every signed AccessList key in that block is projected exactly once. After OptME produces the deterministic final block delta, every validator materializes only keys owned by its logical Home partition; there is no second transaction-level remote writeback protocol.

The older v8/v10/v14/v21 sections below/above remain historical documentation for the superseded MBE multi-PBFT adaptation and for the still-frozen TxAllo transport where applicable. They are not the v22 OptME execution contract.
