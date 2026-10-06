# Porygon mechanism specification — MBE plugin adaptation

## Scheme assembly

The frontend exposes one **Porygon** method card, but the method is assembled through the normal MBE plugin profile. Porygon-specific plugins own admission, account/object sharding, single-ordering-domain routing, TransactionBlock/Witness production, execution classification, Porygon pipeline planning, ESC execution, verified state projection, fixed Storage Role persistence, and logical cross-shard coordination. The shared `pbft_style_consensus`, TCP network and normal durable commit plugins remain common with other baselines.

## Ordering and execution domains

Porygon uses **one physical `porygon-global` MBE/PBFT ordering domain**. The frontend `topology.shards` control is compiled to the logical ESC/StateOwner partition count; increasing it never creates additional PBFT ordering domains. Fixed Storage Roles use the compiled execution-partition identity, while Porygon EC/ESC execution roles rotate deterministically by protocol height.

The MBE adapter preserves the paper's committee lifecycle and role separation but uses deterministic sortition over the fixed validator set rather than claiming exact VRF committee selection.

## Access, StateOwner sharding and locking

`porygon_access_admission` rejects transactions before mempool admission unless the signed transaction is valid and its runtime `AccessList` is non-empty, unique and uses known access modes. `SchedulingAccessList`, MetaTrack `StateVersions`, runtime-discovered future accesses and generic remote CAS are excluded.

`porygon_object_sharding` extracts the stable account/object owner from canonical state keys so related fields of the same object stay in one StateOwner partition. The same Porygon owner rule is used by consensus-bound planning. Logical cross-ESC locking/conflict closure is derived only from signed access truth.

## TransactionBlock and Witness

Full transaction bodies are content-addressed as TransactionBlocks outside the compact PBFT object. Witness requests carry the full body to the deterministic EC members, which validate signatures/body commitments and return authenticated votes. The effective Witness threshold is at least the configured lower bound and the runtime Byzantine-bound `f+1` threshold.

MBE does not add physical machines for storage. A TransactionBlock is durably persisted only by the replicas of one deterministic fixed co-located Storage Role partition; other Witness/Execution nodes keep transient cache copies. Compact Proposal.L advertises only those fixed Storage Role replicas as durable fetch sources.

## L/U/T and state projection

PBFT orders the compact Porygon Proposal containing L/U/T. L binds TransactionBlock references; U carries older CTx commit/rollback updates; T binds the agreed business-state root and per-partition roots for the execution round. A state read is served only from the snapshot matching the exact Proposal.T partition root and its Merkle-treap proof is verified before business execution. `TStateHeight=0` is a real genesis/baseline anchor, not a sentinel for "unanchored" access.

## Execution and cross-shard update

Each transaction has exactly one logical execution ESC. Matching authenticated ESC batch results require `Te=f+1`; validators outside the owning ESC do not re-execute the transaction. CTx pre-execution produces its candidate update without immediately materializing final cross-shard state. A later Proposal.U is applied against Proposal.T, and affected fixed Storage Role replicas authenticate prospective roots; Multi-Shard Update accepts a partition root only after a strict majority agrees.

The generic MBE Relay/Finalize protocol remains disabled for Porygon.

## Cross-round Pending and pipeline

The proposal Pending set is CTx-only and proposal-height scoped. It is derived from consensus-bounded origin heights, not from whether a replica's background Execution/Commit worker happened to finish early. This keeps proposal validation deterministic while preserving cross-round CTx conflict protection.

PBFT ordering heights remain sequential, but Porygon Execution/Materialization of an earlier height may overlap the Ordering of a later height. Cross-Batch Witness and Ordering/Execution or Execution/Commit overlap are reported as real only when runtime timestamps prove the intervals overlap; configuration flags alone never claim speedup.

## Fault-path boundary

The normal 4-round ITx / 6-round CTx path is reproduced. Full paper-faithful future-same-shard ESC retry followed by proposal-carried rollback remains a disclosed incomplete fault-path boundary and must stay fail-closed rather than falling back to the retired generic RPC rollback path.
