# Porygon mechanism specification — MBE v8

## Ordering and execution domains

Porygon v8 uses **one physical `porygon-global` MBE/PBFT shard as the Ordering Committee adaptation**. The frontend `topology.shards` control is compiled directly to `execution_shard_count`, creating the requested number of logical Execution Sub-Committees (ESCs) inside that common ordering domain. Increasing the frontend shard count therefore increases ESCs, not PBFT ordering domains.

Transactions retain block order as the global OC order. Execution ESC is deterministically selected from the signed sender identity. Declared state keys are independently mapped to logical state shards. A transaction is logical cross-shard when its execution ESC plus accessed state ownership spans more than one logical shard.

## Access and locking

Only the signed `AccessList` is used. `SchedulingAccessList`, MetaTrack `StateVersions`, runtime-discovered future accesses, and the generic remote CAS path are excluded. Logical cross-shard locking is conservatively derived from the signed AccessList. Two transactions on different ESCs may share a wave only when their declared locks/dependencies do not conflict.

## Execution

Each transaction is owned by exactly one logical ESC. Every replica in that owning ESC independently executes against the common immutable wave snapshot and signs the same semantic result digest with its existing PBFT Ed25519 identity; a strict majority forms an independently verifiable ESC result certificate. Other ESCs do not re-execute the transaction. Actual reads/writes are checked against the signed AccessList and undeclared access fails closed. Certified results are deterministically materialized into the single common state domain, representing Multi-Shard Update atomically at the MBE adaptation boundary.

## Pipeline

Witness / Ordering / Execution / Commit and Cross-Batch Witness are exported as deterministic protocol-slot evidence. Current MBE PBFT permits only one consensus height in flight, so v8 explicitly does **not** claim real cross-height wall-clock overlap.
