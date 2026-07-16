# Serial MBE Mapping

Legacy reference:

- package: `executor/realism/execution`
- engine: `execution.Engine`
- path: `NodeRuntime.commitOnce -> execution.Engine.ExecuteBlock`

New extension point:

- plugin category: `block_executor`
- plugin id: `serial_block_executor`
- runtime backend: `real_cluster`
- truth boundary: `legacy_faithful_reference_baseline`

Preserved categories:

- `execution`: transaction classification evidence
- `scheduler`: transaction ordering policy

The new block executor consumes the block order supplied by the existing proposer and scheduler path. It is responsible for state execution, receipt generation, execution plan evidence, and deterministic state delta generation.

The state DB, block store, mempool, PBFT-style consensus, TCP network, source-lock relay protocol, durable commit, and finality definition keep their existing ownership.
