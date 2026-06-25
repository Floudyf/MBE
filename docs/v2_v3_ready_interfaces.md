# V2.5 / V3-ready ChainBackend Interfaces

V2.5 introduces a small ChainBackend interface so local dual-chain replay can be swapped later without rewriting the replay coordinator.

## Interface Shape

The interface is intentionally narrow:

- `ChainProfile`
- `BackendCapability`
- `ChainEvent`
- `FinalityObservation`
- `submit_stage(record)`
- `observe_finality(record)`

V2.5 implementations:

- `LocalVirtualBackend`: runnable local virtual-time replay.
- `TraceReplayBackend`: replay using timestamps already present in trace records.
- `UnsupportedLiveBackend`: planned placeholder for V3 live backends.

## V3 Placeholders

`fabric_live` and `evm_live` are visible as planned backend capabilities. They are not runnable in V2.5. Calling them raises a clear unsupported error.

These placeholders do not start Fabric, Docker, network.sh, peer commands, public-chain nodes, or archive-node clients.

## Boundary

V2.5 is local replay infrastructure. It does not implement:

- production Fabric deployment
- EVM live ingestion
- cross-chain protocol baselines
- MetaFlow
- committee bridge
- Pending Pool
- production bridge security

Future V3 work may replace local backends with live backends, but V2.5 keeps the runtime offline and deterministic.
