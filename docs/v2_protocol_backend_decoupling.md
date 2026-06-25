# V2.6 Protocol / Backend Decoupling

V2.6 separates protocol state machines from chain execution backends.

## Interfaces

`CrossChainProtocol` owns protocol state and transitions:

- `get_initial_state(record_or_cross_tx)`
- `plan_initial_actions(state)`
- `handle_event(state, event)`
- `is_terminal(state)`
- `finalize_result(state)`

Protocol data structures:

- `ProtocolState`
- `ProtocolAction`
- `ProtocolEvent`
- `ProtocolStepResult`
- `ProtocolResult`

## Runner Responsibility

`ProtocolReplayRunner` reads V2.4 trace/meta files, validates them, builds chain profiles, constructs protocols, dispatches protocol actions to `ChainBackend`, collects backend events/finality observations, and writes artifacts.

The runner is the only layer that knows both sides. Protocols produce actions and consume events. Backends submit and observe finality.

## ChainBackend Interaction

V2.6 reuses V2.5:

- `ChainProfile`
- `ChainBackend`
- `LocalVirtualBackend`
- `TraceReplayBackend`
- `UnsupportedLiveBackend`
- `ChainEvent`
- `FinalityObservation`

Protocols do not directly mutate local chain clocks, read LocalVirtualBackend internals, or simulate target finality without going through `ChainBackend`.

## V3 Readiness

Future V3 work may replace `LocalVirtualBackend` with `FabricLiveBackend` or `EVMLiveBackend`. V2.6 keeps live backends planned and unsupported.

Stable pieces to preserve for V3:

- `ProtocolState`
- `ProtocolAction`
- `ProtocolEvent`
- `ProtocolResult`
- `CrossChainProtocol`
- `ChainBackend`
- `ProtocolReplayRunner` dispatch boundary

## Boundary

V2.6 is local protocol baseline replay. It is not real chain execution and not a production bridge.

V2.6 does not start Docker/Fabric/network.sh, does not connect to public-chain live nodes, does not implement MetaFlow, does not implement a real committee bridge, does not implement real signatures or proofs, and does not implement Pending Pool.
