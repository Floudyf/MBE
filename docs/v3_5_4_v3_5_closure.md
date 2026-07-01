# V3.5.4 V3.5 Closure

## 1. Closure Scope

V3.5 closes the node topology and local launcher foundation stage. It includes logical node topology, launcher preview artifacts, and a local node process preview entry point. It does not implement real TCP, real PBFT, HotStuff/Raft, Fabric/EVM live backend, BlockEmulator backend, real cross-shard protocol, or paper-grade benchmarking.

## 2. Completed V3.5 Stages

- V3.5.1 Logical Node Topology Runtime
- V3.5.2 Local Multi-process Launcher Preview
- V3.5.3 Local Node Process Runtime
- V3.5.4 V3.5 Closure

## 3. Final V3.5 Capability

The frontend can configure shard/node topology. The runtime generates logical nodes and node/network/message artifacts. It generates local launcher preview scripts and address tables. A local node process preview entry point can load topology, identify its role, validate node identity, and write node-local status/log artifacts. Backend metadata, frontend display, artifact downloads, README, docs, and skill guidance are aligned.

## 4. Final V3.5 Truth Boundary

V3.5 is not real TCP, not real PBFT, not HotStuff/Raft, not Fabric/EVM live, not BlockEmulator backend, not a real cross-shard protocol, not persistent KV, not MPT/proof/witness/state root, and not a paper-grade benchmark.

## 5. Validation

Recorded V3.5.4 validation results:

- `pytest backend/tests -q`: passed, 275 passed, 1 Starlette/httpx2 deprecation warning.
- `pytest tests -q`: passed, 24 passed.
- `go test ./...`: passed.
- `npm.cmd run build`: passed, with the existing Vite CJS Node API deprecation warning.
- `scripts/v0_sanity.py`: passed.
- `git diff --check`: passed with line-ending conversion warnings only.
- `git status --short`: expected V3.5.3/V3.5.4 modified files before commit.

## 6. Next Major Stage

V3.6 TCP Adapter and Consensus Hardening.
