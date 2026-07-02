# V3.5 Node Topology and Local Launcher Plan

## 1. Goal

V3.5 moves MBE from the local Go-backed modular runtime toward a configurable node-topology emulator-like runtime. The goal is to make shard/node topology, node logs, message logs, and local launcher preparation explicit before any real networking or consensus hardening.

Current V3.5 substage: V3.5.4 V3.5 Closure. V3.5.1 logical node topology runtime, V3.5.2 launcher preview, and V3.5.3 local node process preview are complete.

## 2. Why V3.5 Follows V3.4.11

V3.4.11 closed stage/version/frontend/docs/skill alignment around the V3.4.10 controlled smoke runner. V3.5 starts the next foundation layer: node topology, node-level artifacts, and later local multi-process launch capability.

## 3. Target Alignment with BlockEmulator

BlockEmulator-style systems expose shard/node parameters, multi-process nodes, TCP communication, PBFT state machines, and cross-shard mechanisms. MBE V3.5 only starts by aligning topology, launcher preparation, and logs. It does not claim to be a full BlockEmulator backend.

## 4. V3.5 Stages

### V3.5.1 Logical Node Topology Runtime

Frontend configures topology, backend validates topology, Go runtime generates logical single-process nodes, and runs output node/network/message logs.

### V3.5.2 Local Multi-process Launcher Preview

Generate `launch_nodes_windows.bat`, `launch_nodes_linux.sh`, and a node address table from topology.

This stage is launcher preview only. It generates node address and script artifacts; it does not start a real multi-process runtime.

### V3.5.3 Local Node Process Runtime

Add local node process entry points so each node process can load topology, identify its role, and write node logs.

This stage is local node process preview only. It does not implement real TCP, PBFT, or node-to-node communication.

### V3.5.4 V3.5 Closure

Close README/docs/skill/frontend/backend stage fields, validation, and truth boundary wording.

V3.5 closure is complete after V3.5.4. Do not continue adding V3.5 features after closure.

## 5. Truth Boundary

V3.5 is not real TCP networking, not real PBFT, not HotStuff/Raft, not Fabric/EVM live, not a full BlockEmulator backend, not a real cross-shard protocol, and not a paper-grade benchmark.

## 6. Expected V3.5 Final State

Frontend can configure shard/node topology. The backend can validate topology. The runtime can output node/network/consensus message logs, generate local launch scripts, and provide a local node process preview entry point. Real TCP/PBFT hardening remains for V3.6.

## 7. Next Major Stage

V3.6 TCP adapter and consensus hardening.

V3.6 will add configurable `NetworkAdapter` planning and implementation around localhost TCP typed message preview. V3.7 will add configurable `ConsensusRuntime` planning and implementation with `blockemulator_aligned_pbft_preview` as one optional consensus plugin, not a replacement for MBE's modular runtime model.
