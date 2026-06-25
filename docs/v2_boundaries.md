# V2 Boundaries

| Scope | V1 | V2 | V3 |
|---|---|---|---|
| Topology | single_chain runnable | dual_chain/cross_chain replay runnable | multi-server/multi-chain deployment |
| Execution | Go replay executor | dual-chain virtual replay | distributed deployment |
| Fabric | local smoke trace via CLI/WSL | optional trace source, no auto start | production/multi-server Fabric |
| Cross-chain | planned only | replay baseline protocols | production-grade protocol experiments |
| MetaFlow | out of scope | protocol plugin candidate, not default | full protocol implementation |
| Public chain | out of scope | imported trace skeleton | live/public-chain integration |
| Frontend | V1 experiment console | topology/protocol console | deployment console |

## 1. Non-negotiable Rules

```text
Never call synthetic replay real chain execution.
Never auto-start Docker/Fabric from normal frontend APIs.
Never run network.sh unless the stage explicitly allows it.
Never mark planned dual-chain config as runnable.
Never implement MetaFlow before cross-chain substrate exists.
Never submit .cache artifacts.
Never push automatically.
```

## 2. Data Truth Rules

```text
synthetic replay: controlled parameters, not real on-chain execution
chain-backed replay: trace comes from real Fabric smoke, but the current run is replay
real Fabric smoke: CLI/WSL starts Fabric test-network
public-chain imported trace: public-chain data import, default unknown semantics
production deployment: V3 planned
```

## 3. Runnable / Planned / Experimental / Invalid

```text
runnable: current code and tests support it, and the platform can run it
planned: config draft or future stage, not runnable
experimental: runnable but with explicit limitations
invalid: illegal combination
```

## 4. V2 Forbidden Claims

Documentation and UI must not claim:

```text
V2 already supports production cross-chain bridge
V2 already supports MetaFlow full protocol
V2 already supports multi-server Fabric
V2 frontend runs real Fabric automatically
synthetic results are real on-chain data
public-chain trace has reliable semantic access sets by default
```
