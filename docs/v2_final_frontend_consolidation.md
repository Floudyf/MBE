# V2-final Frontend Consolidation

## Goal

V2-final reorganizes the frontend from a developer-oriented long dashboard into an experiment platform interface. It does not add new experiment mechanisms.

## Why Consolidate

V2.0 through V2.9 closed the backend layers for planning, plugin registry, trace source handling, job/artifact management, dual-chain replay, protocol baselines, sweep/report, and realism bridge calibration. The frontend needed a clearer structure for demos and acceptance:

- choose experiment type first
- show only relevant configuration
- keep V0/V1/V2 separated
- keep run history and reports independent
- move raw JSON and API debug details into developer mode

## Final Page Structure

- Platform overview
- Single-chain mechanism experiment
- Single-chain ablation comparison
- Dual-chain replay experiment
- Cross-chain protocol baselines
- Batch comparison and report
- Chain-backed trace calibration
- Run history and artifacts
- System boundaries
- Developer mode

## Experiment Types And Configuration

Single-chain pages are for V1 MetaTrack-style experiments. Dual-chain replay is V2.5 only. Protocol baseline replay is V2.6 only. Batch comparison/report is V2.8 only. Chain-backed calibration is V2.9 only.

## Data Truth Label Display

The UI keeps short Chinese badges for data truth labels:

- synthetic replay: synthetic replay, not real chain execution
- existing trace replay: trace replay, no chain launched
- Fabric chain-backed trace replay: web only replays existing trace
- public-chain imported trace semantic unknown: semantic access sets are unknown

## Backend Type Display

The UI keeps backend badges:

- local virtual: local virtual-time, not real chain
- trace replay: replay backend
- Fabric live planned: V3 planned only
- EVM live planned: V3 planned only

## Run History And Artifacts

Run history is separate from the experiment pages. Downloads use the backend artifact API and do not expose `.cache` absolute paths.

## Developer Mode

Developer mode keeps the previous V2 debug dashboard and raw API snapshots behind collapsed panels. This preserves debugging capability without making it the default user path.

## Non-goals

V2-final does not implement V3 live backend, FabricLiveBackend, EVMLiveBackend, a new replay engine, a new protocol baseline, a new sweep runner, a new calibration runner, MetaFlow, Pending Pool, or production bridges.

V2-final does not start Docker, Fabric, `network.sh`, public-chain live nodes, or archive-node clients.

V2-final only reorganizes the frontend expression of V2.0-V2.9 capabilities.

## Relationship To V3

V3 planning can start after V2-final acceptance. V3 is where live backends, deployment, monitoring, and long-running production-like experiments belong.
