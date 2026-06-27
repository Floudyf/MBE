# V3.3.3 Single-chain Modular Composer Frontend MVP

V3.3.3 renders the V3.3.2 Composer Profile / ExperimentTemplate metadata as a minimal frontend experience. It is a frontend alignment stage: the backend remains the source of truth for `composer_preview`, `module_graph`, `plugin_matrix`, and `fairness_scope`.

## Scope

The MVP shows a single-chain modular research chain:

```text
Workload
  -> TxPool
  -> BlockProducer
  -> Consensus
  -> Committee / Epoch
  -> Routing
  -> Execution
  -> State Access
  -> State Storage
  -> Commit
  -> Metrics / Report
```

Each module is rendered with its plugin, status, role, tags, allowed plugins, metrics, and related artifacts. Valid module statuses are still `fixed`, `variable`, `disabled`, `planned`, and `output`.

## Frontend Panels

- `SingleChainComposer` renders the module chain from backend preview data.
- `ModuleCard` and `ModuleDetailPanel` expose module status, plugin, role, tags, metrics, and artifacts.
- `PluginMatrixTable` shows MetaTrack method rows and highlights plugin differences from the baseline.
- `FairnessScopePanel` shows variable, fixed, disabled, planned, and output modules.
- `RunLevelPanel` exposes only the existing smoke run level; debug, formal, and stress remain planned.
- `ArtifactGroups` groups downloadable run outputs by summary, chain logs, MetaTrack metrics, and used profiles.

## API Alignment

V3.3.3 adds narrow frontend-facing API endpoints:

- `GET /api/v3/composer/templates`
- `GET /api/v3/composer/preview?experiment_profile_id=...`
- `POST /api/v3/composer/run-smoke`

The run endpoint invokes the existing local Go-backed MetaTrack smoke path and registers artifacts in the existing run/artifact system. It does not start Fabric, Docker, `network.sh`, a public-chain connector, or a multi-node network.

## Non-goals

V3.3.3 does not implement Fabric-backed validation, MetaFlow, dual-chain runtime, AFS/FDA, PBFT/HotStuff, committee lifecycle runnable behavior, dynamic resharding runnable behavior, state migration, or a drag-and-drop freeform composer editor. Smoke output remains controlled platform evidence, not final paper-scale performance evidence.

The V3 skill file is intentionally not updated in this commit because the previous attempt was blocked by a tool-layer usage limit. That skill update is deferred to a later docs/skill-only commit.
