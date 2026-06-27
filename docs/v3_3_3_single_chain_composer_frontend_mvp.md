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

## V3.3.4 Localization and Layout Polish

V3.3.4 polishes the V3.3.3 frontend without changing backend runtime semantics or the `composer_preview` schema. It turns the developer-facing Composer page into a Chinese single-chain modular experiment platform page for research-group users.

The frontend now localizes module names, module status badges, tags, template names, experiment identity labels, Plugin Matrix, Fairness Scope, Run Level, and Artifact groups. English `module_id`, `plugin_id`, `method_id`, and artifact filenames remain visible as secondary reproducibility identifiers.

The Composer chain view no longer uses a horizontal scrollbar. It uses a responsive wrapped grid: large screens show a four-column snake-like path, medium screens reduce the column count, and mobile screens fall back to one or two columns while preserving module order and click-to-inspect behavior.

V3.3.4 does not implement Fabric validation, MetaFlow, dual-chain runtime, PBFT/HotStuff, runnable committee lifecycle, dynamic resharding, state migration, or a drag-and-drop freeform composer editor.
