# V2.7 UI Data Truth Labels

## Goal

V2.7 connects the frontend to the V2.1-V2.6 backend capabilities. It adds a V2 dashboard that can inspect plugin/composer state, trace sources, chain backends, dual-chain replay samples, protocol baseline samples, run history, and artifacts.

V2.7 does not add a new replay engine or a new protocol mechanism.

## UI Capabilities

The V2 dashboard displays:

- V2 stage overview
- data truth labels
- trace source list and validation results
- chain backend capabilities
- composer preview results
- V2.5 dual-chain replay sample run
- V2.6 cross-chain protocol baseline sample run
- V2 run history and artifact downloads
- boundaries and non-goals

## Data Truth Label Rules

The UI must show text labels, not color alone:

- `synthetic_replay`: synthetic replay, not real chain execution
- `existing_trace_replay`: existing trace replay, no chain launched
- `fabric_chain_backed_trace_replay`: Fabric smoke trace source, web only replays existing trace
- `public_chain_imported_trace_semantic_unknown`: public-chain imported trace with semantic unknown
- `planned_cross_chain_replay`: planned cross-chain replay, not runnable

Synthetic replay must never be described as real on-chain execution.

## Backend Type Rules

The UI displays:

- `local_virtual`: local virtual-time backend, not real chain execution
- `trace_replay`: trace replay backend
- `fabric_live`: planned V3 live backend, not implemented
- `evm_live`: planned V3 live backend, not implemented

Planned backends do not get run buttons.

## Status Rules

The UI shows `runnable`, `planned`, `experimental`, `invalid`, `created`, `running`, `completed`, `failed`, and `blocked` as text badges. Composer warnings, reasons, limitations, and `blocked_by` values remain visible.

## Replay Panels

The dual-chain replay panel calls V2.5 sample replay and displays run id, summary, artifacts, backend type, and data truth label.

The protocol baseline panel calls V2.6 protocol replay and displays per-protocol metrics and artifacts. It states that protocol baselines are local baseline models, not production bridges and not MetaFlow.

## Run Artifacts

Artifact links use the backend `/api/v2/runs/{run_id}/artifacts/{filename}` API. The UI does not expose `.cache` absolute paths and does not build local filesystem paths.

## Non-goals

V2.7 does not implement real chain backends, V2.8 sweep/report, V2.9 realism bridge, V3 live backend, FabricLiveBackend, EVMLiveBackend, MetaFlow, Pending Pool, real committee bridge, real signatures, MintCert, RefundCert, or FinalityProof.

The web UI does not start Docker, Fabric, network.sh, or public-chain live nodes.

## Relationship to Later Stages

V2.8 will add sweep/report. V2.9 may add a realism bridge. V3 is reserved for multi-server, live backend, deployment, and monitoring layers.
