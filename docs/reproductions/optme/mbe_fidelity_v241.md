# OptME v24.1 Runtime-Fidelity Export Closure

This package is cumulative with v24.0 and keeps the OptME author-source core frozen.

## Problem closed

OptME v24.0 already writes per-block runtime fidelity evidence from the Go executor:

- `optme_projected_unknown_access_count`
- `optme_input_access_fidelity`
- `optme_execution_window_mapping`
- `optme_commit_materialization_model`

The run-group extractor did not export those fields, so downloaded aggregate artifacts could not prove whether a Historical Alien Worlds run contained the conservative `UNKNOWN -> read_write` projection. Absence from the aggregate was therefore not proof of a stale Go binary.

## v24.1 behavior

The extractor now reads these fields from the reference PBFT replica and exposes the logical run-level values, while separately retaining the all-replica physical unknown-access sum. It also emits a runtime fidelity status.

A run is `available` only when every physical OptME block carries the required fidelity fields and the platform mapping strings are consistent. Missing fields are reported as `missing_runtime_fidelity_fields`.

The V18/current paper gate treats missing runtime fidelity evidence as fail-closed (`optme_runtime_fidelity_evidence_unproven`). A Historical run with explicit projected UNKNOWN/RMW evidence remains algorithmically valid, but is diagnosed as projected workload semantics rather than native runtime-RW truth.

## Frozen mechanisms

No change is made to PBFT, AddressBasedConflictGraph, First-Updater-Wins, Early Detection, Hierarchical Sort, reorder eligibility, reschedule epochs, second-pass validation, or the no-third-fallback rule.
