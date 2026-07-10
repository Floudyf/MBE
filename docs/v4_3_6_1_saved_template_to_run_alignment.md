# V4.3.6.1 Saved Template to Run Alignment

## A. Current Break

The Experiment Design page already saves method templates as `V3SavedConfig` records. A saved method contains the Composer draft, module selection, topology compatibility fields, template and preset identity, validation evidence, and the last smoke run id.

The Run Experiment page currently reads a static experiment-flow method catalog instead. Saved V3 method configs therefore do not become selectable run-matrix methods. The page also exposes nodes, shards, validators per shard, transaction count, and repeat count without sending all of them into matrix derivation. Static topology presets and editable topology numbers can appear active at the same time, creating two conflicting sources.

## B. V4.3.6.1 Solution

- Reuse `V3SavedConfig`; do not create another template registry or store.
- Return builtin and saved methods from the experiment-flow methods API.
- Derive saved-method `runnable` state from `validation_status`.
- Carry run conditions through an `ExperimentConditions` model.
- Make preset and custom topology modes mutually exclusive.
- Expand `repeat_count` into explicit `repeat_index` matrix rows.
- Keep main, comparison, ablation, workload-sensitivity, and topology-scaling execution preview-only.

The V3 runtime topology model counts validators per shard as logical roles and does not currently enforce that every validator consumes one unique item from experiment-flow's aggregate `nodes` field. Custom experiment-flow topology validation therefore keeps the existing explicit constraints, including `shards <= nodes`, without inventing `validators_per_shard * shards <= nodes`.

## C. Saved Method Status Semantics

`validation_status=runnable` is previewable and can become a future formal-runner candidate. Formal suite execution remains preview-only in V4.3.6.1.

`validation_status=valid` remains visible and previewable, but generated rows are blocked with a warning that quick validation is required before execution.

`validation_status=unknown` remains visible and previewable, is not selected by default, and generates blocked rows.

`validation_status=blocked` remains visible for traceability, is disabled in the normal method picker, is not previewable, and generates blocked rows if submitted directly.

## D. Boundary

V4.3.6.1 does not dispatch formal runners. Formal runner dispatch is reserved for V4.3.6.2. This round does not change the `V3SavedConfig` store, formal benchmark semantics, V4 realism API paths, V4 realism runner behavior, Go runtime, executor code, result registry, or workload datasets.
