# V2-final Acceptance Findings

This note records the acceptance audit for V1 single-chain metrics, V1 ablation output, and V2 run/artifact UI bindings. It is intentionally conservative: smoke-level validation output is not presented as final paper-level performance evidence.

## 1. V1 Latency Metrics

The audited V1 custom run had `avg_latency_ms`, `p95_latency_ms`, and `p99_latency_ms` all equal to `1`. The source `latency.csv` also contained `latency_ms = 1` for each inspected transaction, and `summary.csv` preserved the same values. This means the equality came from the executor output for that small virtual-time sample, not from a frontend aliasing bug.

The backend parser keeps the three fields separate. A regression test now checks that a response with distinct `avg_latency_ms`, `p95_latency_ms`, and `p99_latency_ms` remains distinct.

## 2. V1 Ablation Metrics

The V1 ablation sweep produces distinct configs for:

- `baseline_hash_only`
- `co_access_only`
- `co_access_dual_track`
- `full_v1`

The configs pass different routing, dual-track, and aggregation switches into the replay config. The Go executor reads those switches and emits mechanism counters such as routing policy, cross-shard count, fast-track count, and aggregation count.

The audited sweep used the small `tests/golden/trace_small.jsonl.gz` smoke trace. Core performance metrics such as TPS and P99 latency can therefore be identical across baselines, while mechanism counters differ. `aggregation_ratio` can remain zero when the trace has no eligible aggregation group.

## 3. Classification

Current V1 single-run and default ablation output should be treated as smoke-level / validation-level output unless a larger workload and mechanism-sensitive timing model are used. It validates configuration flow, artifact generation, and metric plumbing. It should not be used as final paper performance evidence by itself.

## 4. Fixes Applied

- Synthetic V1 mode no longer shows `trace_path` in the normal UI. It shows workload/smoke-input guidance instead.
- Existing trace and Fabric trace modes still show trace-path semantics.
- Frontend metrics are formatted with stable units and precision.
- V1 latency cards now explain that narrow virtual-latency samples may produce identical avg/P95/P99.
- V1 ablation page now labels the default comparison as smoke-level validation and surfaces mechanism counters.
- V2.5 and V2.6 result cards merge top-level run metadata with summary fields, so `run_id`, truth labels, and backend/protocol truth are not hidden behind summary-only rendering.
- V2.5 and V2.6 job service responses now return the updated run metadata after summary/backend/protocol fields are persisted.
- Artifact lists include Chinese descriptions without exposing local `.cache` paths.
- V2.9 calibration UI copy separates synthetic calibration samples from Fabric smoke trace calibration.

## 5. Not Final Performance Evidence

The following are not final paper conclusions:

- Identical V1 avg/P95/P99 latency from a tiny virtual-time sample.
- Identical V1 ablation TPS/P99 from `trace_small`.
- Zero aggregation ratio when the trace has no aggregatable hot-update group.
- Synthetic calibration output, which is only a calibration-flow sample.

## 6. Future Work for Real Mechanism Evaluation

To evaluate mechanism effects, use larger and varied workloads, traces with eligible co-access and hot-update groups, and a replay timing model that intentionally connects routing/execution/commit mechanisms to virtual latency. Any such model must be documented and validated rather than introduced as cosmetic metric variation.
