# V5 Workload Data Plane Design

## 1. Background and goal

V5 Formal Experiment Workflow is complete for the existing
`deterministic_signed_synthetic` workload. This document defines the next
implementation contract: a workload data plane that can deterministically
select, materialize, compile, execute, and evidence a real-observed dataset
without changing V5 consensus, finality definitions, or the cross-shard state
machine.

This is a design-only document. No dataset registry, converter, materializer,
dataset API, trace iterator, or frontend selector is implemented by this
round. The next implementation round is named **V5 Workload Data Plane Full
Implementation**, not V5.3, V5.2.1, or V6.

## 2. Current gap

Today the V5 plugin catalog contains only `workload/deterministic_signed_synthetic`.
`V5FormalRunPage` writes `tx_count`, `cross_shard_ratio`, `timeout_every`, and
`seed` into the synthetic workload config. The compiler emits this as
`workload_plan`; the Go client calls `WorkloadPlugin.BuildWorkloadItem` and
signs generated transactions. The Workload Library is explanatory only.

There is no V5 dataset registry, selector, canonical trace conversion,
materialization cache, real trace replay, derived skew workload, or dataset
artifact contract. V1 trace replay and the V4 BlockEmulator CSV bridge are
historical paths and are not V5 Formal workload sources.

## 3. Scope and non-goals

The implementation will add a V5 run-level `workload_source`, canonical
dataset conversion, deterministic original-window and contract-Zipf variants,
streaming replay, evidence artifacts, and frontend selection/preview.

It will not add arbitrary browser CSV upload, live Marketplace crawling, live
Polygon RPC execution, Polygon EVM opcode replay, storage-slot reconstruction,
price/balance settlement, buyer/seller skew, multi-axis skew, target-TPS or
wall-clock replay, a production blockchain claim, production PBFT, multi-server
deployment, exactly-once production semantics, or changes to consensus,
finality, and cross-shard protocol definitions.

## 4. Dataset provenance and truth boundary

The first proposed dataset is `dcl_sales_polygon_271868`. Its local source is
the user-maintained `dcl_sales_workload_chain_ready.csv`, obtained from the
Decentraland Marketplace API endpoint
`https://marketplace-api.decentraland.org/v1/sales`, primarily for `wearable`
and `emote` sales. It excludes LAND, Parcel, Estate, NAME/domain, rental, bid,
and created-but-unfilled order coverage.

The dataset is not a full Decentraland market export, Subgraph export, or
Polygon RPC scan. The documented source processing uses sale-ID deduplication:
271909 raw records, 41 duplicated pagination/resume records removed, 271868
sales retained, and 271848 unique `tx_hash` values. Repeated `tx_hash` values
are retained because sale IDs, not hashes, are the record identity.

Polygon `eth_getTransactionReceipt` verification is representative stratified
verification only: 50 samples were checked on 2026-07-10. It must never be
reported as complete per-row verification. Source/RPC credentials are read only
from environment variables and never appear in source, manifest, artifacts,
screenshots, or Git.

Truth labels are `synthetic_generated`, `real_observed`, and
`real_derived_resampled`. A derived workload is a deterministic resampling of
observed rows, not an original trace and not Polygon contract execution.

## 5. Directories and data safety

The next round will add Git-managed definitions only:

```text
data/workloads/manifests/
data/workloads/schemas/
data/workloads/samples/
scripts/workloads/
```

The local raw input remains read-only. Canonical, materialized, and report
outputs belong under `.cache/workloads/{canonical,materialized,reports}`.
Full CSV, canonical traces, and materialized workloads never enter Git.
Artifacts expose only dataset IDs and relative logical paths, never local
absolute paths, secrets, private keys, commands, environments, or raw-file
copies. The next implementation must add (but this design round must not edit)
these ignore rules:

```text
/dcl_sales_workload_chain_ready.csv
/data_local/workloads/
/.cache/workloads/
```

## 6. Dataset Manifest contract

`DatasetManifest` is versioned JSON with required fields:

```text
schema_version, dataset_id, display_name, description,
source_platform, source_endpoint, source_chain, dataset_type,
included_categories, excluded_categories, source_format,
local_raw_path, canonical_path, source_sha256, canonical_sha256,
row_count, unique_source_tx_hash_count, time_start_ms, time_end_ms,
collection_date, processing_date, verification_date, fields,
category_counts, natural_skew_metrics, truth_label, collection_method,
processing_pipeline, processing_scripts, raw_fetch_script_archive,
verification_method, verification_sample_count, verification_results,
provenance, usage_note, available, validation_status, generator_version.
```

The Git-managed manifest never stores a Windows absolute path. It records
`local_raw_relative_path="dcl_sales_workload_chain_ready.csv"`, resolved at
runtime relative to repository root. Public APIs and artifacts never return the
resolved path, repository root, or `local_raw_relative_path` resolution.

For the initial manifest: `source_platform=decentraland_marketplace`,
`source_chain=polygon_mainnet`, `dataset_type=marketplace_sales`,
`truth_label=real_observed`, `collection_method=self_crawled_marketplace_api`,
and `raw_fetch_script_archive=partial`. Public responses redact `local_raw_path`.
A manifest is selectable only when its schema, hashes, count, availability, and
validation status pass. An initial static manifest may be
`available=false, validation_status=unvalidated`; only the runtime registry may
report `selectable=true` after validator success. Failure blocks the child
rather than selecting synthetic.

## 7. Canonical and Materialized Workload Records

The converter writes streaming JSONL.GZ canonical records with this exact
schema:

```json
{
  "schema_version": "mbe_workload_record_v1",
  "dataset_id": "dcl_sales_polygon_271868",
  "source_row_index": 0,
  "source_event_id": "sale-id",
  "source_tx_hash": "0x...",
  "timestamp_ms": 0,
  "category": "wearable",
  "buyer_address": "0x...",
  "seller_address": "0x...",
  "contract_address": "0x...",
  "price_raw": "decimal string",
  "price_bucket": 0,
  "runtime_value": 1,
  "state_keys": ["account:buyer:0x...", "account:seller:0x...", "contract:0x..."],
  "provenance": {"source": "decentraland_marketplace_api"}
}
```

`source_event_id` is the source CSV `id` and is unique. `source_tx_hash` may
repeat. Addresses use `*_address` names; `price_raw` remains a string;
`price_bucket` uses deterministic decimal magnitude buckets; and v1 fixes
`runtime_value=1`. `state_keys` are generated during canonical conversion.
`raw_contract_candidates` remains source-CSV-only and is not duplicated in the
canonical public schema. Each row must resolve exactly one `contract_address`;
failure is a conversion error, never a skipped row.

A materialized record contains all canonical fields plus
`materialized_index`, `logical_event_id`, `occurrence_index`, and its retained
`source_row_index`/`source_event_id`. `logical_event_id` is deterministically
derived from `dataset_id + variant_id + materialized_index + source_event_id +
occurrence_index`. It distinguishes repeated resampled occurrences without
inventing a source event.

Canonical order is `(timestamp_ms, source_row_index)`. Serialization is UTF-8,
LF, fixed JSON-key order, and no extra whitespace; gzip uses `mtime=0`, fixed
compression level and header. SHA-256 is over the final compressed bytes, so
identical inputs produce byte-identical `.jsonl.gz` output.

## 8. Original-window selection

`original_window` accepts `dataset_id`, `tx_count`, and `seed`. For Full,
`tx_count=row_count`; otherwise valid choices are 10K, 50K, 100K, and 250K,
bounded by `row_count`.

Let `n` be canonical row count and `k` requested count. Normalize and hash
`dataset_id`, `source_sha256`, `canonical_sha256`, `requested_tx_count`, `seed`,
`selection_mode`, `selector_version`, and `generator_version`; compute
`start_offset = uint64(SHA256(normalized_input)) mod (n-k+1)`.
Select the contiguous ordered range `[start, start+k)`. Ties retain
`source_row_index` order. Full selects all records. The selection hash includes
the normalized inputs, start, count, and selected-row digest. The same inputs
must produce byte-identical records and hash; a different seed may, but need
not, select a different valid window.

## 9. Contract-Zipf derived skew

`contract_zipf` is `real_derived_resampled`. It first chooses the deterministic
original base window. For each category separately, it counts contracts, sorts
by `(-count, contract_address)`, assigns category-local ranks, weights rank `r`
by `r^-alpha`, and draws exactly that category's original record count. Within a
selected contract, it deterministically samples real source rows with
replacement. No buyer/seller/contract tuple is invented.

The two category streams are merged by replaying the base window's category
position template, preserving exact wearable/emote counts and stable category
order. Repeated draws are distinguished by `occurrence_index` and retain both
source identifiers. Original and derived variants always share the same base
window.

Supported alpha values are exactly `0.0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2, 1.4`.
`alpha=0.0` is uniform category-local rank selection, not the natural original
distribution. Sampling uses a SHA-256 counter-based sampler, not Python random,
NumPy random, or Go `math/rand`. Its domain includes dataset ID, source hash,
base-window hash, category, normalized decimal target alpha, seed, generator
version, draw index, and sampling stage; the digest maps deterministically to
`[0,1)`. The report records target alpha, realized Gini/HHI/Top-K, duplicate
source-row ratio, category distribution, and source-row traceability.

## 10. Materialization and cache

`materialized_id` is a SHA-256 digest of the normalized `WorkloadSourceSpec`,
manifest/canonical hashes, selector version, variant, seed, count, and all
variant parameters. Its directory is `.cache/workloads/materialized/<id>/`.
Materialization writes to a temporary sibling directory, validates count and
hash, writes a ready marker last, then atomically publishes. Partial output is
never cache-ready.

Each matrix point materializes before Start. A cache hit is reusable only when
the manifest and canonical hashes match exactly. Cache/open/schema/hash/count
failure blocks every affected row and records `no_fallback=true`; it never
falls back to synthetic, V4 smoke, V1 replay, or Simulation.

## 11. WorkloadSourceSpec

`workload_source` is the sole source of truth for workload data selection. The
next `V5ExperimentSpec` extension is run-level, not method-level:

```json
{
  "schema_version": "mbe_workload_source_v1",
  "source_type": "synthetic|dataset",
  "plugin_id": "deterministic_signed_synthetic|canonical_trace_replay",
  "dataset_id": "required for dataset",
  "variant_mode": "original_window|contract_zipf",
  "variant_id": "stable normalized variant identifier",
  "requested_tx_count": 10000,
  "use_full_dataset": false,
  "seed": 47,
  "selection_mode": "contiguous_window",
  "replay_mode": "max_throughput",
  "skew_axis": "contract",
  "target_alpha": null,
  "materialized_id": null,
  "source_sha256": "required for dataset"
}
```

For legacy synthetic requests without `workload_source`, backend normalizes one
from top-level `tx_count`, `seed`, and workload-plugin config. When both forms
are present, top-level fields are compatibility mirrors and disagreement is a
validation blocker. Compiler and frontend consume only normalized
`workload_source`; compiled plans preserve one `compiled_workload_plan` and do
not reinterpret mirror fields. Synthetic retains current `cross_shard_ratio`
and `timeout_every`; dataset mode rejects both controls. Changing source, count,
seed, alpha, or selector invalidates workload preview and Formal Matrix preview.

## 12. Compiled Workload Plan

`V5CompiledRunPlan.workload_plan` gains a versioned `compiled_workload_plan`:
dataset ID, variant, truth label, manifest/canonical/source hashes, materialized
ID/hash, selected count/range, seed, replay mode, expected state-key scheme,
expected cross-shard count/ratio, cache status, and no-fallback flag. Existing
synthetic fields remain compatible. Formal plan ID and method config ID remain
distinct from workload IDs.

Comparison and ablation methods share exactly one materialized hash per matrix
point. Dataset sensitivity may scan count or alpha, and each point has an
explicit variant/materialized ID. Dataset rows cannot silently apply a synthetic
cross-shard-ratio scan.

## 13. Backend API contract

The next round adds read/validate/materialize/preview endpoints such as:

```text
GET  /api/v5/workloads/datasets
GET  /api/v5/workloads/datasets/{dataset_id}
POST /api/v5/workloads/preview
POST /api/v5/workloads/materialize
```

Responses are DTOs: never expose `local_raw_path`, cache absolute paths, RPC
credentials, or raw records. Formal preview and create both independently
validate manifest availability, source/canonical hash, count, source mode, and
materialization. Any invalid row returns blockers; create returns 422 and
creates no partial RunGroup.

## 14. Go workload runtime

Add `canonical_trace_replay` through the existing Interface + Factory Registry:

```go
type WorkloadIterator interface {
    Next(context.Context) (WorkloadRecord, error)
    Close() error
    Summary() WorkloadReplaySummary
}
```

`WorkloadPlugin.NewIterator(CompiledWorkloadPlan)` creates either
`SyntheticIterator` or `CanonicalTraceIterator`. The latter streams JSONL.GZ
with an explicit maximum record buffer. `io.EOF` is valid only after the exact
actual transaction count; early EOF, excess records, malformed JSON, schema
error, source/materialized hash mismatch, or an unreadable source fails before
submission. It never silently skips a record or delegates to SyntheticIterator.
`Close` is mandatory and `Summary` records read/submitted/rejected counts.

The first workload-data-plane implementation supports only `max_throughput`:
preserve materialized order and submit as fast as practical. Timestamps are
provenance/selection/skew inputs only; no five-year wall-clock waiting, target
TPS, burst, ramp, or trace-shape replay.

## 15. Identity, signing, and nonce

Canonical buyer addresses map deterministically to local Ed25519 key pairs
using the identity domain `dataset_id + source_sha256 + experiment_seed +
identity_mapping_version`. Identical parameters reproduce identical mappings,
and one buyer maps to one sender within a Child. Per-identity nonces start at
zero and increase continuously in materialized order. Public key and sender may
appear in normal transaction evidence; private keys never appear in artifacts.
`identity_mapping_version` is recorded in the compiled plan and summary, while
artifacts expose only counts, digest, nonce continuity, and signature results.

## 16. State keys, sharding, and cross-shard semantics

Each sale models `account:buyer:<buyer>`, `account:seller:<seller>`, and
`contract:<contract>`. Buyer home shard and contract home shard determine
cross-shard classification; seller is an additional accessed state key. Routing
may influence ingress/placement but cannot change source state keys. Expected
cross-shard statistics are compiled; actual statistics come from lifecycle
events. This is MBE research modelling, not Polygon EVM execution, marketplace
storage-slot reproduction, Polygon shard topology, or a recovered call graph.

## 17. Frontend interaction

Workload Library becomes a registry view with dataset metadata, truth label,
coverage, category counts, natural skew, hash summary, verification summary,
availability, variants, and boundaries. Run Experiment selects a source through
generic workload API/schema data, not dataset-ID conditionals. Dataset controls
show size, seed, selection preview, original/derived variant, and contract alpha.
Synthetic retains tx count, cross-shard ratio, timeout, and seed controls.

Any workload edit invalidates workload preview and Formal Matrix preview. Results
show source, dataset/variant, truth label, hashes, selected range, skew,
actual cross-shard ratio, category mix, identity/nonce/signature evidence, and
no-fallback status.

## 18. Runtime artifacts

Dataset children add `workload_manifest_snapshot.json`,
`workload_source_spec.json`, `workload_selection.json`,
`workload_skew_report.json`, `workload_materialization_summary.json`,
`workload_identity_mapping_summary.json`, and `workload_replay_summary.json`.
The latter records IDs/hashes, expected/read/submitted/rejected count, identity
count, nonce/signature checks, expected/actual cross-shard statistics, replay
times/mode, and no fallback. Existing compiled-plan, process, lifecycle,
routing, finality, state, and artifact-catalog evidence remains required.

Paper rows include workload label, dataset ID, variant, alpha, count, seed, and
truth label. They must distinguish `real_observed` from
`real_derived_resampled`.

## 19. Results and truth display

Results must display workload source, dataset ID, original/derived variant,
truth label, count, abbreviated source/materialized hashes, seed, selected time
range, target and realized skew, actual cross-shard ratio, category
distribution, unique identities, duplicate source-row ratio, signature and
nonce evidence, and no-fallback status. `real_derived_resampled` must never be
presented as a complete original trace. The existing finality, terminal, and
incomplete definitions remain unchanged.

## 20. Tests and acceptance

Required tests cover CSV schema/required fields/address/hash/price safety,
sale-ID uniqueness with repeated tx hashes, deterministic streaming conversion,
source hash, read-only input, original windows, deterministic/seed-varying
selection, Full, Zipf traceability/skew/cache atomicity, DTO redaction,
preview/create blockers, compiled row propagation, iterator gzip/EOF/malformed
input/memory bounds, identity/nonce/signatures/state keys/actual cross-shard,
frontend invalidation and 1440/1920 layout, and synthetic regression.

End-to-end acceptance includes DCL original 10K and contract-Zipf alpha 1.0
10K real children: completed, no fallback, no orphan, signature/nonce pass,
submitted 10000, coherent terminal/incomplete counts, state-root consistency,
complete artifacts, distinct original/derived materialized hashes, and higher
derived contract concentration. Method comparison reuses one materialized hash.

## 21. Full implementation checkpoints

**Checkpoint A: Dataset and Materialization:** ignore rules, schemas, registry,
validator, converter, original/Zipf materializers, cache, API, unit tests.

**Checkpoint B: Compiler and Real Runtime:** source spec, compatibility, formal
propagation, compiled workload plan, plugin/factory, iterator, identity/nonce/
signing, state keys/cross-shard evidence, artifacts, backend/Go tests.

**Checkpoint C: Frontend, Results, and Artifacts:** library registry, Run source
controls/preview, matrix, results truth and artifact links, frontend tests.

**Checkpoint D: Full Acceptance:** backend/Go/frontend full suites and the two
real 10K children. Checkpoints are sequential; no later checkpoint starts
before the previous one passes. One implementation round may continue internally
until all four pass, without intermediate outward version labels or commits.

## 22. Truth boundary

This design authorizes a research workload data plane only. Before its full
implementation and acceptance pass, no V5 page, API, RunGroup, artifact, or
paper export may claim that Decentraland data is selectable or executable by
`real_cluster`. After implementation it still does not claim a production
blockchain, production PBFT, full Byzantine security, multi-server deployment,
complete Polygon receipt verification, or native Decentraland/Polygon contract
re-execution.

## 23. Known limits

The planned data plane is research-grade replay modelling. It does not prove
dataset-wide on-chain receipt validation, complete raw-fetch archival, live API
availability, production security, or execution equivalence with Decentraland
or Polygon. `raw_fetch_script_archive=partial` remains an explicit provenance
fact. Until implementation and acceptance pass, V5 Formal remains synthetic
only.
