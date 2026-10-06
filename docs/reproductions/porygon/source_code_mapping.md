# Porygon source-code mapping — current MBE adaptation

- `frontend/src/v5MethodProfile.ts`
  - one visible Porygon method card assembled from the complete Porygon plugin family
- `backend/app/services/v5_plugin_manifest_store.py`
  - plugin manifests/capabilities/truth boundaries for admission, sharding, routing, producer, execution, scheduler, executor, state access/storage and cross-shard coordination
- `backend/app/services/v5_formal_plan_validator.py` / `v5_compatibility_engine.py`
  - builtin Porygon composition and fail-closed requirement that the family is selected together
- `executor/v5/porygon_truth_plugins_v36.go` / `executor/v5/registry.go`
  - Porygon signed-access admission and account/object sharding plugins
  - additive runtime factory registration and complete-family composition validation
- `executor/v5/porygon_plugins.go`
  - existing Porygon routing, TransactionBlock producer, execution classification, pipeline scheduler, state access/storage, cross-shard adapter and physical Relay disablement; kept byte-preserved by this closure
- `executor/v5/porygon_state_owner.go`
  - canonical account/object owner extraction used by Porygon StateOwner semantics
- `executor/v5/porygon_txblock.go` / `porygon_witness.go`
  - content-addressed full TransactionBlocks, real Witness Certificate path, transient EC cache and fixed co-located Storage Role persistence/fetch
- `executor/v5/porygon_proposal.go`
  - compact Proposal L/U/T construction, TransactionBlock refs, Proposal.T partition roots and hydration
- `executor/v5/porygon_state_transport.go`
  - Proposal.T-anchored local/remote state projections and T@0-safe anchor encoding
- `executor/v5/porygon_rounds.go`
  - transaction-level 4/6-round lifecycle, Proposal.U queue, certified T snapshots and proposal-height deterministic CTx Pending evidence
- `executor/v5/porygon_pipeline.go`
  - cross-height Ordering/Execution/Materialization pipeline, proposal evidence checks, background execution/durable cursors and measured overlap
- `executor/v5/porygon_committee.go` / `porygon_ec_lifecycle.go`
  - deterministic MBE committee adapter, dynamic ESC membership, Te/Tw/update thresholds and EC lifecycle descriptors
- `executor/v5/porygon_batch_exchange.go`
  - authenticated ESC batch results/certificates and bounded certificate recovery
- `executor/v5/porygon_paper_partition_root.go` / `porygon_multishard.go`
  - Proposal.T+U prospective partition-root authentication and Multi-Shard Update certificates
- `executor/v5/porygon_executor.go`
  - ESC-owned business execution, access-closure checks, CTx quarantine, certified result collection and Porygon truth metrics
- `backend/app/services/v5_porygon_correctness_oracle.py` / `v5_metric_extractor.py`
  - method-specific correctness and mechanism evidence extraction

PBFT implementation files are deliberately outside the Porygon algorithm patch surface; the method only uses the shared PBFT plugin and the existing outer runtime hook needed to separate ordering from Porygon execution/durability.
