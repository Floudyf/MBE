# V3.9 State Authenticity Layer MVP Plan

## 1. Goal

V3.9 adds a State Authenticity Layer MVP to MBE so the platform no longer only emits logical state access logs. It can generate reproducible state roots, state proofs, witness artifacts, and minimum proof generation / proof verification outputs.

V3.9 = State Authenticity Layer MVP.

V3.9 is not Ethereum-compatible MPT, not a production stateless client, not a complete cross-shard state proof protocol, and not a full stateless blockchain.

V3.5 completed node topology, launcher preview, and node process preview. V3.6 completed NetworkAdapter and typed message runtime. V3.7 completed configurable ConsensusRuntime and PBFT preview over NetworkAdapter. V3.8 completed CrossShardProtocol skeleton and relay_preview artifacts. V3.9 strengthens StateAccess / StateStorage / Commit authenticity observability on top of those boundaries.

## 2. V3.9 Scope

V3.9 implements:

- persistent state backend MVP
- `merkle_trie_mvp` / MPT-like trie backend
- deterministic state root generation
- state proof generation
- state proof verification
- stateless witness artifact generation
- witness verification log
- state authenticity artifacts
- summary metrics
- minimal frontend display
- V3.9 closure

## 3. Non-goals

V3.9 does not implement Ethereum-compatible MPT, production database durability, full stateless execution, a full stateless blockchain, a complete cross-shard state proof protocol, fraud proof / validity proof, atomic cross-shard verified commit, Fabric/EVM live backend, BlockEmulator backend, or paper-grade benchmark evidence.

## 4. StateBackend Options

```yaml
state_backend:
  - memory_kv
  - persistent_kv
  - merkle_trie_mvp
  - ethereum_mpt_compatible
```

Option status:

- `memory_kv`: existing compatibility path.
- `persistent_kv`: runnable in V3.9.
- `merkle_trie_mvp`: runnable in V3.9.
- `ethereum_mpt_compatible`: planned only, not implemented in V3.9.

If MBE has not implemented Ethereum hexary Patricia trie, nibble paths, RLP encoding, and branch/extension/leaf node rules, it must not claim Ethereum-compatible MPT. V3.9 may implement a Merkle/MPT-like trie MVP, but it must not masquerade as Ethereum MPT. `state_backend` must remain selectable and must not be hardcoded to `merkle_trie_mvp`.

## 5. Persistent State Backend

`persistent_kv` supports put / get, key / value / version / shard_id / block_height, deterministic snapshot output, and state snapshot or table artifacts. It may use local JSON/CSV/file-backed KV MVP files under the run output directory. It does not claim production database durability.

## 6. Merkle / MPT-like State Backend

`merkle_trie_mvp` generates deterministic `state_root` after state writes. The same input state must produce the same root, and state changes must change the root. Each shard can maintain an independent state root, and each block height records roots. It supports proof generation and proof verification through a simplified Merkle/MPT-like structure, not Ethereum-compatible MPT.

## 7. State Root Generation

State root records include:

- `shard_id`
- `block_height`
- `state_backend`
- `state_root`
- `state_key_count`
- `state_update_count`
- `root_algorithm`
- `timestamp_ms`

The root algorithm for V3.9 is `merkle_trie_mvp_sha256`. Root generation must be deterministic, stable for the same state, different when state changes, and recorded by shard and block height.

## 8. State Proof Generation and Verification

V3.9 implements:

```text
generate_proof(key, root)
verify_proof(key, value, proof, root)
```

Proof records include key, value, shard_id, block_height, state_root, proof nodes or proof hashes, proof_hash, and proof_node_count.

Verification records include tx_id, key, shard_id, state_root, proof_verified, and verification_error.

Proof verification must execute real deterministic hash checks. It is not an estimated flag. It is also not an Ethereum-compatible Merkle proof and not a complete cross-shard state proof protocol.

## 9. Stateless Witness MVP

V3.9 defines witness as:

```text
witness = tx required state keys + values + proofs + state_root
```

Witness records include tx_id, required_keys, state_values, state_proofs, state_root, proof_verified, missing_keys, and invalid_proofs.

V3.9 generates witness artifacts, records witness verification logs, verifies proofs for each tx required key, and counts witness generated / verified / failed. If execution still directly accesses local StateStorage, this remains a stateless witness artifact MVP, not full stateless execution or a full stateless blockchain.

## 10. Artifacts

V3.9 artifacts:

- `state_storage_log.csv`: state key/value/version/backend rows.
- `state_version_log.csv`: old/new versions and values by tx.
- `state_root_log.csv`: deterministic root by shard and block height.
- `state_proof_log.csv`: generated proof rows.
- `state_proof_verification_log.csv`: proof verification results.
- `witness_log.csv`: tx-level witness summary.
- `witness_verification_log.csv`: tx-level witness verification results.
- `state_authenticity_summary.json`: summary metrics and truth boundary.

`state_storage_log.csv` fields:

```text
key,value,version,shard_id,block_height,state_backend,updated_by_tx,timestamp_ms
```

`state_version_log.csv` fields:

```text
key,old_version,new_version,old_value,new_value,block_height,tx_id,shard_id
```

`state_root_log.csv` fields:

```text
shard_id,block_height,state_backend,state_root,state_key_count,state_update_count,root_algorithm,timestamp_ms
```

`state_proof_log.csv` fields:

```text
tx_id,key,shard_id,block_height,state_root,proof_hash,proof_node_count,proof_generated,error_message
```

`state_proof_verification_log.csv` fields:

```text
tx_id,key,shard_id,state_root,proof_verified,verification_error
```

`witness_log.csv` fields:

```text
tx_id,required_key_count,witness_key_count,state_root,witness_hash,missing_key_count,invalid_proof_count
```

`witness_verification_log.csv` fields:

```text
tx_id,witness_verified,verified_key_count,failed_key_count,verification_error
```

`state_authenticity_summary.json` fields:

```text
state_backend_selected
persistent_state_enabled
state_root_enabled
state_root_count
state_key_count
state_update_count
state_proof_generated_count
state_proof_verified_count
state_proof_failed_count
witness_generated_count
witness_verified_count
witness_failed_count
state_authenticity_error_count
runtime_truth
```

## 11. Summary Metrics

Required summary metrics:

- `state_backend_selected`
- `persistent_state_enabled`
- `state_root_enabled`
- `state_root_count`
- `state_key_count`
- `state_update_count`
- `state_proof_generated_count`
- `state_proof_verified_count`
- `state_proof_failed_count`
- `witness_generated_count`
- `witness_verified_count`
- `witness_failed_count`
- `state_authenticity_error_count`

Reserved optional metrics:

- `state_root_algorithm`
- `state_backend_error_count`
- `witness_missing_key_count`
- `witness_invalid_proof_count`

## 12. Frontend Layout Rule

V3.9 must not add StateProof or Witness as new main-flow cards.

Forbidden frontend changes:

- Do not add a StateProof main-flow card.
- Do not add a Witness main-flow card.
- Do not change the number of main-flow cards.
- Do not refactor the V3 Composer page.
- Do not change the left navigation.
- Do not add a complex multi-page workspace.

The main transaction flow remains:

```text
Workload -> TxPool -> BlockProducer -> ConsensusRuntime -> CommitteeEpoch -> Routing/Sharding -> Execution -> StateAccess -> StateStorage -> Commit -> MetricsReport
```

StateProof and Witness belong under StateAccess / StateStorage / Commit as sub-capabilities.

Allowed minimal frontend changes:

- Show `state_backend` in StateStorage detail or compact runtime config.
- Show `state_root_enabled` / proof verification under StateAccess / StateStorage / Commit details.
- Add a `state_backend` selector in the existing configuration area.
- Add State Authenticity summary in the result panel.
- Add State Authenticity artifacts to ArtifactGroups.
- Treat old runs without V3.9 artifacts as legacy missing, not errors.

## 13. Relationship with Previous Stages

V3.9 reuses V3.5 topology / shard_count / state storage unit count, V3.6 NetworkAdapter and typed message boundary, V3.7 ConsensusRuntime and PBFT preview boundary, and V3.8 CrossShardProtocol skeleton boundary.

V3.9 does not modify V3.6 NetworkAdapter semantics, V3.7 PBFT over NetworkAdapter semantics, V3.8 CrossShardProtocol skeleton semantics, or main flow card layout.

## 14. Truth Boundary

V3.9 can claim:

- persistent state backend MVP
- Merkle/MPT-like state root MVP
- state proof generation MVP
- state proof verification MVP
- stateless witness artifact MVP
- state authenticity artifacts
- frontend minimal state authenticity summary

V3.9 cannot claim:

- Ethereum-compatible MPT
- production database durability
- full stateless execution
- full stateless blockchain
- full cross-shard state proof protocol
- fraud proof / validity proof
- atomic cross-shard verified commit
- Fabric/EVM live backend
- BlockEmulator backend
- paper-grade benchmark evidence

Runtime truth:

```text
state_authenticity_mvp_not_ethereum_compatible_mpt_or_full_stateless_execution
```

## 15. Acceptance Criteria

V3.9 is complete when `state_backend` is backend validated, Go runtime recognizes the selected backend, `memory_kv` remains runnable, `persistent_kv` is runnable, `merkle_trie_mvp` is runnable, `ethereum_mpt_compatible` remains planned-only, deterministic state roots are generated, state proofs are generated and verified, witness artifacts and verification logs are written, all V3.9 artifacts are emitted, summary metrics are visible, the frontend shows State Authenticity summary, ArtifactGroups can download state authenticity artifacts, no StateProof / Witness main-flow card is added, README / execution plan / skill are updated to V3.9 closure, and tests pass.

## 16. Next Stage

V3.10 Benchmark / Experiment Template Hardening is planned next. V3.10 has not started.
