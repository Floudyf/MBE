# Porygon reproduction deviations — v8

1. **Consensus adaptation.** The paper's ordering consensus is not reproduced as BA★; MBE's shared PBFT is used so all baselines keep the same consensus layer.
2. **Single physical ordering domain with unified frontend shard control.** `topology.shards` is interpreted as the requested Porygon ESC count and is compiled to `execution_shard_count`; all validators still share one physical `porygon-global` PBFT ordering/state domain. MBE physical PBFT shards are therefore not multiplied when the frontend shard count increases.
3. **Storage-node deployment.** MBE v8 does not instantiate separate physical Porygon Storage Node processes. Stateless visibility is approximated by projecting each execution snapshot to its signed AccessList. This boundary is exported in metrics and must be disclosed in the paper.
4. **Pipeline timing.** W/O/E/C and Cross-Batch Witness are represented as logical protocol slots. Because shared MBE PBFT is one-height-in-flight, cross-height wall-clock pipeline speedup is not claimed.
5. **Generic smart-contract state mapping.** The paper's account partition is adapted to MBE workloads: sender identity selects the execution ESC and generic state keys are deterministically mapped to logical state shards.
6. **Conservative state locking.** Cross-ESC state locks are derived from declared AccessList keys; this may serialize more work than an optimized implementation but preserves correctness.
7. **ESC result quorum authentication.** Every production ESC result vote reuses the node's PBFT Ed25519 identity and signs `(block_hash, height, wave, execution_shard_id, result_digest)`. The global leader may aggregate votes, but every validator independently verifies the quorum attestations.
8. **Witness adaptation.** `witness_threshold` is retained as reproduction metadata. MBE does not claim a separate physical Witness Committee quorum; validators recompute the Transaction Block body/access evidence before PBFT voting.
