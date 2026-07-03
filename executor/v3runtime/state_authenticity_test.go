package v3runtime

import (
	"os"
	"path/filepath"
	"testing"
)

func TestStateAuthenticityPreviewGeneratesRootsProofsAndWitnesses(t *testing.T) {
	chain := ChainProfile{StateStorageUnitCount: 4, StateBackend: StateBackendMerkleTrieMVP}
	experiment := ExperimentProfile{StateBackend: StateBackendMerkleTrieMVP}
	commits := []StateCommit{
		{TxID: "tx_1", StateKey: "asset_1", OldValue: 0, NewValue: 7, BlockHeight: 1, StateStorageUnitID: stateUnit("asset_1", chain), CommitTimeMS: 10},
		{TxID: "tx_2", StateKey: "asset_2", OldValue: 0, NewValue: 9, BlockHeight: 1, StateStorageUnitID: stateUnit("asset_2", chain), CommitTimeMS: 11},
	}
	txResults := []TxResult{
		{TxID: "tx_1", BlockHeight: 1, Deltas: map[string][3]int{"asset_1": {0, 7, 7}}},
		{TxID: "tx_2", BlockHeight: 1, Deltas: map[string][3]int{"asset_2": {0, 9, 9}}},
	}

	preview := RunStateAuthenticityPreview(chain, experiment, txResults, commits)

	if preview.StateBackend != StateBackendMerkleTrieMVP {
		t.Fatalf("unexpected backend: %s", preview.StateBackend)
	}
	if !preview.StateRootEnabled || !preview.PersistentStateEnabled {
		t.Fatal("merkle_trie_mvp should enable persistent/root MVP flags")
	}
	if preview.StateRootCount == 0 || preview.StateProofGeneratedCount != 2 || preview.StateProofVerifiedCount != 2 {
		t.Fatalf("unexpected proof/root counts: %+v", preview)
	}
	if preview.WitnessGeneratedCount != 2 || preview.WitnessVerifiedCount != 2 || preview.StateAuthenticityErrCount != 0 {
		t.Fatalf("unexpected witness counts: %+v", preview)
	}
	firstRoot := preview.RootLog[len(preview.RootLog)-1].StateRoot
	again := RunStateAuthenticityPreview(chain, experiment, txResults, commits)
	if again.RootLog[len(again.RootLog)-1].StateRoot != firstRoot {
		t.Fatal("same input should generate same deterministic root")
	}
	changedCommits := append([]StateCommit{}, commits...)
	changedCommits[1].NewValue = 10
	changed := RunStateAuthenticityPreview(chain, experiment, txResults, changedCommits)
	if changed.RootLog[len(changed.RootLog)-1].StateRoot == firstRoot {
		t.Fatal("state change should alter deterministic root")
	}
}

func TestStateAuthenticityArtifactsAndSummary(t *testing.T) {
	out := t.TempDir()
	chain := ChainProfile{StateStorageUnitCount: 4, StateBackend: StateBackendPersistentKV}
	experiment := ExperimentProfile{StateBackend: StateBackendPersistentKV}
	commits := []StateCommit{{TxID: "tx_1", StateKey: "asset_1", OldValue: 0, NewValue: 3, BlockHeight: 1, StateStorageUnitID: stateUnit("asset_1", chain), CommitTimeMS: 7}}
	txResults := []TxResult{{TxID: "tx_1", BlockHeight: 1, Deltas: map[string][3]int{"asset_1": {0, 3, 3}}}}
	preview := RunStateAuthenticityPreview(chain, experiment, txResults, commits)

	if err := WriteStateAuthenticityArtifacts(out, preview); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"state_storage_log.csv", "state_version_log.csv", "state_root_log.csv", "state_proof_log.csv", "state_proof_verification_log.csv", "witness_log.csv", "witness_verification_log.csv", "state_authenticity_summary.json"} {
		if _, err := os.Stat(filepath.Join(out, name)); err != nil {
			t.Fatalf("missing artifact %s: %v", name, err)
		}
	}
	assertCSVFields(t, filepath.Join(out, "state_root_log.csv"), []string{"shard_id", "block_height", "state_backend", "state_root", "state_key_count", "state_update_count", "root_algorithm", "timestamp_ms"})
	assertCSVFields(t, filepath.Join(out, "state_proof_verification_log.csv"), []string{"tx_id", "key", "shard_id", "state_root", "proof_verified", "verification_error"})

	var summary Summary
	ApplyStateAuthenticityMetrics(&summary, preview)
	if summary.StateBackendSelected != StateBackendPersistentKV || !summary.PersistentStateEnabled || !summary.StateRootEnabled {
		t.Fatalf("summary did not receive state authenticity metrics: %+v", summary)
	}
}
