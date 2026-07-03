package v3runtime

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	StateBackendMemoryKV              = "memory_kv"
	StateBackendPersistentKV          = "persistent_kv"
	StateBackendMerkleTrieMVP         = "merkle_trie_mvp"
	StateBackendEthereumMPTCompatible = "ethereum_mpt_compatible"
	StateRootAlgorithmMerkleTrieMVP   = "merkle_trie_mvp_sha256"
	StateAuthenticityRuntimeTruth     = "state_authenticity_mvp_not_ethereum_compatible_mpt_or_full_stateless_execution"
)

type StateStorageRecord struct {
	Key          string
	Value        int
	Version      int
	ShardID      int
	BlockHeight  int
	StateBackend string
	UpdatedByTx  string
	TimestampMS  int
}

type StateVersionRecord struct {
	Key         string
	OldVersion  int
	NewVersion  int
	OldValue    int
	NewValue    int
	BlockHeight int
	TxID        string
	ShardID     int
}

type StateRootRecord struct {
	ShardID          int
	BlockHeight      int
	StateBackend     string
	StateRoot        string
	StateKeyCount    int
	StateUpdateCount int
	RootAlgorithm    string
	TimestampMS      int
}

type StateProof struct {
	TxID           string
	Key            string
	Value          int
	Version        int
	ShardID        int
	BlockHeight    int
	StateRoot      string
	ProofHashes    []string
	ProofHash      string
	ProofGenerated bool
	ErrorMessage   string
}

type StateProofVerificationRecord struct {
	TxID              string
	Key               string
	ShardID           int
	StateRoot         string
	ProofVerified     bool
	VerificationError string
}

type WitnessRecord struct {
	TxID              string
	RequiredKeyCount  int
	WitnessKeyCount   int
	StateRoot         string
	WitnessHash       string
	MissingKeyCount   int
	InvalidProofCount int
}

type WitnessVerificationRecord struct {
	TxID              string
	WitnessVerified   bool
	VerifiedKeyCount  int
	FailedKeyCount    int
	VerificationError string
}

type StateAuthenticityPreview struct {
	StateBackend              string
	PersistentStateEnabled    bool
	StateRootEnabled          bool
	StorageLog                []StateStorageRecord
	VersionLog                []StateVersionRecord
	RootLog                   []StateRootRecord
	ProofLog                  []StateProof
	ProofVerificationLog      []StateProofVerificationRecord
	WitnessLog                []WitnessRecord
	WitnessVerificationLog    []WitnessVerificationRecord
	StateRootCount            int
	StateKeyCount             int
	StateUpdateCount          int
	StateProofGeneratedCount  int
	StateProofVerifiedCount   int
	StateProofFailedCount     int
	WitnessGeneratedCount     int
	WitnessVerifiedCount      int
	WitnessFailedCount        int
	StateAuthenticityErrCount int
}

type stateCell struct {
	value   int
	version int
}

type rootSnapshot struct {
	root   string
	leaves map[string]string
}

func NormalizeStateBackend(value string) string {
	switch strings.TrimSpace(value) {
	case "", "memory", "hash_state_storage":
		return StateBackendMemoryKV
	case StateBackendMemoryKV, StateBackendPersistentKV, StateBackendMerkleTrieMVP, StateBackendEthereumMPTCompatible:
		return strings.TrimSpace(value)
	default:
		return strings.TrimSpace(value)
	}
}

func IsRunnableStateBackend(value string) bool {
	backend := NormalizeStateBackend(value)
	return backend == StateBackendMemoryKV || backend == StateBackendPersistentKV || backend == StateBackendMerkleTrieMVP
}

func RunStateAuthenticityPreview(chain ChainProfile, experiment ExperimentProfile, txResults []TxResult, commits []StateCommit) StateAuthenticityPreview {
	backend := NormalizeStateBackend(firstNonEmpty(experiment.StateBackend, chain.StateBackend, StateBackendMemoryKV))
	preview := StateAuthenticityPreview{
		StateBackend:           backend,
		PersistentStateEnabled: backend == StateBackendPersistentKV || backend == StateBackendMerkleTrieMVP,
		StateRootEnabled:       backend == StateBackendPersistentKV || backend == StateBackendMerkleTrieMVP,
	}
	if !IsRunnableStateBackend(backend) {
		preview.StateAuthenticityErrCount = 1
		return preview
	}

	stateByShard := map[int]map[string]stateCell{}
	rootByBlockShard := map[string]rootSnapshot{}
	updatesByBlockShard := map[string]int{}
	for _, commit := range commits {
		shardID := commit.StateStorageUnitID
		if _, ok := stateByShard[shardID]; !ok {
			stateByShard[shardID] = map[string]stateCell{}
		}
		oldCell := stateByShard[shardID][commit.StateKey]
		newVersion := oldCell.version + 1
		newCell := stateCell{value: commit.NewValue, version: newVersion}
		stateByShard[shardID][commit.StateKey] = newCell
		preview.StorageLog = append(preview.StorageLog, StateStorageRecord{
			Key: commit.StateKey, Value: commit.NewValue, Version: newVersion, ShardID: shardID, BlockHeight: commit.BlockHeight,
			StateBackend: backend, UpdatedByTx: commit.TxID, TimestampMS: commit.CommitTimeMS,
		})
		preview.VersionLog = append(preview.VersionLog, StateVersionRecord{
			Key: commit.StateKey, OldVersion: oldCell.version, NewVersion: newVersion, OldValue: commit.OldValue,
			NewValue: commit.NewValue, BlockHeight: commit.BlockHeight, TxID: commit.TxID, ShardID: shardID,
		})
		updatesByBlockShard[rootKey(commit.BlockHeight, shardID)]++
		snapshot := buildStateRootSnapshot(stateByShard[shardID])
		rootByBlockShard[rootKey(commit.BlockHeight, shardID)] = snapshot
		preview.RootLog = append(preview.RootLog, StateRootRecord{
			ShardID: shardID, BlockHeight: commit.BlockHeight, StateBackend: backend, StateRoot: snapshot.root,
			StateKeyCount: len(stateByShard[shardID]), StateUpdateCount: updatesByBlockShard[rootKey(commit.BlockHeight, shardID)],
			RootAlgorithm: StateRootAlgorithmMerkleTrieMVP, TimestampMS: commit.CommitTimeMS,
		})
	}

	latestRootByShard := latestRoots(preview.RootLog)
	for _, result := range txResults {
		requiredKeys := requiredStateKeys(result)
		verified := 0
		failed := 0
		witnessRoot := ""
		witnessParts := []string{result.TxID}
		for _, key := range requiredKeys {
			shardID := stateUnitKey(key, chain)
			rootRecord, ok := latestRootByShard[shardID]
			if !ok {
				proof := StateProof{TxID: result.TxID, Key: key, ShardID: shardID, BlockHeight: result.BlockHeight, ProofGenerated: false, ErrorMessage: "missing_state_root"}
				preview.ProofLog = append(preview.ProofLog, proof)
				preview.ProofVerificationLog = append(preview.ProofVerificationLog, StateProofVerificationRecord{TxID: result.TxID, Key: key, ShardID: shardID, ProofVerified: false, VerificationError: "missing_state_root"})
				failed++
				continue
			}
			witnessRoot = rootRecord.StateRoot
			cell := stateByShard[shardID][key]
			snapshot := rootByBlockShard[rootKey(rootRecord.BlockHeight, shardID)]
			proof := generateStateProof(result.TxID, key, cell, shardID, rootRecord.BlockHeight, snapshot)
			preview.ProofLog = append(preview.ProofLog, proof)
			verifyOK, verifyErr := verifyStateProof(proof)
			preview.ProofVerificationLog = append(preview.ProofVerificationLog, StateProofVerificationRecord{
				TxID: result.TxID, Key: key, ShardID: shardID, StateRoot: proof.StateRoot, ProofVerified: verifyOK, VerificationError: verifyErr,
			})
			if proof.ProofGenerated {
				preview.StateProofGeneratedCount++
			}
			if verifyOK {
				preview.StateProofVerifiedCount++
				verified++
			} else {
				preview.StateProofFailedCount++
				failed++
			}
			witnessParts = append(witnessParts, key, strconv.Itoa(cell.value), proof.ProofHash)
		}
		if len(requiredKeys) > 0 {
			preview.WitnessGeneratedCount++
		}
		witnessHash := hashString(strings.Join(witnessParts, "|"))
		preview.WitnessLog = append(preview.WitnessLog, WitnessRecord{
			TxID: result.TxID, RequiredKeyCount: len(requiredKeys), WitnessKeyCount: verified + failed,
			StateRoot: witnessRoot, WitnessHash: witnessHash, MissingKeyCount: 0, InvalidProofCount: failed,
		})
		witnessVerified := len(requiredKeys) > 0 && failed == 0
		if witnessVerified {
			preview.WitnessVerifiedCount++
		} else if len(requiredKeys) > 0 {
			preview.WitnessFailedCount++
		}
		errMsg := ""
		if failed > 0 {
			errMsg = "invalid_or_missing_proof"
		}
		preview.WitnessVerificationLog = append(preview.WitnessVerificationLog, WitnessVerificationRecord{
			TxID: result.TxID, WitnessVerified: witnessVerified, VerifiedKeyCount: verified, FailedKeyCount: failed, VerificationError: errMsg,
		})
	}
	preview.StateRootCount = len(preview.RootLog)
	preview.StateUpdateCount = len(preview.VersionLog)
	preview.StateKeyCount = countStateKeys(stateByShard)
	preview.StateAuthenticityErrCount += preview.StateProofFailedCount + preview.WitnessFailedCount
	return preview
}

func buildStateRootSnapshot(state map[string]stateCell) rootSnapshot {
	keys := make([]string, 0, len(state))
	for key := range state {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	leaves := map[string]string{}
	hashes := []string{}
	for _, key := range keys {
		cell := state[key]
		leaf := hashString("leaf|" + key + "|" + strconv.Itoa(cell.value) + "|" + strconv.Itoa(cell.version))
		leaves[key] = leaf
		hashes = append(hashes, leaf)
	}
	sort.Strings(hashes)
	if len(hashes) == 0 {
		return rootSnapshot{root: hashString("empty"), leaves: leaves}
	}
	return rootSnapshot{root: hashString(strings.Join(hashes, "|")), leaves: leaves}
}

func generateStateProof(txID, key string, cell stateCell, shardID, blockHeight int, snapshot rootSnapshot) StateProof {
	hashes := make([]string, 0, len(snapshot.leaves))
	for _, hash := range snapshot.leaves {
		hashes = append(hashes, hash)
	}
	sort.Strings(hashes)
	leaf, ok := snapshot.leaves[key]
	if !ok {
		return StateProof{TxID: txID, Key: key, ShardID: shardID, BlockHeight: blockHeight, StateRoot: snapshot.root, ProofGenerated: false, ErrorMessage: "missing_key"}
	}
	return StateProof{
		TxID: txID, Key: key, Value: cell.value, Version: cell.version, ShardID: shardID, BlockHeight: blockHeight,
		StateRoot: snapshot.root, ProofHashes: hashes, ProofHash: hashString("proof|" + key + "|" + leaf + "|" + snapshot.root),
		ProofGenerated: true,
	}
}

func verifyStateProof(proof StateProof) (bool, string) {
	if !proof.ProofGenerated {
		return false, firstNonEmpty(proof.ErrorMessage, "proof_not_generated")
	}
	if len(proof.ProofHashes) == 0 {
		return false, "empty_proof_hashes"
	}
	hashes := append([]string{}, proof.ProofHashes...)
	sort.Strings(hashes)
	recomputedRoot := hashString(strings.Join(hashes, "|"))
	if recomputedRoot != proof.StateRoot {
		return false, "root_mismatch"
	}
	leaf := hashString("leaf|" + proof.Key + "|" + strconv.Itoa(proof.Value) + "|" + strconv.Itoa(proof.Version))
	found := false
	for _, hash := range hashes {
		if hash == leaf {
			found = true
			break
		}
	}
	if !found {
		return false, "leaf_missing"
	}
	if proof.ProofHash != hashString("proof|"+proof.Key+"|"+leaf+"|"+proof.StateRoot) {
		return false, "proof_hash_mismatch"
	}
	return true, ""
}

func latestRoots(records []StateRootRecord) map[int]StateRootRecord {
	out := map[int]StateRootRecord{}
	for _, record := range records {
		current, ok := out[record.ShardID]
		if !ok || record.BlockHeight >= current.BlockHeight {
			out[record.ShardID] = record
		}
	}
	return out
}

func requiredStateKeys(result TxResult) []string {
	keys := map[string]bool{}
	for _, key := range result.HomeStateUnitIDs {
		_ = key
	}
	for key := range result.Deltas {
		keys[key] = true
	}
	out := make([]string, 0, len(keys))
	for key := range keys {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func rootKey(blockHeight, shardID int) string {
	return strconv.Itoa(blockHeight) + ":" + strconv.Itoa(shardID)
}

func stateUnitKey(key string, chain ChainProfile) int {
	return stateUnit(key, chain)
}

func countStateKeys(stateByShard map[int]map[string]stateCell) int {
	total := 0
	for _, state := range stateByShard {
		total += len(state)
	}
	return total
}

func hashString(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func ApplyStateAuthenticityMetrics(summary *Summary, preview StateAuthenticityPreview) {
	summary.StateBackendSelected = preview.StateBackend
	summary.PersistentStateEnabled = preview.PersistentStateEnabled
	summary.StateRootEnabled = preview.StateRootEnabled
	summary.StateRootCount = preview.StateRootCount
	summary.StateKeyCount = preview.StateKeyCount
	summary.StateUpdateCount = preview.StateUpdateCount
	summary.StateProofGeneratedCount = preview.StateProofGeneratedCount
	summary.StateProofVerifiedCount = preview.StateProofVerifiedCount
	summary.StateProofFailedCount = preview.StateProofFailedCount
	summary.WitnessGeneratedCount = preview.WitnessGeneratedCount
	summary.WitnessVerifiedCount = preview.WitnessVerifiedCount
	summary.WitnessFailedCount = preview.WitnessFailedCount
	summary.StateAuthenticityErrorCount = preview.StateAuthenticityErrCount
}

func WriteStateAuthenticityArtifacts(out string, preview StateAuthenticityPreview) error {
	if err := writeStateStorageLog(filepath.Join(out, "state_storage_log.csv"), preview.StorageLog); err != nil {
		return err
	}
	if err := writeStateVersionLog(filepath.Join(out, "state_version_log.csv"), preview.VersionLog); err != nil {
		return err
	}
	if err := writeStateRootLog(filepath.Join(out, "state_root_log.csv"), preview.RootLog); err != nil {
		return err
	}
	if err := writeStateProofLog(filepath.Join(out, "state_proof_log.csv"), preview.ProofLog); err != nil {
		return err
	}
	if err := writeStateProofVerificationLog(filepath.Join(out, "state_proof_verification_log.csv"), preview.ProofVerificationLog); err != nil {
		return err
	}
	if err := writeWitnessLog(filepath.Join(out, "witness_log.csv"), preview.WitnessLog); err != nil {
		return err
	}
	if err := writeWitnessVerificationLog(filepath.Join(out, "witness_verification_log.csv"), preview.WitnessVerificationLog); err != nil {
		return err
	}
	payload, _ := json.MarshalIndent(map[string]any{
		"state_backend_selected":         preview.StateBackend,
		"persistent_state_enabled":       preview.PersistentStateEnabled,
		"state_root_enabled":             preview.StateRootEnabled,
		"state_root_count":               preview.StateRootCount,
		"state_key_count":                preview.StateKeyCount,
		"state_update_count":             preview.StateUpdateCount,
		"state_proof_generated_count":    preview.StateProofGeneratedCount,
		"state_proof_verified_count":     preview.StateProofVerifiedCount,
		"state_proof_failed_count":       preview.StateProofFailedCount,
		"witness_generated_count":        preview.WitnessGeneratedCount,
		"witness_verified_count":         preview.WitnessVerifiedCount,
		"witness_failed_count":           preview.WitnessFailedCount,
		"state_authenticity_error_count": preview.StateAuthenticityErrCount,
		"runtime_truth":                  StateAuthenticityRuntimeTruth,
		"not_ethereum_compatible_mpt":    true,
		"not_full_stateless_execution":   true,
		"not_cross_shard_proof_protocol": true,
		"not_production_database":        true,
	}, "", "  ")
	return os.WriteFile(filepath.Join(out, "state_authenticity_summary.json"), payload, 0o644)
}

func writeStateStorageLog(path string, rows []StateStorageRecord) error {
	fields := []string{"key", "value", "version", "shard_id", "block_height", "state_backend", "updated_by_tx", "timestamp_ms"}
	out := [][]string{}
	for _, row := range rows {
		out = append(out, []string{row.Key, strconv.Itoa(row.Value), strconv.Itoa(row.Version), strconv.Itoa(row.ShardID), strconv.Itoa(row.BlockHeight), row.StateBackend, row.UpdatedByTx, strconv.Itoa(row.TimestampMS)})
	}
	return writeCSV(path, fields, out)
}

func writeStateVersionLog(path string, rows []StateVersionRecord) error {
	fields := []string{"key", "old_version", "new_version", "old_value", "new_value", "block_height", "tx_id", "shard_id"}
	out := [][]string{}
	for _, row := range rows {
		out = append(out, []string{row.Key, strconv.Itoa(row.OldVersion), strconv.Itoa(row.NewVersion), strconv.Itoa(row.OldValue), strconv.Itoa(row.NewValue), strconv.Itoa(row.BlockHeight), row.TxID, strconv.Itoa(row.ShardID)})
	}
	return writeCSV(path, fields, out)
}

func writeStateRootLog(path string, rows []StateRootRecord) error {
	fields := []string{"shard_id", "block_height", "state_backend", "state_root", "state_key_count", "state_update_count", "root_algorithm", "timestamp_ms"}
	out := [][]string{}
	for _, row := range rows {
		out = append(out, []string{strconv.Itoa(row.ShardID), strconv.Itoa(row.BlockHeight), row.StateBackend, row.StateRoot, strconv.Itoa(row.StateKeyCount), strconv.Itoa(row.StateUpdateCount), row.RootAlgorithm, strconv.Itoa(row.TimestampMS)})
	}
	return writeCSV(path, fields, out)
}

func writeStateProofLog(path string, rows []StateProof) error {
	fields := []string{"tx_id", "key", "shard_id", "block_height", "state_root", "proof_hash", "proof_node_count", "proof_generated", "error_message"}
	out := [][]string{}
	for _, row := range rows {
		out = append(out, []string{row.TxID, row.Key, strconv.Itoa(row.ShardID), strconv.Itoa(row.BlockHeight), row.StateRoot, row.ProofHash, strconv.Itoa(len(row.ProofHashes)), strconv.FormatBool(row.ProofGenerated), row.ErrorMessage})
	}
	return writeCSV(path, fields, out)
}

func writeStateProofVerificationLog(path string, rows []StateProofVerificationRecord) error {
	fields := []string{"tx_id", "key", "shard_id", "state_root", "proof_verified", "verification_error"}
	out := [][]string{}
	for _, row := range rows {
		out = append(out, []string{row.TxID, row.Key, strconv.Itoa(row.ShardID), row.StateRoot, strconv.FormatBool(row.ProofVerified), row.VerificationError})
	}
	return writeCSV(path, fields, out)
}

func writeWitnessLog(path string, rows []WitnessRecord) error {
	fields := []string{"tx_id", "required_key_count", "witness_key_count", "state_root", "witness_hash", "missing_key_count", "invalid_proof_count"}
	out := [][]string{}
	for _, row := range rows {
		out = append(out, []string{row.TxID, strconv.Itoa(row.RequiredKeyCount), strconv.Itoa(row.WitnessKeyCount), row.StateRoot, row.WitnessHash, strconv.Itoa(row.MissingKeyCount), strconv.Itoa(row.InvalidProofCount)})
	}
	return writeCSV(path, fields, out)
}

func writeWitnessVerificationLog(path string, rows []WitnessVerificationRecord) error {
	fields := []string{"tx_id", "witness_verified", "verified_key_count", "failed_key_count", "verification_error"}
	out := [][]string{}
	for _, row := range rows {
		out = append(out, []string{row.TxID, strconv.FormatBool(row.WitnessVerified), strconv.Itoa(row.VerifiedKeyCount), strconv.Itoa(row.FailedKeyCount), row.VerificationError})
	}
	return writeCSV(path, fields, out)
}
