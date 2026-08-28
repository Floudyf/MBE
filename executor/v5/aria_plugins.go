package v5

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/tx"
)

const ariaBlockProducerID = "aria_block_producer"

const ariaCandidateSelectionEvidenceID = "aria_candidate_selection_v2"
const ariaCandidateSelectionConsensusWireVersion = "aria_candidate_selection_compact_wire_v1"

type ariaBlockProducer struct{ basicPlugin }

func (p ariaBlockProducer) BlockSize() int {
	if value := intValue(p.config["block_size"]); value > 0 {
		return value
	}
	return 100
}

func (p ariaBlockProducer) Interval() time.Duration {
	if value := intValue(p.config["interval_ms"]); value > 0 {
		return time.Duration(value) * time.Millisecond
	}
	return 75 * time.Millisecond
}

// EstimateProposalValidationWork reports the candidate batch size that an Aria
// validator deterministically recomputes before it can vote. This is an
// implementation-cost hint for the shared proposal timeout only; it does not
// change Aria selection semantics or PBFT behavior.
func (p ariaBlockProducer) EstimateProposalValidationWork(block realblock.Block) int {
	work := len(block.TxList)
	if block.ProposalEvidence == nil || block.ProposalEvidence.AlgorithmID != ariaCandidateSelectionEvidenceID {
		return work
	}
	var hint struct {
		CandidateCount int `json:"candidate_count"`
	}
	if err := json.Unmarshal(block.ProposalEvidence.Payload, &hint); err != nil {
		return work
	}
	if hint.CandidateCount > work {
		return hint.CandidateCount
	}
	return work
}

func (p ariaBlockProducer) ShouldProduce(input BlockProductionInput) bool {
	return (input.Pool != nil && input.Pool.Len() > 0) || input.SystemDeltaReady
}

type ariaCandidateSelectionEvidence struct {
	ShardID                 string                   `json:"shard_id"`
	Height                  uint64                   `json:"height"`
	CandidateCount          int                      `json:"candidate_count"`
	ScanLimit               int                      `json:"scan_limit"`
	SelectionLimit          int                      `json:"selection_limit"`
	CandidateTransactions   []tx.SignedTransaction   `json:"candidate_transactions"`
	CandidateTxIDs          []string                 `json:"candidate_tx_ids"`
	CandidateLogicalIDs     []string                 `json:"candidate_logical_ids"`
	CandidatePayloadDigest  string                   `json:"candidate_payload_digest"`
	SelectedTxIDs           []string                 `json:"selected_tx_ids"`
	SelectedLogicalIDs      []string                 `json:"selected_logical_ids"`
	DeferredTxIDs           []string                 `json:"deferred_tx_ids"`
	DeferredLogicalIDs      []string                 `json:"deferred_logical_ids"`
	DeferredReasons         map[string]string        `json:"deferred_reasons"`
	Reordering              bool                     `json:"reordering"`
	ReadOnlyOptimization    bool                     `json:"read_only_optimization"`
	RetryNonceGaps          bool                     `json:"retry_nonce_gaps"`
	SelectionResultDigest   string                   `json:"selection_result_digest"`
	SelectionSemanticDigest string                   `json:"selection_semantic_digest"`
	Metrics                 execution.AriaMetrics    `json:"metrics"`
	Trace                   execution.AriaEpochTrace `json:"trace"`
}

// ariaCandidateSelectionConsensusWire contains only data that is not already
// present in the accepted block body. Deferred signed transactions are retained
// because MBE's fixed-workload retry lifecycle must be able to reconstruct the
// original Aria batch exactly on every validator.
type ariaCandidateSelectionConsensusWire struct {
	WireVersion             string                 `json:"wire_version"`
	ShardID                 string                 `json:"shard_id"`
	Height                  uint64                 `json:"height"`
	CandidateCount          int                    `json:"candidate_count"`
	ScanLimit               int                    `json:"scan_limit"`
	SelectionLimit          int                    `json:"selection_limit"`
	CandidateTxIDs          []string               `json:"candidate_tx_ids"`
	CandidateLogicalIDs     []string               `json:"candidate_logical_ids"`
	CandidatePayloadDigest  string                 `json:"candidate_payload_digest"`
	SelectedTxIDs           []string               `json:"selected_tx_ids"`
	SelectedLogicalIDs      []string               `json:"selected_logical_ids"`
	DeferredTxIDs           []string               `json:"deferred_tx_ids"`
	DeferredLogicalIDs      []string               `json:"deferred_logical_ids"`
	DeferredTransactions    []tx.SignedTransaction `json:"deferred_transactions,omitempty"`
	DeferredReasons         map[string]string      `json:"deferred_reasons"`
	Reordering              bool                   `json:"reordering"`
	ReadOnlyOptimization    bool                   `json:"read_only_optimization"`
	RetryNonceGaps          bool                   `json:"retry_nonce_gaps"`
	SelectionResultDigest   string                 `json:"selection_result_digest"`
	SelectionSemanticDigest string                 `json:"selection_semantic_digest"`
	// Metrics are small scalar evidence used by existing paper exports. They are
	// retained for compatibility; the O(n) execution Trace is deliberately local.
	Metrics execution.AriaMetrics `json:"metrics"`
}

type ariaSelectionCommitment struct {
	CandidatePayloadDigest string            `json:"candidate_payload_digest"`
	SelectionResultDigest  string            `json:"selection_result_digest"`
	SelectionLimit         int               `json:"selection_limit"`
	SelectedTxIDs          []string          `json:"selected_tx_ids"`
	SelectedLogicalIDs     []string          `json:"selected_logical_ids"`
	DeferredTxIDs          []string          `json:"deferred_tx_ids"`
	DeferredLogicalIDs     []string          `json:"deferred_logical_ids"`
	DeferredReasons        map[string]string `json:"deferred_reasons"`
	WAWDependencyCount     int               `json:"waw_dependency_count"`
	RAWDependencyCount     int               `json:"raw_dependency_count"`
	WARDependencyCount     int               `json:"war_dependency_count"`
	ConflictAbortCount     int               `json:"conflict_abort_count"`
	RetryableNonceCount    int               `json:"retryable_nonce_count"`
	ApplicationFailure     int               `json:"application_failure_count"`
	ReadOnlyFastCommit     int               `json:"read_only_fast_commit_count"`
	Reordering             bool              `json:"reordering"`
	ReadOnlyOptimization   bool              `json:"read_only_optimization"`
	RetryNonceGaps         bool              `json:"retry_nonce_gaps"`
}

func (p ariaBlockProducer) BuildCandidate(input BlockProductionInput) (realblock.Block, error) {
	if input.Proposer == nil || input.Pool == nil {
		return realblock.Block{}, fmt.Errorf("aria block producer requires proposer and pool")
	}
	if input.BaseStateSnapshot == nil {
		return realblock.Block{}, fmt.Errorf("aria block producer requires block-start state snapshot")
	}
	limit := input.Limit
	if limit <= 0 {
		limit = p.BlockSize()
	}
	if limit < 1 {
		return realblock.Block{}, fmt.Errorf("aria block producer requires positive block size")
	}
	multiplier := intValue(p.config["candidate_scan_multiplier"])
	if multiplier < 1 {
		// Canonical Aria: one conflict-analysis epoch is one formal batch.
		// A wider scan changes the epoch conflict set before the block cap.
		multiplier = 1
	}
	depth := input.Pool.Len()
	scanLimit := depth
	maxIntValue := int(^uint(0) >> 1)
	if limit <= maxIntValue/multiplier {
		scanLimit = limit * multiplier
		if scanLimit > depth {
			scanLimit = depth
		}
	}
	candidates := input.Pool.ReserveReady(scanLimit)
	if len(candidates) == 0 {
		return realblock.Block{}, fmt.Errorf("empty_mempool")
	}
	selectionLimit := limit
	if selectionLimit > len(candidates) {
		selectionLimit = len(candidates)
	}

	ctx := input.Context
	if ctx == nil {
		ctx = context.Background()
	}
	workerCount := input.WorkerCount
	if workerCount < 1 {
		workerCount = 1
	}
	executor := execution.NewAriaExecutor(workerCount)
	applyAriaExecutionConfig(executor, p.config)
	selection, err := executor.SelectCandidateTransactions(
		ctx,
		input.Proposer.ShardID,
		input.Proposer.NextHeight,
		candidates,
		input.BaseStateSnapshot,
		selectionLimit,
	)
	if err != nil {
		input.Pool.ReleaseReserved(candidates)
		return realblock.Block{}, err
	}
	if len(selection.Deferred) > 0 {
		input.Pool.ReleaseReserved(selection.Deferred)
	}
	if len(selection.Selected) == 0 {
		return realblock.Block{}, fmt.Errorf("empty_mempool")
	}

	now := input.Now
	if now.IsZero() {
		now = time.Now()
	}
	candidate, err := input.Proposer.BuildFromReserved(selection.Selected, now)
	if err != nil {
		input.Pool.ReleaseReserved(selection.Selected)
		return realblock.Block{}, err
	}
	evidence := ariaCandidateSelectionEvidence{
		ShardID:                candidate.ShardID,
		Height:                 candidate.Height,
		CandidateCount:         len(candidates),
		ScanLimit:              scanLimit,
		SelectionLimit:         selectionLimit,
		CandidateTransactions:  append([]tx.SignedTransaction(nil), candidates...),
		CandidateTxIDs:         transactionIDs(candidates),
		CandidateLogicalIDs:    logicalTransactionIDs(candidates),
		CandidatePayloadDigest: stableJSONDigest(candidates),
		SelectedTxIDs:          transactionIDs(selection.Selected),
		SelectedLogicalIDs:     logicalTransactionIDs(selection.Selected),
		DeferredTxIDs:          transactionIDs(selection.Deferred),
		DeferredLogicalIDs:     logicalTransactionIDs(selection.Deferred),
		DeferredReasons:        cloneStringMap(selection.DeferredReasons),
		Reordering:             executor.Reordering,
		ReadOnlyOptimization:   executor.ReadOnlyOptimization,
		RetryNonceGaps:         executor.RetryNonceGaps,
		SelectionResultDigest:  stableJSONDigest(selection.SelectedDeltas),
		Metrics:                selection.Metrics,
		Trace:                  selection.Trace,
	}
	evidence.SelectionSemanticDigest = ariaSelectionSemanticDigest(evidence, selection)
	wire := ariaCandidateSelectionConsensusWire{
		WireVersion:             ariaCandidateSelectionConsensusWireVersion,
		ShardID:                 evidence.ShardID,
		Height:                  evidence.Height,
		CandidateCount:          evidence.CandidateCount,
		ScanLimit:               evidence.ScanLimit,
		SelectionLimit:          evidence.SelectionLimit,
		CandidateTxIDs:          append([]string(nil), evidence.CandidateTxIDs...),
		CandidateLogicalIDs:     append([]string(nil), evidence.CandidateLogicalIDs...),
		CandidatePayloadDigest:  evidence.CandidatePayloadDigest,
		SelectedTxIDs:           append([]string(nil), evidence.SelectedTxIDs...),
		SelectedLogicalIDs:      append([]string(nil), evidence.SelectedLogicalIDs...),
		DeferredTxIDs:           append([]string(nil), evidence.DeferredTxIDs...),
		DeferredLogicalIDs:      append([]string(nil), evidence.DeferredLogicalIDs...),
		DeferredTransactions:    append([]tx.SignedTransaction(nil), selection.Deferred...),
		DeferredReasons:         cloneStringMap(evidence.DeferredReasons),
		Reordering:              evidence.Reordering,
		ReadOnlyOptimization:    evidence.ReadOnlyOptimization,
		RetryNonceGaps:          evidence.RetryNonceGaps,
		SelectionResultDigest:   evidence.SelectionResultDigest,
		SelectionSemanticDigest: evidence.SelectionSemanticDigest,
		Metrics:                 evidence.Metrics,
	}
	if err := attachProposalEvidence(&candidate, ariaCandidateSelectionEvidenceID, wire); err != nil {
		input.Pool.ReleaseReserved(selection.Selected)
		return realblock.Block{}, err
	}
	return candidate, nil
}

func ariaSelectionSemanticDigest(evidence ariaCandidateSelectionEvidence, selection execution.AriaCandidateSelection) string {
	return stableJSONDigest(ariaSelectionCommitment{
		CandidatePayloadDigest: evidence.CandidatePayloadDigest,
		SelectionResultDigest:  stableJSONDigest(selection.SelectedDeltas),
		SelectionLimit:         evidence.SelectionLimit,
		SelectedTxIDs:          transactionIDs(selection.Selected),
		SelectedLogicalIDs:     logicalTransactionIDs(selection.Selected),
		DeferredTxIDs:          transactionIDs(selection.Deferred),
		DeferredLogicalIDs:     logicalTransactionIDs(selection.Deferred),
		DeferredReasons:        cloneStringMap(selection.DeferredReasons),
		WAWDependencyCount:     selection.Metrics.WAWDependencyCount,
		RAWDependencyCount:     selection.Metrics.RAWDependencyCount,
		WARDependencyCount:     selection.Metrics.WARDependencyCount,
		ConflictAbortCount:     selection.Metrics.ConflictAbortCount,
		RetryableNonceCount:    selection.Metrics.RetryableNonceCount,
		ApplicationFailure:     selection.Metrics.ApplicationFailureCount,
		ReadOnlyFastCommit:     selection.Metrics.ReadOnlyFastCommitCount,
		Reordering:             evidence.Reordering,
		ReadOnlyOptimization:   evidence.ReadOnlyOptimization,
		RetryNonceGaps:         evidence.RetryNonceGaps,
	})
}

func decodeAriaCandidateSelectionEvidence(candidate realblock.Block) (ariaCandidateSelectionEvidence, error) {
	var evidence ariaCandidateSelectionEvidence
	if candidate.ProposalEvidence == nil || candidate.ProposalEvidence.AlgorithmID != ariaCandidateSelectionEvidenceID {
		return evidence, fmt.Errorf("aria block requires full candidate selection evidence")
	}
	var header struct {
		WireVersion string `json:"wire_version"`
	}
	if err := json.Unmarshal(candidate.ProposalEvidence.Payload, &header); err != nil {
		return evidence, fmt.Errorf("decode aria candidate selection evidence header: %w", err)
	}
	if header.WireVersion == "" {
		// Historical v2 payloads carried the complete candidate batch plus Trace.
		if err := json.Unmarshal(candidate.ProposalEvidence.Payload, &evidence); err != nil {
			return evidence, fmt.Errorf("decode legacy aria candidate selection evidence: %w", err)
		}
		return validateAriaCandidateSelectionEvidence(candidate, evidence)
	}
	if header.WireVersion != ariaCandidateSelectionConsensusWireVersion {
		return evidence, fmt.Errorf("unsupported aria candidate evidence wire %q", header.WireVersion)
	}
	var wire ariaCandidateSelectionConsensusWire
	if err := json.Unmarshal(candidate.ProposalEvidence.Payload, &wire); err != nil {
		return evidence, fmt.Errorf("decode compact aria candidate selection evidence: %w", err)
	}
	if !sameStringList(transactionIDs(wire.DeferredTransactions), wire.DeferredTxIDs) ||
		!sameStringList(logicalTransactionIDs(wire.DeferredTransactions), wire.DeferredLogicalIDs) {
		return evidence, fmt.Errorf("aria compact deferred transaction identity mismatch")
	}
	acceptedByID := make(map[string]tx.SignedTransaction, len(candidate.TxList))
	for _, item := range candidate.TxList {
		if item.TxID == "" || acceptedByID[item.TxID].TxID != "" {
			return evidence, fmt.Errorf("aria accepted block contains duplicate or empty transaction id %s", item.TxID)
		}
		acceptedByID[item.TxID] = item
	}
	deferredByID := make(map[string]tx.SignedTransaction, len(wire.DeferredTransactions))
	for _, item := range wire.DeferredTransactions {
		if item.TxID == "" || deferredByID[item.TxID].TxID != "" || acceptedByID[item.TxID].TxID != "" {
			return evidence, fmt.Errorf("aria compact deferred transaction set is invalid for %s", item.TxID)
		}
		deferredByID[item.TxID] = item
	}
	reconstructed := make([]tx.SignedTransaction, 0, len(wire.CandidateTxIDs))
	for _, txID := range wire.CandidateTxIDs {
		if item, ok := acceptedByID[txID]; ok {
			reconstructed = append(reconstructed, item)
			continue
		}
		item, ok := deferredByID[txID]
		if !ok {
			return evidence, fmt.Errorf("aria compact candidate %s cannot be reconstructed", txID)
		}
		reconstructed = append(reconstructed, item)
	}
	evidence = ariaCandidateSelectionEvidence{
		ShardID:                 wire.ShardID,
		Height:                  wire.Height,
		CandidateCount:          wire.CandidateCount,
		ScanLimit:               wire.ScanLimit,
		SelectionLimit:          wire.SelectionLimit,
		CandidateTransactions:   reconstructed,
		CandidateTxIDs:          append([]string(nil), wire.CandidateTxIDs...),
		CandidateLogicalIDs:     append([]string(nil), wire.CandidateLogicalIDs...),
		CandidatePayloadDigest:  wire.CandidatePayloadDigest,
		SelectedTxIDs:           append([]string(nil), wire.SelectedTxIDs...),
		SelectedLogicalIDs:      append([]string(nil), wire.SelectedLogicalIDs...),
		DeferredTxIDs:           append([]string(nil), wire.DeferredTxIDs...),
		DeferredLogicalIDs:      append([]string(nil), wire.DeferredLogicalIDs...),
		DeferredReasons:         cloneStringMap(wire.DeferredReasons),
		Reordering:              wire.Reordering,
		ReadOnlyOptimization:    wire.ReadOnlyOptimization,
		RetryNonceGaps:          wire.RetryNonceGaps,
		SelectionResultDigest:   wire.SelectionResultDigest,
		SelectionSemanticDigest: wire.SelectionSemanticDigest,
		Metrics:                 wire.Metrics,
	}
	return validateAriaCandidateSelectionEvidence(candidate, evidence)
}

func validateAriaCandidateSelectionEvidence(candidate realblock.Block, evidence ariaCandidateSelectionEvidence) (ariaCandidateSelectionEvidence, error) {
	if evidence.ShardID != candidate.ShardID || evidence.Height != candidate.Height {
		return evidence, fmt.Errorf("aria candidate evidence block identity mismatch")
	}
	if evidence.CandidateCount != len(evidence.CandidateTransactions) || evidence.CandidateCount != len(evidence.CandidateTxIDs) || evidence.CandidateCount != len(evidence.CandidateLogicalIDs) {
		return evidence, fmt.Errorf("aria candidate evidence count mismatch")
	}
	if len(evidence.SelectedTxIDs) != len(evidence.SelectedLogicalIDs) || len(evidence.DeferredTxIDs) != len(evidence.DeferredLogicalIDs) || len(evidence.SelectedTxIDs)+len(evidence.DeferredTxIDs) != evidence.CandidateCount {
		return evidence, fmt.Errorf("aria selected/deferred evidence count mismatch")
	}
	if evidence.SelectionLimit < 1 || evidence.SelectionLimit > evidence.CandidateCount {
		return evidence, fmt.Errorf("aria candidate evidence invalid selection limit: %d", evidence.SelectionLimit)
	}
	if !sameStringList(transactionIDs(evidence.CandidateTransactions), evidence.CandidateTxIDs) ||
		!sameStringList(logicalTransactionIDs(evidence.CandidateTransactions), evidence.CandidateLogicalIDs) {
		return evidence, fmt.Errorf("aria candidate transaction identity mismatch")
	}
	candidateSeen := map[string]bool{}
	candidateByID := make(map[string]tx.SignedTransaction, len(evidence.CandidateTransactions))
	for _, item := range evidence.CandidateTransactions {
		if item.TxID == "" || candidateSeen[item.TxID] {
			return evidence, fmt.Errorf("aria candidate batch contains duplicate or empty transaction id %s", item.TxID)
		}
		candidateSeen[item.TxID] = true
		candidateByID[item.TxID] = item
	}
	selectedPayloads := make([]tx.SignedTransaction, 0, len(evidence.SelectedTxIDs))
	for _, txID := range evidence.SelectedTxIDs {
		item, ok := candidateByID[txID]
		if !ok {
			return evidence, fmt.Errorf("aria selected transaction %s is not in candidate batch", txID)
		}
		selectedPayloads = append(selectedPayloads, item)
	}
	if stableJSONDigest(selectedPayloads) != stableJSONDigest(candidate.TxList) {
		return evidence, fmt.Errorf("aria selected transaction payload mismatch")
	}
	if !sameStringList(evidence.SelectedTxIDs, candidate.TxIDs) {
		return evidence, fmt.Errorf("aria selected transaction list mismatch")
	}
	// Preserve the historical precise selected-payload rejection before the
	// whole-batch commitment check. Deferred payload tampering still falls
	// through to CandidatePayloadDigest below.
	if stableJSONDigest(evidence.CandidateTransactions) != evidence.CandidatePayloadDigest {
		return evidence, fmt.Errorf("aria candidate payload mismatch: digest commitment mismatch")
	}
	seen := map[string]string{}
	for _, txID := range evidence.SelectedTxIDs {
		if txID == "" || seen[txID] != "" {
			return evidence, fmt.Errorf("aria selection contains duplicate or empty transaction id")
		}
		seen[txID] = "selected"
	}
	for _, txID := range evidence.DeferredTxIDs {
		if txID == "" || seen[txID] != "" {
			return evidence, fmt.Errorf("aria selection partition contains duplicate transaction id %s", txID)
		}
		if evidence.DeferredReasons[txID] == "" {
			return evidence, fmt.Errorf("aria deferred transaction %s is missing its reason", txID)
		}
		seen[txID] = "deferred"
	}
	for _, txID := range evidence.CandidateTxIDs {
		if seen[txID] == "" {
			return evidence, fmt.Errorf("aria candidate %s is missing from selected/deferred partition", txID)
		}
	}
	if len(seen) != evidence.CandidateCount {
		return evidence, fmt.Errorf("aria selected/deferred partition size mismatch")
	}
	if evidence.SelectionResultDigest == "" {
		return evidence, fmt.Errorf("aria selection result digest is empty")
	}
	if evidence.SelectionSemanticDigest == "" {
		return evidence, fmt.Errorf("aria selection semantic digest is empty")
	}
	return evidence, nil
}

func recomputeAriaCandidateSelection(ctx context.Context, candidate realblock.Block, base map[string]string, workerCount int, config map[string]any) (execution.AriaCandidateSelection, ariaCandidateSelectionEvidence, error) {
	evidence, err := decodeAriaCandidateSelectionEvidence(candidate)
	if err != nil {
		return execution.AriaCandidateSelection{}, evidence, err
	}
	executor := execution.NewAriaExecutor(workerCount)
	applyAriaExecutionConfig(executor, config)
	if executor.Reordering != evidence.Reordering || executor.ReadOnlyOptimization != evidence.ReadOnlyOptimization || executor.RetryNonceGaps != evidence.RetryNonceGaps {
		return execution.AriaCandidateSelection{}, evidence, fmt.Errorf("aria evidence/config mismatch")
	}
	selection, err := executor.SelectCandidateTransactions(ctx, candidate.ShardID, candidate.Height, evidence.CandidateTransactions, base, evidence.SelectionLimit)
	if err != nil {
		return selection, evidence, err
	}
	if !sameStringList(transactionIDs(selection.Selected), evidence.SelectedTxIDs) ||
		!sameStringList(logicalTransactionIDs(selection.Selected), evidence.SelectedLogicalIDs) ||
		!sameStringList(transactionIDs(selection.Deferred), evidence.DeferredTxIDs) ||
		!sameStringList(logicalTransactionIDs(selection.Deferred), evidence.DeferredLogicalIDs) {
		return selection, evidence, fmt.Errorf("aria validator recomputation selected/deferred mismatch")
	}
	if digest := stableJSONDigest(selection.SelectedDeltas); digest != evidence.SelectionResultDigest {
		return selection, evidence, fmt.Errorf("aria validator recomputation selected-result digest mismatch")
	}
	if digest := ariaSelectionSemanticDigest(evidence, selection); digest != evidence.SelectionSemanticDigest {
		return selection, evidence, fmt.Errorf("aria validator recomputation semantic digest mismatch")
	}
	return selection, evidence, nil
}

func sameStringList(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func applyAriaExecutionConfig(executor *execution.AriaExecutor, config map[string]any) {
	if executor == nil {
		return
	}
	if value, ok := config["reordering"].(bool); ok {
		executor.Reordering = value
	}
	if value, ok := config["read_only_optimization"].(bool); ok {
		executor.ReadOnlyOptimization = value
	}
	if value, ok := config["retry_nonce_gaps"].(bool); ok {
		executor.RetryNonceGaps = value
	}
}

func transactionIDs(items []tx.SignedTransaction) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		out = append(out, item.TxID)
	}
	return out
}

func logicalTransactionIDs(items []tx.SignedTransaction) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		out = append(out, tx.SemanticID(item))
	}
	return out
}

func cloneStringMap(input map[string]string) map[string]string {
	out := make(map[string]string, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func ariaBoolConfig(config map[string]any, key string, fallback bool) bool {
	if config == nil {
		return fallback
	}
	value, ok := config[key].(bool)
	if !ok {
		return fallback
	}
	return value
}

func registerAriaPlugins(register func(string, string, Factory)) {
	register("block_producer", ariaBlockProducerID, func(c map[string]any) (Plugin, error) {
		return ariaBlockProducer{makeBasic("block_producer", ariaBlockProducerID, c)}, nil
	})
}

func validateAriaPluginCombination(plugins RuntimePlugins) error {
	producerSelected := plugins.BlockProducer != nil && plugins.BlockProducer.ID() == ariaBlockProducerID
	executorSelected := plugins.BlockExecutor != nil && plugins.BlockExecutor.ID() == execution.AriaBlockExecutorID
	if producerSelected != executorSelected {
		return fmt.Errorf("aria_block_producer and aria_block_executor must be selected together")
	}
	if !producerSelected {
		return nil
	}
	producer, producerOK := plugins.BlockProducer.(ariaBlockProducer)
	executor, executorOK := plugins.BlockExecutor.(ariaBlockExecutor)
	if !producerOK || !executorOK {
		return fmt.Errorf("aria plugin implementation type mismatch")
	}
	for _, key := range []string{"reordering", "read_only_optimization", "retry_nonce_gaps"} {
		producerValue := ariaBoolConfig(producer.config, key, true)
		executorValue := ariaBoolConfig(executor.config, key, true)
		if producerValue != executorValue {
			return fmt.Errorf("aria producer/executor %s mismatch: producer=%t executor=%t", key, producerValue, executorValue)
		}
	}
	required := []struct {
		category string
		actual   Plugin
		id       string
	}{
		{"routing", plugins.Routing, "hash_routing_baseline"},
		{"execution", plugins.Execution, "serial_execution_baseline"},
		{"scheduler", plugins.Scheduler, "fifo_serial_scheduler"},
		{"state_access", plugins.StateAccess, "direct_state_access"},
		{"state_storage", plugins.StateStorage, "persistent_local_state_store"},
		{"commit", plugins.Commit, "normal_commit"},
	}
	for _, item := range required {
		if item.actual == nil || item.actual.ID() != item.id {
			actual := "<nil>"
			if item.actual != nil {
				actual = item.actual.ID()
			}
			return fmt.Errorf("aria requires %s:%s, got %s", item.category, item.id, actual)
		}
	}
	return nil
}
