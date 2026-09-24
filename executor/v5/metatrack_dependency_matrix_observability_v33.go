package v5

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"metaverse-chainlab/executor/realism/tx"
)

// MBE_METATRACK_DEPENDENCY_MATRIX_OBSERVABILITY_V33
// This file is deliberately observation-only. It reuses the same signed,
// pre-execution StateVersions and exact-value semantics as the current V11
// effective-frontier classifier, but it is invoked only while artifacts are
// written after the run. It cannot alter track identity, StateReady, dispatch,
// PBFT, execution, state application, or measured transaction finality.

type metaTrackDependencyStructureBlockCapture struct {
	Height       uint64
	BlockHash    string
	Transactions []tx.SignedTransaction
	Decisions    map[string]ExecutionDecision
}

type metaTrackDependencyStructureEvidence struct {
	RawProducerIDs      []string
	LegacyFrontierIDs   []string
	MatrixFrontierIDs   []string
	LegacyFrontierWidth int
	MatrixFrontierWidth int
	FrontierWidthEqual  bool
	FrontierIDsEqual    bool
	DependencyDepthL    int
	TailDepthH          int
	DescendantCountD    int
	DirectChildCount    int
	CriticalPath        bool
	CriticalPathLength  int
	MatrixAnalysisValid bool
	MatrixAnalysisError string
}

type metaTrackDependencyStructureAnalysis struct {
	ByTx                  map[string]metaTrackDependencyStructureEvidence
	MatrixCheckedCount    int
	FrontierWidthMismatch int
	FrontierIDMismatch    int
	MatrixAnalysisValid   bool
	MatrixAnalysisError   string
	AnalysisUS            int64
	CriticalPathLength    int
	MaxDependencyDepthL   int
	MaxTailDepthH         int
	MaxDescendantCountD   int
}

type metaTrackVersionedStateKey struct {
	Version uint64
	Key     string
}

func captureMetaTrackDependencyTransactions(items []tx.SignedTransaction) []tx.SignedTransaction {
	out := make([]tx.SignedTransaction, 0, len(items))
	for _, item := range items {
		compact := tx.SignedTransaction{
			TxID:       txIdentifier(item),
			AccessList: append([]tx.AccessItem(nil), item.AccessList...),
		}
		if item.ExecutionRouting != nil {
			routing := *item.ExecutionRouting
			routing.StateVersions = append([]tx.StateVersionDependency(nil), item.ExecutionRouting.StateVersions...)
			compact.ExecutionRouting = &routing
		}
		out = append(out, compact)
	}
	return out
}

func analyzeMetaTrackDependencyStructure(items []tx.SignedTransaction) (analysis metaTrackDependencyStructureAnalysis) {
	started := time.Now()
	analysis = metaTrackDependencyStructureAnalysis{
		ByTx:                map[string]metaTrackDependencyStructureEvidence{},
		MatrixAnalysisValid: true,
	}
	defer func() {
		analysis.AnalysisUS = time.Since(started).Microseconds()
	}()
	if len(items) == 0 {
		return
	}

	for _, item := range items {
		if item.ExecutionRouting == nil {
			analysis = invalidateMetaTrackDependencyAnalysis(analysis, items, "missing_execution_routing")
			return
		}
		if !metaTrackStrictFrontierControlPolicy(item.ExecutionRouting.ControlPolicy) {
			analysis = invalidateMetaTrackDependencyAnalysis(analysis, items, "non_strict_frontier_control_policy")
			return
		}
		if item.ExecutionRouting.RoutingOrdinal == 0 {
			analysis = invalidateMetaTrackDependencyAnalysis(analysis, items, "missing_routing_ordinal")
			return
		}
	}

	ownerByVersionKey := map[metaTrackVersionedStateKey]string{}
	for _, item := range items {
		txID := txIdentifier(item)
		for _, dependency := range item.ExecutionRouting.StateVersions {
			if dependency.ProducedVersion == 0 || strings.TrimSpace(dependency.Key) == "" {
				continue
			}
			slot := metaTrackVersionedStateKey{Version: dependency.ProducedVersion, Key: dependency.Key}
			if previous := ownerByVersionKey[slot]; previous != "" && previous != txID {
				analysis = invalidateMetaTrackDependencyAnalysis(analysis, items, "duplicate_version_owner:"+dependency.Key)
				return
			}
			ownerByVersionKey[slot] = txID
		}
	}

	producersByTx := make(map[string][]string, len(items))
	for _, item := range items {
		txID := txIdentifier(item)
		set := map[string]bool{}
		for _, dependency := range item.ExecutionRouting.StateVersions {
			if dependency.RequiredVersion == 0 || !transactionRequiresExactStateValue(item, dependency.Key) {
				continue
			}
			owner := ownerByVersionKey[metaTrackVersionedStateKey{Version: dependency.RequiredVersion, Key: dependency.Key}]
			if owner == "" || owner == txID {
				// Same rule as V11: an already-committed version is StateReady input,
				// not a current-window exact-value frontier.
				continue
			}
			set[owner] = true
		}
		producers := make([]string, 0, len(set))
		for producer := range set {
			producers = append(producers, producer)
		}
		sort.Strings(producers)
		producersByTx[txID] = producers
	}

	legacyByTx := legacyMetaTrackEffectiveFrontiers(producersByTx)
	matrixByTx, valid, matrixError, criticalPathLength := matrixMetaTrackDependencyStructure(items, producersByTx)
	analysis.MatrixAnalysisValid = valid
	analysis.MatrixAnalysisError = matrixError
	analysis.CriticalPathLength = criticalPathLength

	for _, item := range items {
		txID := txIdentifier(item)
		raw := append([]string(nil), producersByTx[txID]...)
		legacy := append([]string(nil), legacyByTx[txID]...)
		evidence := matrixByTx[txID]
		if !valid {
			evidence.MatrixFrontierWidth = -1
		}
		evidence.RawProducerIDs = raw
		evidence.LegacyFrontierIDs = legacy
		evidence.LegacyFrontierWidth = len(legacy)
		evidence.MatrixAnalysisValid = valid
		evidence.MatrixAnalysisError = matrixError
		if valid {
			evidence.FrontierWidthEqual = evidence.MatrixFrontierWidth == len(legacy)
			evidence.FrontierIDsEqual = metaTrackDependencyStringSliceEqual(evidence.MatrixFrontierIDs, legacy)
			analysis.MatrixCheckedCount++
			if !evidence.FrontierWidthEqual {
				analysis.FrontierWidthMismatch++
			}
			if !evidence.FrontierIDsEqual {
				analysis.FrontierIDMismatch++
			}
		}
		analysis.ByTx[txID] = evidence
		if evidence.DependencyDepthL > analysis.MaxDependencyDepthL {
			analysis.MaxDependencyDepthL = evidence.DependencyDepthL
		}
		if evidence.TailDepthH > analysis.MaxTailDepthH {
			analysis.MaxTailDepthH = evidence.TailDepthH
		}
		if evidence.DescendantCountD > analysis.MaxDescendantCountD {
			analysis.MaxDescendantCountD = evidence.DescendantCountD
		}
	}
	return
}

func invalidateMetaTrackDependencyAnalysis(analysis metaTrackDependencyStructureAnalysis, items []tx.SignedTransaction, reason string) metaTrackDependencyStructureAnalysis {
	analysis.MatrixAnalysisValid = false
	analysis.MatrixAnalysisError = reason
	for _, item := range items {
		txID := txIdentifier(item)
		analysis.ByTx[txID] = metaTrackDependencyStructureEvidence{
			MatrixFrontierWidth: -1,
			MatrixAnalysisValid: false,
			MatrixAnalysisError: reason,
		}
	}
	return analysis
}

// legacyMetaTrackEffectiveFrontiers intentionally mirrors the recursive
// ancestor elimination used by applyMetaTrackEffectiveFrontierTracks. Keeping
// it separate from the bitset implementation gives the artifact an internal
// oracle for equivalence checking without changing the production classifier.
func legacyMetaTrackEffectiveFrontiers(producersByTx map[string][]string) map[string][]string {
	ancestorMemo := map[string]map[string]bool{}
	visiting := map[string]bool{}
	var ancestorsOf func(string) map[string]bool
	ancestorsOf = func(txID string) map[string]bool {
		if cached, ok := ancestorMemo[txID]; ok {
			return cached
		}
		if visiting[txID] {
			return map[string]bool{}
		}
		visiting[txID] = true
		ancestors := map[string]bool{}
		for _, producer := range producersByTx[txID] {
			ancestors[producer] = true
			for ancestor := range ancestorsOf(producer) {
				ancestors[ancestor] = true
			}
		}
		delete(visiting, txID)
		ancestorMemo[txID] = ancestors
		return ancestors
	}

	out := make(map[string][]string, len(producersByTx))
	for txID, raw := range producersByTx {
		effective := map[string]bool{}
		for _, producer := range raw {
			effective[producer] = true
		}
		for _, producer := range raw {
			for _, other := range raw {
				if producer == other {
					continue
				}
				if ancestorsOf(other)[producer] {
					delete(effective, producer)
					break
				}
			}
		}
		frontier := make([]string, 0, len(effective))
		for producer := range effective {
			frontier = append(frontier, producer)
		}
		sort.Strings(frontier)
		out[txID] = frontier
	}
	return out
}

// matrixMetaTrackDependencyStructure uses one forward pass and one reverse
// bitset propagation pass for the whole window. It never performs one full
// graph traversal per transaction.
func matrixMetaTrackDependencyStructure(items []tx.SignedTransaction, producersByTx map[string][]string) (map[string]metaTrackDependencyStructureEvidence, bool, string, int) {
	out := make(map[string]metaTrackDependencyStructureEvidence, len(items))
	if len(items) == 0 {
		return out, true, "", 0
	}

	txIDs := make([]string, len(items))
	indexByTx := make(map[string]int, len(items))
	for index, item := range items {
		txID := txIdentifier(item)
		if txID == "" {
			return out, false, "empty_tx_id", 0
		}
		if _, exists := indexByTx[txID]; exists {
			return out, false, "duplicate_tx_id:" + txID, 0
		}
		txIDs[index] = txID
		indexByTx[txID] = index
	}

	predecessors := make([][]int, len(items))
	children := make([][]int, len(items))
	for consumerIndex, txID := range txIDs {
		for _, producerID := range producersByTx[txID] {
			producerIndex, ok := indexByTx[producerID]
			if !ok {
				return out, false, "producer_outside_window:" + producerID + "->" + txID, 0
			}
			if producerIndex >= consumerIndex {
				return out, false, "non_forward_exact_value_edge:" + producerID + "->" + txID, 0
			}
			predecessors[consumerIndex] = append(predecessors[consumerIndex], producerIndex)
			children[producerIndex] = append(children[producerIndex], consumerIndex)
		}
	}

	depth := make([]int, len(items))
	criticalPathLength := 0
	for index := range items {
		depth[index] = 1
		for _, predecessor := range predecessors[index] {
			candidate := depth[predecessor] + 1
			if candidate > depth[index] {
				depth[index] = candidate
			}
		}
		if depth[index] > criticalPathLength {
			criticalPathLength = depth[index]
		}
	}

	wordCount := (len(items) + 63) / 64
	reach := make([][]uint64, len(items))
	tailDepth := make([]int, len(items))
	descendantCount := make([]int, len(items))
	for index := range items {
		reach[index] = make([]uint64, wordCount)
		tailDepth[index] = 1
	}
	for index := len(items) - 1; index >= 0; index-- {
		for _, child := range children[index] {
			reach[index][child>>6] |= uint64(1) << uint(child&63)
			for word := 0; word < wordCount; word++ {
				reach[index][word] |= reach[child][word]
			}
			candidate := tailDepth[child] + 1
			if candidate > tailDepth[index] {
				tailDepth[index] = candidate
			}
		}
		count := 0
		for _, word := range reach[index] {
			for word != 0 {
				word &= word - 1
				count++
			}
		}
		descendantCount[index] = count
	}

	for index, txID := range txIDs {
		raw := producersByTx[txID]
		frontier := make([]string, 0, len(raw))
		for _, producerID := range raw {
			producerIndex := indexByTx[producerID]
			covered := false
			for _, otherID := range raw {
				if producerID == otherID {
					continue
				}
				otherIndex := indexByTx[otherID]
				if metaTrackDependencyBitSet(reach[producerIndex], otherIndex) {
					covered = true
					break
				}
			}
			if !covered {
				frontier = append(frontier, producerID)
			}
		}
		sort.Strings(frontier)
		out[txID] = metaTrackDependencyStructureEvidence{
			MatrixFrontierIDs:   frontier,
			MatrixFrontierWidth: len(frontier),
			DependencyDepthL:    depth[index],
			TailDepthH:          tailDepth[index],
			DescendantCountD:    descendantCount[index],
			DirectChildCount:    len(children[index]),
			CriticalPath:        depth[index]+tailDepth[index]-1 == criticalPathLength,
			CriticalPathLength:  criticalPathLength,
			MatrixAnalysisValid: true,
		}
	}
	return out, true, "", criticalPathLength
}

func metaTrackDependencyBitSet(words []uint64, index int) bool {
	wordIndex := index >> 6
	if wordIndex < 0 || wordIndex >= len(words) {
		return false
	}
	return words[wordIndex]&(uint64(1)<<uint(index&63)) != 0
}

func metaTrackDependencyStringSliceEqual(left, right []string) bool {
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

func metaTrackDependencyStructureRows(nodeID, shardID string, capture metaTrackDependencyStructureBlockCapture) ([][]string, []string) {
	analysis := analyzeMetaTrackDependencyStructure(capture.Transactions)
	rows := make([][]string, 0, len(capture.Transactions))
	for _, item := range capture.Transactions {
		txID := txIdentifier(item)
		evidence := analysis.ByTx[txID]
		decision := capture.Decisions[txID]
		routingOrdinal := uint64(0)
		routeBatchSequence := uint64(0)
		if item.ExecutionRouting != nil {
			routingOrdinal = item.ExecutionRouting.RoutingOrdinal
			routeBatchSequence = item.ExecutionRouting.RouteBatchSequence
		}
		rows = append(rows, []string{
			fmt.Sprint(time.Now().UnixMilli()),
			nodeID,
			shardID,
			fmt.Sprint(capture.Height),
			capture.BlockHash,
			txID,
			fmt.Sprint(routingOrdinal),
			fmt.Sprint(routeBatchSequence),
			strings.Join(evidence.RawProducerIDs, "|"),
			fmt.Sprint(len(evidence.RawProducerIDs)),
			strings.Join(evidence.LegacyFrontierIDs, "|"),
			fmt.Sprint(evidence.LegacyFrontierWidth),
			strings.Join(evidence.MatrixFrontierIDs, "|"),
			fmt.Sprint(evidence.MatrixFrontierWidth),
			fmt.Sprint(evidence.FrontierWidthEqual),
			fmt.Sprint(evidence.FrontierIDsEqual),
			fmt.Sprint(evidence.DependencyDepthL),
			fmt.Sprint(evidence.TailDepthH),
			fmt.Sprint(evidence.DescendantCountD),
			fmt.Sprint(evidence.DirectChildCount),
			fmt.Sprint(evidence.CriticalPath),
			fmt.Sprint(evidence.CriticalPathLength),
			decision.Track,
			decision.Reason,
			fmt.Sprint(evidence.MatrixAnalysisValid),
			evidence.MatrixAnalysisError,
			fmt.Sprint(analysis.AnalysisUS),
		})
	}
	summary := []string{
		nodeID,
		shardID,
		fmt.Sprint(capture.Height),
		capture.BlockHash,
		fmt.Sprint(len(capture.Transactions)),
		fmt.Sprint(analysis.MatrixCheckedCount),
		fmt.Sprint(analysis.FrontierWidthMismatch),
		fmt.Sprint(analysis.FrontierIDMismatch),
		fmt.Sprint(analysis.MatrixAnalysisValid),
		analysis.MatrixAnalysisError,
		fmt.Sprint(analysis.AnalysisUS),
		fmt.Sprint(analysis.CriticalPathLength),
		fmt.Sprint(analysis.MaxDependencyDepthL),
		fmt.Sprint(analysis.MaxTailDepthH),
		fmt.Sprint(analysis.MaxDescendantCountD),
	}
	return rows, summary
}
