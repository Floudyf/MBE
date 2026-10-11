package v5

import (
	"sort"

	"metaverse-chainlab/executor/realism/tx"
)

// MBE_METATRACK_CLASSREUSE_V1: per-call structural result only. Never cache by
// height, block hash, global state, or PBFT view. Scheduler and executor see
// the same original TxList. Only the actual per-block StateReady map is added.
// A mismatch conservatively falls back to the historical full classifier.
func metaTrackReuseScheduleClassificationV1(prepared *BatchClassificationResult, items []tx.SignedTransaction, readiness map[string]bool) (BatchClassificationResult, bool) {
	if prepared == nil || len(prepared.Decisions) != len(items) || len(prepared.StateWaitKeys) != 0 {
		return BatchClassificationResult{}, false
	}
	seen := make(map[string]bool, len(items))
	for _, item := range items {
		id := txIdentifier(item)
		if id == "" || seen[id] {
			return BatchClassificationResult{}, false
		}
		seen[id] = true
		if _, ok := prepared.Decisions[id]; !ok {
			return BatchClassificationResult{}, false
		}
	}
	// Deep-copy mutable maps. Version handoff binding mutates Dependencies and
	// diagnostic mode may mutate Decisions; never change the scheduler snapshot.
	out := *prepared
	out.Decisions = make(map[string]ExecutionDecision, len(prepared.Decisions))
	for id, decision := range prepared.Decisions {
		out.Decisions[id] = decision
	}
	out.Dependencies = make(map[string][]string, len(prepared.Dependencies))
	for id, dependencies := range prepared.Dependencies {
		out.Dependencies[id] = append([]string(nil), dependencies...)
	}
	out.ReasonCodes = make(map[string][]string, len(prepared.ReasonCodes))
	for id, reasons := range prepared.ReasonCodes {
		out.ReasonCodes[id] = append([]string(nil), reasons...)
	}
	out.StateWaitKeys = make(map[string][]string)
	if readiness == nil || len(readiness) == 0 {
		return out, true
	}
	for _, item := range items {
		id := txIdentifier(item)
		for _, access := range classificationAccessItems(item) {
			if access.Key == "" {
				continue
			}
			token := stateReadinessToken(item, access)
			if ready, known := readiness[token]; known && !ready {
				out.StateWaitKeys[id] = append(out.StateWaitKeys[id], token)
			}
		}
	}
	for id, keys := range out.StateWaitKeys {
		sort.Strings(keys)
		out.StateWaitKeys[id] = uniqueStrings(keys)
	}
	return out, true
}

// metaTrackReuseObservedClassificationV1 reuses only the deterministic
// pre-execution decisions for post-commit artifacts. It does not bypass the
// actual execution's dependency, StateReady or proof verification.
func metaTrackReuseObservedClassificationV1(observed *BatchClassificationResult, items []tx.SignedTransaction) (BatchClassificationResult, bool) {
	if observed == nil || len(observed.Decisions) != len(items) {
		return BatchClassificationResult{}, false
	}
	seen := make(map[string]bool, len(items))
	for _, item := range items {
		id := txIdentifier(item)
		if id == "" || seen[id] {
			return BatchClassificationResult{}, false
		}
		seen[id] = true
		if _, ok := observed.Decisions[id]; !ok {
			return BatchClassificationResult{}, false
		}
	}
	return *observed, true
}
