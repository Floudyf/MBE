package v5

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/tx"
)

const (
	literatureGraphPlanVersion            = "mbe_literature_graph_plan_v1"
	literatureGraphPlanCompactWireVersion = "mbe_literature_graph_plan_compact_wire_v1"
	// A JSON edge is at least {"from":0,"to":0} (17 bytes), plus commas and
	// the edges field name. At 16 edges the removed legacy edge field is at
	// least 297 bytes, safely larger than fixed wire_version+edges_digest
	// metadata (~141 bytes). Below this threshold we preserve the exact legacy
	// encoder, avoiding payload expansion and a redundant full serialization.
	literatureGraphPlanCompactEdgeThreshold = 16
)

type literatureTxAccess struct {
	Item      tx.SignedTransaction
	TxID      string
	Ordinal   int
	ReadKeys  []string
	WriteKeys []string
}

type literatureGraphMetrics struct {
	TransactionCount     int   `json:"transaction_count"`
	EdgeCount            int   `json:"edge_count"`
	WaveCount            int   `json:"wave_count"`
	MaximumWaveWidth     int   `json:"maximum_wave_width"`
	ColorCount           int   `json:"color_count,omitempty"`
	PairChecks           int   `json:"pair_checks,omitempty"`
	PlanningWorkerCount  int   `json:"planning_worker_count,omitempty"`
	AbortCount           int   `json:"abort_count,omitempty"`
	CycleResolutionCount int   `json:"cycle_resolution_count,omitempty"`
	GraphConstructionMS  int64 `json:"graph_construction_ms,omitempty"`
	SortingMS            int64 `json:"sorting_ms,omitempty"`
}

type literatureGraphEdge struct {
	From int `json:"from"`
	To   int `json:"to"`
}

type literatureGraphPlan struct {
	AlgorithmID             string                 `json:"algorithm_id"`
	Version                 string                 `json:"version"`
	BlockHeight             uint64                 `json:"block_height"`
	CandidateTransactionIDs []string               `json:"candidate_transaction_ids"`
	Waves                   [][]string             `json:"waves"`
	SerializationOrder      []string               `json:"serialization_order"`
	AbortedTransactionIDs   []string               `json:"aborted_transaction_ids,omitempty"`
	DeclaredAccessSetDigest string                 `json:"declared_access_set_digest"`
	DeclaredReadKeyCount    int                    `json:"declared_read_key_count"`
	DeclaredWriteKeyCount   int                    `json:"declared_write_key_count"`
	Edges                   []literatureGraphEdge  `json:"edges,omitempty"`
	ConsensusWireVersion    string                 `json:"wire_version,omitempty"`
	EdgesDigest             string                 `json:"edges_digest,omitempty"`
	ValidatorMode           string                 `json:"validator_mode,omitempty"`
	Metrics                 literatureGraphMetrics `json:"metrics"`
	PlanDigest              string                 `json:"plan_digest"`
}

func literaturePlanDigest(plan literatureGraphPlan) string {
	clone := plan
	clone.PlanDigest = ""
	// Wire-only fields must never change the literature algorithm's semantic
	// plan digest. Historical v1 plans and new compact consensus wires therefore
	// commit to exactly the same rich plan.
	clone.ConsensusWireVersion = ""
	clone.EdgesDigest = ""
	clone.Metrics.GraphConstructionMS = 0
	clone.Metrics.SortingMS = 0
	return stableDigest(clone)
}

func literatureMarshalPlan(plan literatureGraphPlan) ([]byte, error) {
	return literatureJSONMarshal(plan)
}

func literatureGraphEdgesDigest(edges []literatureGraphEdge) string {
	canonical := append([]literatureGraphEdge(nil), edges...)
	if canonical == nil {
		canonical = []literatureGraphEdge{}
	}
	return stableDigest(canonical)
}

// literatureMarshalConsensusPlan removes only the O(E) edge list from the PBFT
// payload. The complete rich plan remains committed by PlanDigest, while the
// exact ordered/multiplicity-preserving edge list is separately committed by
// EdgesDigest. Validators rebuild the graph from the block access lists and
// verify both commitments before voting.
func literatureMarshalConsensusPlan(plan literatureGraphPlan) ([]byte, error) {
	if plan.PlanDigest == "" || literaturePlanDigest(plan) != plan.PlanDigest {
		return nil, fmt.Errorf("%s rich plan digest mismatch before compact marshal", plan.AlgorithmID)
	}
	// Fail-safe against negative optimization: sparse graphs keep the exact
	// historical encoding. Dense graphs skip the legacy payload marshal entirely
	// and send only an edge commitment, so this optimization does not add a
	// redundant O(E) full-plan JSON pass to the consensus planning hot path.
	if len(plan.Edges) < literatureGraphPlanCompactEdgeThreshold {
		return literatureJSONMarshal(plan)
	}
	wire := plan
	wire.ConsensusWireVersion = literatureGraphPlanCompactWireVersion
	wire.EdgesDigest = literatureGraphEdgesDigest(plan.Edges)
	wire.Edges = nil
	return literatureJSONMarshal(wire)
}

func literatureParsePlan(raw []byte, algorithmID string) (literatureGraphPlan, error) {
	var plan literatureGraphPlan
	if err := literatureJSONUnmarshal(raw, &plan); err != nil {
		return plan, fmt.Errorf("decode %s plan: %w", algorithmID, err)
	}
	if plan.AlgorithmID != algorithmID || plan.Version != literatureGraphPlanVersion {
		return plan, fmt.Errorf("unsupported literature plan %s/%s", plan.AlgorithmID, plan.Version)
	}
	if plan.ConsensusWireVersion != "" {
		if plan.ConsensusWireVersion != literatureGraphPlanCompactWireVersion {
			return plan, fmt.Errorf("unsupported literature consensus wire %q", plan.ConsensusWireVersion)
		}
		if plan.PlanDigest == "" || plan.EdgesDigest == "" {
			return plan, fmt.Errorf("%s compact plan is missing semantic commitments", algorithmID)
		}
		if len(plan.Edges) != 0 {
			return plan, fmt.Errorf("%s compact plan unexpectedly carries graph edges", algorithmID)
		}
		return plan, nil
	}
	if plan.PlanDigest == "" || literaturePlanDigest(plan) != plan.PlanDigest {
		return plan, fmt.Errorf("%s plan digest mismatch", algorithmID)
	}
	return plan, nil
}

func literatureVerifyCompactEdgeCommitment(plan literatureGraphPlan, expected []literatureGraphEdge) error {
	if plan.ConsensusWireVersion == "" {
		return nil
	}
	if plan.ConsensusWireVersion != literatureGraphPlanCompactWireVersion {
		return fmt.Errorf("unsupported literature compact wire %q", plan.ConsensusWireVersion)
	}
	if got := literatureGraphEdgesDigest(expected); got != plan.EdgesDigest {
		return fmt.Errorf("%s compact edge commitment mismatch", plan.AlgorithmID)
	}
	// Reconstitute the original rich plan and verify the historical semantic
	// PlanDigest. This proves compact transport did not weaken plan binding.
	rich := plan
	rich.ConsensusWireVersion = ""
	rich.EdgesDigest = ""
	rich.Edges = append([]literatureGraphEdge(nil), expected...)
	if literaturePlanDigest(rich) != plan.PlanDigest {
		return fmt.Errorf("%s compact rich-plan reconstruction digest mismatch", plan.AlgorithmID)
	}
	return nil
}

// Thin wrappers keep JSON ownership in this package without coupling the
// literature baselines to another algorithm implementation.
func literatureJSONMarshal(value any) ([]byte, error)     { return json.Marshal(value) }
func literatureJSONUnmarshal(raw []byte, value any) error { return json.Unmarshal(raw, value) }

func literatureAccessDescriptors(items []tx.SignedTransaction, shardID string) ([]literatureTxAccess, error) {
	out := make([]literatureTxAccess, 0, len(items))
	seen := map[string]bool{}
	for index, item := range items {
		if item.TxID == "" || seen[item.TxID] {
			return nil, fmt.Errorf("literature scheduler requires unique non-empty tx ids")
		}
		seen[item.TxID] = true
		reads, writes := literatureDeclaredAccess(item, shardID)
		out = append(out, literatureTxAccess{Item: item, TxID: item.TxID, Ordinal: index, ReadKeys: reads, WriteKeys: writes})
	}
	return out, nil
}

func literatureDeclaredAccess(item tx.SignedTransaction, shardID string) ([]string, []string) {
	reads := map[string]bool{}
	writes := map[string]bool{}
	if len(item.AccessList) > 0 {
		for _, access := range item.AccessList {
			if access.Key == "" {
				continue
			}
			switch access.Mode {
			case tx.AccessRead:
				reads[access.Key] = true
			case tx.AccessWrite:
				writes[access.Key] = true
			case tx.AccessReadWrite, tx.AccessCommutativeDelta:
				reads[access.Key] = true
				writes[access.Key] = true
			}
		}
		if item.AccessListSource == "legacy_state_keys" {
			for _, key := range []string{"balance:" + item.Sender, "nonce:" + item.Sender, "balance:" + item.Receiver, "nonce:" + item.Receiver} {
				reads[key] = true
				writes[key] = true
			}
		}
	} else {
		for _, key := range item.StateKeys {
			if key != "" {
				reads[key] = true
				writes[key] = true
			}
		}
		for _, key := range []string{"balance:" + item.Sender, "nonce:" + item.Sender, "balance:" + item.Receiver, "nonce:" + item.Receiver} {
			reads[key] = true
			writes[key] = true
		}
	}
	if literatureIsCrossShardTargetCommit(item, shardID) {
		writes["relay_commit:"+item.TxID] = true
	}
	return literatureSortedBoolKeys(reads), literatureSortedBoolKeys(writes)
}

func literatureSortedBoolKeys(values map[string]bool) []string {
	out := make([]string, 0, len(values))
	for key := range values {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func literatureDeclaredAccessSummary(items []literatureTxAccess) (string, int, int) {
	reads := map[string]bool{}
	writes := map[string]bool{}
	for _, item := range items {
		for _, key := range item.ReadKeys {
			reads[key] = true
		}
		for _, key := range item.WriteKeys {
			writes[key] = true
		}
	}
	payload := struct {
		ReadKeys  []string `json:"read_keys"`
		WriteKeys []string `json:"write_keys"`
	}{ReadKeys: literatureSortedBoolKeys(reads), WriteKeys: literatureSortedBoolKeys(writes)}
	return stableDigest(payload), len(payload.ReadKeys), len(payload.WriteKeys)
}

func literatureConflicts(left, right literatureTxAccess) bool {
	lr, lw := literatureStringSet(left.ReadKeys), literatureStringSet(left.WriteKeys)
	rr, rw := literatureStringSet(right.ReadKeys), literatureStringSet(right.WriteKeys)
	return literatureSetIntersects(lw, rr) || literatureSetIntersects(lr, rw) || literatureSetIntersects(lw, rw)
}

func literatureStringSet(values []string) map[string]bool {
	out := make(map[string]bool, len(values))
	for _, value := range values {
		out[value] = true
	}
	return out
}

func literatureSetIntersects(left, right map[string]bool) bool {
	if len(left) > len(right) {
		left, right = right, left
	}
	for key := range left {
		if right[key] {
			return true
		}
	}
	return false
}

func literatureWavesFromEdges(items []literatureTxAccess, edges map[int]map[int]bool) ([][]string, error) {
	indegree := make([]int, len(items))
	for _, children := range edges {
		for child := range children {
			indegree[child]++
		}
	}
	remaining := len(items)
	waves := [][]string{}
	for remaining > 0 {
		ready := make([]int, 0)
		for index := range items {
			if indegree[index] == 0 {
				ready = append(ready, index)
			}
		}
		if len(ready) == 0 {
			return nil, fmt.Errorf("literature dependency graph contains a cycle")
		}
		sort.Ints(ready)
		wave := make([]string, 0, len(ready))
		for _, index := range ready {
			wave = append(wave, items[index].TxID)
			indegree[index] = -1
			remaining--
		}
		for _, index := range ready {
			for child := range edges[index] {
				indegree[child]--
			}
		}
		waves = append(waves, wave)
	}
	return waves, nil
}

func literatureFinalizePlan(plan literatureGraphPlan) literatureGraphPlan {
	plan.Version = literatureGraphPlanVersion
	plan.SerializationOrder = plan.SerializationOrder[:0]
	maxWidth := 0
	for _, wave := range plan.Waves {
		if len(wave) > maxWidth {
			maxWidth = len(wave)
		}
		plan.SerializationOrder = append(plan.SerializationOrder, wave...)
	}
	plan.Metrics.WaveCount = len(plan.Waves)
	plan.Metrics.MaximumWaveWidth = maxWidth
	plan.PlanDigest = literaturePlanDigest(plan)
	return plan
}

func literatureVerifyPlan(block realblock.Block, plan literatureGraphPlan, algorithmID string, rebuild func(realblock.Block) (literatureGraphPlan, error)) error {
	if block.ExecutionPlan == nil || block.ExecutionPlan.AlgorithmID != algorithmID {
		return fmt.Errorf("%s execution plan is missing", algorithmID)
	}
	if block.ExecutionPlan.PlanDigest != plan.PlanDigest || block.ExecutionPlan.PayloadDigest != stableTextDigest(string(block.ExecutionPlan.Payload)) {
		return fmt.Errorf("%s execution plan envelope mismatch", algorithmID)
	}
	recomputed, err := rebuild(block)
	if err != nil {
		return err
	}
	if err := literatureVerifyCompactEdgeCommitment(plan, recomputed.Edges); err != nil {
		return err
	}
	if recomputed.PlanDigest != plan.PlanDigest {
		return fmt.Errorf("%s deterministic plan mismatch", algorithmID)
	}
	return nil
}

func literatureCopyStringMap(input map[string]string) map[string]string {
	out := make(map[string]string, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func literatureQualifiedKey(shardID, key string) string {
	if strings.Contains(key, "::") {
		return key
	}
	return shardID + "::" + key
}

func literatureIsCrossShardTargetCommit(item tx.SignedTransaction, shardID string) bool {
	if !strings.HasPrefix(item.Payload, "v5_cross:") {
		return false
	}
	target := strings.TrimPrefix(item.Payload, "v5_cross:")
	if colon := strings.Index(target, ":"); colon >= 0 {
		target = target[:colon]
	}
	return strings.TrimSpace(target) != "" && strings.TrimSpace(target) == shardID
}

func literatureStateDelta(before, after map[string]string) []execution.StateUpdate {
	keys := make([]string, 0)
	for key, value := range after {
		if before[key] != value {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	out := make([]execution.StateUpdate, 0, len(keys))
	for _, key := range keys {
		out = append(out, execution.StateUpdate{Key: key, Value: after[key]})
	}
	return out
}
