package v5

import (
	"fmt"
	"sort"
	"strings"

	"metaverse-chainlab/executor/realism/tx"
)

const metaTrackDeclaredAccessFrontierPolicy = "declared_access_frontier_v2"

type metaTrackDeclaredAccessFrontierSeal struct {
	TxID           string                      `json:"tx_id"`
	FrontierDigest string                      `json:"frontier_digest"`
	Dependencies   []string                    `json:"dependencies"`
	StateVersions  []tx.StateVersionDependency `json:"state_versions"`
	ExecutionShard string                      `json:"execution_shard"`
	RoutingOrdinal uint64                      `json:"routing_ordinal"`
	SealDigest     string                      `json:"seal_digest"`
}

func metaTrackDeclaredAccessFrontierPolicyEnabled(config map[string]any) bool {
	if config == nil {
		return false
	}
	return strings.TrimSpace(fmt.Sprint(config["control_policy"])) == metaTrackDeclaredAccessFrontierPolicy
}

func metaTrackStrictFrontierPolicyEnabled(config map[string]any) bool {
	return metaTrackDeclaredAccessFrontierPolicyEnabled(config) || metaTrackLogicalDomainPolicyEnabled(config)
}

func metaTrackStrictFrontierControlPolicy(policy string) bool {
	switch strings.TrimSpace(policy) {
	case metaTrackDeclaredAccessFrontierPolicy, metaTrackLogicalDomainFrontierPolicy:
		return true
	default:
		return false
	}
}

func metaTrackDeclaredAccessItemsForRecord(record WorkloadRecord) []tx.AccessItem {
	source := record.SchedulingAccessList
	if len(source) == 0 {
		source = record.AccessList
	}
	out := append([]tx.AccessItem(nil), source...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Key != out[j].Key {
			return out[i].Key < out[j].Key
		}
		if out[i].Mode != out[j].Mode {
			return out[i].Mode < out[j].Mode
		}
		if out[i].UpdateSemantics != out[j].UpdateSemantics {
			return out[i].UpdateSemantics < out[j].UpdateSemantics
		}
		return out[i].Delta < out[j].Delta
	})
	return out
}

func metaTrackDeclaredAccessFrontierDigest(record WorkloadRecord, executionShard string) string {
	versions := append([]tx.StateVersionDependency(nil), record.StateVersions...)
	sort.Slice(versions, func(i, j int) bool { return versions[i].Key < versions[j].Key })
	payload := struct {
		SchemaVersion  string                      `json:"schema_version"`
		ControlPolicy  string                      `json:"control_policy"`
		LogicalID      string                      `json:"logical_id"`
		TxIndex        int                         `json:"tx_index"`
		RoutingOrdinal uint64                      `json:"routing_ordinal"`
		ExecutionShard string                      `json:"execution_shard"`
		DeclaredAccess []tx.AccessItem             `json:"declared_access"`
		StateVersions  []tx.StateVersionDependency `json:"state_versions"`
	}{
		SchemaVersion:  "metatrack_declared_access_frontier_v2",
		ControlPolicy:  metaTrackDeclaredAccessFrontierPolicy,
		LogicalID:      firstNonEmpty(record.LogicalID, fmt.Sprintf("tx-%d", record.Index)),
		TxIndex:        record.Index,
		RoutingOrdinal: record.RoutingOrdinal,
		ExecutionShard: executionShard,
		DeclaredAccess: metaTrackDeclaredAccessItemsForRecord(record),
		StateVersions:  versions,
	}
	return stableJSONDigest(payload)
}

func applyMetaTrackDeclaredAccessFrontierV2(plan *BatchRoutingPlan, records []WorkloadRecord) {
	if plan == nil {
		return
	}
	recordsByIndex := map[int]WorkloadRecord{}
	for _, record := range records {
		recordsByIndex[record.Index] = record
	}
	plan.ControlPolicy = metaTrackDeclaredAccessFrontierPolicy
	plan.LogicalDomainCount = 0
	for index := range plan.TransactionPlacements {
		placement := &plan.TransactionPlacements[index]
		record, ok := recordsByIndex[placement.TxIndex]
		if !ok {
			continue
		}
		placement.LogicalDomains = nil
		placement.Local = false
		placement.Bridge = false
		placement.FrontierDigest = metaTrackDeclaredAccessFrontierDigest(record, placement.ExecutionShard)
	}
}

func metaTrackDeclaredAccessFrontierDigestV6567(record WorkloadRecord, executionShard string) string {
	versions := append([]tx.StateVersionDependency(nil), record.StateVersions...)
	sort.Slice(versions, func(i, j int) bool {
		if versions[i].Key != versions[j].Key {
			return versions[i].Key < versions[j].Key
		}
		if versions[i].RequiredVersion != versions[j].RequiredVersion {
			return versions[i].RequiredVersion < versions[j].RequiredVersion
		}
		if versions[i].ProducedVersion != versions[j].ProducedVersion {
			return versions[i].ProducedVersion < versions[j].ProducedVersion
		}
		return versions[i].RequiredExecutionRound < versions[j].RequiredExecutionRound
	})
	executionPreds := append([]uint64(nil), record.ConsensusExecutionPredecessorOrdinals...)
	orderingPreds := append([]uint64(nil), record.ConsensusOrderingPredecessorOrdinals...)
	sort.Slice(executionPreds, func(i, j int) bool { return executionPreds[i] < executionPreds[j] })
	sort.Slice(orderingPreds, func(i, j int) bool { return orderingPreds[i] < orderingPreds[j] })
	payload := struct {
		SchemaVersion         string                      `json:"schema_version"`
		ControlPolicy         string                      `json:"control_policy"`
		LogicalID             string                      `json:"logical_id"`
		TxIndex               int                         `json:"tx_index"`
		RoutingOrdinal        uint64                      `json:"routing_ordinal"`
		ExecutionShard        string                      `json:"execution_shard"`
		DeclaredAccess        []tx.AccessItem             `json:"declared_access"`
		StateVersions         []tx.StateVersionDependency `json:"state_versions"`
		ExecutionPredecessors []uint64                    `json:"execution_predecessors"`
		OrderingPredecessors  []uint64                    `json:"ordering_predecessors"`
		ExecutionDepth        int                         `json:"execution_depth"`
		ExecutionRound        int                         `json:"execution_round"`
	}{
		SchemaVersion:         "metatrack_declared_access_frontier_v6567",
		ControlPolicy:         metaTrackDeclaredAccessFrontierPolicy,
		LogicalID:             firstNonEmpty(record.LogicalID, fmt.Sprintf("tx-%d", record.Index)),
		TxIndex:               record.Index,
		RoutingOrdinal:        record.RoutingOrdinal,
		ExecutionShard:        executionShard,
		DeclaredAccess:        metaTrackDeclaredAccessItemsForRecord(record),
		StateVersions:         versions,
		ExecutionPredecessors: executionPreds,
		OrderingPredecessors:  orderingPreds,
		ExecutionDepth:        record.ConsensusExecutionDepth,
		ExecutionRound:        record.ConsensusExecutionRound,
	}
	return stableJSONDigest(payload)
}

func metaTrackDeclaredAccessFrontierDigestV6568(record WorkloadRecord, executionShard string) string {
	versions := append([]tx.StateVersionDependency(nil), record.StateVersions...)
	sort.Slice(versions, func(i, j int) bool {
		if versions[i].Key != versions[j].Key { return versions[i].Key < versions[j].Key }
		if versions[i].RequiredVersion != versions[j].RequiredVersion { return versions[i].RequiredVersion < versions[j].RequiredVersion }
		if versions[i].ProducedVersion != versions[j].ProducedVersion { return versions[i].ProducedVersion < versions[j].ProducedVersion }
		return versions[i].RequiredExecutionRound < versions[j].RequiredExecutionRound
	})
	executionPreds := append([]uint64(nil), record.ConsensusExecutionPredecessorOrdinals...)
	orderingPreds := append([]uint64(nil), record.ConsensusOrderingPredecessorOrdinals...)
	sort.Slice(executionPreds, func(i,j int) bool { return executionPreds[i] < executionPreds[j] })
	sort.Slice(orderingPreds, func(i,j int) bool { return orderingPreds[i] < orderingPreds[j] })
	payload := struct {
		SchemaVersion string `json:"schema_version"`; ControlPolicy string `json:"control_policy"`; LogicalID string `json:"logical_id"`; TxIndex int `json:"tx_index"`; RoutingOrdinal uint64 `json:"routing_ordinal"`; ExecutionShard string `json:"execution_shard"`
		DeclaredAccess []tx.AccessItem `json:"declared_access"`; StateVersions []tx.StateVersionDependency `json:"state_versions"`; ExecutionPredecessors []uint64 `json:"execution_predecessors"`; OrderingPredecessors []uint64 `json:"ordering_predecessors"`; ExecutionDepth int `json:"execution_depth"`; ExecutionRound int `json:"execution_round"`
		ConsensusWindowSequence uint64 `json:"consensus_window_sequence"`; ConsensusWindowStartBatchSequence uint64 `json:"consensus_window_start_batch_sequence"`; ConsensusWindowEndBatchSequence uint64 `json:"consensus_window_end_batch_sequence"`; ConsensusWindowRouteBatchCount int `json:"consensus_window_route_batch_count"`; ConsensusWindowTransactionCount int `json:"consensus_window_transaction_count"`; ConsensusWindowShardTransactionCount int `json:"consensus_window_shard_transaction_count"`; ConsensusWindowCriticalPath int `json:"consensus_window_critical_path"`
	}{
		SchemaVersion:"metatrack_declared_access_frontier_v6568", ControlPolicy:metaTrackDeclaredAccessFrontierPolicy, LogicalID:firstNonEmpty(record.LogicalID,fmt.Sprintf("tx-%d",record.Index)), TxIndex:record.Index, RoutingOrdinal:record.RoutingOrdinal, ExecutionShard:executionShard,
		DeclaredAccess:metaTrackDeclaredAccessItemsForRecord(record), StateVersions:versions, ExecutionPredecessors:executionPreds, OrderingPredecessors:orderingPreds, ExecutionDepth:record.ConsensusExecutionDepth, ExecutionRound:record.ConsensusExecutionRound,
		ConsensusWindowSequence:record.ConsensusWindowSequence, ConsensusWindowStartBatchSequence:record.ConsensusWindowStartBatchSequence, ConsensusWindowEndBatchSequence:record.ConsensusWindowEndBatchSequence, ConsensusWindowRouteBatchCount:record.ConsensusWindowRouteBatchCount, ConsensusWindowTransactionCount:record.ConsensusWindowTransactionCount, ConsensusWindowShardTransactionCount:record.ConsensusWindowShardTransactionCount, ConsensusWindowCriticalPath:record.ConsensusWindowCriticalPath,
	}
	return stableJSONDigest(payload)
}

func validateMetaTrackDeclaredAccessFrontierBinding(item tx.SignedTransaction) error {
	if item.ExecutionRouting == nil {
		return fmt.Errorf("metatrack declared-access frontier missing execution routing")
	}
	routing := item.ExecutionRouting
	if routing.ControlPolicy != metaTrackDeclaredAccessFrontierPolicy {
		return fmt.Errorf("metatrack declared-access frontier control policy mismatch")
	}
	if strings.TrimSpace(routing.FrontierDigest) == "" {
		return fmt.Errorf("metatrack declared-access frontier digest missing")
	}
	if len(routing.LogicalDomains) != 0 || routing.Local || routing.Bridge {
		return fmt.Errorf("metatrack declared-access frontier must not carry logical-domain admission state")
	}
	record := WorkloadRecord{
		Index:                int(routing.RoutingOrdinal - 1),
		LogicalID:            firstNonEmpty(item.LogicalTxID, item.TxID),
		AccessList:           append([]tx.AccessItem(nil), item.AccessList...),
		SchedulingAccessList: append([]tx.AccessItem(nil), item.SchedulingAccessList...),
		RoutingOrdinal:                         routing.RoutingOrdinal,
		StateVersions:                          append([]tx.StateVersionDependency(nil), routing.StateVersions...),
		ConsensusExecutionPredecessorOrdinals: append([]uint64(nil), routing.ConsensusExecutionPredecessorOrdinals...),
		ConsensusOrderingPredecessorOrdinals:  append([]uint64(nil), routing.ConsensusOrderingPredecessorOrdinals...),
		ConsensusExecutionDepth:               routing.ConsensusExecutionDepth,
		ConsensusExecutionRound:               routing.ConsensusExecutionRound,
		ConsensusWindowSequence:              routing.ConsensusWindowSequence,
		ConsensusWindowStartBatchSequence:    routing.ConsensusWindowStartBatchSequence,
		ConsensusWindowEndBatchSequence:      routing.ConsensusWindowEndBatchSequence,
		ConsensusWindowRouteBatchCount:       routing.ConsensusWindowRouteBatchCount,
		ConsensusWindowTransactionCount:      routing.ConsensusWindowTransactionCount,
		ConsensusWindowShardTransactionCount: routing.ConsensusWindowShardTransactionCount,
		ConsensusWindowCriticalPath:          routing.ConsensusWindowCriticalPath,
	}
	expected := metaTrackDeclaredAccessFrontierDigest(record, routing.ExecutionShard)
	if routing.ConsensusWindowSequence > 0 {
		expected = metaTrackDeclaredAccessFrontierDigestV6568(record, routing.ExecutionShard)
	} else if routing.ConsensusExecutionRound > 0 {
		expected = metaTrackDeclaredAccessFrontierDigestV6567(record, routing.ExecutionShard)
	}
	if expected != routing.FrontierDigest {
		return fmt.Errorf("metatrack declared-access frontier digest mismatch")
	}
	return nil
}

func buildMetaTrackDeclaredAccessFrontierSeal(item tx.SignedTransaction, dependencies []string) (metaTrackDeclaredAccessFrontierSeal, error) {
	if err := validateMetaTrackDeclaredAccessFrontierBinding(item); err != nil {
		return metaTrackDeclaredAccessFrontierSeal{}, err
	}
	deps := append([]string(nil), dependencies...)
	sort.Strings(deps)
	versions := append([]tx.StateVersionDependency(nil), item.ExecutionRouting.StateVersions...)
	sort.Slice(versions, func(i, j int) bool { return versions[i].Key < versions[j].Key })
	payload := struct {
		SchemaVersion  string                      `json:"schema_version"`
		TxID           string                      `json:"tx_id"`
		FrontierDigest string                      `json:"frontier_digest"`
		Dependencies   []string                    `json:"dependencies"`
		StateVersions  []tx.StateVersionDependency `json:"state_versions"`
		ExecutionShard string                      `json:"execution_shard"`
		RoutingOrdinal uint64                      `json:"routing_ordinal"`
	}{
		SchemaVersion:  "metatrack_declared_access_frontier_seal_v2",
		TxID:           txIdentifier(item),
		FrontierDigest: item.ExecutionRouting.FrontierDigest,
		Dependencies:   deps,
		StateVersions:  versions,
		ExecutionShard: item.ExecutionRouting.ExecutionShard,
		RoutingOrdinal: item.ExecutionRouting.RoutingOrdinal,
	}
	return metaTrackDeclaredAccessFrontierSeal{
		TxID:           payload.TxID,
		FrontierDigest: payload.FrontierDigest,
		Dependencies:   deps,
		StateVersions:  versions,
		ExecutionShard: payload.ExecutionShard,
		RoutingOrdinal: payload.RoutingOrdinal,
		SealDigest:     stableJSONDigest(payload),
	}, nil
}

func metaTrackRequiredVersionTicketCount(item tx.SignedTransaction) int {
	if item.ExecutionRouting == nil {
		return 0
	}
	count := 0
	for _, dependency := range item.ExecutionRouting.StateVersions {
		if dependency.RequiredVersion > 0 {
			count++
		}
	}
	return count
}
