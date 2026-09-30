package tx

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

// ExecutionRoutingMetadata is compiler/runtime metadata produced by MBE after
// a source dataset has been normalized. It is signed with the transaction and
// therefore cannot be silently rewritten by a proposer or validator.
type StateVersionDependency struct {
	Key                          string `json:"key"`
	RequiredVersion              uint64 `json:"required_version"`
	RequiredExecutionRound int `json:"required_execution_round,omitempty"`
	ProducedVersion              uint64 `json:"produced_version,omitempty"`
	LivenessClass                string `json:"liveness_class,omitempty"`
	LivenessDigest               string `json:"liveness_digest,omitempty"`
	RequiredLivenessClass        string `json:"required_liveness_class,omitempty"`
	RequiredLivenessDigest       string `json:"required_liveness_digest,omitempty"`
	BatchFinal                   bool   `json:"batch_final,omitempty"`
	ValueSuccessorCount          int    `json:"value_successor_count,omitempty"`
	LocalValueSuccessorCount     int    `json:"local_value_successor_count,omitempty"`
	RemoteValueSuccessorCount    int    `json:"remote_value_successor_count,omitempty"`
	LocalOrderingSuccessorCount  int    `json:"local_ordering_successor_count,omitempty"`
	RemoteOrderingSuccessorCount int    `json:"remote_ordering_successor_count,omitempty"`
}

type ExecutionRoutingMetadata struct {
	SenderID                        string                   `json:"sender_id"`
	ReceiverID                      string                   `json:"receiver_id"`
	RoutingEpoch                    uint64                   `json:"routing_epoch"`
	RoutingOrdinal                  uint64                   `json:"routing_ordinal"`
	ExecutionShard                  string                   `json:"execution_shard"`
	RoutingReason                   string                   `json:"routing_reason"`
	RoutePlanDigest                 string                   `json:"route_plan_digest"`
	RouteBatchSequence              uint64                   `json:"route_batch_sequence,omitempty"`
	RouteBatchTransactionCount      int                      `json:"route_batch_transaction_count,omitempty"`
	RouteBatchShardTransactionCount int                      `json:"route_batch_shard_transaction_count,omitempty"`
	ConsensusExecutionPredecessorOrdinals []uint64            `json:"consensus_execution_predecessor_ordinals,omitempty"`
	ConsensusOrderingPredecessorOrdinals  []uint64            `json:"consensus_ordering_predecessor_ordinals,omitempty"`
	ConsensusExecutionDepth               int                 `json:"consensus_execution_depth,omitempty"`
	ConsensusExecutionRound int `json:"consensus_execution_round,omitempty"`
	ConsensusWindowSequence uint64 `json:"consensus_window_sequence,omitempty"`
	ConsensusWindowStartBatchSequence uint64 `json:"consensus_window_start_batch_sequence,omitempty"`
	ConsensusWindowEndBatchSequence uint64 `json:"consensus_window_end_batch_sequence,omitempty"`
	ConsensusWindowRouteBatchCount int `json:"consensus_window_route_batch_count,omitempty"`
	ConsensusWindowTransactionCount int `json:"consensus_window_transaction_count,omitempty"`
	ConsensusWindowShardTransactionCount int `json:"consensus_window_shard_transaction_count,omitempty"`
	ConsensusWindowCriticalPath int `json:"consensus_window_critical_path,omitempty"`
	RouteEntryDigest                string                   `json:"route_entry_digest"`
	PredictedRemoteReads            int                      `json:"predicted_remote_reads"`
	PredictedRemoteWrites           int                      `json:"predicted_remote_writes"`
	StateVersions                   []StateVersionDependency `json:"state_versions,omitempty"`
	ControlPolicy                   string                   `json:"control_policy,omitempty"`
	LogicalDomains                  []string                 `json:"logical_domains,omitempty"`
	Local                           bool                     `json:"local,omitempty"`
	Bridge                          bool                     `json:"bridge,omitempty"`
	FrontierDigest                  string                   `json:"frontier_digest,omitempty"`
}

type executionRoutingDigestPayload struct {
	SenderID                        string                   `json:"sender_id"`
	ReceiverID                      string                   `json:"receiver_id"`
	Nonce                           uint64                   `json:"nonce"`
	StateKeys                       []string                 `json:"state_keys"`
	AccessListDigest                string                   `json:"access_list_digest"`
	Payload                         string                   `json:"payload"`
	RoutingEpoch                    uint64                   `json:"routing_epoch"`
	RoutingOrdinal                  uint64                   `json:"routing_ordinal"`
	ExecutionShard                  string                   `json:"execution_shard"`
	RoutingReason                   string                   `json:"routing_reason"`
	RoutePlanDigest                 string                   `json:"route_plan_digest"`
	RouteBatchSequence              uint64                   `json:"route_batch_sequence,omitempty"`
	RouteBatchTransactionCount      int                      `json:"route_batch_transaction_count,omitempty"`
	RouteBatchShardTransactionCount int                      `json:"route_batch_shard_transaction_count,omitempty"`
	ConsensusExecutionPredecessorOrdinals []uint64            `json:"consensus_execution_predecessor_ordinals,omitempty"`
	ConsensusOrderingPredecessorOrdinals  []uint64            `json:"consensus_ordering_predecessor_ordinals,omitempty"`
	ConsensusExecutionDepth               int                 `json:"consensus_execution_depth,omitempty"`
	ConsensusExecutionRound int `json:"consensus_execution_round,omitempty"`
	ConsensusWindowSequence uint64 `json:"consensus_window_sequence,omitempty"`
	ConsensusWindowStartBatchSequence uint64 `json:"consensus_window_start_batch_sequence,omitempty"`
	ConsensusWindowEndBatchSequence uint64 `json:"consensus_window_end_batch_sequence,omitempty"`
	ConsensusWindowRouteBatchCount int `json:"consensus_window_route_batch_count,omitempty"`
	ConsensusWindowTransactionCount int `json:"consensus_window_transaction_count,omitempty"`
	ConsensusWindowShardTransactionCount int `json:"consensus_window_shard_transaction_count,omitempty"`
	ConsensusWindowCriticalPath int `json:"consensus_window_critical_path,omitempty"`
	PredictedRemoteReads            int                      `json:"predicted_remote_reads"`
	PredictedRemoteWrites           int                      `json:"predicted_remote_writes"`
	StateVersions                   []StateVersionDependency `json:"state_versions,omitempty"`
	ControlPolicy                   string                   `json:"control_policy,omitempty"`
	LogicalDomains                  []string                 `json:"logical_domains,omitempty"`
	Local                           bool                     `json:"local,omitempty"`
	Bridge                          bool                     `json:"bridge,omitempty"`
	FrontierDigest                  string                   `json:"frontier_digest,omitempty"`
	AccessList                      []AccessItem             `json:"access_list,omitempty"`
}

// ComputeExecutionRoutingDigest binds a route entry to the signed transaction
// identity, declared access set and the batch route-plan digest. The entry
// digest deliberately excludes TxID and Signature to avoid a circular hash.
func ComputeExecutionRoutingDigest(t SignedTransaction, routing ExecutionRoutingMetadata) (string, error) {
	payload := executionRoutingDigestPayload{
		SenderID:                        t.Sender,
		ReceiverID:                      t.Receiver,
		Nonce:                           t.Nonce,
		StateKeys:                       append([]string(nil), t.StateKeys...),
		AccessListDigest:                t.AccessListDigest,
		Payload:                         t.Payload,
		RoutingEpoch:                    routing.RoutingEpoch,
		RoutingOrdinal:                  routing.RoutingOrdinal,
		ExecutionShard:                  routing.ExecutionShard,
		RoutingReason:                   routing.RoutingReason,
		RoutePlanDigest:                 routing.RoutePlanDigest,
		RouteBatchSequence:              routing.RouteBatchSequence,
		RouteBatchTransactionCount:      routing.RouteBatchTransactionCount,
		RouteBatchShardTransactionCount: routing.RouteBatchShardTransactionCount,
		ConsensusExecutionPredecessorOrdinals: append([]uint64(nil), routing.ConsensusExecutionPredecessorOrdinals...),
		ConsensusOrderingPredecessorOrdinals: append([]uint64(nil), routing.ConsensusOrderingPredecessorOrdinals...),
		ConsensusExecutionDepth: routing.ConsensusExecutionDepth,
		ConsensusExecutionRound: routing.ConsensusExecutionRound,
		ConsensusWindowSequence:              routing.ConsensusWindowSequence,
		ConsensusWindowStartBatchSequence:    routing.ConsensusWindowStartBatchSequence,
		ConsensusWindowEndBatchSequence:      routing.ConsensusWindowEndBatchSequence,
		ConsensusWindowRouteBatchCount:       routing.ConsensusWindowRouteBatchCount,
		ConsensusWindowTransactionCount:      routing.ConsensusWindowTransactionCount,
		ConsensusWindowShardTransactionCount: routing.ConsensusWindowShardTransactionCount,
		ConsensusWindowCriticalPath:          routing.ConsensusWindowCriticalPath,
		PredictedRemoteReads:            routing.PredictedRemoteReads,
		PredictedRemoteWrites:           routing.PredictedRemoteWrites,
		StateVersions:                   append([]StateVersionDependency(nil), routing.StateVersions...),
		ControlPolicy:                   routing.ControlPolicy,
		LogicalDomains:                  append([]string(nil), routing.LogicalDomains...),
		Local:                           routing.Local,
		Bridge:                          routing.Bridge,
		FrontierDigest:                  routing.FrontierDigest,
		AccessList:                      append([]AccessItem(nil), t.AccessList...),
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func ValidateExecutionRouting(t SignedTransaction) error {
	if t.ExecutionRouting == nil {
		return nil
	}
	routing := *t.ExecutionRouting
	if strings.TrimSpace(routing.SenderID) == "" || routing.SenderID != t.Sender {
		return fmt.Errorf("invalid execution routing sender_id")
	}
	if strings.TrimSpace(routing.ReceiverID) == "" || routing.ReceiverID != t.Receiver {
		return fmt.Errorf("invalid execution routing receiver_id")
	}
	if routing.RoutingOrdinal == 0 {
		return fmt.Errorf("invalid execution routing routing_ordinal")
	}
	if strings.TrimSpace(routing.ExecutionShard) == "" {
		return fmt.Errorf("invalid execution routing execution_shard")
	}
	if strings.TrimSpace(routing.RoutingReason) == "" {
		return fmt.Errorf("invalid execution routing routing_reason")
	}
	if strings.TrimSpace(routing.RoutePlanDigest) == "" {
		return fmt.Errorf("invalid execution routing route_plan_digest")
	}
	batchMetadataPresent := routing.RouteBatchSequence != 0 || routing.RouteBatchTransactionCount != 0 || routing.RouteBatchShardTransactionCount != 0
	if batchMetadataPresent {
		if routing.RouteBatchSequence == 0 || routing.RouteBatchTransactionCount <= 0 || routing.RouteBatchShardTransactionCount <= 0 || routing.RouteBatchShardTransactionCount > routing.RouteBatchTransactionCount {
			return fmt.Errorf("invalid execution routing route batch metadata")
		}
	}
	if routing.ConsensusExecutionDepth < 0 {
		return fmt.Errorf("invalid execution routing consensus execution depth")
	}
	if routing.ConsensusExecutionRound < 0 {
		return fmt.Errorf("invalid execution routing consensus execution round")
	}
	windowPresent := routing.ConsensusWindowSequence != 0 || routing.ConsensusWindowStartBatchSequence != 0 || routing.ConsensusWindowEndBatchSequence != 0 || routing.ConsensusWindowRouteBatchCount != 0 || routing.ConsensusWindowTransactionCount != 0 || routing.ConsensusWindowShardTransactionCount != 0 || routing.ConsensusWindowCriticalPath != 0
	if windowPresent {
		if routing.ConsensusWindowSequence == 0 || routing.ConsensusWindowStartBatchSequence == 0 || routing.ConsensusWindowEndBatchSequence < routing.ConsensusWindowStartBatchSequence || routing.ConsensusWindowRouteBatchCount != int(routing.ConsensusWindowEndBatchSequence-routing.ConsensusWindowStartBatchSequence+1) || routing.ConsensusWindowTransactionCount <= 0 || routing.ConsensusWindowShardTransactionCount <= 0 || routing.ConsensusWindowShardTransactionCount > routing.ConsensusWindowTransactionCount || routing.ConsensusWindowCriticalPath <= 0 || routing.RouteBatchSequence < routing.ConsensusWindowStartBatchSequence || routing.RouteBatchSequence > routing.ConsensusWindowEndBatchSequence {
			return fmt.Errorf("invalid execution routing consensus window metadata")
		}
	}
	seenConsensusPred := map[uint64]bool{}
	for _, values := range [][]uint64{routing.ConsensusExecutionPredecessorOrdinals, routing.ConsensusOrderingPredecessorOrdinals} {
		last := uint64(0)
		for _, predecessor := range values {
			if predecessor == 0 || predecessor >= routing.RoutingOrdinal || (last != 0 && predecessor <= last) || seenConsensusPred[predecessor] {
				return fmt.Errorf("invalid execution routing consensus predecessor ordinals")
			}
			seenConsensusPred[predecessor] = true
			last = predecessor
		}
	}
	if len(seenConsensusPred) > 0 && routing.ConsensusExecutionDepth <= 0 {
		return fmt.Errorf("invalid execution routing consensus execution depth")
	}
	if routing.PredictedRemoteReads < 0 || routing.PredictedRemoteWrites < 0 {
		return fmt.Errorf("invalid execution routing remote-access prediction")
	}
	seenVersions := map[string]bool{}
	lastKey := ""
	for _, dependency := range routing.StateVersions {
		if strings.TrimSpace(dependency.Key) == "" || seenVersions[dependency.Key] {
			return fmt.Errorf("invalid execution routing state version dependency")
		}
		if lastKey != "" && dependency.Key < lastKey {
			return fmt.Errorf("execution routing state versions must be sorted by key")
		}
		seenVersions[dependency.Key] = true
		lastKey = dependency.Key
		if dependency.ProducedVersion != 0 && dependency.ProducedVersion != routing.RoutingOrdinal {
			return fmt.Errorf("invalid execution routing produced state version")
		}
		if dependency.ProducedVersion != 0 && dependency.RequiredVersion >= dependency.ProducedVersion {
			return fmt.Errorf("invalid execution routing state version ordering")
		}
		if dependency.RequiredExecutionRound < 0 || (dependency.RequiredVersion == 0 && dependency.RequiredExecutionRound != 0) {
			return fmt.Errorf("invalid execution routing required execution round")
		}
		if dependency.RequiredExecutionRound > 0 && routing.ConsensusExecutionRound > 0 && dependency.RequiredExecutionRound >= routing.ConsensusExecutionRound {
			return fmt.Errorf("invalid execution routing execution round ordering")
		}
	}
	if routing.ControlPolicy != "" {
		switch routing.ControlPolicy {
		case "logical_domain_frontier_v1":
			if len(routing.LogicalDomains) == 0 || strings.TrimSpace(routing.FrontierDigest) == "" {
				return fmt.Errorf("invalid execution routing logical-domain frontier")
			}
			lastDomain := ""
			for _, domain := range routing.LogicalDomains {
				if strings.TrimSpace(domain) == "" || (lastDomain != "" && domain <= lastDomain) {
					return fmt.Errorf("execution routing logical domains must be sorted unique non-empty")
				}
				lastDomain = domain
			}
			if routing.Local == routing.Bridge {
				return fmt.Errorf("execution routing must be exactly one of local or bridge")
			}
			if routing.Local != (len(routing.LogicalDomains) == 1) || routing.Bridge != (len(routing.LogicalDomains) > 1) {
				return fmt.Errorf("execution routing local/bridge classification mismatch")
			}
		case "declared_access_frontier_v2":
			if strings.TrimSpace(routing.FrontierDigest) == "" {
				return fmt.Errorf("invalid execution routing declared-access frontier")
			}
			if len(routing.LogicalDomains) != 0 || routing.Local || routing.Bridge {
				return fmt.Errorf("declared-access frontier must not carry logical-domain admission state")
			}
		default:
			return fmt.Errorf("invalid execution routing control_policy")
		}
	}
	expected, err := ComputeExecutionRoutingDigest(t, routing)
	if err != nil {
		return err
	}
	if routing.RouteEntryDigest == "" || routing.RouteEntryDigest != expected {
		return fmt.Errorf("invalid execution routing route_entry_digest")
	}
	return nil
}
