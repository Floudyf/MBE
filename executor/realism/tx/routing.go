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
	Key             string `json:"key"`
	RequiredVersion uint64 `json:"required_version"`
	ProducedVersion uint64 `json:"produced_version,omitempty"`
}

type ExecutionRoutingMetadata struct {
	SenderID              string                   `json:"sender_id"`
	ReceiverID            string                   `json:"receiver_id"`
	RoutingEpoch          uint64                   `json:"routing_epoch"`
	RoutingOrdinal        uint64                   `json:"routing_ordinal"`
	ExecutionShard        string                   `json:"execution_shard"`
	RoutingReason         string                   `json:"routing_reason"`
	RoutePlanDigest       string                   `json:"route_plan_digest"`
	RouteEntryDigest      string                   `json:"route_entry_digest"`
	PredictedRemoteReads  int                      `json:"predicted_remote_reads"`
	PredictedRemoteWrites int                      `json:"predicted_remote_writes"`
	StateVersions         []StateVersionDependency `json:"state_versions,omitempty"`
}

type executionRoutingDigestPayload struct {
	SenderID              string                   `json:"sender_id"`
	ReceiverID            string                   `json:"receiver_id"`
	Nonce                 uint64                   `json:"nonce"`
	StateKeys             []string                 `json:"state_keys"`
	AccessListDigest      string                   `json:"access_list_digest"`
	Payload               string                   `json:"payload"`
	RoutingEpoch          uint64                   `json:"routing_epoch"`
	RoutingOrdinal        uint64                   `json:"routing_ordinal"`
	ExecutionShard        string                   `json:"execution_shard"`
	RoutingReason         string                   `json:"routing_reason"`
	RoutePlanDigest       string                   `json:"route_plan_digest"`
	PredictedRemoteReads  int                      `json:"predicted_remote_reads"`
	PredictedRemoteWrites int                      `json:"predicted_remote_writes"`
	StateVersions         []StateVersionDependency `json:"state_versions,omitempty"`
	AccessList            []AccessItem             `json:"access_list,omitempty"`
}

// ComputeExecutionRoutingDigest binds a route entry to the signed transaction
// identity, declared access set and the batch route-plan digest. The entry
// digest deliberately excludes TxID and Signature to avoid a circular hash.
func ComputeExecutionRoutingDigest(t SignedTransaction, routing ExecutionRoutingMetadata) (string, error) {
	payload := executionRoutingDigestPayload{
		SenderID:              t.Sender,
		ReceiverID:            t.Receiver,
		Nonce:                 t.Nonce,
		StateKeys:             append([]string(nil), t.StateKeys...),
		AccessListDigest:      t.AccessListDigest,
		Payload:               t.Payload,
		RoutingEpoch:          routing.RoutingEpoch,
		RoutingOrdinal:        routing.RoutingOrdinal,
		ExecutionShard:        routing.ExecutionShard,
		RoutingReason:         routing.RoutingReason,
		RoutePlanDigest:       routing.RoutePlanDigest,
		PredictedRemoteReads:  routing.PredictedRemoteReads,
		PredictedRemoteWrites: routing.PredictedRemoteWrites,
		StateVersions:         append([]StateVersionDependency(nil), routing.StateVersions...),
		AccessList:            append([]AccessItem(nil), t.AccessList...),
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
