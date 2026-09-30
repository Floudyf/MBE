package v5

import (
	"testing"

	"metaverse-chainlab/executor/realism/tx"
)

func TestMetaTrackV65672RoundFrontierDigestBindsSignedRoundMetadata(t *testing.T) {
	access := []tx.AccessItem{{Key: "asset:k", Mode: tx.AccessRead, UpdateSemantics: "validate"}}
	record := WorkloadRecord{
		Index:                                 1,
		LogicalID:                             "consumer",
		AccessList:                            append([]tx.AccessItem(nil), access...),
		SchedulingAccessList:                  append([]tx.AccessItem(nil), access...),
		RoutingOrdinal:                        2,
		StateVersions:                         []tx.StateVersionDependency{{Key: "asset:k", RequiredVersion: 1, RequiredExecutionRound: 1}},
		ConsensusExecutionPredecessorOrdinals: []uint64{1},
		ConsensusOrderingPredecessorOrdinals:  nil,
		ConsensusExecutionDepth:               1,
		ConsensusExecutionRound:               2,
	}
	digest := metaTrackDeclaredAccessFrontierDigestV6567(record, "s1")
	item := tx.SignedTransaction{
		TxID:                 "consumer",
		LogicalTxID:          "consumer",
		AccessList:           append([]tx.AccessItem(nil), access...),
		SchedulingAccessList: append([]tx.AccessItem(nil), access...),
		ExecutionRouting: &tx.ExecutionRoutingMetadata{
			RoutingOrdinal:                        2,
			ExecutionShard:                        "s1",
			ControlPolicy:                         metaTrackDeclaredAccessFrontierPolicy,
			FrontierDigest:                        digest,
			StateVersions:                         append([]tx.StateVersionDependency(nil), record.StateVersions...),
			ConsensusExecutionPredecessorOrdinals: append([]uint64(nil), record.ConsensusExecutionPredecessorOrdinals...),
			ConsensusOrderingPredecessorOrdinals:  append([]uint64(nil), record.ConsensusOrderingPredecessorOrdinals...),
			ConsensusExecutionDepth:               record.ConsensusExecutionDepth,
			ConsensusExecutionRound:               record.ConsensusExecutionRound,
		},
	}
	if err := validateMetaTrackDeclaredAccessFrontierBinding(item); err != nil {
		t.Fatalf("v6.5.6.7 frontier digest should validate after round annotation: %v", err)
	}

	item.ExecutionRouting.ConsensusExecutionRound = 3
	if err := validateMetaTrackDeclaredAccessFrontierBinding(item); err == nil {
		t.Fatal("tampered consensus execution round must invalidate frontier digest")
	}
	item.ExecutionRouting.ConsensusExecutionRound = 2
	item.ExecutionRouting.StateVersions[0].RequiredExecutionRound = 0
	if err := validateMetaTrackDeclaredAccessFrontierBinding(item); err == nil {
		t.Fatal("tampered required execution round must invalidate frontier digest")
	}
}

func TestMetaTrackV65672LegacyDeclaredAccessDigestRemainsCompatible(t *testing.T) {
	access := []tx.AccessItem{{Key: "asset:k", Mode: tx.AccessRead, UpdateSemantics: "validate"}}
	record := WorkloadRecord{
		Index:          0,
		LogicalID:      "legacy",
		AccessList:     append([]tx.AccessItem(nil), access...),
		RoutingOrdinal: 1,
		StateVersions:  []tx.StateVersionDependency{{Key: "asset:k", RequiredVersion: 0}},
	}
	digest := metaTrackDeclaredAccessFrontierDigest(record, "s0")
	item := tx.SignedTransaction{
		TxID:        "legacy",
		LogicalTxID: "legacy",
		AccessList:  append([]tx.AccessItem(nil), access...),
		ExecutionRouting: &tx.ExecutionRoutingMetadata{
			RoutingOrdinal: 1,
			ExecutionShard: "s0",
			ControlPolicy:  metaTrackDeclaredAccessFrontierPolicy,
			FrontierDigest: digest,
			StateVersions:  append([]tx.StateVersionDependency(nil), record.StateVersions...),
		},
	}
	if err := validateMetaTrackDeclaredAccessFrontierBinding(item); err != nil {
		t.Fatalf("legacy/current MetaTrack declared-access digest changed unexpectedly: %v", err)
	}
}
