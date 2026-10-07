package v5

import (
	"crypto/ed25519"
	"encoding/base64"
	"strings"
	"testing"
)

func porygonV19BatchAttestationForTest(t *testing.T, result PorygonESCBatchResult, nodeID string, key ed25519.PrivateKey) PorygonESCBatchAttestation {
	t.Helper()
	att := PorygonESCBatchAttestation{
		NodeID:               nodeID,
		BlockHash:            result.BlockHash,
		Height:               result.Height,
		ExecutionShardID:     result.ExecutionShardID,
		CommitteeEpochDigest: result.CommitteeEpochDigest,
		ResultDigest:         result.ResultDigest,
	}
	att.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(key, porygonBatchAttestationBytes(att)))
	return att
}

func TestPorygonV19BatchCertificateAcceptsMultipleReplicaAttestations(t *testing.T) {
	runtime, privateKeys := porygonV26AuthenticatedRuntime(t)
	const height uint64 = 1
	const executionShard = "s0"

	members := runtime.porygonExecutionRoleMembers(height, executionShard)
	threshold := porygonShardedExecutionResultThreshold(len(members))
	if len(members) < 2 || threshold < 2 {
		t.Fatalf("fixture must expose a multi-replica ESC threshold: members=%v threshold=%d", members, threshold)
	}

	result := porygonSealBatchResult(PorygonESCBatchResult{
		BlockHash:            "porygon-v19-batch-cert",
		Height:               height,
		ExecutionShardID:     executionShard,
		CommitteeEpochDigest: porygonCommitteeEpochSeed(height, runtime.node.ShardID),
		SenderNodeID:         members[0],
	})
	voters := append([]string(nil), members[:threshold]...)
	atts := make([]PorygonESCBatchAttestation, 0, threshold)
	for _, nodeID := range voters {
		key, ok := privateKeys[nodeID]
		if !ok {
			t.Fatalf("missing private key for %s", nodeID)
		}
		atts = append(atts, porygonV19BatchAttestationForTest(t, result, nodeID, key))
	}

	cert := PorygonESCBatchCertificate{
		BlockHash:            result.BlockHash,
		Height:               height,
		CommitteeEpochDigest: result.CommitteeEpochDigest,
		LeaderNodeID:         runtime.node.NodeID,
		Entries: []PorygonESCBatchCertificateEntry{{
			ExecutionShardID: executionShard,
			ResultDigest:     result.ResultDigest,
			Threshold:        threshold,
			Voters:           voters,
			Attestations:     atts,
			Result:           result,
		}},
	}
	cert.CertificateDigest = porygonBatchCertDigest(cert)

	if err := runtime.validatePorygonBatchCertificate(cert, []string{executionShard}); err != nil {
		t.Fatalf("multi-replica batch certificate rejected: %v", err)
	}

	foreign := atts[1]
	if err := runtime.verifyPorygonBatchResultAttestation(result, foreign); err == nil || !strings.Contains(err.Error(), "sender mismatch") {
		t.Fatalf("direct result accepted an attestation from a different sender: %v", err)
	}
}

func TestPorygonV19BatchCertificateRecomputesRepresentativeDigest(t *testing.T) {
	runtime, privateKeys := porygonV26AuthenticatedRuntime(t)
	const height uint64 = 1
	const executionShard = "s0"

	members := runtime.porygonExecutionRoleMembers(height, executionShard)
	threshold := porygonShardedExecutionResultThreshold(len(members))
	result := porygonSealBatchResult(PorygonESCBatchResult{
		BlockHash:            "porygon-v19-tamper",
		Height:               height,
		ExecutionShardID:     executionShard,
		CommitteeEpochDigest: porygonCommitteeEpochSeed(height, runtime.node.ShardID),
		SenderNodeID:         members[0],
	})
	voters := append([]string(nil), members[:threshold]...)
	atts := make([]PorygonESCBatchAttestation, 0, threshold)
	for _, nodeID := range voters {
		atts = append(atts, porygonV19BatchAttestationForTest(t, result, nodeID, privateKeys[nodeID]))
	}
	cert := PorygonESCBatchCertificate{
		BlockHash:            result.BlockHash,
		Height:               height,
		CommitteeEpochDigest: result.CommitteeEpochDigest,
		LeaderNodeID:         runtime.node.NodeID,
		Entries: []PorygonESCBatchCertificateEntry{{
			ExecutionShardID: executionShard,
			ResultDigest:     result.ResultDigest,
			Threshold:        threshold,
			Voters:           voters,
			Attestations:     atts,
			Result:           result,
		}},
	}
	cert.Entries[0].Result.BusinessExecutionUS = 99 // excluded from digest and intentionally diagnostic
	cert.Entries[0].Result.Results = []PorygonBatchTxResult{{TxID: "tampered-business-result"}}
	cert.CertificateDigest = porygonBatchCertDigest(cert)

	if err := runtime.validatePorygonBatchCertificate(cert, []string{executionShard}); err == nil || !strings.Contains(err.Error(), "entry mismatch") {
		t.Fatalf("tampered representative result was accepted: %v", err)
	}
}
