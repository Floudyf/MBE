package v5

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"

	"metaverse-chainlab/executor/realism/p2p"
)

const porygonESCWaveCertificateRequestMessage = "PORYGON_ESC_WAVE_CERTIFICATE_REQUEST_V1"
const porygonESCWaveCertificateCacheLimit = 16384

// PorygonESCAttestation binds one authenticated validator identity to exactly
// one semantic ESC result. It intentionally reuses the PBFT node identity so
// the execution-result quorum has the same validator identity root as ordering.
type PorygonESCAttestation struct {
	NodeID           string `json:"node_id"`
	BlockHash        string `json:"block_hash"`
	Height           uint64 `json:"height"`
	Wave             int    `json:"wave"`
	ExecutionShardID string `json:"execution_shard_id"`
	ResultDigest     string `json:"result_digest"`
	Signature        string `json:"signature"`
}

type PorygonESCWaveResultWire struct {
	Result      PorygonESCWaveResult  `json:"result"`
	Attestation PorygonESCAttestation `json:"attestation"`
}

type PorygonESCWaveCertificateRequest struct {
	BlockHash       string `json:"block_hash"`
	Height          uint64 `json:"height"`
	Wave            int    `json:"wave"`
	RequesterNodeID string `json:"requester_node_id"`
}

func porygonESCAttestationSigningBytes(att PorygonESCAttestation) []byte {
	projection := struct {
		NodeID           string `json:"node_id"`
		BlockHash        string `json:"block_hash"`
		Height           uint64 `json:"height"`
		Wave             int    `json:"wave"`
		ExecutionShardID string `json:"execution_shard_id"`
		ResultDigest     string `json:"result_digest"`
	}{
		NodeID: att.NodeID, BlockHash: att.BlockHash, Height: att.Height, Wave: att.Wave,
		ExecutionShardID: att.ExecutionShardID, ResultDigest: att.ResultDigest,
	}
	payload, _ := json.Marshal(projection)
	return payload
}

func signPorygonESCAttestationWithPrivateKey(result PorygonESCWaveResult, nodeID string, privateKey ed25519.PrivateKey) (PorygonESCAttestation, error) {
	if err := validatePorygonESCWaveResult(result); err != nil {
		return PorygonESCAttestation{}, err
	}
	if nodeID == "" || nodeID != result.SenderNodeID {
		return PorygonESCAttestation{}, fmt.Errorf("porygon ESC attestation signer mismatch")
	}
	att := PorygonESCAttestation{
		NodeID: nodeID, BlockHash: result.BlockHash, Height: result.Height, Wave: result.Wave,
		ExecutionShardID: result.ExecutionShardID, ResultDigest: result.ResultDigest,
	}
	att.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, porygonESCAttestationSigningBytes(att)))
	return att, nil
}

func (r *NodeRuntime) signPorygonESCAttestation(result PorygonESCWaveResult) (PorygonESCAttestation, error) {
	privateKey, err := r.pbftSigningPrivateKey()
	if err != nil {
		return PorygonESCAttestation{}, err
	}
	return signPorygonESCAttestationWithPrivateKey(result, r.node.NodeID, privateKey)
}

func (r *NodeRuntime) verifyPorygonESCAttestation(result PorygonESCWaveResult, att PorygonESCAttestation) error {
	if att.NodeID == "" || att.Signature == "" {
		return fmt.Errorf("porygon ESC attestation is incomplete")
	}
	if att.NodeID != result.SenderNodeID || att.BlockHash != result.BlockHash || att.Height != result.Height || att.Wave != result.Wave || att.ExecutionShardID != result.ExecutionShardID || att.ResultDigest != result.ResultDigest {
		return fmt.Errorf("porygon ESC attestation/result identity mismatch for %s", att.NodeID)
	}
	return r.verifyPorygonESCAttestationIdentity(att)
}

func (r *NodeRuntime) verifyPorygonESCAttestationIdentity(att PorygonESCAttestation) error {
	publicKey, err := r.pbftPublicKey(att.NodeID)
	if err != nil {
		return err
	}
	signature, err := base64.StdEncoding.DecodeString(att.Signature)
	if err != nil {
		return fmt.Errorf("porygon ESC attestation signature encoding for %s: %w", att.NodeID, err)
	}
	if !ed25519.Verify(publicKey, porygonESCAttestationSigningBytes(att), signature) {
		return fmt.Errorf("porygon ESC attestation signature verification failed for %s", att.NodeID)
	}
	return nil
}

func sortedPorygonESCAttestations(values map[string]PorygonESCAttestation) []PorygonESCAttestation {
	out := make([]PorygonESCAttestation, 0, len(values))
	for _, value := range values {
		out = append(out, value)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out
}

func (r *NodeRuntime) verifyPorygonCertificateEntryAttestations(entry PorygonESCWaveCertificateEntry) error {
	if len(entry.Attestations) == 0 {
		if r.pbftAuthenticationRequired() {
			return fmt.Errorf("porygon ESC certificate for %s has no authenticated attestations", entry.ExecutionShardID)
		}
		return nil
	}
	members := r.porygonExecutionShardMembers(entry.ExecutionShardID)
	threshold := porygonExecutionResultThreshold(len(members))
	if len(entry.Attestations) < threshold {
		return fmt.Errorf("porygon ESC certificate attestation threshold not met for %s: got=%d want>=%d", entry.ExecutionShardID, len(entry.Attestations), threshold)
	}
	voterSet := map[string]bool{}
	for _, voter := range entry.Voters {
		voterSet[voter] = true
	}
	seen := map[string]bool{}
	for _, att := range entry.Attestations {
		if seen[att.NodeID] || !containsString(members, att.NodeID) {
			return fmt.Errorf("porygon ESC certificate has invalid attestation voter %s", att.NodeID)
		}
		if !voterSet[att.NodeID] {
			return fmt.Errorf("porygon ESC attestation voter %s missing from certificate voter set", att.NodeID)
		}
		if att.BlockHash != entry.Result.BlockHash || att.Height != entry.Result.Height || att.Wave != entry.Result.Wave || att.ExecutionShardID != entry.ExecutionShardID || att.ResultDigest != entry.ResultDigest {
			return fmt.Errorf("porygon ESC certificate attestation identity mismatch for %s", att.NodeID)
		}
		if err := r.verifyPorygonESCAttestationIdentity(att); err != nil {
			return err
		}
		seen[att.NodeID] = true
	}
	if r.pbftAuthenticationRequired() {
		for _, voter := range entry.Voters {
			if !seen[voter] {
				return fmt.Errorf("porygon ESC certificate voter %s lacks authenticated attestation", voter)
			}
		}
	}
	return nil
}

func storePorygonESCWaveCertificateLocked(state *porygonESCExchangeState, key string, cert PorygonESCWaveCertificate) {
	if _, exists := state.certificates[key]; !exists {
		state.certificateOrder = append(state.certificateOrder, key)
	}
	state.certificates[key] = cert
	prunePorygonESCWaveCertificatesLocked(state)
}

func prunePorygonESCWaveCertificatesLocked(state *porygonESCExchangeState) {
	for len(state.certificateOrder) > porygonESCWaveCertificateCacheLimit {
		oldest := state.certificateOrder[0]
		state.certificateOrder = state.certificateOrder[1:]
		delete(state.certificates, oldest)
	}
}

func (r *NodeRuntime) addPorygonRuntimeMetric(name string, delta int64) {
	if delta == 0 {
		return
	}
	r.mu.Lock()
	if r.runtimeMetricCounts == nil {
		r.runtimeMetricCounts = map[string]int64{}
	}
	r.runtimeMetricCounts[name] += delta
	r.mu.Unlock()
}

func (r *NodeRuntime) sendPorygonESCWaveCertificateToNode(ctx context.Context, nodeID string, cert PorygonESCWaveCertificate) error {
	envelope, err := p2p.NewEnvelope(porygonESCWaveCertificateMessage, r.node.NodeID, nodeID, r.node.ShardID, cert.Height, r.currentPBFTView(), cert.Height, cert)
	if err != nil {
		return err
	}
	return r.sendToNode(ctx, nodeID, envelope)
}

func (r *NodeRuntime) requestPorygonESCWaveCertificate(ctx context.Context, blockHash string, height uint64, wave int) error {
	leader := r.leaderID(r.node.ShardID)
	if leader == "" || leader == r.node.NodeID {
		return nil
	}
	request := PorygonESCWaveCertificateRequest{BlockHash: blockHash, Height: height, Wave: wave, RequesterNodeID: r.node.NodeID}
	envelope, err := p2p.NewEnvelope(porygonESCWaveCertificateRequestMessage, r.node.NodeID, leader, r.node.ShardID, height, r.currentPBFTView(), height, request)
	if err != nil {
		return err
	}
	if err := r.sendToNode(ctx, leader, envelope); err != nil {
		return err
	}
	r.addPorygonRuntimeMetric("porygon_esc_certificate_recovery_request_count", 1)
	return nil
}

func (r *NodeRuntime) handlePorygonESCWaveCertificateRequest(ctx context.Context, msg p2p.MessageEnvelope) error {
	if !r.isCurrentLeader() {
		return fmt.Errorf("porygon ESC certificate request sent to non-leader")
	}
	request, err := p2p.DecodePayload[PorygonESCWaveCertificateRequest](msg)
	if err != nil {
		return err
	}
	if request.RequesterNodeID == "" || msg.FromNode != request.RequesterNodeID || msg.Height != request.Height || !containsString(r.node.Validators, request.RequesterNodeID) {
		return fmt.Errorf("porygon ESC certificate request identity mismatch")
	}
	key := porygonESCWaveKey(request.BlockHash, request.Wave)
	state := r.porygonESCState()
	state.mu.Lock()
	cert, ok := state.certificates[key]
	state.mu.Unlock()
	if !ok {
		return nil
	}
	if err := r.sendPorygonESCWaveCertificateToNode(ctx, request.RequesterNodeID, cert); err != nil {
		return err
	}
	r.addPorygonRuntimeMetric("porygon_esc_certificate_recovery_resend_count", 1)
	return nil
}
