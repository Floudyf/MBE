package v5

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"testing"
	"time"

	"metaverse-chainlab/executor/realism/consensus/pbft"
	"metaverse-chainlab/executor/realism/p2p"
)

func porygonV26AuthenticatedRuntime(t *testing.T) (*NodeRuntime, map[string]ed25519.PrivateKey) {
	t.Helper()
	nodes := make([]NodePlan, 4)
	validators := []string{"n0", "n1", "n2", "n3"}
	publicKeys := map[string]string{}
	privateKeys := map[string]ed25519.PrivateKey{}
	for i := range nodes {
		id := fmt.Sprintf("n%d", i)
		privateKey := deterministicPBFTTestPrivateKey(id)
		privateKeys[id] = privateKey
		publicKeys[id] = pbft.EncodePublicKey(privateKey.Public().(ed25519.PublicKey))
		nodes[i] = NodePlan{NodeID: id, ShardID: "porygon-global", ConsensusDomainID: "porygon-global", ExecutionShardID: "s0", Leader: i == 0, Validators: append([]string(nil), validators...)}
	}
	runtime := &NodeRuntime{plan: Plan{PBFTIdentityScheme: PBFTIdentitySchemeEd25519V1, PBFTPublicKeys: publicKeys, NodeConfigs: nodes}, node: nodes[0], runtimeMetricCounts: map[string]int64{}}
	return runtime, privateKeys
}

func TestPorygonV26AuthenticatedQuorumRejectsForgedAttestation(t *testing.T) {
	runtime, privateKeys := porygonV26AuthenticatedRuntime(t)
	resultByNode := map[string]PorygonESCWaveResult{}
	for _, nodeID := range []string{"n0", "n1", "n2"} {
		result := sealPorygonESCWaveResult(PorygonESCWaveResult{BlockHash: "block-v26", Height: 1, Wave: 0, ExecutionShardID: "s0", SenderNodeID: nodeID})
		att, err := signPorygonESCAttestationWithPrivateKey(result, nodeID, privateKeys[nodeID])
		if err != nil {
			t.Fatal(err)
		}
		if err := runtime.acceptPorygonESCWaveResultWithAttestation(result, att); err != nil {
			t.Fatal(err)
		}
		resultByNode[nodeID] = result
	}
	cert, ready, err := runtime.tryBuildPorygonESCWaveCertificate("block-v26", 1, 0, []string{"s0"})
	if err != nil || !ready {
		t.Fatalf("certificate not ready: ready=%v err=%v", ready, err)
	}
	if len(cert.Entries) != 1 || len(cert.Entries[0].Attestations) != 3 {
		t.Fatalf("authenticated quorum missing attestations: %+v", cert)
	}
	if err := runtime.validatePorygonCertificateAgainstPlan(cert, []string{"s0"}); err != nil {
		t.Fatal(err)
	}
	cert.Entries[0].Attestations[1].Signature = cert.Entries[0].Attestations[0].Signature
	if err := runtime.validatePorygonCertificateAgainstPlan(cert, []string{"s0"}); err == nil {
		t.Fatal("forged/reused attestation signature must be rejected")
	}
}

func TestPorygonV26CertificateBroadcastToleratesMinoritySendFailure(t *testing.T) {
	runtime := &NodeRuntime{node: NodePlan{NodeID: "n0", ShardID: "porygon-global", Leader: true, Validators: []string{"n0", "n1", "n2", "n3"}}, runtimeMetricCounts: map[string]int64{}}
	runtime.sendToNodeHook = func(_ context.Context, nodeID string, _ p2p.MessageEnvelope) error {
		if nodeID == "n3" {
			return fmt.Errorf("injected minority send failure")
		}
		return nil
	}
	cert := PorygonESCWaveCertificate{BlockHash: "block", Height: 1, Wave: 0, LeaderNodeID: "n0"}
	if err := runtime.broadcastPorygonESCWaveCertificate(context.Background(), cert); err != nil {
		t.Fatalf("minority send failure must not abort certificate broadcast: %v", err)
	}
	if got := runtime.runtimeMetricCounts["porygon_esc_certificate_broadcast_send_failure_count"]; got != 1 {
		t.Fatalf("send failure metric=%d want=1", got)
	}
}

func TestPorygonV28CertificateBroadcastFansOutConcurrently(t *testing.T) {
	runtime := &NodeRuntime{node: NodePlan{NodeID: "n0", ShardID: "porygon-global", Leader: true, Validators: []string{"n0", "n1", "n2", "n3"}}, runtimeMetricCounts: map[string]int64{}}
	entered := make(chan string, 3)
	release := make(chan struct{})
	runtime.sendToNodeHook = func(_ context.Context, nodeID string, _ p2p.MessageEnvelope) error {
		entered <- nodeID
		<-release
		return nil
	}
	done := make(chan error, 1)
	go func() {
		done <- runtime.broadcastPorygonESCWaveCertificate(context.Background(), PorygonESCWaveCertificate{BlockHash: "block", Height: 1, Wave: 0, LeaderNodeID: "n0"})
	}()
	seen := map[string]bool{}
	timer := time.NewTimer(500 * time.Millisecond)
	defer timer.Stop()
	for len(seen) < 3 {
		select {
		case nodeID := <-entered:
			seen[nodeID] = true
		case <-timer.C:
			close(release)
			t.Fatal("certificate fanout did not enter all peer sends concurrently")
		}
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := runtime.runtimeMetricCounts["porygon_esc_certificate_broadcast_target_count"]; got != 3 {
		t.Fatalf("broadcast target metric=%d want=3", got)
	}
	if got := runtime.runtimeMetricCounts["porygon_esc_certificate_broadcast_success_count"]; got != 3 {
		t.Fatalf("broadcast success metric=%d want=3", got)
	}
}

func TestPorygonV26CertificateRecoveryRequestResendsCachedCertificate(t *testing.T) {
	nodes := []NodePlan{
		{NodeID: "n0", ShardID: "porygon-global", Leader: true, Validators: []string{"n0", "n1"}},
		{NodeID: "n1", ShardID: "porygon-global", Validators: []string{"n0", "n1"}},
	}
	leader := &NodeRuntime{plan: Plan{NodeConfigs: nodes}, node: nodes[0], runtimeMetricCounts: map[string]int64{}}
	cert := sealPorygonESCWaveCertificate(PorygonESCWaveCertificate{BlockHash: "block-recovery", Height: 2, Wave: 3, LeaderNodeID: "n0", Entries: []PorygonESCWaveCertificateEntry{{ExecutionShardID: "s0", ResultDigest: "d", Voters: []string{"n0"}, Result: sealPorygonESCWaveResult(PorygonESCWaveResult{BlockHash: "block-recovery", Height: 2, Wave: 3, ExecutionShardID: "s0", SenderNodeID: "n0"})}}})
	state := leader.porygonESCState()
	state.mu.Lock()
	storePorygonESCWaveCertificateLocked(state, porygonESCWaveKey(cert.BlockHash, cert.Wave), cert)
	state.mu.Unlock()

	resent := false
	leader.sendToNodeHook = func(_ context.Context, nodeID string, msg p2p.MessageEnvelope) error {
		if nodeID == "n1" && msg.MessageType == porygonESCWaveCertificateMessage {
			resent = true
		}
		return nil
	}
	request := PorygonESCWaveCertificateRequest{BlockHash: cert.BlockHash, Height: cert.Height, Wave: cert.Wave, RequesterNodeID: "n1"}
	envelope, err := p2p.NewEnvelope(porygonESCWaveCertificateRequestMessage, "n1", "n0", "porygon-global", cert.Height, 0, cert.Height, request)
	if err != nil {
		t.Fatal(err)
	}
	if err := leader.handlePorygonESCWaveCertificateRequest(context.Background(), envelope); err != nil {
		t.Fatal(err)
	}
	if !resent {
		t.Fatal("cached certificate was not resent to requester")
	}
	if got := leader.runtimeMetricCounts["porygon_esc_certificate_recovery_resend_count"]; got != 1 {
		t.Fatalf("recovery resend metric=%d want=1", got)
	}
}
