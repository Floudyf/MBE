package v5

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/mempool"
	"metaverse-chainlab/executor/realism/tx"
)

func TestAriaPluginsRegisterAndRequirePairing(t *testing.T) {
	registry := BuiltinRegistry()
	producer, err := registry.Create("block_producer", ariaBlockProducerID, map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := producer.(BlockProducerPlugin); !ok {
		t.Fatalf("aria producer does not satisfy BlockProducerPlugin: %T", producer)
	}
	profile := ariaTestProfile()
	profile["block_executor"] = PluginConfig{PluginID: "serial_block_executor", Config: map[string]any{"worker_count": 1}}
	if _, err := InstantiatePlugins(profile); err == nil || !strings.Contains(err.Error(), "must be selected together") {
		t.Fatalf("expected Aria producer/executor pairing rejection, got %v", err)
	}
}

func TestAriaProducerSelectsOneBatchAndPersistsSelectionEnvelope(t *testing.T) {
	items, _, _, err := tx.Generate(tx.GenerateOptions{
		Count: 3, Sender: "aria-sender", Receiver: "aria-receiver", StartNonce: 0,
		Value: 10, Seed: "aria-producer-test", StartTimeMS: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	pool := mempool.New("n0", "s0", mempool.DefaultPolicy(), nil)
	for _, item := range items {
		if admitted := pool.AdmitAt(item, time.UnixMilli(item.Timestamp)); !admitted.Accepted {
			t.Fatalf("failed to admit %s: %#v", item.TxID, admitted)
		}
	}
	plugin := ariaBlockProducer{makeBasic("block_producer", ariaBlockProducerID, map[string]any{
		"block_size": 3, "candidate_scan_multiplier": 1,
		"reordering": true, "read_only_optimization": true, "retry_nonce_gaps": true,
	})}
	candidate, err := plugin.BuildCandidate(BlockProductionInput{
		Pool: pool, Proposer: realblock.NewProposer("n0", "s0"), Limit: 3,
		Now: time.UnixMilli(10), Context: context.Background(), BaseStateSnapshot: map[string]string{}, WorkerCount: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(candidate.TxList) != 1 || candidate.TxList[0].TxID != items[0].TxID {
		t.Fatalf("one Aria batch should select only the current nonce: %#v", candidate.TxList)
	}
	if pool.ReservedCount() != 1 {
		t.Fatalf("only selected transactions should remain reserved, got %d", pool.ReservedCount())
	}
	if candidate.ProposalEvidence == nil || candidate.ProposalEvidence.AlgorithmID != ariaCandidateSelectionEvidenceID {
		t.Fatalf("missing Aria proposal evidence: %#v", candidate.ProposalEvidence)
	}
	if stableTextDigest(string(candidate.ProposalEvidence.Payload)) != candidate.ProposalEvidence.PayloadDigest {
		t.Fatal("Aria proposal evidence digest mismatch")
	}
	var wire ariaCandidateSelectionConsensusWire
	if err := json.Unmarshal(candidate.ProposalEvidence.Payload, &wire); err != nil {
		t.Fatal(err)
	}
	if wire.WireVersion != ariaCandidateSelectionConsensusWireVersion || wire.CandidateCount != 3 || len(wire.SelectedTxIDs) != 1 || len(wire.DeferredTxIDs) != 2 || len(wire.DeferredTransactions) != 2 {
		t.Fatalf("incomplete compact Aria selection evidence: %#v", wire)
	}
	if wire.CandidatePayloadDigest == "" || wire.SelectionResultDigest == "" || wire.SelectionSemanticDigest == "" {
		t.Fatalf("missing consensus-bound Aria digests: %#v", wire)
	}
	payloadText := string(candidate.ProposalEvidence.Payload)
	if strings.Contains(payloadText, "\"candidate_transactions\"") || strings.Contains(payloadText, "\"trace\"") {
		t.Fatalf("compact Aria evidence still carries duplicate/diagnostic payload: %s", payloadText)
	}
	decoded, err := decodeAriaCandidateSelectionEvidence(candidate)
	if err != nil {
		t.Fatalf("compact evidence failed validation: %v", err)
	}
	if len(decoded.CandidateTransactions) != 3 {
		t.Fatalf("compact Aria evidence did not reconstruct full candidate batch: %#v", decoded)
	}
	pool.ReleaseReserved(candidate.TxList)
}

func TestAriaProducerAndExecutorConfigMustMatch(t *testing.T) {
	profile := ariaTestProfile()
	profile["block_producer"] = PluginConfig{PluginID: ariaBlockProducerID, Config: map[string]any{
		"reordering": false, "read_only_optimization": true, "retry_nonce_gaps": true,
	}}
	if _, err := InstantiatePlugins(profile); err == nil || !strings.Contains(err.Error(), "reordering mismatch") {
		t.Fatalf("expected Aria config mismatch rejection, got %v", err)
	}
}

func ariaTestProfile() map[string]PluginConfig {
	return map[string]PluginConfig{
		"workload":              {PluginID: "deterministic_signed_synthetic", Config: map[string]any{}},
		"transaction_admission": {PluginID: "signature_nonce_admission", Config: map[string]any{}},
		"txpool":                {PluginID: "fifo_per_node_mempool", Config: map[string]any{}},
		"sharding":              {PluginID: "deterministic_state_key_sharding", Config: map[string]any{}},
		"routing":               {PluginID: "hash_routing_baseline", Config: map[string]any{}},
		"block_producer":        {PluginID: ariaBlockProducerID, Config: map[string]any{"reordering": true, "read_only_optimization": true, "retry_nonce_gaps": true}},
		"consensus":             {PluginID: "pbft_style_consensus", Config: map[string]any{}},
		"network":               {PluginID: "localhost_tcp_typed_network", Config: map[string]any{}},
		"execution":             {PluginID: "serial_execution_baseline", Config: map[string]any{}},
		"scheduler":             {PluginID: "fifo_serial_scheduler", Config: map[string]any{}},
		"block_executor":        {PluginID: "aria_block_executor", Config: map[string]any{"worker_count": 4, "reordering": true, "read_only_optimization": true, "retry_nonce_gaps": true}},
		"state_access":          {PluginID: "direct_state_access", Config: map[string]any{}},
		"state_storage":         {PluginID: "persistent_local_state_store", Config: map[string]any{}},
		"cross_shard":           {PluginID: "relay_certificate_protocol", Config: map[string]any{}},
		"commit":                {PluginID: "normal_commit", Config: map[string]any{}},
		"fault_injection":       {PluginID: "faults_disabled", Config: map[string]any{}},
		"metrics":               {PluginID: "runtime_core_metrics", Config: map[string]any{}},
		"observability":         {PluginID: "node_network_consensus_observer", Config: map[string]any{}},
	}
}

func TestProposalSelectionEvidenceVerificationRejectsTampering(t *testing.T) {
	items, _, _, err := tx.Generate(tx.GenerateOptions{
		Count: 2, Sender: "aria-tamper-sender", Receiver: "aria-tamper-receiver", StartNonce: 0,
		Value: 10, Seed: "aria-tamper-test", StartTimeMS: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	pool := mempool.New("n0", "s0", mempool.DefaultPolicy(), nil)
	for _, item := range items {
		if admitted := pool.AdmitAt(item, time.UnixMilli(item.Timestamp)); !admitted.Accepted {
			t.Fatalf("failed to admit %s: %#v", item.TxID, admitted)
		}
	}
	producer := ariaBlockProducer{makeBasic("block_producer", ariaBlockProducerID, map[string]any{
		"block_size": 2, "candidate_scan_multiplier": 1,
		"reordering": true, "read_only_optimization": true, "retry_nonce_gaps": true,
	})}
	candidate, err := producer.BuildCandidate(BlockProductionInput{
		Pool: pool, Proposer: realblock.NewProposer("n0", "s0"), Limit: 2,
		Now: time.UnixMilli(10), Context: context.Background(), BaseStateSnapshot: map[string]string{}, WorkerCount: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime := &NodeRuntime{plugins: RuntimePlugins{BlockProducer: producer}}
	if err := runtime.verifyProposalEvidenceEnvelope(candidate); err != nil {
		t.Fatalf("valid evidence rejected: %v", err)
	}

	var evidence ariaCandidateSelectionEvidence
	if err := json.Unmarshal(candidate.ProposalEvidence.Payload, &evidence); err != nil {
		t.Fatal(err)
	}
	evidence.SelectedTxIDs = []string{"tx-other"}
	encoded, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	candidate.ProposalEvidence.Payload = encoded
	candidate.ProposalEvidence.PayloadDigest = stableTextDigest(string(encoded))
	if err := runtime.verifyProposalEvidenceEnvelope(candidate); err == nil {
		t.Fatal("tampered selected list was accepted")
	}
}

func TestAriaEvidenceRejectsSelectedPayloadTampering(t *testing.T) {
	items, _, _, err := tx.Generate(tx.GenerateOptions{
		Count: 1, Sender: "aria-payload-sender", Receiver: "aria-payload-receiver", StartNonce: 0,
		Value: 10, Seed: "aria-payload-test", StartTimeMS: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	pool := mempool.New("n0", "s0", mempool.DefaultPolicy(), nil)
	if admitted := pool.AdmitAt(items[0], time.UnixMilli(items[0].Timestamp)); !admitted.Accepted {
		t.Fatalf("failed to admit transaction: %#v", admitted)
	}
	producer := ariaBlockProducer{makeBasic("block_producer", ariaBlockProducerID, map[string]any{"block_size": 1, "candidate_scan_multiplier": 1})}
	candidate, err := producer.BuildCandidate(BlockProductionInput{
		Pool: pool, Proposer: realblock.NewProposer("n0", "s0"), Limit: 1,
		Now: time.UnixMilli(10), Context: context.Background(), BaseStateSnapshot: map[string]string{}, WorkerCount: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	candidate.TxList[0].Payload += "-tampered"
	if _, err := decodeAriaCandidateSelectionEvidence(candidate); err == nil || !strings.Contains(err.Error(), "payload mismatch") {
		t.Fatalf("selected payload tampering was not rejected: %v", err)
	}
}
