package v3runtime

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCrossShardRelayPreviewSkeletonWritesArtifactsAndMessages(t *testing.T) {
	adapter := NetworkAdapterPreview{SelectedAdapter: NetworkAdapterInMemory}
	preview := RunCrossShardProtocolPreview(
		ExperimentProfile{CrossShardProtocol: CrossShardProtocolRelayPreview},
		adapter,
		[]TxResult{{
			TxID:                 "tx_cross_1",
			BlockHeight:          1,
			CommitTimeMS:         20,
			ShardID:              0,
			AccessedStateUnitIDs: []int{0, 1},
			CrossStateUnitAccess: true,
		}},
		[]RoutingRecord{{
			TxID:          "tx_cross_1",
			BlockHeight:   1,
			PrimaryShard:  0,
			TouchedShards: []int{0, 1},
			CrossShard:    true,
			RoutingPlugin: "metatrack_coaccess_routing",
		}},
	)
	if preview.ProtocolSelected != CrossShardProtocolRelayPreview || preview.TxCount != 1 || preview.RelayPreviewCount != 1 || preview.CompletedCount != 1 {
		t.Fatalf("unexpected relay preview summary: %+v", preview)
	}
	if len(preview.MessageRows) != 1 || preview.MessageRows[0].MessageType != "cross_shard_relay" {
		t.Fatalf("expected one cross_shard_relay message: %+v", preview.MessageRows)
	}
	if len(preview.TypedMessages) != 1 || preview.TypedMessages[0].MessageType != "cross_shard_relay" {
		t.Fatalf("expected typed relay message: %+v", preview.TypedMessages)
	}
	dir := t.TempDir()
	if err := WriteCrossShardProtocolArtifacts(dir, preview); err != nil {
		t.Fatal(err)
	}
	for _, filename := range []string{"cross_shard_tx_log.csv", "cross_shard_message_log.csv", "relay_preview_log.csv", "cross_shard_status.csv", "cross_shard_summary.json"} {
		if _, err := os.Stat(filepath.Join(dir, filename)); err != nil {
			t.Fatalf("missing artifact %s: %v", filename, err)
		}
	}
	summaryBytes, err := os.ReadFile(filepath.Join(dir, "cross_shard_summary.json"))
	if err != nil {
		t.Fatal(err)
	}
	summary := string(summaryBytes)
	if !strings.Contains(summary, `"runtime_truth": "cross_shard_protocol_skeleton_not_atomic_cross_shard_commit"`) || !strings.Contains(summary, `"not_atomic_cross_shard_commit": true`) {
		t.Fatalf("summary must contain V3.8 truth boundary:\n%s", summary)
	}
}

func TestCrossShardProtocolNoneOnlyDetectsAndSkips(t *testing.T) {
	preview := RunCrossShardProtocolPreview(
		ExperimentProfile{CrossShardProtocol: CrossShardProtocolNone},
		NetworkAdapterPreview{SelectedAdapter: NetworkAdapterInMemory},
		[]TxResult{{TxID: "tx_cross_1", ShardID: 0, AccessedStateUnitIDs: []int{0, 2}, CrossStateUnitAccess: true}},
		[]RoutingRecord{{TxID: "tx_cross_1", PrimaryShard: 0, TouchedShards: []int{0, 2}, CrossShard: true}},
	)
	if preview.TxCount != 1 || preview.MessageCount != 0 || preview.CompletedCount != 0 || preview.SkippedCount != 1 {
		t.Fatalf("protocol none should detect but not relay: %+v", preview)
	}
	if preview.StatusRows[0].State != "detected_no_protocol" {
		t.Fatalf("unexpected status row: %+v", preview.StatusRows)
	}
}

func TestCrossShardRelayMVPSuccessPathWritesArtifactsAndMetrics(t *testing.T) {
	preview := RunCrossShardProtocolPreview(
		ExperimentProfile{CrossShardProtocol: CrossShardProtocolRelayMVP},
		NetworkAdapterPreview{SelectedAdapter: NetworkAdapterInMemory},
		[]TxResult{{
			TxID:                 "tx_cross_1",
			BlockHeight:          1,
			CommitTimeMS:         20,
			ShardID:              0,
			AccessedStateUnitIDs: []int{0, 1},
			CrossStateUnitAccess: true,
		}},
		[]RoutingRecord{{
			TxID:          "tx_cross_1",
			BlockHeight:   1,
			PrimaryShard:  0,
			TouchedShards: []int{0, 1},
			CrossShard:    true,
			RoutingPlugin: "metatrack_coaccess_routing",
		}},
	)
	if !preview.RelayMVP.Enabled || preview.ProtocolSelected != CrossShardProtocolRelayMVP {
		t.Fatalf("expected Relay MVP enabled: %+v", preview)
	}
	if preview.RelayMVP.TxCount != 1 || preview.RelayMVP.SourceLockCount != 1 || preview.RelayMVP.CertificateCount != 1 {
		t.Fatalf("unexpected Relay MVP source/certificate counts: %+v", preview.RelayMVP)
	}
	if preview.RelayMVP.ProofVerifiedCount != 1 || preview.RelayMVP.TargetCommitCount != 1 || preview.RelayMVP.SourceFinalizedCount != 1 {
		t.Fatalf("unexpected Relay MVP success counts: %+v", preview.RelayMVP)
	}
	if preview.RelayMVP.SuccessCount != 1 || preview.RelayMVP.FailedCount != 0 || preview.CompletedCount != 1 || preview.FailedCount != 0 {
		t.Fatalf("unexpected Relay MVP result counts: %+v", preview.RelayMVP)
	}
	if len(preview.TypedMessages) != 1 || preview.TypedMessages[0].MessageType != "cross_shard_relay_mvp" {
		t.Fatalf("expected one Relay MVP typed message: %+v", preview.TypedMessages)
	}
	dir := t.TempDir()
	if err := WriteCrossShardProtocolArtifacts(dir, preview); err != nil {
		t.Fatal(err)
	}
	for _, filename := range []string{"relay_state_machine_log.csv", "source_lock_log.csv", "relay_certificate_log.csv", "relay_proof_verification_log.csv", "target_verification_log.csv", "target_commit_log.csv", "source_finalize_log.csv", "cross_shard_timeout_refund_log.csv", "cross_shard_failure_log.csv", "relay_mvp_summary.json"} {
		if _, err := os.Stat(filepath.Join(dir, filename)); err != nil {
			t.Fatalf("missing Relay MVP artifact %s: %v", filename, err)
		}
	}
	summaryBytes, err := os.ReadFile(filepath.Join(dir, "relay_mvp_summary.json"))
	if err != nil {
		t.Fatal(err)
	}
	summary := string(summaryBytes)
	if !strings.Contains(summary, `"runtime_truth": "relay_mvp_not_production_atomic_commit"`) || !strings.Contains(summary, `"not_byzantine_secure_relay": true`) {
		t.Fatalf("Relay MVP summary must contain truth boundary:\n%s", summary)
	}
}

func TestCrossShardRelayMVPProofFailureRefundPath(t *testing.T) {
	preview := RunCrossShardProtocolPreview(
		ExperimentProfile{CrossShardProtocol: CrossShardProtocolRelayMVP, RelayFailureMode: "proof_fail"},
		NetworkAdapterPreview{SelectedAdapter: NetworkAdapterInMemory},
		[]TxResult{{TxID: "tx_cross_1", BlockHeight: 1, CommitTimeMS: 20, ShardID: 0, AccessedStateUnitIDs: []int{0, 1}, CrossStateUnitAccess: true}},
		[]RoutingRecord{{TxID: "tx_cross_1", BlockHeight: 1, PrimaryShard: 0, TouchedShards: []int{0, 1}, CrossShard: true}},
	)
	if preview.RelayMVP.ProofFailedCount != 1 || preview.RelayMVP.RefundCount != 1 || preview.RelayMVP.FailedCount != 1 {
		t.Fatalf("expected proof failure refund path: %+v", preview.RelayMVP)
	}
	if preview.RelayMVP.TargetCommitCount != 0 || preview.CompletedCount != 0 || preview.FailedCount != 1 {
		t.Fatalf("proof failure should not target commit: preview=%+v relay=%+v", preview, preview.RelayMVP)
	}
}

func TestCrossShardRelayMVPTimeoutRefundPath(t *testing.T) {
	preview := RunCrossShardProtocolPreview(
		ExperimentProfile{CrossShardProtocol: CrossShardProtocolRelayMVP, RelayFailureMode: "timeout"},
		NetworkAdapterPreview{SelectedAdapter: NetworkAdapterInMemory},
		[]TxResult{{TxID: "tx_cross_1", BlockHeight: 1, CommitTimeMS: 20, ShardID: 0, AccessedStateUnitIDs: []int{0, 1}, CrossStateUnitAccess: true}},
		[]RoutingRecord{{TxID: "tx_cross_1", BlockHeight: 1, PrimaryShard: 0, TouchedShards: []int{0, 1}, CrossShard: true}},
	)
	if preview.RelayMVP.TimeoutCount != 1 || preview.RelayMVP.RefundCount != 1 || preview.RelayMVP.FailedCount != 1 {
		t.Fatalf("expected timeout refund path: %+v", preview.RelayMVP)
	}
	if preview.RelayMVP.CertificateCount != 0 || preview.RelayMVP.TargetCommitCount != 0 {
		t.Fatalf("timeout should happen before certificate/target commit: %+v", preview.RelayMVP)
	}
}

func TestRuntimeSummaryIncludesCrossShardProtocolMetrics(t *testing.T) {
	dir := t.TempDir()
	chain := filepath.Join(dir, "chain.yaml")
	plugin := filepath.Join(dir, "plugin.yaml")
	experiment := filepath.Join(dir, "experiment.yaml")
	if err := os.WriteFile(chain, []byte("chain_id: c\nblock_interval_ms: 100\nmax_tx_per_block: 8\nvalidator_count: 4\nexecution:\n  shard_count: 4\nstate:\n  storage_unit_count: 4\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(plugin, []byte("profiles:\n- plugin_profile_id: p\n  plugins:\n    TxPoolPlugin: fifo_pool\n    BlockProducer: time_or_count_block_producer\n    ConsensusPlugin: simple_leader\n    ConsensusRuntimePlugin: simple_leader\n    ShardingPlugin: metatrack_coaccess_routing\n    ExecutionSchedulerPlugin: serial_execution\n    StateAccessPlugin: remote_state_access_model\n    CommitPlugin: normal_commit\n    MetricsPlugin: basic_metrics\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(experiment, []byte("profile_id: e\nstage: V3.8\ntx_count: 20\nseed: 7\nkey_count: 16\nhot_key_count: 4\nshard_count: 4\nvalidators_per_shard: 4\ncross_shard_protocol: relay_preview\nnetwork_adapter: in_memory_message_bus\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	result, err := Run(Input{RunID: "cross_shard_runtime", ChainProfilePath: chain, PluginProfilePath: plugin, PluginProfileID: "p", ExperimentProfilePath: experiment, OutputDir: filepath.Join(dir, "out")})
	if err != nil {
		t.Fatal(err)
	}
	if result.Summary.CrossShardProtocolSelected != CrossShardProtocolRelayPreview {
		t.Fatalf("missing protocol selection: %+v", result.Summary)
	}
	if _, err := os.Stat(filepath.Join(dir, "out", "cross_shard_summary.json")); err != nil {
		t.Fatalf("missing cross-shard summary artifact: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "out", "cross_shard_tx_log.csv")); err != nil {
		t.Fatalf("missing cross-shard tx log artifact: %v", err)
	}
}

func TestRuntimeSummaryIncludesRelayMVPMetrics(t *testing.T) {
	dir := t.TempDir()
	chain := filepath.Join(dir, "chain.yaml")
	plugin := filepath.Join(dir, "plugin.yaml")
	experiment := filepath.Join(dir, "experiment.yaml")
	if err := os.WriteFile(chain, []byte("chain_id: c\nblock_interval_ms: 100\nmax_tx_per_block: 8\nvalidator_count: 4\nexecution:\n  shard_count: 4\nstate:\n  storage_unit_count: 4\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(plugin, []byte("profiles:\n- plugin_profile_id: p\n  plugins:\n    TxPoolPlugin: fifo_pool\n    BlockProducer: time_or_count_block_producer\n    ConsensusPlugin: simple_leader\n    ConsensusRuntimePlugin: simple_leader\n    ShardingPlugin: metatrack_coaccess_routing\n    ExecutionSchedulerPlugin: serial_execution\n    StateAccessPlugin: remote_state_access_model\n    CommitPlugin: normal_commit\n    MetricsPlugin: basic_metrics\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(experiment, []byte("profile_id: e\nstage: V3.11\ntx_count: 20\nseed: 7\nkey_count: 16\nhot_key_count: 4\nshard_count: 4\nvalidators_per_shard: 4\ncross_shard_protocol: relay_mvp\nnetwork_adapter: in_memory_message_bus\naccess_list_enabled: true\naggregation_candidates_enabled: true\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	result, err := Run(Input{RunID: "relay_mvp_runtime", ChainProfilePath: chain, PluginProfilePath: plugin, PluginProfileID: "p", ExperimentProfilePath: experiment, OutputDir: filepath.Join(dir, "out")})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Summary.RelayMVPEnabled || result.Summary.RelayMVPTxCount == 0 || result.Summary.RelaySuccessCount == 0 {
		t.Fatalf("missing Relay MVP summary metrics: %+v", result.Summary)
	}
	if result.Summary.RelayMVPTruth != RelayMVPRuntimeTruth {
		t.Fatalf("unexpected Relay MVP truth: %s", result.Summary.RelayMVPTruth)
	}
	for _, filename := range []string{"relay_state_machine_log.csv", "source_lock_log.csv", "relay_certificate_log.csv", "target_commit_log.csv", "relay_mvp_summary.json"} {
		if _, err := os.Stat(filepath.Join(dir, "out", filename)); err != nil {
			t.Fatalf("missing Relay MVP runtime artifact %s: %v", filename, err)
		}
	}
}
