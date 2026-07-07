package node

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

var v43Fixture struct {
	once      sync.Once
	outDir    string
	summary   FinalSummaryV43
	artifacts []string
	err       error
}

func TestV43FinalSmoke(t *testing.T) {
	outDir, summary, artifacts := v43SmokeFixture(t)
	if !summary.ReadyToCommit {
		t.Fatalf("expected ready V4.3 summary: %+v", summary)
	}
	if !summary.SenderPublicKeyBinding || !summary.RealCrossShardNetworkCommit || !summary.RealFaultInjection || !summary.BlockEmulatorTraceToSignedTx {
		t.Fatalf("missing V4.3 truth fields: %+v", summary)
	}
	for _, name := range []string{"v4_3_realism_final_summary.json", "v4_3_acceptance_report.json", "v4_3_self_check_report.md", "xshard_finality_summary.json", "xshard_pbft_message_log.csv", "xshard_refund_log.csv", "network_fault_log.csv", "blockemulator_import_summary.json", "blockemulator_signed_txs.jsonl"} {
		info, err := os.Stat(filepath.Join(outDir, name))
		if err != nil {
			t.Fatalf("missing artifact %s: %v", name, err)
		}
		if info.Size() == 0 {
			t.Fatalf("empty artifact %s", name)
		}
	}
	if len(artifacts) == 0 {
		t.Fatal("expected artifact list")
	}
}

func TestV43CrossShardRealNetworkCommitSuccess(t *testing.T) {
	outDir, summary, _ := v43SmokeFixture(t)
	if !summary.RealCrossShardNetworkCommit || summary.CrossShardTxCount == 0 || summary.ProductionAtomicCommit || summary.ByzantineSecureRelay {
		t.Fatalf("unexpected cross-shard truth fields: %+v", summary)
	}
	var evidence struct {
		RealCrossShardNetworkCommit bool   `json:"real_cross_shard_network_commit"`
		PBFTMessageCount            int    `json:"pbft_message_count"`
		SourceLockBlockHash         string `json:"source_lock_block_hash"`
		TargetCommitBlockHash       string `json:"target_commit_block_hash"`
		SourceFinalizeBlockHash     string `json:"source_finalize_block_hash"`
	}
	raw, err := os.ReadFile(filepath.Join(outDir, "xshard_finality_summary.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &evidence); err != nil {
		t.Fatal(err)
	}
	if !evidence.RealCrossShardNetworkCommit || evidence.PBFTMessageCount == 0 || evidence.SourceLockBlockHash == "" || evidence.TargetCommitBlockHash == "" || evidence.SourceFinalizeBlockHash == "" {
		t.Fatalf("incomplete xshard network commit evidence: %+v", evidence)
	}
}

func TestV43CrossShardArtifactsWritten(t *testing.T) {
	outDir, _, _ := v43SmokeFixture(t)
	for _, name := range []string{"xshard_network_log.csv", "xshard_pbft_message_log.csv", "xshard_source_commit_log.csv", "xshard_target_commit_log.csv", "xshard_certificate_log.csv", "xshard_refund_log.csv"} {
		info, err := os.Stat(filepath.Join(outDir, name))
		if err != nil {
			t.Fatalf("missing artifact %s: %v", name, err)
		}
		if info.Size() == 0 {
			t.Fatalf("empty artifact %s", name)
		}
	}
}

func TestBlockEmulatorImportedTxsRunV43Smoke(t *testing.T) {
	outDir := t.TempDir()
	csvPath := filepath.Join(outDir, "selectedTxs_300K_subset.csv")
	if err := os.WriteFile(csvPath, []byte("from,to,amount,time\nalice,bob,1,1\ncarol,dave,2,2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	summary, _, err := RunV43FinalSmoke(context.Background(), SmokeOptionsV43{OutDir: outDir, Nodes: 4, Shards: 2, TxCount: 4, EnableCrossShard: true, EnableFaults: true, FaultProfile: "network_delay", BlockEmulatorCSV: csvPath, BlockEmulatorTxLimit: 2, FrontendAvailable: true, FrontendE2EPass: true})
	if err != nil {
		t.Fatal(err)
	}
	if !summary.BlockEmulatorTraceToSignedTx || !summary.BlockEmulatorV4RunCompleted || summary.BlockEmulatorImportedTxCount != 2 || summary.SignedTxVerifyPassCount != 2 || summary.FullBlockEmulatorCompatibility {
		t.Fatalf("unexpected BlockEmulator bridge summary: %+v", summary)
	}
}

func v43SmokeFixture(t *testing.T) (string, FinalSummaryV43, []string) {
	t.Helper()
	v43Fixture.once.Do(func() {
		v43Fixture.outDir, v43Fixture.err = os.MkdirTemp("", "mbe-v43-smoke-*")
		if v43Fixture.err != nil {
			return
		}
		v43Fixture.summary, v43Fixture.artifacts, v43Fixture.err = RunV43FinalSmoke(context.Background(), SmokeOptionsV43{OutDir: v43Fixture.outDir, Nodes: 4, Shards: 2, TxCount: 4, EnableCrossShard: true, EnableFaults: true, FaultProfile: "network_delay", BlockEmulatorTxLimit: 2, FrontendAvailable: true, FrontendE2EPass: true})
	})
	if v43Fixture.err != nil {
		t.Fatal(v43Fixture.err)
	}
	return v43Fixture.outDir, v43Fixture.summary, v43Fixture.artifacts
}
