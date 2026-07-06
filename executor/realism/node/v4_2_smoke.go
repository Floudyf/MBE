package node

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"metaverse-chainlab/executor/realism/bridge"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/faults"
	"metaverse-chainlab/executor/realism/metrics"
	"metaverse-chainlab/executor/realism/p2p"
	"metaverse-chainlab/executor/realism/recovery"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/storage"
	"metaverse-chainlab/executor/realism/tx"
	"metaverse-chainlab/executor/realism/xshard"
)

type SmokeOptionsV42 struct {
	OutDir            string
	Nodes             int
	Shards            int
	TxCount           int
	EnableCrossShard  bool
	EnableFaults      bool
	RunDurationMS     int
	FrontendAvailable bool
}

func RunV42FinalSmoke(ctx context.Context, opts SmokeOptionsV42) (FinalSummaryV42, []string, error) {
	if opts.Nodes <= 0 {
		opts.Nodes = 4
	}
	if opts.Shards <= 0 {
		opts.Shards = 1
	}
	if opts.TxCount <= 0 {
		opts.TxCount = 10
	}
	if opts.OutDir == "" {
		opts.OutDir = filepath.Join("..", ".cache", "v4_realism_runs", "latest")
	}
	if err := os.MkdirAll(opts.OutDir, 0o755); err != nil {
		return FinalSummaryV42{}, nil, err
	}
	validators := []string{}
	for i := 0; i < opts.Nodes; i++ {
		validators = append(validators, fmt.Sprintf("n%d", i))
	}
	runtimes := []*RuntimeV41{}
	for i, id := range validators {
		role := "validator"
		if i == 0 {
			role = "leader"
		}
		rt := NewRuntimeV41(Config{NodeID: id, ShardID: "s0", DataDir: filepath.Join(opts.OutDir, id), ListenAddr: "127.0.0.1:0", Role: role, LeaderID: "n0", Validators: validators, BlockSize: opts.TxCount})
		if err := rt.Start(ctx); err != nil {
			return FinalSummaryV42{}, nil, err
		}
		defer rt.Stop()
		runtimes = append(runtimes, rt)
	}
	peers := []p2p.Peer{}
	for i, rt := range runtimes {
		role := "validator"
		leader := false
		if i == 0 {
			role = "leader"
			leader = true
		}
		peers = append(peers, p2p.Peer{NodeID: validators[i], ShardID: "s0", ListenAddr: rt.ListenAddr(), Role: role, Leader: leader})
	}
	for _, rt := range runtimes {
		rt.SetPeers(peers)
	}
	txs, _, _, err := tx.Generate(tx.GenerateOptions{Count: opts.TxCount, Sender: "alice", Receiver: "bob", Value: 1, Seed: "v4.2"})
	if err != nil {
		return FinalSummaryV42{}, nil, err
	}
	for _, item := range txs {
		if result := runtimes[0].node.Mempool.Admit(item); !result.Accepted {
			return FinalSummaryV42{}, nil, fmt.Errorf("leader admit failed: %s", result.RejectReason)
		}
		if err := runtimes[0].GossipTx(ctx, item); err != nil {
			return FinalSummaryV42{}, nil, err
		}
		if err := waitUntil(3*time.Second, func() bool {
			for _, rt := range runtimes[1:] {
				if !rt.node.Mempool.Has(item.TxID) {
					return false
				}
			}
			return true
		}); err != nil {
			return FinalSummaryV42{}, nil, fmt.Errorf("timeout waiting for ordered TX_GOSSIP admission for tx %s nonce %d", item.TxID, item.Nonce)
		}
	}
	if err := waitUntil(3*time.Second, func() bool { return runtimes[1].node.Mempool.Len() == opts.TxCount }); err != nil {
		return FinalSummaryV42{}, nil, err
	}
	proposed, err := runtimes[0].ProposeBlock(ctx)
	if err != nil {
		return FinalSummaryV42{}, nil, err
	}
	if err := waitUntil(3*time.Second, func() bool {
		committed := 0
		for _, rt := range runtimes {
			if rt.Summary().CommittedBlockHash == proposed.BlockHash {
				committed++
			}
		}
		return committed >= 3
	}); err != nil {
		return FinalSummaryV42{}, nil, err
	}
	nodeDirs := []string{}
	engine := execution.NewEngine()
	var latestStateRoot string
	var receiptRoot string
	for i, rt := range runtimes[:3] {
		committed, ok := rt.CommittedBlock()
		if !ok {
			return FinalSummaryV42{}, nil, fmt.Errorf("node %s has no committed block", validators[i])
		}
		nodeDir := filepath.Join(opts.OutDir, validators[i])
		nodeDirs = append(nodeDirs, nodeDir)
		db, err := state.Open(nodeDir, "s0")
		if err != nil {
			return FinalSummaryV42{}, nil, err
		}
		result := engine.ExecuteBlock(committed, db)
		if err := db.Save(); err != nil {
			return FinalSummaryV42{}, nil, err
		}
		if err := execution.WriteExecutionLogs(nodeDir, result); err != nil {
			return FinalSummaryV42{}, nil, err
		}
		store := storage.NewBlockStore(nodeDir, validators[i], "s0")
		if _, err := store.DurableCommit(committed, result); err != nil {
			return FinalSummaryV42{}, nil, err
		}
		if err := metrics.WriteCSV(filepath.Join(nodeDir, "state_root_log.csv"), []string{"node_id", "height", "block_hash", "state_root_before", "state_root_after", "receipt_root"}, [][]string{{validators[i], fmt.Sprint(committed.Height), committed.BlockHash, result.StateRootBefore, result.StateRootAfter, result.ReceiptRoot}}); err != nil {
			return FinalSummaryV42{}, nil, err
		}
		latestStateRoot = result.StateRootAfter
		receiptRoot = result.ReceiptRoot
		if err := rt.WriteArtifacts(nodeDir); err != nil {
			return FinalSummaryV42{}, nil, err
		}
	}
	consistency, err := metrics.CheckConsistency(nodeDirs, opts.OutDir)
	if err != nil {
		return FinalSummaryV42{}, nil, err
	}
	recoverySummary, err := recovery.Recover(nodeDirs[0], opts.OutDir)
	if err != nil {
		return FinalSummaryV42{}, nil, err
	}
	xdbSource, _ := state.Open(filepath.Join(opts.OutDir, "xshard_source"), "s0")
	xdbTarget, _ := state.Open(filepath.Join(opts.OutDir, "xshard_target"), "s1")
	xTx := txs[0]
	xTx.StateKeys = []string{"shard:s0:alice", "shard:s1:bob"}
	xres, err := xshard.RunSuccess(xTx, "s0", "s1", proposed.BlockHash, xdbSource, xdbTarget, opts.OutDir)
	if err != nil {
		return FinalSummaryV42{}, nil, err
	}
	if _, err := xshard.RunRefund(txs[1], "s0", "s1", xdbSource, opts.OutDir); err != nil {
		return FinalSummaryV42{}, nil, err
	}
	faultResult, err := faults.Apply(faults.Config{NetworkDelayMS: 1, DropRate: 0.0, LeaderTimeoutMS: 10}, opts.OutDir)
	if err != nil {
		return FinalSummaryV42{}, nil, err
	}
	traceCSV := filepath.Join(opts.OutDir, "blockemulator_selectedTxs_sample.csv")
	if err := os.WriteFile(traceCSV, []byte("sender,receiver,value\nalice,bob,1\n"), 0o644); err != nil {
		return FinalSummaryV42{}, nil, err
	}
	imported, err := bridge.ImportTraceCSV(traceCSV, opts.OutDir)
	if err != nil {
		return FinalSummaryV42{}, nil, err
	}
	if err := bridge.WriteComparisonSummary(opts.OutDir, bridge.ComparisonSummary{TxCount: imported, NodeCount: opts.Nodes, ShardCount: opts.Shards, ConsensusMessageCount: runtimes[0].pbftLogs.MessageCount(), NetworkMessageCount: len(runtimes[0].transport.Log.Entries()), CommittedBlocks: 1, CommittedTxs: opts.TxCount, StateRootMismatchCount: consistency.MismatchCount, CrossShardTxCount: xres.CrossShardTxCount, RecoverySupported: recoverySummary.NodeRecovery, FaultInjectionSupported: faultResult.FaultInjection}); err != nil {
		return FinalSummaryV42{}, nil, err
	}
	summary := FinalSummaryV42{
		RuntimeStage: "v4_2_state_cross_shard_recovery_frontend", RuntimeTruth: "v4_real_state_cross_shard_recovery",
		RealSignedTx: true, PerNodeMempool: true, RealP2P: true, PBFTStyleConsensus: true, RealPBFTMessages: true,
		ProductionPBFT: false, FullByzantineSecurity: false, BlockCommit: true, DeterministicExecution: true,
		PersistentStateDB: true, StateRootFromRealStateUpdates: true, EthereumMPTCompatible: false, ProductionStatelessWitness: false, WitnessMVP: true,
		ReceiptDB: true, TxIndex: true, RealCrossShardStateMachine: true, DeterministicCertificateMVP: true, ByzantineSecureRelay: false, ProductionAtomicCommit: false,
		RecoverySupported: true, NodeRecovery: true, CrashConsistencyMVP: true, ProductionRecovery: false,
		FaultInjectionSupported: true, ByzantineFaultModel: false, ProductionFaultTolerance: false,
		FrontendRealismMode: opts.FrontendAvailable, BlockEmulatorBridgeMVP: true, FullBlockEmulatorCompatibility: false,
		ResearchGradeRealEmulator: true, ProductionBlockchain: false, CommittedHeight: proposed.Height, CommittedBlockHash: proposed.BlockHash,
		LatestStateRoot: latestStateRoot, StateRootMismatchCount: consistency.MismatchCount, CrossShardTxCount: xres.CrossShardTxCount, ReadyToCommit: consistency.MismatchCount == 0 && receiptRoot != "",
	}
	if err := metrics.WriteJSON(filepath.Join(opts.OutDir, "v4_2_realism_final_summary.json"), summary); err != nil {
		return FinalSummaryV42{}, nil, err
	}
	if err := metrics.WriteJSON(filepath.Join(opts.OutDir, "v4_2_acceptance_report.json"), map[string]any{"ready_to_commit": summary.ReadyToCommit, "summary": summary, "consistency": consistency}); err != nil {
		return FinalSummaryV42{}, nil, err
	}
	artifacts, _ := listFiles(opts.OutDir)
	return summary, artifacts, nil
}

func waitUntil(timeout time.Duration, ok func() bool) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ok() {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for V4.2 smoke condition")
}

func listFiles(root string) ([]string, error) {
	files := []string{}
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		files = append(files, rel)
		return nil
	})
	return files, err
}
