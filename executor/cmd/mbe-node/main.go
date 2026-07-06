package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"metaverse-chainlab/executor/realism/node"
)

func main() {
	nodeID := flag.String("node-id", "", "node id")
	shardID := flag.String("shard-id", "", "shard id")
	dataDir := flag.String("data-dir", "", "node data dir")
	capacity := flag.Int("mempool-capacity", 10000, "mempool capacity")
	inputJSONL := flag.String("input-jsonl", "", "signed transaction JSONL input")
	runMode := flag.String("run-mode", "once", "once|server")
	summaryOut := flag.String("summary-out", "", "summary output path")
	mempoolLogOut := flag.String("mempool-log-out", "", "mempool log output path")
	admissionLogOut := flag.String("admission-log-out", "", "admission log output path")
	realTraceImport := flag.Bool("real-trace-import", false, "mark input JSONL as produced by the V4.0 real trace importer")
	configPath := flag.String("config", "", "reserved V4 config path")
	addressTable := flag.String("address-table", "", "v4_address_table.json")
	listenAddr := flag.String("listen-addr", "127.0.0.1:0", "local TCP listen address")
	peers := flag.String("peers", "", "comma-separated peer mapping node_id=addr")
	role := flag.String("role", "validator", "validator|leader|observer")
	blockSize := flag.Int("block-size", 10, "block tx limit")
	blockIntervalMS := flag.Int("block-interval-ms", 100, "leader block interval")
	consensus := flag.String("consensus", "pbft", "pbft")
	runDurationMS := flag.Int("run-duration-ms", 1000, "server run duration before graceful shutdown; 0 uses default")
	leaderID := flag.String("leader-id", "", "current shard leader id")
	flag.Parse()
	_ = *configPath

	peerList := node.ParsePeers(*peers, *shardID)
	if *addressTable != "" {
		loaded, err := node.LoadPeersFromAddressTable(*addressTable)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		peerList = loaded
	}
	if *leaderID == "" && *role == "leader" {
		*leaderID = *nodeID
	}
	validators := []string{*nodeID}
	for _, peer := range peerList {
		if peer.ShardID == "" || peer.ShardID == *shardID {
			validators = append(validators, peer.NodeID)
		}
	}
	cfg := node.Config{
		NodeID:          *nodeID,
		ShardID:         *shardID,
		DataDir:         *dataDir,
		MempoolCapacity: *capacity,
		InputJSONL:      *inputJSONL,
		RunMode:         *runMode,
		SummaryOut:      *summaryOut,
		MempoolLogOut:   *mempoolLogOut,
		AdmissionLogOut: *admissionLogOut,
		RealTraceImport: *realTraceImport,
		ListenAddr:      *listenAddr,
		Peers:           peerList,
		Role:            *role,
		BlockSize:       *blockSize,
		BlockIntervalMS: *blockIntervalMS,
		Consensus:       *consensus,
		RunDurationMS:   *runDurationMS,
		LeaderID:        *leaderID,
		Validators:      validators,
	}
	var (
		result node.RunResult
		err    error
	)
	switch *runMode {
	case "once":
		result, err = node.RunOnce(cfg)
	case "server":
		var summary node.RuntimeSummaryV41
		summary, err = node.RunServer(context.Background(), cfg)
		if err == nil {
			fmt.Printf("mbe-node %s wrote V4.1 summary; real_p2p=%t pbft_style_consensus=%t state_commit=%t\n", summary.NodeID, summary.RealP2P, summary.PBFTStyleConsensus, summary.StateCommit)
			return
		}
	default:
		err = fmt.Errorf("unsupported run mode %q", *runMode)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("mbe-node %s wrote summary %s; real_p2p=false real_pbft=false state_commit=false\n", result.Summary.NodeID, result.SummaryPath)
}
