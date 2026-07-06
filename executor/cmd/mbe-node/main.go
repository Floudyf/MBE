package main

import (
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
	flag.Parse()

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
	}
	var (
		result node.RunResult
		err    error
	)
	switch *runMode {
	case "once":
		result, err = node.RunOnce(cfg)
	case "server":
		result, err = node.RunServerSkeleton(cfg)
	default:
		err = fmt.Errorf("unsupported run mode %q", *runMode)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("mbe-node %s wrote summary %s; real_p2p=false real_pbft=false state_commit=false\n", result.Summary.NodeID, result.SummaryPath)
}
