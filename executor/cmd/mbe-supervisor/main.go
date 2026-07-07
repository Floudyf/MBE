package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	v4config "metaverse-chainlab/executor/realism/config"
	"metaverse-chainlab/executor/realism/metrics"
	"metaverse-chainlab/executor/realism/node"
)

func main() {
	mode := flag.String("mode", "plan", "plan|v4.2-smoke|v4.3-smoke")
	nodes := flag.Int("nodes", 4, "node count")
	shards := flag.Int("shards", 2, "shard count")
	txCount := flag.Int("tx-count", 10, "smoke tx count")
	enableCrossShard := flag.Bool("enable-cross-shard", true, "enable V4.2 cross-shard smoke")
	enableFaults := flag.Bool("enable-faults", true, "enable V4.2 fault smoke")
	faultProfile := flag.String("fault-profile", "network_delay", "V4.3 fault profile")
	blockEmulatorCSV := flag.String("blockemulator-csv", "", "BlockEmulator selectedTxs CSV input")
	blockEmulatorTxLimit := flag.Int("blockemulator-tx-limit", 20, "BlockEmulator bridge tx import limit")
	runDurationMS := flag.Int("run-duration-ms", 1000, "smoke run duration")
	dataDir := flag.String("data-dir", ".cache/v4_realism_runs", "root data dir")
	outConfig := flag.String("out-config", "", "v4_node_config.json output")
	outAddressTable := flag.String("out-address-table", "", "v4_address_table.json output")
	outPlan := flag.String("out-plan", "", "v4_1_supervisor_plan.json output")
	flag.Parse()
	if *mode == "v4.2-smoke" {
		summary, artifacts, err := node.RunV42FinalSmoke(context.Background(), node.SmokeOptionsV42{OutDir: *dataDir, Nodes: *nodes, Shards: *shards, TxCount: *txCount, EnableCrossShard: *enableCrossShard, EnableFaults: *enableFaults, RunDurationMS: *runDurationMS, FrontendAvailable: true})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Printf("V4.2 smoke complete: ready_to_commit=%t artifacts=%d summary=%s\n", summary.ReadyToCommit, len(artifacts), filepath.Join(*dataDir, "v4_2_realism_final_summary.json"))
		return
	}
	if *mode == "v4.3-smoke" {
		summary, artifacts, err := node.RunV43FinalSmoke(context.Background(), node.SmokeOptionsV43{OutDir: *dataDir, Nodes: *nodes, Shards: *shards, TxCount: *txCount, EnableCrossShard: *enableCrossShard, EnableFaults: *enableFaults, FaultProfile: *faultProfile, BlockEmulatorCSV: *blockEmulatorCSV, BlockEmulatorTxLimit: *blockEmulatorTxLimit, RunDurationMS: *runDurationMS, FrontendAvailable: true})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Printf("V4.3 smoke complete: ready_to_commit=%t artifacts=%d summary=%s\n", summary.ReadyToCommit, len(artifacts), filepath.Join(*dataDir, "v4_3_realism_final_summary.json"))
		return
	}

	if *outConfig == "" {
		*outConfig = filepath.Join(*dataDir, "v4_node_config.json")
	}
	if *outAddressTable == "" {
		*outAddressTable = filepath.Join(*dataDir, "v4_address_table.json")
	}
	if *outPlan == "" {
		*outPlan = filepath.Join(*dataDir, "v4_1_supervisor_plan.json")
	}
	cfg, err := v4config.Generate(*nodes, *shards, *dataDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := metrics.WriteJSON(*outConfig, cfg); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	table := v4config.BuildAddressTable(cfg)
	if err := metrics.WriteJSON(*outAddressTable, table); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	plan := v4config.BuildSupervisorPlan(cfg)
	if err := metrics.WriteJSON(*outPlan, plan); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("wrote V4.1 supervisor plan: %s, %s, %s; real_p2p=true pbft_style=true state_commit=false\n", *outConfig, *outAddressTable, *outPlan)
}
