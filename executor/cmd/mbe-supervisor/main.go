package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	v4config "metaverse-chainlab/executor/realism/config"
	"metaverse-chainlab/executor/realism/metrics"
)

func main() {
	nodes := flag.Int("nodes", 4, "node count")
	shards := flag.Int("shards", 2, "shard count")
	dataDir := flag.String("data-dir", ".cache/v4_realism_runs", "root data dir")
	outConfig := flag.String("out-config", "", "v4_node_config.json output")
	outAddressTable := flag.String("out-address-table", "", "v4_address_table.json output")
	flag.Parse()

	if *outConfig == "" {
		*outConfig = filepath.Join(*dataDir, "v4_node_config.json")
	}
	if *outAddressTable == "" {
		*outAddressTable = filepath.Join(*dataDir, "v4_address_table.json")
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
	fmt.Printf("wrote V4.0 supervisor skeleton config: %s and %s; real_p2p=false real_pbft=false\n", *outConfig, *outAddressTable)
}
