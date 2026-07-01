package main

import (
	"flag"
	"fmt"
	"log"
	"metaverse-chainlab/executor/core"
	"metaverse-chainlab/executor/v3runtime"
	"os"
	"strings"
)

func main() {
	mode := flag.String("mode", "replay", "")
	c := flag.String("config", "../configs/experiments/v0_default_asset_hotspot.yaml", "")
	t := flag.String("trace", "../experiments/runs/v0_default_asset_hotspot/trace.jsonl.gz", "")
	o := flag.String("output", "../experiments/runs/v0_default_asset_hotspot", "")
	outputDir := flag.String("output-dir", "", "")
	chainProfile := flag.String("chain-profile", "", "")
	pluginProfile := flag.String("plugin-profile", "", "")
	pluginProfileID := flag.String("plugin-profile-id", "", "")
	experimentProfile := flag.String("experiment-profile", "", "")
	nodeID := flag.String("node-id", "", "")
	role := flag.String("role", "", "")
	topologyFile := flag.String("topology-file", "", "")
	statusFile := flag.String("status-file", "", "")
	logFile := flag.String("log-file", "", "")
	shardID := flag.Int("shard-id", 0, "")
	previewOnly := flag.Bool("preview-only", false, "")
	flag.Parse()
	if *mode == "node-preview" {
		nodeOutputDir := *outputDir
		if nodeOutputDir == "" && containsArg("output") {
			nodeOutputDir = *o
		}
		result, err := v3runtime.RunNodeProcessPreview(v3runtime.NodeProcessPreviewInput{
			NodeID:       *nodeID,
			Role:         *role,
			ShardID:      *shardID,
			HasShardID:   flag.Lookup("shard-id").Value.String() != "0" || containsArg("shard-id"),
			TopologyFile: *topologyFile,
			OutputDir:    nodeOutputDir,
			StatusFile:   *statusFile,
			LogFile:      *logFile,
			PreviewOnly:  *previewOnly,
		})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("node preview ready: %s wrote %s\n", result.Node.NodeID, result.StatusCSV)
		return
	}
	if *mode == "v3-runtime" {
		result, err := v3runtime.Run(v3runtime.Input{
			ChainProfilePath:      *chainProfile,
			PluginProfilePath:     *pluginProfile,
			PluginProfileID:       *pluginProfileID,
			ExperimentProfilePath: *experimentProfile,
			OutputDir:             *o,
		})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("v3 runtime wrote %d transactions\n", result.Summary.TxCount)
		return
	}
	s, e := core.Replay(*c, *t, *o)
	if e != nil {
		log.Fatal(e)
	}
	fmt.Printf("replayed %d transactions\n", s.TxCount)
}

func containsArg(name string) bool {
	for _, arg := range os.Args[1:] {
		if arg == "-"+name || arg == "--"+name || strings.HasPrefix(arg, "-"+name+"=") || strings.HasPrefix(arg, "--"+name+"=") {
			return true
		}
	}
	return false
}
