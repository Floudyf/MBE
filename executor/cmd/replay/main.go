package main

import (
	"flag"
	"fmt"
	"log"
	"metaverse-chainlab/executor/core"
	"metaverse-chainlab/executor/v3runtime"
)

func main() {
	mode := flag.String("mode", "replay", "")
	c := flag.String("config", "../configs/experiments/v0_default_asset_hotspot.yaml", "")
	t := flag.String("trace", "../experiments/runs/v0_default_asset_hotspot/trace.jsonl.gz", "")
	o := flag.String("output", "../experiments/runs/v0_default_asset_hotspot", "")
	chainProfile := flag.String("chain-profile", "", "")
	pluginProfile := flag.String("plugin-profile", "", "")
	pluginProfileID := flag.String("plugin-profile-id", "", "")
	experimentProfile := flag.String("experiment-profile", "", "")
	flag.Parse()
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
