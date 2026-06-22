package main
import("flag";"fmt";"log";"metaverse-chainlab/executor/core")
func main(){c:=flag.String("config","../configs/experiments/v0_default_asset_hotspot.yaml","");t:=flag.String("trace","../experiments/runs/v0_default_asset_hotspot/trace.jsonl.gz","");o:=flag.String("output","../experiments/runs/v0_default_asset_hotspot","");flag.Parse();s,e:=core.Replay(*c,*t,*o);if e!=nil{log.Fatal(e)};fmt.Printf("replayed %d transactions\n",s.TxCount)}
