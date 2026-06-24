"""Run the small, local V1.8 baseline sweep without Fabric or Docker."""
from __future__ import annotations
import argparse,csv,json,subprocess,sys
from pathlib import Path
import yaml
ROOT=Path(__file__).resolve().parents[1]
FIELDS=["tx_count","success_count","failed_count","throughput_tps","avg_latency_ms","p95_latency_ms","p99_latency_ms","routing_policy","routing_cross_shard_tx_count","routing_cross_shard_tx_ratio","routing_remote_key_count","co_access_group_count","routing_time_ms","dual_track_enabled","fast_track_tx_count","conservative_track_tx_count","fast_track_executed_count","conservative_track_executed_count","blocked_or_deferred_tx_count","scheduler_idle_count","hot_update_aggregation_enabled","aggregation_policy","aggregation_candidate_tx_count","aggregated_tx_count","aggregated_commit_count","conservative_commit_count","aggregation_saved_commit_count","aggregation_group_count","aggregation_hot_key_count","aggregation_constraint_failure_count","aggregation_missing_delta_count","aggregation_non_commutative_count"]
def config_for(item,shards):return {"state_sharding":{"shard_count":shards},"execution_sharding":{"shard_count":shards},"routing":{"policy":item["routing_policy"],"co_access_min_weight":1,"co_access_max_group_size":64,"co_access_balance_weight":1},"execution":{"dual_track_enabled":item["dual_track_enabled"],"fast_track_max_access_size":2,"conservative_on_conflict_hint":True,"conservative_on_missing_access_set":True,"scheduler_policy":"fast_first"},"commit":{"hot_update_aggregation_enabled":item["hot_update_aggregation_enabled"],"aggregation_min_hot_count":2,"aggregation_max_group_size":64,"aggregation_require_fast_track":True,"conservative_on_constraint_failure":True,"aggregation_policy":"by_primary_key"}}
def report(rows):return "# V1.8 baseline sweep\n\nReplay/virtual-time comparison only; not production Fabric, cross-chain, MetaFlow, or multi-server deployment.\n\n| baseline | tx | routing | fast | aggregated commits |\n|---|---:|---|---:|---:|\n"+"".join(f"| {r['name']} | {r.get('tx_count','')} | {r.get('routing_policy','')} | {r.get('fast_track_tx_count','')} | {r.get('aggregated_commit_count','')} |\n" for r in rows)
def main():
 p=argparse.ArgumentParser();p.add_argument("--sweep",type=Path,default=ROOT/"configs/sweeps/v1_8_baselines.yaml");p.add_argument("--out",type=Path,default=ROOT/".cache/v1_8_sweeps/latest");p.add_argument("--dry-run",action="store_true");a=p.parse_args();spec=yaml.safe_load(a.sweep.read_text());out=a.out;rows=[]
 for item in spec["baselines"]:
  run=out/item["name"];cfg=run/"config.yaml";run.mkdir(parents=True,exist_ok=True);cfg.write_text(yaml.safe_dump(config_for(item,spec["execution_shards"]),sort_keys=False))
  if a.dry_run: rows.append({"name":item["name"],"planned":True});continue
  subprocess.run(["go","run","./cmd/replay","-config",str(cfg.resolve()),"-trace",str((ROOT/spec["trace"]).resolve()),"-output",str(run.resolve())],cwd=ROOT/"executor",check=True);row=next(csv.DictReader((run/"summary.csv").open()));row["name"]=item["name"];rows.append(row)
 out.mkdir(parents=True,exist_ok=True);(out/"sweep_summary.json").write_text(json.dumps(rows,indent=2)+"\n");
 with (out/"sweep_summary.csv").open("w",newline="") as f: w=csv.DictWriter(f,fieldnames=["name",*FIELDS],extrasaction="ignore");w.writeheader();w.writerows(rows)
 (out/"report.md").write_text(report(rows));print(f"wrote {out/'report.md'}")
if __name__=="__main__":main()
