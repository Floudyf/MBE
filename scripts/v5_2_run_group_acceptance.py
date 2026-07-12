from __future__ import annotations

import json
import sys
import time
import argparse
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.app.models.v5_experiment_spec import V5ExperimentSpec, V5PluginSelection, V5Topology
from backend.app.models.v5_formal_experiment import V5FormalExperimentPlan, V5FormalMethod
from backend.app.services.v5_formal_run_store import children, create_group, group_dir, read_group
from backend.app.services.v5_formal_scheduler import expand, start
from backend.app.services.v5_plugin_manifest_store import CATEGORIES, STORE


def selections(overrides: dict[str, str]) -> list[V5PluginSelection]:
    return [V5PluginSelection(category=category, plugin_id=overrides.get(category, next(item.plugin_id for item in STORE.list() if item.category == category))) for category in CATEGORIES]


def main() -> int:
    parser=argparse.ArgumentParser(); parser.add_argument("--verify-group", default=""); args=parser.parse_args()
    if args.verify_group:
        group_id=args.verify_group; current=read_group(group_id); items=children(group_id); directory=group_dir(group_id)
        return _validate(group_id,current,items,directory)
    methods = [V5FormalMethod(method_id="metatrack", display_name="MetaTrack", plugin_overrides={"routing": "metatrack_coaccess_routing", "execution": "dual_track_execution", "scheduler": "fast_first_scheduler", "commit": "commutative_hot_update_aggregation"}), V5FormalMethod(method_id="hash", display_name="Hash", plugin_overrides={"routing": "hash_routing_baseline", "execution": "serial_execution_baseline", "scheduler": "fifo_serial_scheduler", "commit": "normal_commit"})]
    plan = V5FormalExperimentPlan(name="v5_2_run_group_acceptance", base_spec=V5ExperimentSpec(name="v5_2_run_group", execution_backend="real_cluster", plugin_selections=selections({}), topology=V5Topology(nodes=8, shards=2, validators_per_shard=4), tx_count=100, seed=61, duration_ms=9000), suites=["comparison_experiment"], methods=methods, seeds=[61, 62], repeats=2)
    matrix = expand(plan, "real_cluster")
    group = create_group({"execution_backend":"real_cluster", "runtime_truth":"v5_real_cluster_candidate", "plan":plan.model_dump(), "matrix":matrix, "total_child_runs":len(matrix), "completed_child_runs":0, "cancel_requested":False, "max_concurrent_real_clusters":1})
    start(group["run_group_id"])
    deadline=time.monotonic()+600
    while time.monotonic()<deadline:
        current=read_group(group["run_group_id"])
        if current.get("status") in {"completed","failed","cancelled"}: break
        time.sleep(1)
    current=read_group(group["run_group_id"]); items=children(group["run_group_id"]); directory=group_dir(group["run_group_id"])
    return _validate(group["run_group_id"],current,items,directory)


def _validate(group_id: str, current: dict, items: list[dict], directory: Path) -> int:
    required=["raw_summary.csv","aggregate_summary.csv","confidence_interval.csv","formal_matrix.csv","fairness_matrix.csv","artifacts.zip"]
    blockers=[]
    if current.get("status")!="completed": blockers.append(f"group status {current.get('status')}")
    if len(items)!=8 or not all(item.get("status")=="completed" for item in items): blockers.append("children incomplete")
    if any(not (directory/name).is_file() or (directory/name).stat().st_size==0 for name in required): blockers.append("artifacts incomplete")
    if any(item.get("result",{}).get("summary",{}).get("orphan_process_count")!=0 or item.get("result",{}).get("summary",{}).get("no_fallback") is not True for item in items): blockers.append("runtime cleanup/no fallback evidence failed")
    report={"acceptance_passed":not blockers,"run_group_id":group_id,"child_count":len(items),"blockers":blockers}
    (directory/"v5_2_run_group_acceptance.json").write_text(json.dumps(report,indent=2)+"\n",encoding="utf-8")
    print(json.dumps(report)); return 0 if not blockers else 1


if __name__=="__main__": raise SystemExit(main())
