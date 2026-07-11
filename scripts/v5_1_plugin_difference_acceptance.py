from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.app.models.v5_experiment_spec import V5ExperimentSpec, V5PluginSelection, V5Topology
from backend.app.services.v5_experiment_compiler import compile_plan
from backend.app.services.v5_plugin_manifest_store import CATEGORIES, STORE


METHODS = {
    "metatrack_full": {"routing": "metatrack_coaccess_routing", "execution": "dual_track_execution", "scheduler": "fast_first_scheduler", "commit": "commutative_hot_update_aggregation"},
    "hash_serial_baseline": {"routing": "hash_routing_baseline", "execution": "serial_execution_baseline", "scheduler": "fifo_serial_scheduler", "commit": "normal_commit"},
    "no_aggregation": {"routing": "metatrack_coaccess_routing", "execution": "dual_track_execution", "scheduler": "fast_first_scheduler", "commit": "normal_commit"},
    "routing_only": {"routing": "metatrack_coaccess_routing", "execution": "serial_execution_baseline", "scheduler": "fifo_serial_scheduler", "commit": "normal_commit"},
}


def selections(overrides: dict[str, str]) -> list[V5PluginSelection]:
    result = []
    for category in CATEGORIES:
        plugin_id = overrides.get(category) or next(item.plugin_id for item in STORE.list() if item.category == category)
        result.append(V5PluginSelection(category=category, plugin_id=plugin_id))
    return result


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as stream:
        return list(csv.DictReader(stream))


def run_method(name: str, overrides: dict[str, str], output: Path) -> dict:
    output.mkdir(parents=True, exist_ok=True)
    spec = V5ExperimentSpec(execution_backend="real_cluster", name=name, plugin_selections=selections(overrides), topology=V5Topology(nodes=8, shards=2, validators_per_shard=4), tx_count=100, seed=53, duration_ms=9000)
    plan = compile_plan(spec, output)
    plan_path = output / "compiled_run_plan.json"
    plan_path.write_text(plan.model_dump_json(indent=2), encoding="utf-8")
    process = subprocess.run(["go", "run", "./cmd/mbe-supervisor", "--mode", "v5-real-cluster", "--plan", str(plan_path), "--data-dir", str(output)], cwd=ROOT / "executor", text=True, capture_output=True, timeout=180)
    if process.returncode:
        raise RuntimeError(f"{name}: {process.stderr}")
    summary = json.loads((output / "real_cluster_summary.json").read_text(encoding="utf-8"))
    leader_dir = output / "nodes" / "n0"
    plugin_snapshot = json.loads((leader_dir / "plugin_snapshot.json").read_text(encoding="utf-8"))
    plugin_load = json.loads((leader_dir / "plugin_load_log.json").read_text(encoding="utf-8"))
    routing = read_csv(output / "client" / "routing_decision_log.csv")
    execution = read_csv(leader_dir / "execution_log.csv")
    commits = read_csv(leader_dir / "commit_log.csv")
    return {
        "run_id": output.name,
        "summary": summary,
        "plugin_snapshot": plugin_snapshot,
        "plugin_load": plugin_load,
        "routing": routing,
        "execution": execution,
        "commits": commits,
        "leader_dir": str(leader_dir),
    }


def evidence(name: str, result: dict) -> dict:
    routing = result["routing"]
    execution = result["execution"]
    commits = result["commits"]
    return {
        "method": name,
        "routing_plugin": result["plugin_snapshot"]["routing"]["plugin_id"],
        "execution_plugin": result["plugin_snapshot"]["execution"]["plugin_id"],
        "commit_plugin": result["plugin_snapshot"]["commit"]["plugin_id"],
        "routed_shard_distribution": dict(Counter(row["assigned_shard"] for row in routing)),
        "routing_assignments": {row["tx_id"]: row["assigned_shard"] for row in routing},
        "cross_shard_count": sum(row["cross_shard"] == "true" for row in routing),
        "fast_track_count": sum(row["track"] == "fast" for row in execution),
        "conservative_track_count": sum(row["track"] == "conservative" for row in execution),
        "aggregation_group_count": sum(row["aggregation_applied"] == "true" for row in commits),
        "logical_update_count": sum(int(row["logical_update_count"]) for row in commits),
        "physical_update_count": sum(int(row["physical_update_count"]) for row in commits),
        "finalized_count": result["summary"]["shard_blocks"],
        "state_root_consistent": result["summary"].get("state_root_consistent"),
        "orphan_process_count": result["summary"].get("orphan_process_count"),
        "no_fallback": result["summary"].get("no_fallback"),
        "receipts_present": (Path(result["leader_dir"]) / "receipts.jsonl").is_file(),
        "tx_index_present": (Path(result["leader_dir"]) / "tx_index.jsonl").is_file(),
    }


def validate(results: dict[str, dict]) -> list[str]:
    blockers: list[str] = []
    for name, item in results.items():
        for category in ("routing", "execution", "commit"):
            loaded = item["plugin_load"].get("plugins", {}).get(category, {})
            if not loaded.get("initialization_success") or not loaded.get("version") or not loaded.get("runtime_factory"):
                blockers.append(f"{name}: missing runtime loading evidence for {category}")
        summary = item["summary"]
        if not (summary.get("no_fallback") and summary.get("orphan_process_count") == 0 and summary.get("state_root_consistent") and summary.get("real_client_submission")):
            blockers.append(f"{name}: correctness/no-fallback process evidence failed")
    meta, hashed, no_agg, routing_only = (results[key] for key in METHODS)
    meta_e, hash_e, noagg_e, routing_e = (evidence(key, item) for key, item in results.items())
    if meta_e["routing_assignments"] == hash_e["routing_assignments"]:
        blockers.append("MetaTrack and hash routing produced no actual assignment difference")
    if not (meta_e["fast_track_count"] > 0 and noagg_e["fast_track_count"] > 0 and hash_e["fast_track_count"] == 0 and routing_e["fast_track_count"] == 0):
        blockers.append("dual-track versus serial execution evidence failed")
    if not (meta_e["aggregation_group_count"] > 0 and noagg_e["aggregation_group_count"] == 0 and hash_e["aggregation_group_count"] == 0 and routing_e["aggregation_group_count"] == 0 and meta_e["physical_update_count"] < meta_e["logical_update_count"]):
        blockers.append("aggregation commit evidence failed")
    for item in (meta_e, hash_e, noagg_e, routing_e):
        if not (item["receipts_present"] and item["tx_index_present"]): blockers.append(f"{item['method']}: receipt or tx-index missing")
    return blockers


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", default=str(ROOT / ".cache" / "v5_1_plugin_difference"))
    args = parser.parse_args()
    output = Path(args.output_root).resolve()
    results = {name: run_method(name, overrides, output / name) for name, overrides in METHODS.items()}
    rows = [evidence(name, item) for name, item in results.items()]
    blockers = validate(results)
    report = {"acceptance_passed": not blockers, "workload_id": "deterministic_signed_synthetic_crafted_v1", "seed": 53, "topology": {"nodes": 8, "shards": 2, "validators_per_shard": 4}, "per_method_run_id": {name: value["run_id"] for name, value in results.items()}, "per_method_plugin_snapshot": {name: value["plugin_snapshot"] for name, value in results.items()}, "routing_evidence": {row["method"]: row["routing_assignments"] for row in rows}, "execution_evidence": {row["method"]: {"fast": row["fast_track_count"], "conservative": row["conservative_track_count"]} for row in rows}, "commit_evidence": {row["method"]: {"groups": row["aggregation_group_count"], "logical": row["logical_update_count"], "physical": row["physical_update_count"]} for row in rows}, "state_correctness": {row["method"]: row["state_root_consistent"] for row in rows}, "no_fallback": {row["method"]: row["no_fallback"] for row in rows}, "orphan_process_count": {row["method"]: row["orphan_process_count"] for row in rows}, "blockers": blockers}
    (output / "v5_1_plugin_difference_acceptance.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    fields = ["method", "routing_plugin", "execution_plugin", "commit_plugin", "routed_shard_distribution", "cross_shard_count", "fast_track_count", "conservative_track_count", "aggregation_group_count", "logical_update_count", "physical_update_count", "finalized_count", "state_root_consistent", "orphan_process_count"]
    with (output / "v5_1_plugin_difference_summary.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields); writer.writeheader(); writer.writerows([{key: json.dumps(value[key]) if isinstance(value[key], (dict, list)) else value[key] for key in fields} for value in rows])
    print(json.dumps({"acceptance_passed": not blockers, "blockers": blockers}))
    return 0 if not blockers else 1


if __name__ == "__main__":
    raise SystemExit(main())
