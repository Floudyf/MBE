from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.app.models.v5_experiment_spec import V5ExperimentSpec, V5PluginSelection, V5Topology
from backend.app.services.v5_experiment_compiler import compile_plan
from backend.app.services.v5_plugin_manifest_store import CATEGORIES, STORE


def build_spec(nodes: int, shards: int, tx_count: int) -> V5ExperimentSpec:
    selections = [V5PluginSelection(category=category, plugin_id=next(item for item in STORE.list() if item.category == category).plugin_id) for category in CATEGORIES]
    return V5ExperimentSpec(execution_backend="real_cluster", plugin_selections=selections, topology=V5Topology(nodes=nodes, shards=shards, validators_per_shard=nodes // shards), tx_count=tx_count, seed=17, duration_ms=9000)


def run_case(nodes: int, shards: int, tx_count: int, root: Path) -> dict:
    root.mkdir(parents=True, exist_ok=True)
    plan = compile_plan(build_spec(nodes, shards, tx_count), root)
    plan_path = root / "compiled_run_plan.json"
    plan_path.write_text(plan.model_dump_json(indent=2), encoding="utf-8")
    result = subprocess.run(["go", "run", "./cmd/mbe-supervisor", "--mode", "v5-real-cluster", "--plan", str(plan_path), "--data-dir", str(root)], cwd=ROOT / "executor", text=True, capture_output=True, timeout=180)
    if result.returncode:
        raise RuntimeError(result.stderr)
    summary = json.loads((root / "real_cluster_summary.json").read_text(encoding="utf-8"))
    required = ["one_node_one_os_process", "independent_tcp_ports", "all_shards_active", "per_shard_multiple_blocks", "real_client_submission", "state_root_consistent", "real_cross_shard_network", "no_fallback"]
    if any(summary.get(key) is not True for key in required) or summary.get("distinct_process_count") != nodes or summary.get("orphan_process_count") != 0 or summary.get("cross_shard_success_count", 0) < 1 or summary.get("cross_shard_refund_count", 0) < 1:
        raise RuntimeError(json.dumps(summary, indent=2))
    return summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", default=str(ROOT / ".cache" / "v5_1_acceptance"))
    parser.add_argument("--include-16", action="store_true")
    args = parser.parse_args()
    output = Path(args.output_root).resolve()
    report = {"eight_node": run_case(8, 2, 100, output / "eight_nodes")}
    if args.include_16:
        report["sixteen_node"] = run_case(16, 4, 1000, output / "sixteen_nodes")
    (output / "acceptance_report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: value["ready_to_commit"] for key, value in report.items()}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
