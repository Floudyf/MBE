from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.app.models.v5_experiment_spec import V5ExperimentSpec, V5PluginSelection, V5Topology
from backend.app.services.v5_experiment_compiler import compile_plan
from backend.app.services.v5_plugin_manifest_store import CATEGORIES, STORE


def build_spec(nodes: int, shards: int, tx_count: int, *, duration_ms: int, block_size: int, block_interval_ms: int) -> V5ExperimentSpec:
    selections = []
    for category in CATEGORIES:
        plugin_id = next(item for item in STORE.list() if item.category == category).plugin_id
        config = {"cross_shard_ratio": 0, "timeout_every": 0} if category == "workload" and shards == 1 else {}
        if category == "block_producer":
            config = {"block_size": block_size, "interval_ms": block_interval_ms}
        selections.append(V5PluginSelection(category=category, plugin_id=plugin_id, config=config))
    return V5ExperimentSpec(execution_backend="real_cluster", plugin_selections=selections, topology=V5Topology(nodes=nodes, shards=shards, validators_per_shard=nodes // shards), tx_count=tx_count, seed=17, duration_ms=duration_ms)


def run_case(nodes: int, shards: int, tx_count: int, root: Path, *, duration_ms: int, timeout_seconds: int, block_size: int, block_interval_ms: int) -> dict:
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)
    plan = compile_plan(build_spec(nodes, shards, tx_count, duration_ms=duration_ms, block_size=block_size, block_interval_ms=block_interval_ms), root)
    plan_path = root / "compiled_run_plan.json"
    plan_path.write_text(plan.model_dump_json(indent=2), encoding="utf-8")
    result = subprocess.run(["go", "run", "./cmd/mbe-supervisor", "--mode", "v5-real-cluster", "--plan", str(plan_path), "--data-dir", str(root)], cwd=ROOT / "executor", text=True, capture_output=True, timeout=timeout_seconds)
    if result.returncode:
        raise RuntimeError(result.stderr)
    summary = json.loads((root / "real_cluster_summary.json").read_text(encoding="utf-8"))
    required = ["one_node_one_os_process", "independent_tcp_ports", "all_shards_active", "per_shard_multiple_blocks", "real_client_submission", "state_root_consistent", "no_fallback", "block_executor_consistent", "plan_digest_consistent"]
    finality = summary.get("finality_evidence", {})
    cross_shard_ok = True
    if shards > 1:
        cross_shard_ok = summary.get("real_cross_shard_network") is True and finality.get("cross_shard_requested_unique_count", 0) >= 1 and finality.get("cross_shard_finalized_unique_count", 0) >= 1
    if (
        any(summary.get(key) is not True for key in required)
        or summary.get("distinct_process_count") != nodes
        or summary.get("orphan_process_count") != 0
        or summary.get("block_executor_id") != "serial_block_executor"
        or not cross_shard_ok
        or finality.get("terminal_unique_tx_count") != tx_count
        or finality.get("incomplete_unique_tx_count") != 0
    ):
        raise RuntimeError(json.dumps(summary, indent=2))
    leader_summary = root / "nodes" / "n0" / "block_execution_summary.json"
    if not leader_summary.is_file():
        raise RuntimeError(f"missing block execution artifact: {leader_summary}")
    return summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", default=str(ROOT / ".cache" / "v5_1_acceptance"))
    parser.add_argument("--include-16", action="store_true")
    parser.add_argument("--skip-single", action="store_true")
    parser.add_argument("--single-tx-count", type=int, default=1000)
    parser.add_argument("--multi-tx-count", type=int, default=1000)
    parser.add_argument("--duration-ms", type=int, default=120000)
    parser.add_argument("--timeout-seconds", type=int, default=300)
    parser.add_argument("--block-size", type=int, default=100)
    parser.add_argument("--block-interval-ms", type=int, default=75)
    args = parser.parse_args()
    output = Path(args.output_root).resolve()
    report = {}
    if not args.skip_single:
        report["single_shard"] = run_case(4, 1, args.single_tx_count, output / "single_shard", duration_ms=args.duration_ms, timeout_seconds=args.timeout_seconds, block_size=args.block_size, block_interval_ms=args.block_interval_ms)
    if args.include_16:
        report["sixteen_node"] = run_case(16, 4, args.multi_tx_count, output / "sixteen_nodes", duration_ms=args.duration_ms, timeout_seconds=args.timeout_seconds, block_size=args.block_size, block_interval_ms=args.block_interval_ms)
    (output / "acceptance_report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: value["ready_to_commit"] for key, value in report.items()}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
