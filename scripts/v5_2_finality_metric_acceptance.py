from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.app.models.v5_experiment_spec import V5ExperimentSpec, V5PluginSelection, V5Topology
from backend.app.services.v5_experiment_compiler import compile_plan
from backend.app.services.v5_plugin_manifest_store import CATEGORIES, STORE


def spec() -> V5ExperimentSpec:
    selections = [V5PluginSelection(category=category, plugin_id=next(item for item in STORE.list() if item.category == category).plugin_id) for category in CATEGORIES]
    return V5ExperimentSpec(execution_backend="real_cluster", plugin_selections=selections, topology=V5Topology(nodes=8, shards=2, validators_per_shard=4), tx_count=100, seed=29, duration_ms=9000)


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate V5.2 finality from raw real-cluster lifecycle artifacts.")
    parser.add_argument("--output-root", default=str(ROOT / ".cache" / "v5_2_finality_acceptance"))
    args = parser.parse_args()
    root = Path(args.output_root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    compiled = compile_plan(spec(), root)
    plan = root / "compiled_run_plan.json"
    plan.write_text(compiled.model_dump_json(indent=2), encoding="utf-8")
    result = subprocess.run(["go", "run", "./cmd/mbe-supervisor", "--mode", "v5-real-cluster", "--plan", str(plan), "--data-dir", str(root)], cwd=ROOT / "executor", text=True, capture_output=True, timeout=180)
    if result.returncode:
        raise RuntimeError(result.stderr)
    summary = json.loads((root / "real_cluster_summary.json").read_text(encoding="utf-8"))
    finality = json.loads((root / "finality_summary.json").read_text(encoding="utf-8"))
    required = [root / "transaction_lifecycle.jsonl", root / "transaction_lifecycle.csv", root / "transaction_finality.csv", root / "client_receipt_log.csv", root / "latency_distribution.csv", root / "throughput_windows.csv"]
    if any(not path.is_file() for path in required):
        raise RuntimeError("missing finality artifact")
    with (root / "transaction_lifecycle.csv").open(newline="", encoding="utf-8") as handle:
        lifecycle = list(csv.DictReader(handle))
    with (root / "transaction_finality.csv").open(newline="", encoding="utf-8") as handle:
        finality_rows = list(csv.DictReader(handle))
    stages = {row["stage"].lower() for row in lifecycle}
    required_stages = {"submitted", "received", "admitted", "proposed", "quorum_committed", "durable_committed", "sourcelock", "targetcommit", "sourcefinalize", "refund"}
    if not required_stages.issubset(stages):
        raise RuntimeError(f"lifecycle stages missing: {sorted(required_stages - stages)}")
    if finality.get("metric_truth") != "derived_from_raw_runtime_lifecycle" or finality.get("tcp_send_latency_excluded") is not True:
        raise RuntimeError("finality truth boundary invalid")
    if finality.get("logical_transaction_count") != 100 or finality.get("finalized_unique_logical_tx_count", 0) < 100:
        raise RuntimeError("logical transaction finality incomplete")
    if any(row["finality_ms"] == "-1" for row in finality_rows):
        raise RuntimeError("terminal lifecycle evidence missing")
    if not any(row["cross_shard"] == "true" and row["terminal_stage"].lower() in {"sourcefinalize", "refund"} for row in finality_rows):
        raise RuntimeError("cross-shard finality was not settled by finalization/refund")
    if summary.get("no_fallback") is not True or summary.get("orphan_process_count") != 0:
        raise RuntimeError("real cluster fallback/orphan check failed")
    report = {"acceptance_passed": True, "run_dir": str(root), "finality_summary": finality, "stage_count": len(stages)}
    (root / "v5_2_finality_metric_acceptance.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
