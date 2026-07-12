from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.app.models.v5_experiment_spec import V5ExperimentSpec, V5PluginSelection, V5Topology
from backend.app.models.v5_formal_experiment import V5FormalExperimentPlan, V5FormalMethod
from backend.app.services.v5_formal_run_store import children, create_group, group_dir, read_group, write_child
from backend.app.services.v5_formal_scheduler import expand
from backend.app.services.v5_plugin_manifest_store import CATEGORIES, STORE


def selections() -> list[V5PluginSelection]:
    return [
        V5PluginSelection(category=category, plugin_id=next(item.plugin_id for item in STORE.list() if item.category == category))
        for category in CATEGORIES
    ]


def validate_single_child(directory: Path) -> list[str]:
    blockers: list[str] = []
    try:
        drain = json.loads((directory / "drain_status.json").read_text(encoding="utf-8"))
        summary = json.loads((directory / "real_cluster_summary.json").read_text(encoding="utf-8"))
        finality = json.loads((directory / "finality_summary.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return [f"single child artifacts unavailable: {exc}"]
    if {
        drain.get("submitted"),
        drain.get("terminal"),
    } != {10000} or drain.get("incomplete") != 0 or drain.get("completion_reason") != "drain_quiescent":
        blockers.append("single child drain barrier incomplete")
    required_true = [
        "real_client_submission",
        "real_signed_tx",
        "real_pbft_style_messages",
        "real_cross_shard_network",
        "state_root_consistent",
        "no_fallback",
    ]
    if any(summary.get(key) is not True for key in required_true):
        blockers.append("single child truth or consistency evidence incomplete")
    if summary.get("distinct_process_count") != 16 or summary.get("orphan_process_count") != 0:
        blockers.append("single child process evidence incomplete")
    if finality.get("logical_transaction_count") != 10000 or finality.get("finalized_unique_logical_tx_count") != 10000:
        blockers.append("single child finality incomplete")
    return blockers


def build_formal_plan() -> V5FormalExperimentPlan:
    methods = [
        V5FormalMethod(
            method_id="metatrack_full",
            display_name="MetaTrack Full",
            plugin_overrides={
                "routing": "metatrack_coaccess_routing",
                "execution": "dual_track_execution",
                "scheduler": "fast_first_scheduler",
                "commit": "commutative_hot_update_aggregation",
            },
        ),
        V5FormalMethod(
            method_id="hash_serial",
            display_name="Hash Serial Baseline",
            plugin_overrides={
                "routing": "hash_routing_baseline",
                "execution": "serial_execution_baseline",
                "scheduler": "fifo_serial_scheduler",
                "commit": "normal_commit",
            },
        ),
        V5FormalMethod(
            method_id="no_aggregation",
            display_name="No Aggregation",
            plugin_overrides={
                "routing": "metatrack_coaccess_routing",
                "execution": "dual_track_execution",
                "scheduler": "fast_first_scheduler",
                "commit": "normal_commit",
            },
        ),
    ]
    return V5FormalExperimentPlan(
        name="v5_2_formal_matrix_compile_acceptance",
        base_spec=V5ExperimentSpec(
            name="v5_2_formal_matrix",
            execution_backend="real_cluster",
            plugin_selections=selections(),
            topology=V5Topology(nodes=16, shards=4, validators_per_shard=4),
            tx_count=10000,
            seed=71,
            duration_ms=3600000,
        ),
        suites=["comparison_experiment"],
        methods=methods,
        seeds=[71, 72],
        repeats=2,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate one completed child and compile, persist, and reload the 12-child formal matrix.")
    parser.add_argument("--single-run-dir", default=str(ROOT / ".cache" / "v5_single_16n_4s_10000_final5"))
    args = parser.parse_args()

    blockers = validate_single_child(Path(args.single_run_dir).resolve())
    plan = build_formal_plan()
    matrix = expand(plan, "real_cluster")
    if len(matrix) != 12:
        blockers.append(f"formal matrix child count {len(matrix)} != 12")
    if any(row.get("execution_backend") != "real_cluster" or row.get("estimated_transactions") != 10000 for row in matrix):
        blockers.append("formal matrix row contract incomplete")
    group = create_group(
        {
            "execution_backend": "real_cluster",
            "runtime_truth": "v5_real_cluster_candidate",
            "plan": plan.model_dump(),
            "matrix": matrix,
            "total_child_runs": len(matrix),
            "completed_child_runs": 0,
            "cancel_requested": False,
            "matrix_execution_started": False,
            "max_concurrent_real_clusters": 1,
        }
    )
    group_id = group["run_group_id"]
    for row in matrix:
        write_child(group_id, {**row, "status": "queued", "execution_started": False, "paper_candidate": False})
    reloaded = read_group(group_id)
    persisted_rows = children(group_id)
    if len(persisted_rows) != 12 or reloaded.get("matrix_execution_started") is not False:
        blockers.append("formal matrix persistence/reload incomplete")
    directory = group_dir(group_id)
    report = {
        "acceptance_passed": not blockers,
        "single_child_run_dir": str(Path(args.single_run_dir).resolve()),
        "single_child_validated": not any("single child" in item for item in blockers),
        "run_group_id": group_id,
        "matrix_child_count": len(matrix),
        "matrix_execution_started": False,
        "paper_candidate": False,
        "blockers": blockers,
    }
    (directory / "v5_2_final_acceptance.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report))
    return 0 if not blockers else 1


if __name__ == "__main__":
    raise SystemExit(main())
