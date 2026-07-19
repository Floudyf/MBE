from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.app.models.v5_experiment_spec import V5ExperimentSpec, V5PluginSelection, V5Topology, V5WorkloadSourceSpec
from backend.app.models.v5_formal_experiment import V5FormalExperimentPlan
from backend.app.services.v5_experiment_compiler import compile_plan
from backend.app.services.v5_formal_plan_validator import BUILTIN_METHODS, validate_request
from backend.app.services.v5_formal_scheduler import _spec_for
from backend.app.services.v5_plugin_manifest_store import CATEGORIES, STORE


METHOD_ORDER = ["hash_serial", "hash_block_stm", "metatrack_serial", "metatrack_block_stm"]
DATASET_ID = "dcl_sales_polygon_271868"
DATASET_SOURCE_SHA256 = "f690db630e061a15dfab3f2b8a654006bccb010517a8d67379817fdda522474e"


def selections(*, dataset: bool = False) -> list[V5PluginSelection]:
    result: list[V5PluginSelection] = []
    for category in CATEGORIES:
        manifest = next(item for item in STORE.list() if item.category == category)
        plugin_id = "canonical_trace_replay" if dataset and category == "workload" else manifest.plugin_id
        config = dict(manifest.default_config)
        if category == "workload":
            config = {} if dataset else (config | {"cross_shard_ratio": 0.25, "timeout_every": 17})
        result.append(V5PluginSelection(category=category, plugin_id=plugin_id, config=config))
    return result


def workload_source(kind: str, *, tx_count: int, seed: int) -> V5WorkloadSourceSpec:
    if kind == "synthetic":
        return V5WorkloadSourceSpec(source_type="synthetic", plugin_id="deterministic_signed_synthetic", requested_tx_count=tx_count, seed=seed)
    if kind == "dataset-original":
        return V5WorkloadSourceSpec(
            source_type="dataset",
            plugin_id="canonical_trace_replay",
            dataset_id=DATASET_ID,
            requested_tx_count=tx_count,
            seed=seed,
            variant_mode="original_window",
            source_sha256=DATASET_SOURCE_SHA256,
        )
    if kind == "dataset-derived":
        return V5WorkloadSourceSpec(
            source_type="dataset",
            plugin_id="canonical_trace_replay",
            dataset_id=DATASET_ID,
            requested_tx_count=tx_count,
            seed=seed,
            variant_mode="contract_zipf",
            skew_axis="contract",
            target_alpha=1.0,
            source_sha256=DATASET_SOURCE_SHA256,
        )
    raise ValueError(f"unsupported workload source: {kind}")


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as stream:
        return list(csv.DictReader(stream))


def node_dirs(run_dir: Path) -> list[Path]:
    nodes = run_dir / "nodes"
    if not nodes.is_dir():
        return []
    return sorted([item for item in nodes.iterdir() if item.is_dir()], key=lambda item: item.name)


def any_artifact(run_dir: Path, name: str) -> bool:
    if (run_dir / name).is_file() or (run_dir / "client" / name).is_file():
        return True
    return any((directory / name).is_file() for directory in node_dirs(run_dir))


def run_method(row: dict, plan: V5FormalExperimentPlan, output: Path, *, timeout_seconds: int) -> dict:
    if output.exists():
        shutil.rmtree(output)
    output.mkdir(parents=True, exist_ok=True)
    spec = _spec_for(plan, row)
    compiled = compile_plan(spec, output)
    plan_path = output / "compiled_run_plan.json"
    plan_path.write_text(compiled.model_dump_json(indent=2), encoding="utf-8")
    process = subprocess.run(
        ["go", "run", "./cmd/mbe-supervisor", "--mode", "v5-real-cluster", "--plan", str(plan_path), "--data-dir", str(output)],
        cwd=ROOT / "executor",
        text=True,
        capture_output=True,
        timeout=timeout_seconds,
    )
    if process.returncode:
        (output / "supervisor_stdout.log").write_text(process.stdout, encoding="utf-8")
        (output / "supervisor_stderr.log").write_text(process.stderr, encoding="utf-8")
        raise RuntimeError(f"{row['method_config_id']}: supervisor failed with {process.returncode}: {process.stderr[-4000:]}")
    return load_evidence(row["method_config_id"], output)


def load_evidence(method_id: str, output: Path) -> dict:
    summary = read_json(output / "real_cluster_summary.json")
    finality = read_json(output / "finality_summary.json")
    drain = read_json(output / "drain_status.json")
    plugins = read_json(output / "nodes" / "n0" / "plugin_snapshot.json")
    routing = read_csv(output / "client" / "routing_decision_log.csv")
    leader = output / "nodes" / "n0"
    execution = read_csv(leader / "execution_log.csv")
    commits = read_csv(leader / "commit_log.csv")
    block_stm_summaries = []
    serial_equivalence = []
    workload_replay = read_json(output / "workload_replay_summary.json") if (output / "workload_replay_summary.json").is_file() else {}
    for directory in node_dirs(output):
        block_stm = directory / "block_stm_summary.json"
        equivalent = directory / "serial_equivalence.json"
        if block_stm.is_file():
            block_stm_summaries.append(read_json(block_stm))
        if equivalent.is_file():
            serial_equivalence.append(read_json(equivalent))
    return {
        "method_id": method_id,
        "run_dir": str(output.relative_to(ROOT)),
        "summary": summary,
        "finality": finality,
        "drain": drain,
        "plugins": plugins,
        "routing_assignments": {row["tx_id"]: row["assigned_shard"] for row in routing},
        "routing_distribution": dict(Counter(row["assigned_shard"] for row in routing)),
        "fast_track_count": sum(row["track"] == "fast" for row in execution),
        "conservative_track_count": sum(row["track"] == "conservative" for row in execution),
        "aggregation_group_count": sum(row["aggregation_applied"] == "true" for row in commits),
        "logical_update_count": sum(int(row["logical_update_count"]) for row in commits),
        "physical_update_count": sum(int(row["physical_update_count"]) for row in commits),
        "block_stm_summaries": block_stm_summaries,
        "serial_equivalence": serial_equivalence,
        "workload_replay": workload_replay,
        "artifacts": {
            "block_execution_summary": all((directory / "block_execution_summary.json").is_file() for directory in node_dirs(output)),
            "execution_plan": all((directory / "execution_plan.jsonl").is_file() for directory in node_dirs(output)),
            "transaction_execution_trace": all((directory / "transaction_execution_trace.csv").is_file() for directory in node_dirs(output)),
            "plan_digest_consistency": all((directory / "plan_digest_consistency.csv").is_file() for directory in node_dirs(output)),
            "metatrack_batch_plan": any_artifact(output, "metatrack_batch_plan.jsonl"),
            "remote_state_access": any_artifact(output, "remote_state_access.csv"),
        },
    }


def validate(results: dict[str, dict], *, tx_count: int, workload_source: str) -> list[str]:
    blockers: list[str] = []
    synthetic = workload_source == "synthetic"
    for method_id, result in results.items():
        summary = result["summary"]
        finality = result["finality"]
        drain = result["drain"]
        plugins = result["plugins"]
        expected_block_executor = "block_stm_block_executor" if method_id.endswith("block_stm") else "serial_block_executor"
        expected_routing = "metatrack_coaccess_routing" if method_id.startswith("metatrack") else "hash_routing_baseline"
        if plugins["block_executor"]["plugin_id"] != expected_block_executor:
            blockers.append(f"{method_id}: wrong block executor plugin")
        if plugins["routing"]["plugin_id"] != expected_routing:
            blockers.append(f"{method_id}: wrong routing plugin")
        if summary.get("block_executor_id") != expected_block_executor or not summary.get("block_executor_consistent"):
            blockers.append(f"{method_id}: block executor consistency evidence failed")
        if not (summary.get("no_fallback") and summary.get("orphan_process_count") == 0 and summary.get("state_root_consistent") and summary.get("plan_digest_consistent")):
            blockers.append(f"{method_id}: correctness/no-fallback/root/digest evidence failed")
        if finality.get("submitted_unique_tx_count") != tx_count or finality.get("terminal_unique_tx_count") != tx_count or finality.get("incomplete_unique_tx_count") != 0:
            blockers.append(f"{method_id}: finality incomplete")
        if drain.get("submitted") != tx_count or drain.get("terminal") != tx_count or drain.get("incomplete") != 0 or drain.get("completion_reason") != "drain_quiescent":
            blockers.append(f"{method_id}: drain barrier incomplete")
        if not all(result["artifacts"][key] for key in ("block_execution_summary", "execution_plan", "transaction_execution_trace", "plan_digest_consistency")):
            blockers.append(f"{method_id}: generic block execution artifacts missing")
        if method_id.endswith("block_stm"):
            if not result["block_stm_summaries"] or not result["serial_equivalence"]:
                blockers.append(f"{method_id}: Block-STM artifacts missing")
            if any(item.get("serial_equivalent") is not True for item in result["block_stm_summaries"] + result["serial_equivalence"]):
                blockers.append(f"{method_id}: Block-STM serial equivalence failed")
        if method_id.startswith("metatrack"):
            if not (result["artifacts"]["metatrack_batch_plan"] and result["artifacts"]["remote_state_access"]):
                blockers.append(f"{method_id}: MetaTrack artifacts missing")
            if result["summary"].get("remote_state_access_count", 0) <= 0:
                blockers.append(f"{method_id}: MetaTrack remote state access evidence missing")
        if not synthetic:
            replay = result["workload_replay"]
            truth = "real_observed" if workload_source == "dataset-original" else "real_derived_resampled"
            if replay.get("expected_count") != tx_count or replay.get("read_count") != tx_count or replay.get("submitted_count") != tx_count:
                blockers.append(f"{method_id}: dataset replay counts incomplete")
            if replay.get("truth_label") != truth or replay.get("no_fallback") is not True:
                blockers.append(f"{method_id}: dataset truth/no-fallback evidence failed")
    if synthetic:
        if results["hash_serial"]["routing_assignments"] == results["metatrack_serial"]["routing_assignments"]:
            blockers.append("Hash and MetaTrack serial routing assignments did not differ")
        if results["hash_block_stm"]["routing_assignments"] == results["metatrack_block_stm"]["routing_assignments"]:
            blockers.append("Hash and MetaTrack Block-STM routing assignments did not differ")
        if not (results["metatrack_serial"]["fast_track_count"] > 0 and results["metatrack_block_stm"]["fast_track_count"] > 0):
            blockers.append("MetaTrack methods did not produce dual-track fast execution evidence")
        if not (results["hash_serial"]["fast_track_count"] == 0 and results["hash_block_stm"]["fast_track_count"] == 0):
            blockers.append("Hash methods unexpectedly produced dual-track fast execution evidence")
        for method_id in ("metatrack_serial", "metatrack_block_stm"):
            item = results[method_id]
            if not (item["aggregation_group_count"] > 0 and item["physical_update_count"] < item["logical_update_count"]):
                blockers.append(f"{method_id}: MetaTrack aggregation evidence failed")
        for method_id in ("hash_serial", "hash_block_stm"):
            if results[method_id]["aggregation_group_count"] != 0:
                blockers.append(f"{method_id}: hash baseline unexpectedly used aggregation")
    return blockers


def main() -> int:
    parser = argparse.ArgumentParser(description="Run V5 execution-methods closure acceptance for Hash/MetaTrack x Serial/Block-STM.")
    parser.add_argument("--output-root", default=str(ROOT / ".cache" / "v5_execution_methods_closure"))
    parser.add_argument("--tx-count", type=int, default=100)
    parser.add_argument("--workload-source", choices=["synthetic", "dataset-original", "dataset-derived"], default="synthetic")
    parser.add_argument("--timeout-seconds", type=int, default=240)
    args = parser.parse_args()
    if args.workload_source.startswith("dataset") and args.tx_count not in {10_000, 50_000, 100_000, 250_000}:
        existing = {item for item in os.environ.get("MBE_V5_LOCAL_SMOKE_COUNTS", "").split(",") if item}
        os.environ["MBE_V5_LOCAL_SMOKE_COUNTS"] = ",".join(sorted(existing | {str(args.tx_count)}))
    output = Path(args.output_root).resolve()
    output.mkdir(parents=True, exist_ok=True)
    base_spec = V5ExperimentSpec(
        name="v5_execution_methods_closure",
        execution_backend="real_cluster",
        plugin_selections=selections(dataset=args.workload_source.startswith("dataset")),
        topology=V5Topology(nodes=8, shards=2, validators_per_shard=4),
        tx_count=args.tx_count,
        seed=73,
        workload_source=workload_source(args.workload_source, tx_count=args.tx_count, seed=73),
        duration_ms=3_600_000,
    )
    plan = V5FormalExperimentPlan(
        name="v5_execution_methods_closure",
        base_spec=base_spec,
        suites=["comparison_experiment"],
        methods=[BUILTIN_METHODS[key] for key in METHOD_ORDER],
        seeds=[73],
        repeats=1,
        source_label="script",
        tags=["execution_methods_closure"],
    )
    checked = validate_request(type("Request", (), {"execution_backend": "real_cluster", "plan": plan})())
    rows = sorted(checked.rows, key=lambda row: METHOD_ORDER.index(row["method_config_id"]))
    results = {row["method_config_id"]: run_method(row, checked.plan, output / row["method_config_id"], timeout_seconds=args.timeout_seconds) for row in rows}
    blockers = validate(results, tx_count=args.tx_count, workload_source=args.workload_source)
    report = {
        "acceptance_passed": not blockers,
        "methods": METHOD_ORDER,
        "tx_count": args.tx_count,
        "workload_source": args.workload_source,
        "seed": 73,
        "topology": {"nodes": 8, "shards": 2, "validators_per_shard": 4},
        "per_method": {
            key: {
                "run_dir": value["run_dir"],
                "block_executor": value["plugins"]["block_executor"]["plugin_id"],
                "routing": value["plugins"]["routing"]["plugin_id"],
                "submitted": value["finality"].get("submitted_unique_tx_count"),
                "terminal": value["finality"].get("terminal_unique_tx_count"),
                "incomplete": value["finality"].get("incomplete_unique_tx_count"),
                "state_root_consistent": value["summary"].get("state_root_consistent"),
                "plan_digest_consistent": value["summary"].get("plan_digest_consistent"),
                "no_fallback": value["summary"].get("no_fallback"),
                "orphan_process_count": value["summary"].get("orphan_process_count"),
                "fast_track_count": value["fast_track_count"],
                "aggregation_group_count": value["aggregation_group_count"],
                "remote_state_access_count": value["summary"].get("remote_state_access_count"),
                "block_stm_serial_equivalent": all(item.get("serial_equivalent") is True for item in value["block_stm_summaries"] + value["serial_equivalence"]) if key.endswith("block_stm") else None,
                "workload_truth_label": value["workload_replay"].get("truth_label"),
            }
            for key, value in results.items()
        },
        "routing_assignment_differs": {
            "serial": results["hash_serial"]["routing_assignments"] != results["metatrack_serial"]["routing_assignments"],
            "block_stm": results["hash_block_stm"]["routing_assignments"] != results["metatrack_block_stm"]["routing_assignments"],
        },
        "blockers": blockers,
    }
    (output / "v5_execution_methods_closure_acceptance.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"acceptance_passed": not blockers, "report": str((output / "v5_execution_methods_closure_acceptance.json").relative_to(ROOT)), "blockers": blockers}, sort_keys=True))
    return 0 if not blockers else 1


if __name__ == "__main__":
    raise SystemExit(main())
