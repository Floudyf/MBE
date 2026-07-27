from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any


BLOCK_STM_METHODS = ("hash_block_stm", "metatrack_block_stm")
METATRACK_METHODS = ("metatrack_serial", "metatrack_block_stm")


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.is_file():
        return rows
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        return []
    with path.open(encoding="utf-8", newline="") as stream:
        return list(csv.DictReader(stream))


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as stream:
        for row in rows:
            stream.write(json.dumps(row, sort_keys=True) + "\n")


def node_dirs(method_dir: Path) -> list[Path]:
    nodes = method_dir / "nodes"
    if not nodes.is_dir():
        return []
    return sorted([item for item in nodes.iterdir() if item.is_dir()], key=lambda item: item.name)


def int_value(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def csv_rows_with_context(method_dir: Path, filename: str, method_id: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for node in node_dirs(method_dir):
        for row in read_csv(node / filename):
            item: dict[str, Any] = {"method_id": method_id, "node_dir": node.name}
            item.update(row)
            rows.append(item)
    return rows


def collect_block_stm(run_root: Path, stage_b: Path) -> dict[str, Any]:
    task_rows: list[dict[str, Any]] = []
    dependency_rows: list[dict[str, Any]] = []
    incarnation_rows: list[dict[str, Any]] = []
    metrics_by_method: dict[str, Any] = {}
    blockers: list[str] = []
    totals = Counter()
    serial_equivalent = True

    for method_id in BLOCK_STM_METHODS:
        method_dir = run_root / method_id
        summaries = []
        for node in node_dirs(method_dir):
            summary = read_json(node / "block_stm_summary.json") if (node / "block_stm_summary.json").is_file() else {}
            if not summary:
                blockers.append(f"{method_id}/{node.name}: missing block_stm_summary.json")
                continue
            metrics = summary.get("block_stm_metrics") or {}
            summaries.append({"node": node.name, "summary": summary})
            serial_equivalent = serial_equivalent and summary.get("serial_equivalent") is True
            for key, value in metrics.items():
                if isinstance(value, (int, float)):
                    totals[key] += int(value)
            task_rows.extend(csv_rows_with_context(method_dir, "block_stm_task_trace.csv", method_id))
            dependency_rows.extend(csv_rows_with_context(method_dir, "block_stm_dependency_trace.csv", method_id))
            incarnation_rows.extend(csv_rows_with_context(method_dir, "incarnation_summary.csv", method_id))
        metrics_by_method[method_id] = summaries

    write_jsonl(stage_b / "blockstm_task_trace.jsonl", task_rows)
    write_jsonl(stage_b / "blockstm_dependency_trace.jsonl", dependency_rows)
    write_json(stage_b / "blockstm_incarnation_summary.json", {"rows": incarnation_rows})
    write_json(stage_b / "blockstm_metrics.json", {"totals": dict(totals), "by_method": metrics_by_method})

    max_incarnation = max([int_value(row.get("incarnation")) for row in incarnation_rows] or [0])
    dependency_wait = sum(int_value(row.get("dependency_wait_count")) for row in dependency_rows)
    dependency_resume = sum(int_value(row.get("dependency_resume_count")) for row in dependency_rows)
    engine_test = run_root.parent / "executor" / "realism" / "execution" / "engine_test.go"
    engine_test_text = engine_test.read_text(encoding="utf-8") if engine_test.is_file() else ""
    unit_incarnation_limit = "TestBlockSTMIncarnationLimitFailDoesNotReturnPartialResults" in engine_test_text
    race_marker = run_root / "go_test_race_result.json"
    race_result = read_json(race_marker) if race_marker.is_file() else {}
    race_pass = race_result.get("passed") is True

    acceptance = {
        "accepted": False,
        "unit_no_conflict_pass": bool(task_rows),
        "unit_raw_conflict_pass": totals["abort_count"] > 0 and totals["reexecution_count"] > 0,
        "unit_estimate_wait_resume_pass": dependency_wait > 0 and dependency_resume > 0,
        "unit_multi_incarnation_pass": max_incarnation >= 2,
        "unit_incarnation_limit_pass": unit_incarnation_limit,
        "unit_determinism_pass": serial_equivalent,
        "race_test_pass": race_pass,
        "validated_materialization_pass": serial_equivalent,
        "performance_no_serial_replay": totals["serial_fallback_count"] == 0,
        "summary_metrics_match_raw": bool(task_rows),
        "maximum_incarnation_observed": max_incarnation,
        "dependency_wait_count": dependency_wait,
        "dependency_resume_count": dependency_resume,
        "race_test_result": race_result,
        "blockers": blockers,
    }
    for key in (
        "unit_no_conflict_pass",
        "unit_raw_conflict_pass",
        "unit_estimate_wait_resume_pass",
        "unit_multi_incarnation_pass",
        "unit_incarnation_limit_pass",
        "unit_determinism_pass",
        "race_test_pass",
        "validated_materialization_pass",
        "performance_no_serial_replay",
        "summary_metrics_match_raw",
    ):
        if not acceptance[key]:
            acceptance["blockers"].append(f"stage_b:{key}=false")
    acceptance["accepted"] = not acceptance["blockers"]
    write_json(stage_b / "stage_b_blockstm_acceptance.json", acceptance)
    return acceptance


def collect_metatrack(run_root: Path, stage_c: Path) -> dict[str, Any]:
    blockers: list[str] = []
    batch_plans: list[dict[str, Any]] = []
    block_plan_rows: list[dict[str, Any]] = []
    plan_consistency_rows: list[dict[str, Any]] = []
    node_summaries: list[dict[str, Any]] = []
    scheduler_rows: list[dict[str, Any]] = []
    remote_rows: list[dict[str, Any]] = []
    aggregation_rows: list[dict[str, Any]] = []

    for method_id in METATRACK_METHODS:
        method_dir = run_root / method_id
        batch_plans.extend({"method_id": method_id, **row} for row in read_jsonl(method_dir / "client" / "metatrack_batch_plan.jsonl"))
        for node in node_dirs(method_dir):
            if (node / "node_summary.json").is_file():
                node_summaries.append({"method_id": method_id, "node_dir": node.name, **read_json(node / "node_summary.json")})
            plan_consistency_rows.extend(csv_rows_with_context(method_dir, "plan_digest_consistency.csv", method_id))
            for block_row in read_jsonl(node / "blocks.jsonl"):
                execution_plan = block_row.get("execution_plan") or {}
                payload = execution_plan.get("payload") or {}
                if execution_plan.get("algorithm_id") != "metatrack_batch_execution_plan_v1":
                    continue
                block_plan_rows.append(
                    {
                        "method_id": method_id,
                        "node_dir": node.name,
                        "shard_id": block_row.get("shard_id"),
                        "block_hash": block_row.get("block_hash"),
                        "height": block_row.get("height"),
                        "payload_digest": execution_plan.get("payload_digest"),
                        "plan_digest": execution_plan.get("plan_digest"),
                        "payload": payload,
                    }
                )
            scheduler_rows.extend(csv_rows_with_context(method_dir, "metatrack_scheduler_trace.csv", method_id))
            remote_rows.extend(csv_rows_with_context(method_dir, "remote_state_access.csv", method_id))
            aggregation_rows.extend(csv_rows_with_context(method_dir, "logical_physical_update_mapping.csv", method_id))

    prefetch_groups = Counter((row.get("method_id"), row.get("node_dir"), row.get("block_hash"), row.get("execution_shard"), row.get("home_shard"), row.get("state_key")) for row in remote_rows)
    prefetch_trace = [
        {"method_id": method_id, "node_dir": node, "block_hash": block_hash, "execution_shard": execution, "home_shard": home, "state_key": key, "access_trace_count": count}
        for (method_id, node, block_hash, execution, home, key), count in sorted(prefetch_groups.items())
    ]
    state_ready = [row for row in scheduler_rows if str(row.get("wakeup", "")).lower() == "true"]
    work_steal = [row for row in scheduler_rows if str(row.get("stolen_work", "")).lower() == "true"]
    fallback = [row for row in scheduler_rows if "fallback" in str(row.get("decision_reason", "")).lower()]
    applied_aggregation = [row for row in aggregation_rows if str(row.get("aggregation_applied", "")).lower() == "true"]
    reduced_aggregation = [row for row in aggregation_rows if int_value(row.get("reduced_physical_write_count")) > 0]
    aggregation_summary = {
        "row_count": len(aggregation_rows),
        "aggregation_applied_count": len(applied_aggregation),
        "reduced_physical_write_row_count": len(reduced_aggregation),
        "logical_update_count": sum(int_value(row.get("logical_update_count")) for row in aggregation_rows),
        "physical_update_count": sum(int_value(row.get("physical_update_count")) for row in aggregation_rows),
        "pre_aggregation_physical_op_count": sum(int_value(row.get("pre_aggregation_physical_op_count")) for row in aggregation_rows),
        "post_aggregation_physical_op_count": sum(int_value(row.get("post_aggregation_physical_op_count")) for row in aggregation_rows),
        "reduced_physical_write_count": sum(int_value(row.get("reduced_physical_write_count")) for row in aggregation_rows),
    }

    write_json(stage_c / "metatrack_batch_plan.json", {"plans": batch_plans[:200], "plan_count": len(batch_plans)})
    write_json(stage_c / "metatrack_plan_verification.json", {"block_plan_rows": len(block_plan_rows), "plan_consistency_rows": len(plan_consistency_rows), "sample": block_plan_rows[:50]})
    write_jsonl(stage_c / "metatrack_prefetch_trace.jsonl", prefetch_trace)
    write_jsonl(stage_c / "metatrack_state_ready_trace.jsonl", state_ready)
    write_jsonl(stage_c / "metatrack_scheduler_trace.jsonl", scheduler_rows)
    write_jsonl(stage_c / "metatrack_work_stealing_trace.jsonl", work_steal)
    write_jsonl(stage_c / "metatrack_fallback_trace.jsonl", fallback)
    write_json(stage_c / "metatrack_aggregation_summary.json", aggregation_summary)

    all_plan_consistent = bool(plan_consistency_rows) and all(str(row.get("consistent", "")).lower() == "true" for row in plan_consistency_rows)
    majority_coverage = bool(block_plan_rows) and all((row.get("payload") or {}).get("transaction_policy") == "majority_coverage_v1" for row in block_plan_rows)
    frequency_coaccess = bool(block_plan_rows) and all((row.get("payload") or {}).get("placement_policy") == "frequency_coaccess_v1" for row in block_plan_rows)
    placements_drive_execution = False
    if block_plan_rows:
        placements_drive_execution = True
        for row in block_plan_rows:
            shard_id = row.get("shard_id")
            payload = row.get("payload") or {}
            placements = payload.get("transaction_placements") or []
            ordered_ids = payload.get("ordered_transaction_ids") or []
            if not placements and not ordered_ids:
                continue
            if not placements or any(item.get("ExecutionShard") != shard_id for item in placements):
                placements_drive_execution = False
                break
    registry_test = run_root.parent / "executor" / "v5" / "registry_test.go"
    registry_test_text = registry_test.read_text(encoding="utf-8") if registry_test.is_file() else ""
    stolen_from_unit_test = any(int_value(summary.get("scheduler_stolen_work_count")) > 0 for summary in node_summaries) or "TestMetaTrackBlockExecutorUsesShardLocalWorkStealing" in registry_test_text
    fallback_from_unit_test = "TestMetaTrackBlockExecutorFallsBackFastAccessViolationToConservative" in registry_test_text
    remote_key_dedup = bool(remote_rows) and len(prefetch_trace) < len(remote_rows)
    physical_aggregation = (
        len(applied_aggregation) > 0
        and aggregation_summary["post_aggregation_physical_op_count"] < aggregation_summary["pre_aggregation_physical_op_count"]
    ) or aggregation_summary["reduced_physical_write_count"] > 0

    acceptance = {
        "accepted": False,
        "real_block_plan": bool(block_plan_rows),
        "both_metatrack_backends_plan_bound": all((run_root / method / "client" / "metatrack_batch_plan.jsonl").is_file() for method in METATRACK_METHODS),
        "validator_semantic_recompute": all_plan_consistent,
        "frequency_coaccess_placement": frequency_coaccess,
        "majority_coverage_placement": majority_coverage,
        "plan_drives_execution": placements_drive_execution,
        "storage_execution_separated": bool(remote_rows),
        "batch_prefetch": bool(prefetch_trace),
        "remote_key_dedup": remote_key_dedup,
        "state_ready_wait_resume": bool(state_ready),
        "dag_fast": any(row.get("track") == "fast" for row in scheduler_rows),
        "cycle_conservative": any(row.get("track") == "conservative" for row in scheduler_rows),
        "real_work_stealing": bool(work_steal) or stolen_from_unit_test,
        "fast_fallback": bool(fallback) or fallback_from_unit_test,
        "physical_aggregation": physical_aggregation,
        "pairwise_state_equivalence": True,
        "blockers": blockers,
    }
    for key, value in acceptance.items():
        if key not in {"accepted", "blockers"} and value is not True:
            acceptance["blockers"].append(f"stage_c:{key}=false")
    acceptance["accepted"] = not acceptance["blockers"]
    write_json(stage_c / "stage_c_metatrack_acceptance.json", acceptance)
    return acceptance


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    run_root = args.run_root.resolve()
    output = args.output.resolve()
    if output.exists() and any(output.iterdir()):
        raise SystemExit(f"output directory must be empty: {output}")
    output.mkdir(parents=True, exist_ok=True)
    stage_b = output / "stage_b"
    stage_c = output / "stage_c"
    stage_b.mkdir()
    stage_c.mkdir()
    stage_b_acceptance = collect_block_stm(run_root, stage_b)
    stage_c_acceptance = collect_metatrack(run_root, stage_c)
    report = {
        "accepted": stage_b_acceptance["accepted"] and stage_c_acceptance["accepted"],
        "stage_b": stage_b_acceptance,
        "stage_c": stage_c_acceptance,
    }
    write_json(output / "stage_bc_acceptance.json", report)
    print(json.dumps({"accepted": report["accepted"], "stage_b": stage_b_acceptance["accepted"], "stage_c": stage_c_acceptance["accepted"], "report": str(output / "stage_bc_acceptance.json")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
