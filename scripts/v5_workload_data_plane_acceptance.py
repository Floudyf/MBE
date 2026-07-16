from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.app.models.v5_experiment_spec import V5ExperimentSpec, V5PluginSelection, V5Topology, V5WorkloadSourceSpec
from backend.app.models.v5_formal_experiment import V5FormalExperimentPlan, V5FormalMethod
from backend.app.services.v5_formal_run_store import children, create_group, group_dir, read_group
from backend.app.services.v5_formal_scheduler import expand, start
from backend.app.services.v5_plugin_manifest_store import CATEGORIES, STORE
from backend.app.services.v5_workload_data_plane import (
    WORKLOAD_CACHE_ROOT,
    WorkloadPreviewRequest,
    build_canonical,
    dataset_summary,
    load_manifest,
    materialize,
    preview_workload,
    raw_source_path,
    validate_csv,
    write_validation_report,
)


DATASET_ID = "dcl_sales_polygon_271868"
SOURCE_HASH = "f690db630e061a15dfab3f2b8a654006bccb010517a8d67379817fdda522474e"
SEED = 11
TX_COUNT = 10_000
ACCEPTANCE_BLOCK_PRODUCER_CONFIG = {"block_size": 100, "interval_ms": 75}
WORKLOAD_ARTIFACTS = {
    "workload_manifest_snapshot.json",
    "workload_source_spec.json",
    "workload_selection.json",
    "workload_skew_report.json",
    "workload_materialization_summary.json",
    "workload_identity_mapping_summary.json",
    "workload_replay_summary.json",
}


def plugin_selections() -> list[V5PluginSelection]:
    selections: list[V5PluginSelection] = []
    for category in CATEGORIES:
        plugin_id = "canonical_trace_replay" if category == "workload" else next(item.plugin_id for item in STORE.list() if item.category == category)
        config = ACCEPTANCE_BLOCK_PRODUCER_CONFIG if category == "block_producer" else {}
        selections.append(V5PluginSelection(category=category, plugin_id=plugin_id, config=config))
    return selections


def method() -> V5FormalMethod:
    return V5FormalMethod(
        method_id="metatrack_dataset_acceptance",
        display_name="MetaTrack Dataset Acceptance",
        role="main",
        plugin_overrides={
            "routing": "metatrack_coaccess_routing",
            "execution": "dual_track_execution",
            "scheduler": "fast_first_scheduler",
            "commit": "commutative_hot_update_aggregation",
        },
    )


def audit_csv(path: Path) -> dict[str, Any]:
    ids: set[str] = set()
    tx_hashes: set[str] = set()
    category_counts = {"wearable": 0, "emote": 0}
    missing = 0
    duplicate_sale_ids = 0
    with path.open("r", encoding="utf-8", newline="") as stream:
        for row in csv.DictReader(stream):
            if any(not (row.get(column) or "").strip() for column in ("id", "tx_hash", "buyer", "seller", "price", "timestamp", "category", "raw_contract_candidates")):
                missing += 1
            sale_id = row["id"].strip()
            if sale_id in ids:
                duplicate_sale_ids += 1
            ids.add(sale_id)
            tx_hashes.add(row["tx_hash"].strip().lower())
            category = row["category"].strip()
            if category in category_counts:
                category_counts[category] += 1
    return {
        "unique_sale_id_count": len(ids),
        "duplicate_sale_id_count": duplicate_sale_ids,
        "unique_tx_hash_count": len(tx_hashes),
        "category_counts": category_counts,
        "missing_key_field_rows": missing,
    }


def materialize_pair(manifest: dict[str, Any], source: Path) -> dict[str, Any]:
    canonical_first = build_canonical(source, WORKLOAD_CACHE_ROOT, manifest)
    canonical_second = build_canonical(source, WORKLOAD_CACHE_ROOT, manifest)
    canonical_path = WORKLOAD_CACHE_ROOT / canonical_first["canonical_relative_path"]
    original_first = materialize(
        canonical_path,
        WORKLOAD_CACHE_ROOT,
        dataset_id=DATASET_ID,
        source_sha256=SOURCE_HASH,
        requested_tx_count=TX_COUNT,
        seed=SEED,
        variant_mode="original_window",
    )
    original_second = materialize(
        canonical_path,
        WORKLOAD_CACHE_ROOT,
        dataset_id=DATASET_ID,
        source_sha256=SOURCE_HASH,
        requested_tx_count=TX_COUNT,
        seed=SEED,
        variant_mode="original_window",
    )
    derived_first = materialize(
        canonical_path,
        WORKLOAD_CACHE_ROOT,
        dataset_id=DATASET_ID,
        source_sha256=SOURCE_HASH,
        requested_tx_count=TX_COUNT,
        seed=SEED,
        variant_mode="contract_zipf",
        target_alpha=1.0,
    )
    derived_second = materialize(
        canonical_path,
        WORKLOAD_CACHE_ROOT,
        dataset_id=DATASET_ID,
        source_sha256=SOURCE_HASH,
        requested_tx_count=TX_COUNT,
        seed=SEED,
        variant_mode="contract_zipf",
        target_alpha=1.0,
    )
    return {
        "canonical_first": canonical_first,
        "canonical_second": canonical_second,
        "original_first": original_first,
        "original_second": original_second,
        "derived_first": derived_first,
        "derived_second": derived_second,
        "comparison": compare_skew(original_first, derived_first),
    }


def compare_skew(original: dict[str, Any], derived: dict[str, Any]) -> dict[str, Any]:
    concentration_metrics = {
        "hhi_higher": derived["hhi"] > original["hhi"],
        "top_10_higher": derived["top_10_ratio"] > original["top_10_ratio"],
        "top_100_higher": derived["top_100_ratio"] > original["top_100_ratio"],
    }
    resampling_descriptors = {
        "duplicate_ratio_higher": derived["duplicate_source_row_ratio"] > original["duplicate_source_row_ratio"],
        "maximum_reuse_higher": derived["maximum_reuse"] > original["maximum_reuse"],
    }
    return {
        "base_window_same": original["base_window_sha256"] == derived["base_window_sha256"],
        "category_counts_same": original["category_counts"] == derived["category_counts"],
        "source_hash_same": original["source_sha256"] == derived["source_sha256"],
        "canonical_hash_same": original["canonical_sha256"] == derived["canonical_sha256"],
        "materialized_hash_different": original["materialized_sha256"] != derived["materialized_sha256"],
        "contract_concentration_metrics": concentration_metrics,
        "contract_concentration_higher_by": [key for key, value in concentration_metrics.items() if value],
        "resampling_descriptors": resampling_descriptors,
        "resampling_described_by": [key for key, value in resampling_descriptors.items() if value],
        "derived_may_be_less_concentrated_than_original_window": True,
        "original": skew_fields(original),
        "derived": skew_fields(derived),
    }


def skew_fields(summary: dict[str, Any]) -> dict[str, Any]:
    keys = ("gini", "hhi", "top_1_ratio", "top_10_ratio", "top_100_ratio", "unique_contract_count", "maximum_reuse", "duplicate_source_row_ratio", "category_counts")
    return {key: summary.get(key) for key in keys}


def workload_source(variant: str) -> V5WorkloadSourceSpec:
    return V5WorkloadSourceSpec(
        source_type="dataset",
        plugin_id="canonical_trace_replay",
        dataset_id=DATASET_ID,
        variant_mode=variant,
        requested_tx_count=TX_COUNT,
        use_full_dataset=False,
        seed=SEED,
        source_sha256=SOURCE_HASH,
        skew_axis="contract" if variant == "contract_zipf" else None,
        target_alpha=1.0 if variant == "contract_zipf" else None,
    )


def run_dataset_child(variant: str, timeout_seconds: int, duration_ms: int, expected_materialization: dict[str, Any]) -> dict[str, Any]:
    spec = V5ExperimentSpec(
        name=f"v5_workload_data_plane_{variant}_{TX_COUNT}_acceptance",
        execution_backend="real_cluster",
        plugin_selections=plugin_selections(),
        topology=V5Topology(nodes=8, shards=2, validators_per_shard=4),
        tx_count=TX_COUNT,
        seed=SEED,
        duration_ms=duration_ms,
        workload_source=workload_source(variant),
    )
    plan = V5FormalExperimentPlan(
        name=f"v5_workload_data_plane_{variant}_{TX_COUNT}_acceptance",
        base_spec=spec,
        suites=["main_experiment"],
        methods=[method()],
        seeds=[SEED],
        repeats=1,
        source_label="script",
        tags=["v5_workload_data_plane", "checkpoint_d"],
    )
    matrix = expand(plan, "real_cluster")
    group = create_group(
        {
            "execution_backend": "real_cluster",
            "runtime_truth": "v5_real_cluster_candidate",
            "plan": plan.model_dump(),
            "matrix": matrix,
            "total_child_runs": len(matrix),
            "completed_child_runs": 0,
            "cancel_requested": False,
            "max_concurrent_real_clusters": 1,
        }
    )
    start(group["run_group_id"])
    deadline = time.monotonic() + timeout_seconds
    current = read_group(group["run_group_id"])
    while time.monotonic() < deadline:
        current = read_group(group["run_group_id"])
        if current.get("status") in {"completed", "completed_with_failures", "failed", "cancelled"}:
            break
        time.sleep(2)
    items = children(group["run_group_id"])
    item = items[0] if items else {}
    result = item.get("result") or {}
    run_dir = ROOT / result.get("output_dir", "")
    gates = validate_child(item, run_dir, variant, expected_materialization)
    return {
        "run_group_id": group["run_group_id"],
        "group_status": current.get("status"),
        "child_id": item.get("child_run_id"),
        "child_status": item.get("status"),
        "run_id": result.get("run_id"),
        "run_dir": result.get("output_dir"),
        "gates": gates,
    }


def validate_child(child: dict[str, Any], run_dir: Path, variant: str, expected_materialization: dict[str, Any]) -> dict[str, Any]:
    blockers: list[str] = []
    result = child.get("result") or {}
    summary = result.get("summary") or {}
    finality = summary.get("finality_evidence") or read_json(run_dir / "finality_summary.json")
    drain = read_json(run_dir / "drain_status.json")
    runtime_closure = summarize_runtime_closure(run_dir)
    replay = read_json(run_dir / "workload_replay_summary.json") or summary.get("workload_replay_summary") or {}
    identity = read_json(run_dir / "workload_identity_mapping_summary.json") or {}
    plan = read_json(run_dir / "compiled_run_plan.json")
    workload_plan = (plan or {}).get("workload_plan") or {}
    artifacts = {path.name for path in run_dir.glob("workload_*.json")} if run_dir.is_dir() else set()
    if child.get("status") != "completed" or result.get("status") != "completed":
        blockers.append("child did not complete")
    if result.get("no_fallback") is not True or summary.get("no_fallback") is not True or replay.get("no_fallback") is not True:
        blockers.append("no_fallback evidence missing")
    if finality.get("submitted_unique_tx_count") != TX_COUNT or finality.get("terminal_unique_tx_count") != TX_COUNT or finality.get("incomplete_unique_tx_count") != 0:
        blockers.append("finality counts are incomplete")
    if finality.get("finalized_unique_logical_tx_count") != TX_COUNT:
        blockers.append("durable finality count is incomplete")
    if runtime_closure["node_status_count"] and any(value for key, value in runtime_closure.items() if key != "node_status_count"):
        blockers.append(f"runtime drain not closed: {runtime_closure}")
    if summary.get("state_root_consistent") is not True:
        blockers.append("state root consistency failed")
    if summary.get("orphan_process_count") != 0:
        blockers.append("orphan process count is non-zero")
    if summary.get("distinct_process_count") != 8 or summary.get("expected_process_count") != 8:
        blockers.append("8-node process evidence missing")
    if replay.get("expected_count") != TX_COUNT or replay.get("read_count") != TX_COUNT or replay.get("submitted_count") != TX_COUNT:
        blockers.append("workload replay counts are incomplete")
    if replay.get("signature_pass_count") != TX_COUNT:
        blockers.append("signature verification did not pass for every transaction")
    if replay.get("nonce_continuity") is not True:
        blockers.append("nonce continuity failed")
    if not replay.get("identity_count"):
        blockers.append("identity count missing")
    if not WORKLOAD_ARTIFACTS.issubset(artifacts):
        blockers.append("workload artifact set is incomplete")
    truth = "real_derived_resampled" if variant == "contract_zipf" else "real_observed"
    if workload_plan.get("truth_label") != truth:
        blockers.append("compiled workload truth_label mismatch")
    expected_provenance = {
        "materialized_id": expected_materialization.get("materialized_id"),
        "materialized_sha256": expected_materialization.get("materialized_sha256"),
        "base_window_sha256": expected_materialization.get("base_window_sha256"),
        "source_sha256": expected_materialization.get("source_sha256"),
        "canonical_sha256": expected_materialization.get("canonical_sha256"),
        "dataset_id": expected_materialization.get("dataset_id"),
        "variant_mode": expected_materialization.get("variant_mode"),
        "truth_label": truth,
    }
    observed_provenance = {
        "materialized_id": workload_plan.get("materialized_id"),
        "materialized_sha256": workload_plan.get("materialized_sha256"),
        "base_window_sha256": workload_plan.get("base_window_sha256") or workload_plan.get("base_window_hash"),
        "source_sha256": workload_plan.get("source_sha256"),
        "canonical_sha256": workload_plan.get("canonical_sha256"),
        "dataset_id": workload_plan.get("dataset_id"),
        "variant_mode": workload_plan.get("variant_mode"),
        "truth_label": workload_plan.get("truth_label"),
    }
    missing = [key for key, value in observed_provenance.items() if value in {None, ""}]
    if missing:
        blockers.append(f"compiled workload provenance missing: {missing}")
    mismatched = [key for key, value in expected_provenance.items() if observed_provenance.get(key) != value]
    if mismatched:
        blockers.append(f"compiled workload provenance mismatch: {mismatched}")
    return {
        "passed": not blockers,
        "blockers": blockers,
        "truth_label": workload_plan.get("truth_label"),
        "materialized_id": workload_plan.get("materialized_id"),
        "materialized_sha256": workload_plan.get("materialized_sha256"),
        "base_window_sha256": workload_plan.get("base_window_sha256") or workload_plan.get("base_window_hash"),
        "source_sha256": workload_plan.get("source_sha256"),
        "canonical_sha256": workload_plan.get("canonical_sha256"),
        "dataset_id": workload_plan.get("dataset_id"),
        "variant_mode": workload_plan.get("variant_mode"),
        "provenance_matches_materialization": not missing and not mismatched,
        "expected_cross_shard_count": replay.get("expected_cross_shard_count"),
        "expected_cross_shard_ratio": replay.get("expected_cross_shard_ratio"),
        "actual_cross_shard_count": replay.get("actual_cross_shard_count"),
        "actual_cross_shard_ratio": replay.get("actual_cross_shard_ratio"),
        "submitted": finality.get("submitted_unique_tx_count"),
        "terminal": finality.get("terminal_unique_tx_count"),
        "incomplete": finality.get("incomplete_unique_tx_count"),
        "drain_status": drain,
        "runtime_closure": runtime_closure,
        "state_root_consistent": summary.get("state_root_consistent"),
        "orphan_process_count": summary.get("orphan_process_count"),
        "identity_count": replay.get("identity_count") or identity.get("identity_count"),
        "signature_pass_count": replay.get("signature_pass_count"),
        "nonce_continuity": replay.get("nonce_continuity"),
        "artifact_count": len(artifacts),
    }


def read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def summarize_runtime_closure(run_dir: Path) -> dict[str, Any]:
    totals = {
        "node_status_count": 0,
        "pending_relay": 0,
        "reserved": 0,
        "pending_commit": 0,
        "pending_future_block": 0,
        "proposal_in_flight": False,
    }
    if not run_dir.is_dir():
        return totals
    for status_path in sorted((run_dir / "nodes").glob("*/node_runtime_status.json")):
        status = read_json(status_path)
        if not status:
            continue
        totals["node_status_count"] += 1
        totals["pending_relay"] += int(status.get("pending_cross_shard_count", 0) or 0)
        totals["reserved"] += int(status.get("reserved_tx_count", 0) or 0)
        totals["pending_commit"] += int(status.get("pending_commit_count", 0) or 0)
        totals["pending_future_block"] += int(status.get("pending_future_block_count", 0) or 0)
        totals["proposal_in_flight"] = totals["proposal_in_flight"] or bool(status.get("proposal_in_flight", False))
    return totals


def ci_no_data_check(manifest: dict[str, Any]) -> dict[str, Any]:
    unavailable = dict(manifest)
    unavailable["local_raw_relative_path"] = "missing_dcl_sales_workload_chain_ready.csv"
    summary = dataset_summary(unavailable)
    synthetic = preview_workload(
        WorkloadPreviewRequest(
            source_type="synthetic",
            plugin_id="deterministic_signed_synthetic",
            requested_tx_count=100,
            seed=1,
        )
    )
    return {
        "dataset_available": summary.available,
        "dataset_selectable": summary.selectable,
        "validation_status": summary.validation_status,
        "synthetic_preview_blockers": synthetic.blockers,
        "passed": summary.available is False and summary.selectable is False and synthetic.blockers == [],
    }


def main() -> int:
    global TX_COUNT
    parser = argparse.ArgumentParser(description="Run V5 workload data plane Checkpoint D local acceptance.")
    parser.add_argument("--timeout-seconds", type=int, default=1_200)
    parser.add_argument("--duration-ms", type=int, default=1_800_000)
    parser.add_argument("--skip-real-cluster", action="store_true")
    parser.add_argument("--tx-count", type=int, default=TX_COUNT)
    parser.add_argument("--variants", default="original,derived", help="Comma-separated: original,derived")
    args = parser.parse_args()
    TX_COUNT = args.tx_count
    if TX_COUNT not in {10_000, 50_000, 100_000, 250_000}:
        os.environ["MBE_V5_LOCAL_SMOKE_COUNTS"] = ",".join(sorted({*os.environ.get("MBE_V5_LOCAL_SMOKE_COUNTS", "").split(","), str(TX_COUNT)}))
    requested_variants = {item.strip() for item in args.variants.split(",") if item.strip()}
    unknown_variants = requested_variants - {"original", "derived"}
    if unknown_variants:
        raise SystemExit(f"unsupported variants: {sorted(unknown_variants)}")
    reports = WORKLOAD_CACHE_ROOT / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    manifest = load_manifest(DATASET_ID)
    source = raw_source_path(manifest)
    validation = validate_csv(source, expected_sha256=manifest["source_sha256"])
    validation_report = write_validation_report(validation, reports)
    materials = materialize_pair(manifest, source)
    source_after = validate_csv(source, expected_sha256=manifest["source_sha256"])
    ci_check = ci_no_data_check(manifest)
    original_run = None
    derived_run = None
    if not args.skip_real_cluster:
        if "original" in requested_variants:
            original_run = run_dataset_child("original_window", args.timeout_seconds, args.duration_ms, materials["original_second"])
        if "derived" in requested_variants:
            derived_run = run_dataset_child("contract_zipf", args.timeout_seconds, args.duration_ms, materials["derived_second"])
    blockers: list[str] = []
    audit = audit_csv(source)
    if validation.row_count != 271_868 or audit["unique_sale_id_count"] != 271_868 or audit["unique_tx_hash_count"] != 271_848:
        blockers.append("CSV cardinality audit mismatch")
    if audit["category_counts"] != {"wearable": 252_898, "emote": 18_970} or audit["missing_key_field_rows"] != 0 or audit["duplicate_sale_id_count"] != 0:
        blockers.append("CSV category/missing/id audit mismatch")
    if asdict(validation) != asdict(source_after):
        blockers.append("source CSV changed during processing")
    if materials["canonical_first"]["canonical_sha256"] != materials["canonical_second"]["canonical_sha256"] or not materials["canonical_second"].get("cache_hit"):
        blockers.append("canonical determinism/cache check failed")
    if materials["original_first"]["materialized_sha256"] != materials["original_second"]["materialized_sha256"] or not materials["original_second"].get("cache_hit"):
        blockers.append("original materialization determinism/cache check failed")
    if materials["derived_first"]["materialized_sha256"] != materials["derived_second"]["materialized_sha256"] or not materials["derived_second"].get("cache_hit"):
        blockers.append("derived materialization determinism/cache check failed")
    comparison = materials["comparison"]
    if not all(comparison[key] for key in ("base_window_same", "category_counts_same", "source_hash_same", "canonical_hash_same", "materialized_hash_different")):
        blockers.append("original/derived fairness invariants failed")
    if not ci_check["passed"]:
        blockers.append("CI no-data compatibility check failed")
    for label, run in (("original", original_run), ("derived", derived_run)):
        if run and not run["gates"]["passed"]:
            blockers.append(f"{label} real_cluster gates failed: {run['gates']['blockers']}")
        if run and run.get("group_status") != "completed":
            blockers.append(f"{label} run group did not complete")
    report = {
        "acceptance_passed": not blockers,
        "blockers": blockers,
        "source_validation_report": validation_report.name,
        "csv_validation": asdict(validation),
        "csv_audit": audit,
        "canonical": materials["canonical_second"],
        "original_materialization": materials["original_second"],
        "derived_materialization": materials["derived_second"],
        "original_derived_comparison": comparison,
        "ci_no_data": ci_check,
        "original_real_cluster": original_run,
        "derived_real_cluster": derived_run,
        "ready_to_commit": not blockers,
    }
    suffix = ""
    if TX_COUNT != 10_000 or requested_variants != {"original", "derived"}:
        suffix = "_" + str(TX_COUNT) + "_" + "_".join(sorted(requested_variants))
    target = reports / f"v5_workload_data_plane_acceptance{suffix}.json"
    target.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"acceptance_passed": report["acceptance_passed"], "report": str(target.relative_to(ROOT)), "blockers": blockers}, sort_keys=True))
    return 0 if not blockers else 1


if __name__ == "__main__":
    raise SystemExit(main())
