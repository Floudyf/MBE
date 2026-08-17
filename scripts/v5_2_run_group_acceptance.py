from __future__ import annotations

import json
import sys
import time
import argparse
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.app.models.v5_experiment_spec import V5ExperimentSpec, V5PluginSelection, V5Topology
from backend.app.models.v5_formal_experiment import V5FormalExperimentPlan, V5FormalMethod
from backend.app.services.v5_formal_run_store import children, create_group, group_dir, read_group
from backend.app.services.v5_formal_scheduler import expand, start
from backend.app.services.v5_plugin_manifest_store import CATEGORIES, STORE


REQUIRED_GROUP_ARTIFACTS = (
    "raw_summary.csv",
    "aggregate_summary.csv",
    "confidence_interval.csv",
    "formal_matrix.csv",
    "fairness_matrix.csv",
    "artifacts.zip",
)
REQUIRED_BUNDLE_MEMBERS = {"reproducibility_manifest.json", "artifact_manifest.json"}
TERMINAL_GROUP_STATUSES = {"completed", "completed_with_failures", "failed", "cancelled", "interrupted"}
BUNDLE_WAIT_SECONDS = 90.0
BUNDLE_POLL_SECONDS = 0.25


def _artifact_problems(directory: Path) -> list[str]:
    problems: list[str] = []
    for name in REQUIRED_GROUP_ARTIFACTS:
        path = directory / name
        if not path.is_file():
            problems.append(f"{name}:missing")
            continue
        try:
            if path.stat().st_size <= 0:
                problems.append(f"{name}:empty")
        except OSError as exc:
            problems.append(f"{name}:stat_error:{exc}")

    bundle = directory / "artifacts.zip"
    if bundle.is_file():
        try:
            if bundle.stat().st_size > 0:
                with zipfile.ZipFile(bundle, "r") as archive:
                    bad = archive.testzip()
                    if bad is not None:
                        problems.append(f"artifacts.zip:crc:{bad}")
                    names = set(archive.namelist())
                    for name in sorted(REQUIRED_BUNDLE_MEMBERS - names):
                        problems.append(f"artifacts.zip:missing_member:{name}")
        except (OSError, zipfile.BadZipFile, RuntimeError) as exc:
            problems.append(f"artifacts.zip:invalid:{type(exc).__name__}:{exc}")
    return problems


def _wait_for_terminal_artifacts(
    group_id: str,
    directory: Path,
    *,
    timeout_seconds: float = BUNDLE_WAIT_SECONDS,
    poll_seconds: float = BUNDLE_POLL_SECONDS,
) -> dict:
    """Wait for group-level evidence after terminal execution state is persisted."""
    current = read_group(group_id)
    if str(current.get("status") or "") not in TERMINAL_GROUP_STATUSES:
        return current
    deadline = time.monotonic() + max(0.0, float(timeout_seconds))
    while True:
        if not _artifact_problems(directory):
            return current
        if str(current.get("bundle_status") or "") == "failed":
            return current
        if time.monotonic() >= deadline:
            return current
        time.sleep(max(0.01, float(poll_seconds)))
        current = read_group(group_id)


def selections(overrides: dict[str, str]) -> list[V5PluginSelection]:
    return [V5PluginSelection(category=category, plugin_id=overrides.get(category, next(item.plugin_id for item in STORE.list() if item.category == category))) for category in CATEGORIES]


def main() -> int:
    parser=argparse.ArgumentParser(); parser.add_argument("--verify-group", default=""); args=parser.parse_args()
    if args.verify_group:
        group_id=args.verify_group
        directory=group_dir(group_id)
        current=_wait_for_terminal_artifacts(group_id,directory)
        items=children(group_id)
        return _validate(group_id,current,items,directory)
    methods = [V5FormalMethod(method_id="metatrack", display_name="MetaTrack", plugin_overrides={"routing": "metatrack_coaccess_routing", "execution": "dual_track_execution", "scheduler": "fast_first_scheduler", "commit": "commutative_hot_update_aggregation"}), V5FormalMethod(method_id="hash", display_name="Hash", plugin_overrides={"routing": "hash_routing_baseline", "execution": "serial_execution_baseline", "scheduler": "fifo_serial_scheduler", "commit": "normal_commit"})]
    plan = V5FormalExperimentPlan(name="v5_2_run_group_acceptance", base_spec=V5ExperimentSpec(name="v5_2_run_group", execution_backend="real_cluster", plugin_selections=selections({}), topology=V5Topology(nodes=8, shards=2, validators_per_shard=4), tx_count=1000, seed=61, duration_ms=9000), suites=["comparison_experiment"], methods=methods, seeds=[61, 62], repeats=2)
    matrix = expand(plan, "real_cluster")
    group = create_group({"execution_backend":"real_cluster", "runtime_truth":"v5_real_cluster_candidate", "plan":plan.model_dump(), "matrix":matrix, "total_child_runs":len(matrix), "completed_child_runs":0, "cancel_requested":False, "max_concurrent_real_clusters":1})
    start(group["run_group_id"])
    deadline=time.monotonic()+600
    while time.monotonic()<deadline:
        current=read_group(group["run_group_id"])
        if current.get("status") in TERMINAL_GROUP_STATUSES: break
        time.sleep(1)
    directory=group_dir(group["run_group_id"])
    current=_wait_for_terminal_artifacts(group["run_group_id"],directory)
    items=children(group["run_group_id"])
    return _validate(group["run_group_id"],current,items,directory)


def _validate(group_id: str, current: dict, items: list[dict], directory: Path) -> int:
    blockers=[]
    if current.get("status")!="completed": blockers.append(f"group status {current.get('status')}")
    expected_child_count = current.get("total_child_runs")
    if not isinstance(expected_child_count, int) or expected_child_count <= 0:
        matrix = current.get("matrix")
        if isinstance(matrix, list) and matrix:
            expected_child_count = len(matrix)
        else:
            expected_child_count = len(items)

    if expected_child_count <= 0:
        blockers.append("group has no children")
    elif len(items) != expected_child_count or not all(
        item.get("status") == "completed" for item in items
    ):
        blockers.append(
            f"children incomplete: expected={expected_child_count} "
            f"actual={len(items)}"
        )
    artifact_problems = _artifact_problems(directory)
    if artifact_problems:
        bundle_error = str(current.get("bundle_error") or "")
        detail = ", ".join(artifact_problems)
        if bundle_error:
            detail += f"; bundle_error={bundle_error}"
        blockers.append(f"artifacts incomplete: {detail}")
    elif str(current.get("bundle_status") or "") == "failed":
        blockers.append("bundle failed" + (f": {current.get('bundle_error')}" if current.get("bundle_error") else ""))
    for item in items:
        summary = item.get("result", {}).get("summary", {})
        finality = summary.get("finality_evidence", {})
        expected_tx_count = item.get("estimated_transactions")
        if finality.get("submitted_unique_tx_count") != expected_tx_count or finality.get("terminal_unique_tx_count") != finality.get("submitted_unique_tx_count") or finality.get("incomplete_unique_tx_count") != 0:
            blockers.append(f"child {item.get('child_run_id')} finality incomplete: terminal={finality.get('terminal_unique_tx_count')} incomplete={finality.get('incomplete_unique_tx_count')}")
        if summary.get("orphan_process_count") != 0 or summary.get("no_fallback") is not True:
            blockers.append(f"child {item.get('child_run_id')} runtime cleanup/no fallback evidence failed")
    report={"acceptance_passed":not blockers,"run_group_id":group_id,"child_count":len(items),"blockers":blockers}
    (directory/"v5_2_run_group_acceptance.json").write_text(json.dumps(report,indent=2)+"\n",encoding="utf-8")
    print(json.dumps(report)); return 0 if not blockers else 1


if __name__=="__main__": raise SystemExit(main())
