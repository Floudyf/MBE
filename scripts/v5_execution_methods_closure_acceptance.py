from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import subprocess
import sys
import time
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


PERSISTENCE_METRIC_KEYS = (
    "checkpoint_read_ms",
    "wal_append_ms",
    "wal_sync_ms",
    "snapshot_write_ms",
    "receipt_batch_write_ms",
    "tx_index_batch_write_ms",
    "durable_commit_ms",
    "persistence_written_bytes",
    "wal_record_count",
    "snapshot_count",
)


def collect_node_persistence(node: Path) -> dict:
    path = node / "block_execution_summary.json"
    if not path.is_file():
        return {"node_id": node.name, "missing": "block_execution_summary.json"}
    data = read_json(path)
    blocks = data.get("blocks")
    if not isinstance(blocks, list):
        return {"node_id": node.name, "missing": "block_execution_summary.json.blocks"}
    totals: dict[str, object] = {"node_id": node.name, "block_count": len(blocks)}
    missing: list[str] = []
    for key in PERSISTENCE_METRIC_KEYS:
        total = 0.0
        found = False
        for index, block in enumerate(blocks):
            if not isinstance(block, dict) or key not in block:
                missing.append(f"blocks[{index}].{key}")
                continue
            value = block[key]
            if isinstance(value, (int, float)):
                total += float(value)
                found = True
            else:
                missing.append(f"blocks[{index}].{key}:non_numeric")
        if found:
            totals[key] = total
    if missing:
        totals["missing"] = missing
    return totals


def collect_metatrack_track_counts(output: Path) -> dict:
    fast: set[str] = set()
    conservative: set[str] = set()
    fast_instances = 0
    conservative_instances = 0
    for directory in node_dirs(output):
        for row in read_csv(directory / "execution_log.csv"):
            tx_id = row.get("tx_id") or row.get("logical_tx_id") or ""
            track = row.get("track") or ""
            if track == "fast":
                fast_instances += 1
                if tx_id:
                    fast.add(tx_id)
            elif track == "conservative":
                conservative_instances += 1
                if tx_id:
                    conservative.add(tx_id)
    return {
        "fast_logical_tx_count": len(fast),
        "conservative_logical_tx_count": len(conservative),
        "fast_validator_execution_count": fast_instances,
        "conservative_validator_execution_count": conservative_instances,
    }


def any_artifact(run_dir: Path, name: str) -> bool:
    if (run_dir / name).is_file() or (run_dir / "client" / name).is_file():
        return True
    return any((directory / name).is_file() for directory in node_dirs(run_dir))


def logical_path(path: Path) -> str:
    resolved = path.resolve()
    roots = [
        ("repo", ROOT.resolve()),
        ("formal_run_root", Path(os.environ["MBE_FORMAL_RUN_ROOT"]).resolve()) if os.environ.get("MBE_FORMAL_RUN_ROOT") else None,
        ("runtime_root", Path(os.environ["MBE_RUNTIME_ROOT"]).resolve()) if os.environ.get("MBE_RUNTIME_ROOT") else None,
        ("workload_cache_root", Path(os.environ["MBE_WORKLOAD_CACHE_ROOT"]).resolve()) if os.environ.get("MBE_WORKLOAD_CACHE_ROOT") else None,
    ]
    for item in roots:
        if item is None:
            continue
        label, root = item
        try:
            return f"{label}:{resolved.relative_to(root).as_posix()}"
        except ValueError:
            continue
    return resolved.name


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def git_output(*args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()


def working_tree_diff_sha256() -> str:
    diff = subprocess.check_output(["git", "diff", "--binary", "HEAD"], cwd=ROOT)
    return hashlib.sha256(diff).hexdigest()


def stable_json_sha256(value: object) -> str:
    return sha256_text(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str))


def build_supervisor(output: Path) -> tuple[Path, str]:
    bin_dir = output / "_bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    binary = bin_dir / ("mbe-supervisor.exe" if os.name == "nt" else "mbe-supervisor")
    process = subprocess.run(
        ["go", "build", "-o", str(binary), "./cmd/mbe-supervisor"],
        cwd=ROOT / "executor",
        text=True,
        capture_output=True,
        timeout=240,
    )
    if process.returncode:
        raise RuntimeError(f"build mbe-supervisor failed with {process.returncode}: {process.stderr[-4000:]}")
    return binary, sha256_file(binary)


def run_method(row: dict, plan: V5FormalExperimentPlan, output: Path, *, timeout_seconds: int, binary: Path, identity: dict, reuse_existing: bool = False) -> dict:
    if (output / "real_cluster_summary.json").is_file() and reuse_existing:
        return load_evidence(row["method_config_id"], output)
    if output.exists() and any(output.iterdir()):
        raise RuntimeError(f"{row['method_config_id']}: fresh acceptance requires an empty run directory: {logical_path(output)}")
    output.mkdir(parents=True, exist_ok=True)
    spec = _spec_for(plan, row)
    compiled = compile_plan(spec, output)
    plan_path = output / "compiled_run_plan.json"
    plan_path.write_text(compiled.model_dump_json(indent=2), encoding="utf-8")
    plugin_snapshot = {item.category: {"plugin_id": item.plugin_id, "config": item.config} for item in spec.plugin_selections}
    run_identity = dict(identity)
    run_identity.update(
        {
            "compiled_plan_digest": stable_json_sha256(compiled.model_dump(mode="json")),
            "plugin_snapshot_digest": stable_json_sha256(plugin_snapshot),
        }
    )
    (output / "run_identity.json").write_text(json.dumps(run_identity, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    process = subprocess.run(
        [str(binary), "--mode", "v5-real-cluster", "--plan", str(plan_path), "--data-dir", str(output)],
        cwd=ROOT / "executor",
        text=True,
        capture_output=True,
        timeout=timeout_seconds,
    )
    if process.returncode:
        (output / "supervisor_stdout.log").write_text(process.stdout, encoding="utf-8")
        (output / "supervisor_stderr.log").write_text(process.stderr, encoding="utf-8")
        raise RuntimeError(f"{row['method_config_id']}: supervisor failed with {process.returncode}: {process.stderr[-4000:]}")
    evidence = load_evidence(row["method_config_id"], output)
    workload_hash = evidence["workload_replay"].get("materialized_sha256") or evidence["workload_replay"].get("materialized_workload_sha256")
    run_identity["workload_hash"] = workload_hash
    (output / "run_identity.json").write_text(json.dumps(run_identity, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    evidence["identity"] = run_identity
    return evidence


def next_retry_output(output: Path) -> Path:
    for index in range(2, 100):
        candidate = output.with_name(f"{output.name}_retry_{index}")
        if not candidate.exists() or not any(candidate.iterdir()):
            return candidate
        if (candidate / "real_cluster_summary.json").is_file():
            return candidate
    raise RuntimeError(f"no available retry directory for {logical_path(output)}")


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
    run_identity = read_json(output / "run_identity.json") if (output / "run_identity.json").is_file() else {}
    persistence = [collect_node_persistence(directory) for directory in node_dirs(output)]
    track_counts = collect_metatrack_track_counts(output)
    for directory in node_dirs(output):
        block_stm = directory / "block_stm_summary.json"
        equivalent = directory / "serial_equivalence.json"
        if block_stm.is_file():
            block_stm_summaries.append(read_json(block_stm))
        if equivalent.is_file():
            serial_equivalence.append(read_json(equivalent))
    return {
        "method_id": method_id,
        "run_dir": logical_path(output),
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
        "identity": run_identity,
        "persistence_summaries": persistence,
        "metatrack_track_counts": track_counts,
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
    identity_values: dict[str, set[str]] = {"git_head": set(), "working_tree_diff_sha256": set(), "executable_sha256": set(), "workload_hash": set()}
    for method_id, result in results.items():
        summary = result["summary"]
        finality = result["finality"]
        drain = result["drain"]
        plugins = result["plugins"]
        expected_block_executor = "block_stm_block_executor" if method_id.endswith("block_stm") else ("metatrack_block_executor" if method_id.startswith("metatrack") else "serial_block_executor")
        expected_routing = "metatrack_coaccess_routing" if method_id.startswith("metatrack") else "hash_routing_baseline"
        for key in ("git_head", "working_tree_diff_sha256", "executable_sha256", "compiled_plan_digest", "plugin_snapshot_digest", "workload_hash"):
            value = result["identity"].get(key)
            if not value:
                blockers.append(f"{method_id}: run identity missing {key}")
            elif key in identity_values:
                identity_values[key].add(str(value))
        if plugins["block_executor"]["plugin_id"] != expected_block_executor:
            blockers.append(f"{method_id}: wrong block executor plugin")
        if plugins["routing"]["plugin_id"] != expected_routing:
            blockers.append(f"{method_id}: wrong routing plugin")
        if summary.get("block_executor_id") != expected_block_executor or not summary.get("block_executor_consistent"):
            blockers.append(f"{method_id}: block executor consistency evidence failed")
        if not (summary.get("no_fallback") and summary.get("orphan_process_count") == 0 and summary.get("state_root_consistent") and summary.get("receipt_root_consistent") and summary.get("plan_digest_consistent")):
            blockers.append(f"{method_id}: correctness/no-fallback/state-root/receipt-root/digest evidence failed")
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
            track_counts = result["metatrack_track_counts"]
            if track_counts.get("fast_logical_tx_count", 0) + track_counts.get("conservative_logical_tx_count", 0) != tx_count:
                blockers.append(f"{method_id}: fast + conservative logical track counts do not equal submitted tx count")
        if not result["persistence_summaries"]:
            blockers.append(f"{method_id}: persistence metrics missing")
        for index, persistence in enumerate(result["persistence_summaries"]):
            if persistence.get("missing"):
                blockers.append(f"{method_id}: node {index} persistence metrics missing {persistence['missing']}")
            if persistence.get("wal_record_count", 0) <= 0 or persistence.get("persistence_written_bytes", 0) <= 0:
                blockers.append(f"{method_id}: node {index} WAL/persistence metrics are not positive")
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
    for key, values in identity_values.items():
        if len(values) > 1:
            blockers.append(f"method runs do not share one {key}: {sorted(values)}")
    return blockers


def main() -> int:
    parser = argparse.ArgumentParser(description="Run V5 execution-methods closure acceptance for Hash/MetaTrack x Serial/Block-STM.")
    parser.add_argument("--output-root", default=str(ROOT / ".cache" / "v5_execution_methods_closure"))
    parser.add_argument("--tx-count", type=int, default=100)
    parser.add_argument("--workload-source", choices=["synthetic", "dataset-original", "dataset-derived"], default="synthetic")
    parser.add_argument("--timeout-seconds", type=int, default=240)
    parser.add_argument("--rerun-method", action="append", default=[])
    freshness = parser.add_mutually_exclusive_group()
    freshness.add_argument("--fresh-run-all", action="store_true", help="require empty method directories and run every method; default")
    freshness.add_argument("--reuse-existing", action="store_true", help="allow loading existing method summaries")
    args = parser.parse_args()
    if not args.reuse_existing:
        args.fresh_run_all = True
    if args.workload_source.startswith("dataset") and args.tx_count not in {10_000, 50_000, 100_000, 250_000}:
        existing = {item for item in os.environ.get("MBE_V5_LOCAL_SMOKE_COUNTS", "").split(",") if item}
        os.environ["MBE_V5_LOCAL_SMOKE_COUNTS"] = ",".join(sorted(existing | {str(args.tx_count)}))
    output = Path(args.output_root).resolve()
    if args.fresh_run_all and output.exists() and any(output.iterdir()):
        raise SystemExit(f"fresh acceptance requires a new empty output root: {logical_path(output)}; use --reuse-existing only for explicit audit")
    output.mkdir(parents=True, exist_ok=True)
    binary, executable_sha256 = build_supervisor(output)
    identity = {
        "git_head": git_output("rev-parse", "HEAD"),
        "working_tree_diff_sha256": working_tree_diff_sha256(),
        "executable_sha256": executable_sha256,
        "executable_path": logical_path(binary),
        "fresh_run_all": bool(args.fresh_run_all and not args.reuse_existing),
        "created_at_unix_ms": int(time.time() * 1000),
    }
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
    results = {row["method_config_id"]: run_method(row, checked.plan, output / row["method_config_id"], timeout_seconds=args.timeout_seconds, binary=binary, identity=identity, reuse_existing=args.reuse_existing) for row in rows}
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
                "receipt_root_consistent": value["summary"].get("receipt_root_consistent"),
                "plan_digest_consistent": value["summary"].get("plan_digest_consistent"),
                "no_fallback": value["summary"].get("no_fallback"),
                "orphan_process_count": value["summary"].get("orphan_process_count"),
                "fast_track_count": value["fast_track_count"],
                "aggregation_group_count": value["aggregation_group_count"],
                "remote_state_access_count": value["summary"].get("remote_state_access_count"),
                "block_stm_serial_equivalent": all(item.get("serial_equivalent") is True for item in value["block_stm_summaries"] + value["serial_equivalence"]) if key.endswith("block_stm") else None,
                "workload_truth_label": value["workload_replay"].get("truth_label"),
                "identity": value["identity"],
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
    print(json.dumps({"acceptance_passed": not blockers, "report": logical_path(output / "v5_execution_methods_closure_acceptance.json"), "blockers": blockers}, sort_keys=True))
    return 0 if not blockers else 1


if __name__ == "__main__":
    raise SystemExit(main())
