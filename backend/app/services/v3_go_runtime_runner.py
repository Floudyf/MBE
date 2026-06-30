from __future__ import annotations

import csv
import json
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from backend.app.services.run_id import new_run_id
from backend.app.services.v3_profile_loader import V3_CONFIG_ROOT, load_profile_store
from backend.app.services.v3_profile_validator import validate_experiment_profile

ROOT = Path(__file__).resolve().parents[3]
EXECUTOR_ROOT = ROOT / "executor"
CHAIN_PROFILE = V3_CONFIG_ROOT / "chains" / "chain_x_default.yaml"
ROLE_SEPARATED_CHAIN_PROFILE = V3_CONFIG_ROOT / "chains" / "single_chain_research_default.yaml"
MINIMAL_PLUGIN_PROFILE = V3_CONFIG_ROOT / "plugins" / "v3_2_minimal_plugin_profile.yaml"
METATRACK_PLUGIN_PROFILE = V3_CONFIG_ROOT / "plugins" / "metatrack_plugin_profiles.yaml"
SMOKE_PROFILE = V3_CONFIG_ROOT / "experiments" / "single_chain_runtime_smoke.yaml"
ROLE_SEPARATION_SMOKE_PROFILE = V3_CONFIG_ROOT / "experiments" / "single_chain_role_separation_smoke.yaml"
METATRACK_PROFILE = V3_CONFIG_ROOT / "experiments" / "metatrack_go_backed_ablation_smoke.yaml"

MECHANISM_FIELDS = [
    "plugin_combination",
    "throughput_tps",
    "avg_latency_ms",
    "p95_latency_ms",
    "p99_latency_ms",
    "remote_fetch_count",
    "cross_shard_ratio",
    "fast_track_count",
    "conservative_track_count",
    "aggregated_update_count",
    "aggregation_ratio",
    "conflict_count",
    "queue_wait_ms",
    "txpool_admitted_count",
    "txpool_rejected_count",
    "txpool_peak_size",
    "txpool_avg_wait_ms",
    "txpool_p95_wait_ms",
    "empty_block_count",
    "avg_block_size",
    "max_block_size",
    "block_interval_ms",
    "avg_block_interval_ms",
    "blockproducer_count_cut_count",
    "blockproducer_time_cut_count",
    "blockproducer_drain_cut_count",
    "blockproducer_empty_cut_count",
    "block_commit_latency_ms",
    "consensus_latency_ms",
    "avg_consensus_latency_ms",
    "p95_consensus_latency_ms",
    "consensus_message_count",
    "avg_consensus_message_count",
    "consensus_round_count",
    "view_change_count",
    "finalized_block_count",
    "failed_block_count",
    "routing_plugin",
    "routing_decision_count",
    "cross_shard_tx_count",
    "local_tx_count",
    "remote_state_access_count",
    "avg_touched_shards",
    "max_touched_shards",
    "hotspot_key_count",
    "coaccess_group_count",
    "avg_routing_overhead_ms",
    "execution_plugin",
    "execution_tx_count",
    "blocked_tx_count",
    "dependency_edge_count",
    "avg_dependency_edges_per_tx",
    "avg_execution_latency_ms",
    "p95_execution_latency_ms",
    "max_execution_latency_ms",
    "logical_worker_count",
    "parallelizable_tx_count",
    "serial_tx_count",
    "state_access_plugin",
    "state_access_count",
    "local_state_access_count",
    "remote_state_access_ratio",
    "cache_hit_count",
    "cache_miss_count",
    "cache_hit_rate",
    "prefetch_hit_count",
    "prefetch_miss_count",
    "prefetch_hit_rate",
    "avg_state_access_latency_ms",
    "p95_state_access_latency_ms",
    "max_state_access_latency_ms",
    "remote_state_access_latency_ms",
    "witness_estimated_count",
    "proof_estimated_count",
    "estimated_witness_bytes",
    "estimated_proof_bytes",
    "execution_shard_count",
    "state_storage_unit_count",
    "cross_state_unit_access_count",
    "remote_state_fetch_count",
    "state_locality_ratio",
    "execution_shard_load_balance",
    "state_unit_load_balance",
]


@dataclass(frozen=True)
class GoRuntimeRun:
    output_dir: Path
    summary: dict[str, Any]
    stdout: str
    stderr: str


def run_go_v3_runtime(
    *,
    experiment_profile_path: Path = SMOKE_PROFILE,
    plugin_profile_path: Path = MINIMAL_PLUGIN_PROFILE,
    plugin_profile_id: str = "v3_2_minimal_single_chain",
    chain_profile_path: Path = CHAIN_PROFILE,
    output_dir: Path,
) -> GoRuntimeRun:
    output_dir.mkdir(parents=True, exist_ok=True)
    command = [
        "go",
        "run",
        "./cmd/replay",
        "-mode",
        "v3-runtime",
        "-chain-profile",
        str(chain_profile_path),
        "-plugin-profile",
        str(plugin_profile_path),
        "-plugin-profile-id",
        plugin_profile_id,
        "-experiment-profile",
        str(experiment_profile_path),
        "-output",
        str(output_dir),
    ]
    completed = subprocess.run(command, cwd=EXECUTOR_ROOT, text=True, capture_output=True, check=False)
    if completed.returncode != 0:
        raise RuntimeError(f"Go V3 runtime failed: {completed.stderr or completed.stdout}")
    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    return GoRuntimeRun(output_dir=output_dir, summary=summary, stdout=completed.stdout, stderr=completed.stderr)


def run_metatrack_go_backed_ablation(output_root: Path | None = None, run_id: str | None = None) -> dict[str, Any]:
    store = load_profile_store()
    profile = store.experiments["metatrack_go_backed_ablation_smoke"]
    validation = validate_experiment_profile(profile, store)
    if not validation["valid"] or not validation["runnable"]:
        raise ValueError("metatrack_go_backed_ablation_smoke is not runnable")
    run_id = run_id or new_run_id().replace("v2run", "v3mt", 1)
    root = (output_root or Path(".cache/v3_metatrack_runs")) / run_id
    root.mkdir(parents=True, exist_ok=True)
    combinations = ["baseline_hash_only", "co_access_only", "co_access_dual_track", "full_MetaTrack"]
    runs: list[GoRuntimeRun] = []
    for combination in combinations:
        runs.append(
            run_go_v3_runtime(
                experiment_profile_path=METATRACK_PROFILE,
                plugin_profile_path=METATRACK_PLUGIN_PROFILE,
                plugin_profile_id=combination,
                chain_profile_path=ROLE_SEPARATED_CHAIN_PROFILE,
                output_dir=root / combination,
            )
        )
    _write_metatrack_artifacts(root, runs)
    return {"run_id": run_id, "output_dir": root, "runs": runs}


def _write_metatrack_artifacts(root: Path, runs: list[GoRuntimeRun]) -> None:
    summary_rows = []
    mechanism_rows = []
    latency_rows = []
    for run in runs:
        summary = run.summary
        combo = summary["plugin_profile_id"]
        summary_rows.append(summary)
        mechanism_rows.append({field: summary.get(field, "") for field in MECHANISM_FIELDS} | {"plugin_combination": combo})
        latency_file = run.output_dir / "tx_results.csv"
        with latency_file.open(newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                latency_rows.append(
                    {
                        "plugin_combination": combo,
                        "tx_id": row["tx_id"],
                        "latency_ms": row["latency_ms"],
                        "status": row["status"],
                    }
                )
    _write_csv(root / "metatrack_summary.csv", list(summary_rows[0]), summary_rows)
    (root / "metatrack_summary.json").write_text(json.dumps(summary_rows, indent=2, sort_keys=True), encoding="utf-8")
    _write_csv(root / "metatrack_mechanism_metrics.csv", MECHANISM_FIELDS, mechanism_rows)
    _write_csv(root / "metatrack_latency.csv", ["plugin_combination", "tx_id", "latency_ms", "status"], latency_rows)
    (root / "metatrack_ablation_report.md").write_text(
        "\n".join(
            [
                "# V3.3 Go-backed MetaTrack Evaluation Smoke Report",
                "",
                "This is V3.3 Go-backed MetaTrack plugin evaluation smoke/controlled run.",
                "It uses identical workload, seed, ChainProfile, block config, and consensus config across combinations.",
                "It uses fixed state placement plus variable execution-side routing and execution/access/commit plugins.",
                "Co-access routing changes execution-side routing M_t; it does not migrate persistent state placement phi(key).",
                "It is not Fabric live execution.",
                "It is not a final paper-scale result unless a later paper-scale workload is run.",
                "Fabric-backed validation is deferred to V3.5 after V3.4 runtime hardening.",
                "",
            ]
        ),
        encoding="utf-8",
    )
    for forbidden in ("fabric_validation_summary.csv", "fabric_tx_results.csv", "fabric_commit_latency.csv", "fabric_block_log.csv", "metaflow_events.csv", "control_decisions.csv"):
        target = root / forbidden
        if target.exists():
            target.unlink()
    representative = next((run for run in runs if run.summary.get("plugin_profile_id") == "full_MetaTrack"), runs[0])
    for filename in ("block_log.csv", "tx_results.csv", "state_commit_log.csv", "txpool_log.csv", "consensus_log.csv", "routing_log.csv", "execution_log.csv", "state_access_log.csv", "summary.csv", "summary.json", "runtime.log", "report.md", "used_chain_profile.yaml"):
        source = representative.output_dir / filename
        if source.is_file():
            shutil.copyfile(source, root / filename)
    shutil.copyfile(METATRACK_PLUGIN_PROFILE, root / "used_plugin_profile.yaml")
    shutil.copyfile(METATRACK_PROFILE, root / "used_experiment_profile.yaml")


def _write_csv(path: Path, fields: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})
