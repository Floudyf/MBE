"""Run the small, local V1.8 baseline sweep without Fabric or Docker."""
from __future__ import annotations

import argparse
import csv
import json
import subprocess
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]

FIELDS = [
    "tx_count",
    "success_count",
    "failed_count",
    "throughput_tps",
    "avg_latency_ms",
    "p95_latency_ms",
    "p99_latency_ms",
    "virtual_time_ms",
    "routing_policy",
    "routing_cross_shard_tx_count",
    "routing_cross_shard_tx_ratio",
    "routing_remote_key_count",
    "co_access_group_count",
    "routing_time_ms",
    "dual_track_enabled",
    "fast_track_tx_count",
    "conservative_track_tx_count",
    "fast_track_tx_ratio",
    "conservative_track_tx_ratio",
    "fast_track_executed_count",
    "conservative_track_executed_count",
    "blocked_or_deferred_tx_count",
    "scheduler_idle_count",
    "hot_update_aggregation_enabled",
    "aggregation_policy",
    "aggregation_candidate_tx_count",
    "aggregated_tx_count",
    "aggregated_commit_count",
    "conservative_commit_count",
    "aggregation_saved_commit_count",
    "aggregation_group_count",
    "aggregation_hot_key_count",
    "aggregation_constraint_failure_count",
    "aggregation_missing_delta_count",
    "aggregation_non_commutative_count",
    "wall_clock_runtime_ms",
]

REPORT_FIELDS = [
    ("baseline", "name"),
    ("tx", "tx_count"),
    ("routing", "routing_policy"),
    ("dual_track_enabled", "dual_track_enabled"),
    ("fast_track_tx_count", "fast_track_tx_count"),
    ("conservative_track_tx_count", "conservative_track_tx_count"),
    ("hot_update_aggregation_enabled", "hot_update_aggregation_enabled"),
    ("aggregated_commit_count", "aggregated_commit_count"),
    ("aggregation_saved_commit_count", "aggregation_saved_commit_count"),
]


def config_for(item: dict, shards: int) -> dict:
    return {
        "state_sharding": {"shard_count": shards},
        "execution_sharding": {"shard_count": shards},
        "routing": {
            "policy": item["routing_policy"],
            "co_access_min_weight": 1,
            "co_access_max_group_size": 64,
            "co_access_balance_weight": 1,
        },
        "execution": {
            "dual_track_enabled": item["dual_track_enabled"],
            "fast_track_max_access_size": 2,
            "conservative_on_conflict_hint": True,
            "conservative_on_missing_access_set": True,
            "scheduler_policy": "fast_first",
        },
        "commit": {
            "hot_update_aggregation_enabled": item["hot_update_aggregation_enabled"],
            "aggregation_min_hot_count": 2,
            "aggregation_max_group_size": 64,
            "aggregation_require_fast_track": True,
            "conservative_on_constraint_failure": True,
            "aggregation_policy": "by_primary_key",
        },
    }


def cell(row: dict, key: str) -> str:
    value = row.get(key)
    if value is None:
        return ""
    return str(value)


def report(rows: list[dict]) -> str:
    headers = [label for label, _ in REPORT_FIELDS]
    align = ["---", "---:", "---", "---", "---:", "---:", "---", "---:", "---:"]
    lines = [
        "# V1.8 baseline sweep",
        "",
        "Replay/virtual-time comparison only; not production Fabric, cross-chain, MetaFlow, or multi-server deployment.",
        "",
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(align) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(cell(row, key) for _, key in REPORT_FIELDS) + " |")
    return "\n".join(lines) + "\n"


def read_summary(path: Path) -> dict:
    with path.open(encoding="utf-8", newline="") as stream:
        return next(csv.DictReader(stream))


def run_replay(config: Path, trace: Path, output: Path) -> None:
    subprocess.run(
        [
            "go",
            "run",
            "./cmd/replay",
            "-config",
            str(config.resolve()),
            "-trace",
            str(trace.resolve()),
            "-output",
            str(output.resolve()),
        ],
        cwd=ROOT / "executor",
        check=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sweep", type=Path, default=ROOT / "configs/sweeps/v1_8_baselines.yaml")
    parser.add_argument("--out", type=Path, default=ROOT / ".cache/v1_8_sweeps/latest")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    spec = yaml.safe_load(args.sweep.read_text(encoding="utf-8"))
    out = args.out
    rows = []
    trace = ROOT / spec["trace"]

    for item in spec["baselines"]:
        run_dir = out / item["name"]
        config = run_dir / "config.yaml"
        run_dir.mkdir(parents=True, exist_ok=True)
        config.write_text(yaml.safe_dump(config_for(item, spec["execution_shards"]), sort_keys=False), encoding="utf-8")
        if args.dry_run:
            rows.append({"name": item["name"], "planned": True})
            continue
        run_replay(config, trace, run_dir)
        row = read_summary(run_dir / "summary.csv")
        row["name"] = item["name"]
        rows.append(row)

    out.mkdir(parents=True, exist_ok=True)
    (out / "sweep_summary.json").write_text(json.dumps(rows, indent=2) + "\n", encoding="utf-8")
    with (out / "sweep_summary.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=["name", *FIELDS], extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    (out / "report.md").write_text(report(rows), encoding="utf-8")
    print(f"wrote {out / 'report.md'}")


if __name__ == "__main__":
    main()
