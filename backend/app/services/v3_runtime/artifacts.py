from __future__ import annotations

import csv
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

import yaml

from backend.app.services.v3_runtime.models import RuntimeResult, RuntimeSummary, StateCommit, TxResult


SUMMARY_FIELDS = [
    "run_id",
    "stage",
    "backend_type",
    "truth_label",
    "chain_profile_id",
    "plugin_profile_id",
    "experiment_profile_id",
    "tx_count",
    "success_count",
    "failure_count",
    "block_count",
    "throughput_tps",
    "avg_latency_ms",
    "p95_latency_ms",
    "p99_latency_ms",
    "runtime_mode",
]


def write_runtime_artifacts(
    output_dir: Path,
    chain_profile: dict[str, Any],
    plugin_profile: dict[str, Any],
    experiment_profile: dict[str, Any],
    block_log: list[dict[str, Any]],
    tx_results: list[TxResult],
    state_commit_log: list[StateCommit],
    summary: RuntimeSummary,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    artifacts = {
        "used_chain_profile.yaml": output_dir / "used_chain_profile.yaml",
        "used_plugin_profile.yaml": output_dir / "used_plugin_profile.yaml",
        "used_experiment_profile.yaml": output_dir / "used_experiment_profile.yaml",
        "runtime.log": output_dir / "runtime.log",
        "summary.csv": output_dir / "summary.csv",
        "summary.json": output_dir / "summary.json",
        "report.md": output_dir / "report.md",
        "block_log.csv": output_dir / "block_log.csv",
        "tx_results.csv": output_dir / "tx_results.csv",
        "state_commit_log.csv": output_dir / "state_commit_log.csv",
    }
    artifacts["used_chain_profile.yaml"].write_text(yaml.safe_dump(chain_profile, sort_keys=False), encoding="utf-8")
    artifacts["used_plugin_profile.yaml"].write_text(yaml.safe_dump(plugin_profile, sort_keys=False), encoding="utf-8")
    artifacts["used_experiment_profile.yaml"].write_text(yaml.safe_dump(experiment_profile, sort_keys=False), encoding="utf-8")
    artifacts["runtime.log"].write_text(
        "\n".join(
            [
                "V3.2 minimal single-chain modular runtime smoke run",
                "runtime_mode=python_reference_single_process_logical_nodes",
                "truth_label=modular_runtime",
                "fabric_live=false",
                "metatrack_full_evaluation=false",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    _write_csv(artifacts["summary.csv"], SUMMARY_FIELDS, [asdict(summary)])
    artifacts["summary.json"].write_text(json.dumps(asdict(summary), indent=2, sort_keys=True), encoding="utf-8")
    _write_csv(
        artifacts["block_log.csv"],
        ["block_height", "block_id", "proposer_node", "tx_count", "cut_time_ms", "ordered_time_ms", "finalized_time_ms", "consensus_plugin", "status"],
        block_log,
    )
    _write_csv(
        artifacts["tx_results.csv"],
        [
            "tx_id",
            "submit_time_ms",
            "admit_time_ms",
            "block_height",
            "execution_start_ms",
            "execution_end_ms",
            "commit_time_ms",
            "latency_ms",
            "status",
            "shard_id",
            "read_count",
            "write_count",
            "remote_fetch_count",
        ],
        [_tx_result_row(tx) for tx in tx_results],
    )
    _write_csv(
        artifacts["state_commit_log.csv"],
        ["block_height", "tx_id", "state_key", "old_value", "delta", "new_value", "commit_plugin", "commit_time_ms", "status"],
        [asdict(item) for item in state_commit_log],
    )
    artifacts["report.md"].write_text(_report(summary), encoding="utf-8")
    return artifacts


def _write_csv(path: Path, fields: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def _tx_result_row(tx: TxResult) -> dict[str, Any]:
    row = asdict(tx)
    row.pop("deltas", None)
    return row


def _report(summary: RuntimeSummary) -> str:
    return "\n".join(
        [
            "# V3.2 Minimal Single-chain Runtime Smoke Report",
            "",
            "This is a V3.2 minimal single-chain modular runtime smoke run.",
            "",
            f"- truth_label = {summary.truth_label}",
            "- This is not Fabric live execution.",
            "- This is not MetaTrack full evaluation.",
            "- This is not final paper-scale performance evidence.",
            "",
            f"tx_count: {summary.tx_count}",
            f"block_count: {summary.block_count}",
            f"throughput_tps: {summary.throughput_tps}",
            f"avg_latency_ms: {summary.avg_latency_ms}",
            f"p95_latency_ms: {summary.p95_latency_ms}",
            f"p99_latency_ms: {summary.p99_latency_ms}",
            "",
        ]
    )
