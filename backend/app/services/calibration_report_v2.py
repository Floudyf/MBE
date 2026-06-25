from __future__ import annotations

from pathlib import Path
from typing import Any


def generate_calibration_report(result: dict[str, Any]) -> str:
    summary = result.get("summary", {})
    config = result.get("config", {})
    calibration = config.get("calibration", {})
    input_config = config.get("input", {})
    warnings = result.get("warnings", [])
    lines = [
        "# V2.9 Realism Bridge Report",
        "",
        "## Scope",
        "",
        "This is chain-backed trace calibration for the V2 local replay platform. It is not a V3 live backend and not a web-controlled chain execution path.",
        "",
        "## Data Truth",
        "",
        f"- data_truth_label: {summary.get('data_truth_label', calibration.get('data_truth_label', ''))}",
        f"- backend_type: {summary.get('backend_type', calibration.get('backend_type', ''))}",
        f"- calibration_truth: {summary.get('calibration_truth', calibration.get('calibration_truth', ''))}",
        "",
        "## Input Trace",
        "",
        f"- source_type: {input_config.get('source_type', '')}",
        f"- trace_file: {input_config.get('trace_file', '')}",
        f"- meta_file: {input_config.get('meta_file', '')}",
        f"- observed_record_count: {summary.get('observed_record_count', '')}",
        "Fabric smoke trace, when used, is generated outside the web UI by CLI/WSL/Docker tooling. The web API only reads existing trace files.",
        "",
        "## Replay Model",
        "",
        "The comparison uses local virtual-time replay outputs or trace-derived expected timing fields. It does not start Docker, Fabric, network.sh, public-chain nodes, or archive-node clients.",
        "",
        "## Comparison Summary",
        "",
        f"- matched_record_count: {summary.get('matched_record_count', '')}",
        f"- unmatched_observed_count: {summary.get('unmatched_observed_count', '')}",
        f"- unmatched_replay_count: {summary.get('unmatched_replay_count', '')}",
        f"- avg_abs_commit_error_ms: {summary.get('avg_abs_commit_error_ms', '')}",
        f"- avg_abs_finality_error_ms: {summary.get('avg_abs_finality_error_ms', '')}",
        f"- avg_abs_latency_error_ms: {summary.get('avg_abs_latency_error_ms', '')}",
        f"- max_abs_latency_error_ms: {summary.get('max_abs_latency_error_ms', '')}",
        "",
        "## Calibration Suggestions",
        "",
        f"- suggested_block_interval_ms: {summary.get('suggested_block_interval_ms', '')}",
        f"- suggested_finality_depth: {summary.get('suggested_finality_depth', '')}",
        "Blank suggestions mean the observed trace did not provide enough reliable timing information.",
        "",
        "## Warnings",
        "",
    ]
    lines.extend(f"- {warning}" for warning in warnings) if warnings else lines.append("- None")
    lines.extend(
        [
            "",
            "## Artifacts",
            "",
            "- calibration_summary.csv",
            "- calibration_summary.json",
            "- replay_vs_observed.csv",
            "- runtime.log",
            "",
            "## Non-goals",
            "",
            "- This is not a production bridge.",
            "- This is not FabricLiveBackend or EVMLiveBackend.",
            "- This is not web control of Fabric.",
            "- This does not implement MetaFlow, Pending Pool, real committee signatures, MintCert, RefundCert, or FinalityProof.",
            "- V3 is reserved for multi-server, live backend, monitoring, and production-like deployment work.",
        ]
    )
    return "\n".join(lines) + "\n"


def write_calibration_report(path: Path, result: dict[str, Any]) -> None:
    path.write_text(generate_calibration_report(result), encoding="utf-8")
