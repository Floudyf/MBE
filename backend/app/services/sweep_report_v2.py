from __future__ import annotations

from pathlib import Path
from typing import Any


def _cell(value: Any) -> str:
    if value is None or value == "":
        return ""
    return str(value)


def _summary_rows(rows: list[dict[str, Any]], limit: int = 12) -> list[str]:
    header = "| case_id | case_type | protocol_name | status | avg_e2e_latency_ms | p99_e2e_latency_ms | chain_speed_imbalance |"
    divider = "|---|---|---|---|---:|---:|---:|"
    lines = [header, divider]
    for row in rows[:limit]:
        lines.append(
            "| "
            + " | ".join(
                [
                    _cell(row.get("case_id")),
                    _cell(row.get("case_type")),
                    _cell(row.get("protocol_name")),
                    _cell(row.get("status")),
                    _cell(row.get("avg_e2e_latency_ms")),
                    _cell(row.get("p99_e2e_latency_ms")),
                    _cell(row.get("chain_speed_imbalance")),
                ]
            )
            + " |"
        )
    if len(rows) > limit:
        lines.append(f"| ... | ... | ... | {len(rows) - limit} more cases omitted from table |  |  |  |")
    return lines


def _observations(result: dict[str, Any]) -> list[str]:
    rows = result.get("rows", [])
    sweep_id = str(result.get("sweep_id", ""))
    observations = [
        "All rows are local virtual-time or local protocol baseline replay results, not real chain execution.",
    ]
    target_intervals = sorted({row.get("target_block_interval_ms") for row in rows if row.get("target_block_interval_ms") not in {"", None}})
    if len(target_intervals) > 1:
        observations.append("The sweep varies target block interval, so chain_speed_imbalance and wait-time fields can be compared across slower target-chain profiles.")
    if sweep_id == "v2_window_size_sweep":
        observations.append("The fixed_window_baseline cases vary window_size; pending and latency fields should be read as local baseline-model signals only.")
    if sweep_id == "v2_committee_delay_sweep":
        observations.append("The committee_bridge_basic cases vary committee_delay_ms; this is a local delay parameter, not real committee signature latency.")
    if "protocol" in sweep_id or any(row.get("case_type") == "protocol_baseline" for row in rows):
        observations.append("Protocol rows compare baseline models and must not be interpreted as production bridge security claims.")
    return observations[:5]


def generate_markdown_report(sweep_result: dict[str, Any]) -> str:
    rows = list(sweep_result.get("rows", []))
    config = sweep_result.get("config", {})
    sweep = config.get("sweep", {})
    parameters = config.get("parameters", {})
    protocols = config.get("protocols", [])
    artifacts = sweep_result.get("artifacts", [])
    lines = [
        "# V2 Sweep Report",
        "",
        "## Scope",
        "",
        "This report summarizes a V2.8 local sweep over V2 dual-chain replay and cross-chain protocol baseline replay.",
        "It is local virtual-time replay only. It is not real chain execution, not Fabric execution, not public-chain replay, and not a production bridge.",
        "",
        "## Data Truth",
        "",
        f"- data_truth_label: {sweep.get('data_truth_label', 'synthetic_replay')}",
        f"- backend_type: {sweep.get('backend_type', 'local_virtual')}",
        f"- protocol_truth: {sweep.get('protocol_truth', 'local_baseline_model')}",
        "- Input data is the V2.4 synthetic schema sample unless the sweep config states otherwise.",
        "",
        "## Sweep Config",
        "",
        f"- sweep_id: {sweep_result.get('sweep_id', sweep.get('id', ''))}",
        f"- name: {sweep.get('name', '')}",
        f"- case_count: {len(rows)}",
        f"- parameters: `{parameters}`",
        f"- protocols: `{protocols}`",
        "",
        "## Summary Table",
        "",
        *_summary_rows(rows),
        "",
        "## Key Observations",
        "",
    ]
    lines.extend(f"- {item}" for item in _observations(sweep_result))
    lines.extend(
        [
            "",
            "## Artifacts",
            "",
        ]
    )
    if artifacts:
        lines.extend(f"- {artifact}" for artifact in artifacts)
    else:
        lines.extend(["- sweep_summary.csv", "- sweep_summary.json", "- runtime.log"])
    lines.extend(
        [
            "",
            "## Non-goals",
            "",
            "- This is not a production cross-chain bridge.",
            "- This is not MetaFlow.",
            "- This does not implement real committee signatures, MintCert, RefundCert, FinalityProof, or Pending Pool.",
            "- This does not start Docker, Fabric, network.sh, or public-chain live nodes.",
            "- V2.9 may later add chain-backed calibration; V3 is reserved for live/multi-server backend work.",
        ]
    )
    return "\n".join(lines) + "\n"


def write_report(path: Path, sweep_result: dict[str, Any]) -> None:
    path.write_text(generate_markdown_report(sweep_result), encoding="utf-8")
