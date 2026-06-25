from __future__ import annotations

import csv
import json
import shutil
from itertools import product
from pathlib import Path
from typing import Any

import yaml

from backend.app.services.artifact_manager import list_artifacts
from backend.app.services.dual_chain_replay import run_dual_chain_replay
from backend.app.services.job_manager import DEFAULT_JOBS_ROOT, JobManager
from backend.app.services.protocol_replay import run_protocol_replay
from backend.app.services.sweep_report_v2 import write_report

ROOT = Path(__file__).resolve().parents[3]
SWEEP_STAGE = "V2.8"
DEFAULT_SWEEP_CONFIG_DIR = ROOT / "configs/sweeps"
CASE_CONFIG_ROOT = ROOT / ".cache/v2_8_case_configs"

SWEEP_CONFIGS = {
    "v2_baseline_sweep": DEFAULT_SWEEP_CONFIG_DIR / "v2_baseline_sweep.yaml",
    "v2_chain_speed_imbalance_sweep": DEFAULT_SWEEP_CONFIG_DIR / "v2_chain_speed_imbalance_sweep.yaml",
    "v2_protocol_baseline_sweep": DEFAULT_SWEEP_CONFIG_DIR / "v2_protocol_baseline_sweep.yaml",
    "v2_window_size_sweep": DEFAULT_SWEEP_CONFIG_DIR / "v2_window_size_sweep.yaml",
    "v2_committee_delay_sweep": DEFAULT_SWEEP_CONFIG_DIR / "v2_committee_delay_sweep.yaml",
}

SWEEP_FIELDNAMES = [
    "sweep_id",
    "case_id",
    "case_type",
    "protocol_name",
    "status",
    "success",
    "data_truth_label",
    "backend_type",
    "protocol_truth",
    "source_chain_id",
    "target_chain_id",
    "source_block_interval_ms",
    "target_block_interval_ms",
    "source_finality_depth",
    "target_finality_depth",
    "window_size",
    "committee_delay_ms",
    "cross_tx_count",
    "stage_record_count",
    "success_count",
    "timeout_count",
    "refund_count",
    "failed_count",
    "avg_e2e_latency_ms",
    "p99_e2e_latency_ms",
    "avg_stage_latency_ms",
    "avg_source_wait_time_ms",
    "avg_target_wait_time_ms",
    "avg_finality_wait_time_ms",
    "max_pending_count",
    "avg_pending_count",
    "chain_speed_imbalance",
    "artifact_ref",
    "error_message",
]


class SweepError(ValueError):
    """Raised when a V2.8 sweep cannot be loaded or executed."""


def _resolve_workspace_path(path_text: str, root: Path = ROOT) -> Path:
    path = Path(path_text)
    if not path.is_absolute():
        path = root / path
    resolved = path.resolve()
    try:
        resolved.relative_to(root.resolve())
    except ValueError as exc:
        raise SweepError(f"path must stay inside workspace: {path_text}") from exc
    return resolved


def _as_list(value: Any, default: list[Any]) -> list[Any]:
    if value is None:
        return list(default)
    if isinstance(value, list):
        return value
    return [value]


def load_sweep_config(path: Path, root: Path = ROOT) -> dict[str, Any]:
    config_path = _resolve_workspace_path(str(path), root)
    document = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise SweepError("sweep config must be a mapping")
    if document.get("version") != "v2" or str(document.get("stage")).lower() != "v2.8":
        raise SweepError("sweep config must declare version: v2 and stage: v2.8")
    sweep = document.get("sweep")
    if not isinstance(sweep, dict):
        raise SweepError("sweep config must include a sweep mapping")
    if sweep.get("runnable") is not True:
        raise SweepError("planned or non-runnable sweep configs are not executable")
    if sweep.get("backend_type") not in {"local_virtual", "trace_replay"}:
        raise SweepError("V2.8 sweep can only run local_virtual or trace_replay backends")
    if sweep.get("data_truth_label") != "synthetic_replay":
        raise SweepError("default V2.8 sweeps must preserve synthetic_replay data truth label")
    if sweep.get("protocol_truth") != "local_baseline_model":
        raise SweepError("V2.8 protocol sweeps must declare local_baseline_model")
    runner = document.get("runner", {})
    if runner.get("sleep_enabled") is not False:
        raise SweepError("V2.8 sweeps must declare sleep_enabled: false")
    protocols = [str(item) for item in document.get("protocols", [])]
    if "metaflow" in protocols:
        raise SweepError("MetaFlow is planned and is not runnable in V2.8")
    input_config = document.get("input", {})
    for key in ["trace_file", "meta_file"]:
        target = _resolve_workspace_path(str(input_config.get(key, "")), root)
        if not target.is_file():
            raise SweepError(f"{key} does not exist: {input_config.get(key)}")
    return document


def summarize_sweep_config(config: dict[str, Any]) -> dict[str, Any]:
    sweep = config["sweep"]
    return {
        "id": sweep["id"],
        "name": sweep.get("name", sweep["id"]),
        "status": "runnable" if sweep.get("runnable") else "planned",
        "stage": SWEEP_STAGE,
        "data_truth_label": sweep.get("data_truth_label", "synthetic_replay"),
        "backend_type": sweep.get("backend_type", "local_virtual"),
        "protocol_truth": sweep.get("protocol_truth", "local_baseline_model"),
        "description": sweep.get("description", ""),
        "parameters": config.get("parameters", {}),
        "protocols": config.get("protocols", []),
        "limitations": config.get("limitations", []),
    }


def list_sweeps(root: Path = ROOT) -> list[dict[str, Any]]:
    return [summarize_sweep_config(load_sweep_config(path, root)) for path in SWEEP_CONFIGS.values()]


def get_sweep_config(sweep_id: str, root: Path = ROOT) -> dict[str, Any]:
    if sweep_id not in SWEEP_CONFIGS:
        raise SweepError(f"unknown V2.8 sweep_id: {sweep_id}")
    return load_sweep_config(SWEEP_CONFIGS[sweep_id], root)


def _base_case(config: dict[str, Any]) -> dict[str, Any]:
    sweep = config["sweep"]
    chains = config["chains"]
    params = config.get("parameters", {})
    return {
        "sweep_id": sweep["id"],
        "data_truth_label": sweep.get("data_truth_label", "synthetic_replay"),
        "backend_type": sweep.get("backend_type", "local_virtual"),
        "protocol_truth": sweep.get("protocol_truth", "local_baseline_model"),
        "source_chain_id": chains["source"]["chain_id"],
        "target_chain_id": chains["target"]["chain_id"],
        "source_backend": chains["source"].get("backend", "mock_chain"),
        "target_backend": chains["target"].get("backend", "mock_chain"),
        "source_block_interval_ms": int(_as_list(params.get("source_block_interval_ms"), [100])[0]),
        "target_block_interval_ms": int(_as_list(params.get("target_block_interval_ms"), [300])[0]),
        "source_finality_depth": int(_as_list(params.get("source_finality_depth"), [3])[0]),
        "target_finality_depth": int(_as_list(params.get("target_finality_depth"), [5])[0]),
        "window_size": "",
        "committee_delay_ms": "",
    }


def _assign_case_ids(cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    for index, case in enumerate(cases, start=1):
        case["case_id"] = f"case_{index:06d}"
    return cases


def expand_sweep_cases(config: dict[str, Any]) -> list[dict[str, Any]]:
    sweep_id = str(config["sweep"]["id"])
    params = config.get("parameters", {})
    protocols = [str(protocol) for protocol in config.get("protocols", [])]
    base = _base_case(config)
    cases: list[dict[str, Any]] = []

    if sweep_id == "v2_baseline_sweep":
        cases.append({**base, "case_type": "dual_chain_replay", "protocol_name": "dual_chain_sample"})
        cases.extend({**base, "case_type": "protocol_baseline", "protocol_name": protocol} for protocol in protocols)
    elif sweep_id in {"v2_protocol_baseline_sweep", "v2_window_size_sweep", "v2_committee_delay_sweep"}:
        windows = _as_list(params.get("window_size"), [""])
        delays = _as_list(params.get("committee_delay_ms"), [""])
        for protocol in protocols:
            protocol_windows = windows if protocol == "fixed_window_baseline" else [""]
            protocol_delays = delays if protocol == "committee_bridge_basic" else [""]
            for window_size, committee_delay in product(protocol_windows, protocol_delays):
                cases.append({
                    **base,
                    "case_type": "protocol_baseline",
                    "protocol_name": protocol,
                    "window_size": window_size,
                    "committee_delay_ms": committee_delay,
                })
    elif sweep_id == "v2_chain_speed_imbalance_sweep":
        dimensions = product(
            _as_list(params.get("source_block_interval_ms"), [100]),
            _as_list(params.get("target_block_interval_ms"), [300]),
            _as_list(params.get("source_finality_depth"), [3]),
            _as_list(params.get("target_finality_depth"), [5]),
            protocols,
        )
        for source_block, target_block, source_finality, target_finality, protocol in dimensions:
            cases.append({
                **base,
                "case_type": "protocol_baseline",
                "protocol_name": protocol,
                "source_block_interval_ms": int(source_block),
                "target_block_interval_ms": int(target_block),
                "source_finality_depth": int(source_finality),
                "target_finality_depth": int(target_finality),
            })
    else:
        raise SweepError(f"unsupported V2.8 sweep type: {sweep_id}")
    return _assign_case_ids(cases)


def _case_config_dir(output_dir: Path) -> Path:
    safe_name = output_dir.resolve().name.replace("/", "_").replace("\\", "_")
    path = CASE_CONFIG_ROOT / safe_name
    path.mkdir(parents=True, exist_ok=True)
    return path


def _write_case_config(path: Path, config: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")


def _dual_chain_config(config: dict[str, Any], case: dict[str, Any]) -> dict[str, Any]:
    trace = config["input"]
    return {
        "version": "v2",
        "stage": "V2.5",
        "experiment_name": f"{case['sweep_id']}_{case['case_id']}",
        "topology": "dual_chain",
        "status": "runnable",
        "runnable": True,
        "data_truth_label": case["data_truth_label"],
        "reason": "V2.8 local sweep case invoking V2.5 dual-chain replay.",
        "trace": {"path": trace["trace_file"], "meta_path": trace["meta_file"]},
        "chains": {
            case["source_chain_id"]: {
                "chain_id": case["source_chain_id"],
                "role": "source",
                "backend": case["source_backend"],
                "backend_type": case["backend_type"],
                "block_interval_ms": case["source_block_interval_ms"],
                "finality_depth": case["source_finality_depth"],
            },
            case["target_chain_id"]: {
                "chain_id": case["target_chain_id"],
                "role": "target",
                "backend": case["target_backend"],
                "backend_type": case["backend_type"],
                "block_interval_ms": case["target_block_interval_ms"],
                "finality_depth": case["target_finality_depth"],
            },
        },
        "notes": [
            "Generated by V2.8 sweep runner.",
            "Local virtual-time replay only; not real chain execution.",
        ],
    }


def _protocol_config(config: dict[str, Any], case: dict[str, Any]) -> dict[str, Any]:
    trace = config["input"]
    protocol: dict[str, Any] = {"name": case["protocol_name"], "enabled": True}
    if case.get("window_size") not in {"", None}:
        protocol["window_size"] = int(case["window_size"])
    if case.get("committee_delay_ms") not in {"", None}:
        protocol["committee_delay_ms"] = int(case["committee_delay_ms"])
    return {
        "version": "v2",
        "stage": "v2.6",
        "experiment": {
            "name": f"{case['sweep_id']}_{case['case_id']}",
            "runnable": True,
            "description": "V2.8 local sweep case invoking V2.6 protocol baseline replay.",
        },
        "trace": {
            "trace_file": trace["trace_file"],
            "meta_file": trace["meta_file"],
            "data_truth_label": case["data_truth_label"],
        },
        "chains": {
            case["source_chain_id"]: {
                "chain_id": case["source_chain_id"],
                "role": "source",
                "backend_type": case["backend_type"],
                "backend": case["source_backend"],
                "block_interval_ms": case["source_block_interval_ms"],
                "finality_depth": case["source_finality_depth"],
            },
            case["target_chain_id"]: {
                "chain_id": case["target_chain_id"],
                "role": "target",
                "backend_type": case["backend_type"],
                "backend": case["target_backend"],
                "block_interval_ms": case["target_block_interval_ms"],
                "finality_depth": case["target_finality_depth"],
            },
        },
        "protocols": [protocol],
        "replay": {
            "mode": "virtual_time",
            "sleep_enabled": False,
            "enforce_schema_validation": True,
            "backend_interface": "ChainBackend",
            "protocol_interface": "CrossChainProtocol",
        },
        "notes": [
            "Generated by V2.8 sweep runner.",
            "Local protocol baseline only; not a production bridge.",
        ],
    }


def _blank_row(case: dict[str, Any], status: str, success: bool, artifact_ref: str, error_message: str = "") -> dict[str, Any]:
    row = {key: "" for key in SWEEP_FIELDNAMES}
    row.update({
        "sweep_id": case["sweep_id"],
        "case_id": case["case_id"],
        "case_type": case["case_type"],
        "protocol_name": case["protocol_name"],
        "status": status,
        "success": success,
        "data_truth_label": case["data_truth_label"],
        "backend_type": case["backend_type"],
        "protocol_truth": case["protocol_truth"],
        "source_chain_id": case["source_chain_id"],
        "target_chain_id": case["target_chain_id"],
        "source_block_interval_ms": case["source_block_interval_ms"],
        "target_block_interval_ms": case["target_block_interval_ms"],
        "source_finality_depth": case["source_finality_depth"],
        "target_finality_depth": case["target_finality_depth"],
        "window_size": case.get("window_size", ""),
        "committee_delay_ms": case.get("committee_delay_ms", ""),
        "artifact_ref": artifact_ref,
        "error_message": error_message,
    })
    return row


def _row_from_dual(case: dict[str, Any], result: dict[str, Any], artifact_ref: str) -> dict[str, Any]:
    summary = result["summary"]
    cross_tx_count = max(1, int(summary.get("cross_tx_count", 1)))
    row = _blank_row(case, "completed", True, artifact_ref)
    row.update({
        "cross_tx_count": summary.get("cross_tx_count", ""),
        "stage_record_count": summary.get("stage_record_count", ""),
        "success_count": summary.get("completed_cross_tx_count", ""),
        "timeout_count": summary.get("timeout_cross_tx_count", ""),
        "refund_count": summary.get("refunded_cross_tx_count", ""),
        "failed_count": summary.get("failed_cross_tx_count", ""),
        "avg_e2e_latency_ms": summary.get("avg_e2e_latency_ms", ""),
        "p99_e2e_latency_ms": summary.get("p99_e2e_latency_ms", ""),
        "avg_stage_latency_ms": summary.get("avg_stage_latency_ms", ""),
        "avg_source_wait_time_ms": round(float(summary.get("source_wait_time_ms", 0)) / cross_tx_count, 6),
        "avg_target_wait_time_ms": round(float(summary.get("target_wait_time_ms", 0)) / cross_tx_count, 6),
        "avg_finality_wait_time_ms": round(float(summary.get("finality_wait_time_ms", 0)) / cross_tx_count, 6),
        "chain_speed_imbalance": summary.get("chain_speed_imbalance", ""),
    })
    return row


def _row_from_protocol(case: dict[str, Any], result: dict[str, Any], artifact_ref: str) -> dict[str, Any]:
    summary_items = result.get("summary", {}).get("items", [])
    summary = next((item for item in summary_items if item.get("protocol_name") == case["protocol_name"]), summary_items[0] if summary_items else {})
    row = _blank_row(case, "completed", True, artifact_ref)
    row.update({
        "cross_tx_count": summary.get("cross_tx_count", ""),
        "success_count": summary.get("success_count", ""),
        "timeout_count": summary.get("timeout_count", ""),
        "refund_count": summary.get("refund_count", ""),
        "failed_count": summary.get("failed_count", ""),
        "avg_e2e_latency_ms": summary.get("avg_e2e_latency_ms", ""),
        "p99_e2e_latency_ms": summary.get("p99_e2e_latency_ms", ""),
        "avg_source_wait_time_ms": summary.get("avg_source_wait_time_ms", ""),
        "avg_target_wait_time_ms": summary.get("avg_target_wait_time_ms", ""),
        "avg_finality_wait_time_ms": summary.get("avg_finality_wait_time_ms", ""),
        "max_pending_count": summary.get("max_pending_count", ""),
        "avg_pending_count": summary.get("avg_pending_count", ""),
        "chain_speed_imbalance": summary.get("chain_speed_imbalance", ""),
    })
    return row


def run_sweep_case(case: dict[str, Any], config: dict[str, Any], output_dir: Path, root: Path = ROOT) -> dict[str, Any]:
    case_dir = output_dir / "case_results" / case["case_id"]
    artifact_ref = f"case_results/{case['case_id']}"
    case_config_path = _case_config_dir(output_dir) / f"{case['case_id']}.yaml"
    if case["case_type"] == "dual_chain_replay":
        _write_case_config(case_config_path, _dual_chain_config(config, case))
        result = run_dual_chain_replay(case_config_path, case_dir, root)
        return _row_from_dual(case, result, artifact_ref)
    if case["case_type"] == "protocol_baseline":
        _write_case_config(case_config_path, _protocol_config(config, case))
        result = run_protocol_replay(case_config_path, case_dir, root)
        return _row_from_protocol(case, result, artifact_ref)
    raise SweepError(f"unknown sweep case_type: {case['case_type']}")


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=SWEEP_FIELDNAMES)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in SWEEP_FIELDNAMES})


def write_runtime_log(path: Path, result: dict[str, Any]) -> None:
    lines = [
        "stage=V2.8",
        f"sweep_id={result['sweep_id']}",
        f"case_count={len(result['rows'])}",
        "mode=local virtual-time sweep/report only",
        "data_truth_label=synthetic_replay",
        "backend_type=local_virtual",
        "protocol_truth=local_baseline_model",
        "no_time_sleep=true",
        "docker_fabric_network_sh_started=false",
        "public_chain_live_nodes_connected=false",
        "not production bridge",
        "not MetaFlow",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_sweep_artifacts(result: dict[str, Any], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    config = result["config"]
    (output_dir / "used_config.yaml").write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    (output_dir / "used_config.json").write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
    write_csv(output_dir / "sweep_summary.csv", result["rows"])
    (output_dir / "sweep_summary.json").write_text(json.dumps({"items": result["rows"], "summary": result["summary"]}, indent=2) + "\n", encoding="utf-8")
    (output_dir / "case_artifacts_index.json").write_text(json.dumps(result["case_artifacts"], indent=2) + "\n", encoding="utf-8")
    write_report(output_dir / "sweep_report.md", {**result, "artifacts": ["sweep_summary.csv", "sweep_summary.json", "sweep_report.md", "runtime.log", "case_artifacts_index.json"]})
    write_runtime_log(output_dir / "runtime.log", result)


def run_sweep(config_path: Path, output_dir: Path, root: Path = ROOT) -> dict[str, Any]:
    config = load_sweep_config(config_path, root)
    cases = expand_sweep_cases(config)
    rows: list[dict[str, Any]] = []
    case_artifacts: list[dict[str, Any]] = []
    output_dir.mkdir(parents=True, exist_ok=True)
    for case in cases:
        try:
            row = run_sweep_case(case, config, output_dir, root)
        except Exception as exc:
            row = _blank_row(case, "failed", False, f"case_results/{case['case_id']}", str(exc))
        rows.append(row)
        case_dir = output_dir / "case_results" / case["case_id"]
        case_artifacts.append({
            "case_id": case["case_id"],
            "artifact_ref": f"case_results/{case['case_id']}",
            "files": sorted(path.name for path in case_dir.iterdir()) if case_dir.is_dir() else [],
        })
    summary = {
        "sweep_id": config["sweep"]["id"],
        "case_count": len(rows),
        "completed_count": sum(1 for row in rows if row["status"] == "completed"),
        "failed_count": sum(1 for row in rows if row["status"] == "failed"),
        "data_truth_label": config["sweep"].get("data_truth_label", "synthetic_replay"),
        "backend_type": config["sweep"].get("backend_type", "local_virtual"),
        "protocol_truth": config["sweep"].get("protocol_truth", "local_baseline_model"),
    }
    result = {
        "stage": SWEEP_STAGE,
        "sweep_id": config["sweep"]["id"],
        "config": config,
        "cases": cases,
        "rows": rows,
        "summary": summary,
        "case_artifacts": case_artifacts,
    }
    write_sweep_artifacts(result, output_dir)
    metadata = {
        "stage": SWEEP_STAGE,
        "source": "v2_sweep_report",
        "experiment_name": config["sweep"]["id"],
        "status": "completed" if summary["failed_count"] == 0 else "failed",
        "status_message": "completed" if summary["failed_count"] == 0 else f"{summary['failed_count']} sweep cases failed",
        "output_dir": str(output_dir),
        "data_truth_label": summary["data_truth_label"],
        "backend_type": summary["backend_type"],
        "protocol_truth": summary["protocol_truth"],
        "summary": summary,
        "artifact_count": len(list_artifacts(output_dir, "manual")),
    }
    metadata_path = output_dir / "metadata.json"
    if metadata_path.is_file():
        existing = json.loads(metadata_path.read_text(encoding="utf-8"))
        existing.update(metadata)
        metadata = existing
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    return {**metadata, "summary": summary, "rows": rows, "artifacts": list_artifacts(output_dir, "manual")}


def run_sweep_job(config_path: Path, jobs_root: Path = DEFAULT_JOBS_ROOT, root: Path = ROOT) -> dict[str, Any]:
    config = load_sweep_config(config_path, root)
    manager = JobManager(jobs_root)
    run = manager.create_run(
        source="v2_sweep_report",
        experiment_name=str(config["sweep"]["id"]),
        data_truth_label=str(config["sweep"].get("data_truth_label", "synthetic_replay")),
        stage=SWEEP_STAGE,
        extra_metadata={
            "backend_type": config["sweep"].get("backend_type", "local_virtual"),
            "protocol_truth": config["sweep"].get("protocol_truth", "local_baseline_model"),
        },
    )
    run_id = run["run_id"]
    manager.mark_running(run_id)
    try:
        result = run_sweep(config_path, manager.run_dir(run_id), root)
    except Exception as exc:
        manager.mark_failed(run_id, str(exc))
        raise
    if result["status"] == "failed":
        manager.mark_failed(run_id, result["status_message"])
    else:
        manager.mark_completed(run_id, data_truth_label=result["data_truth_label"])
    completed = manager.update_run(run_id, summary=result["summary"], backend_type=result["backend_type"], protocol_truth=result["protocol_truth"])
    artifacts = list_artifacts(manager.run_dir(run_id), run_id)
    return {**completed, "summary": result["summary"], "rows": result["rows"], "artifacts": artifacts}
