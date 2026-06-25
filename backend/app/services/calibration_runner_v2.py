from __future__ import annotations

import csv
import json
from pathlib import Path
from statistics import mean
from typing import Any

import yaml

from backend.app.services.artifact_manager import list_artifacts
from backend.app.services.calibration_report_v2 import write_calibration_report
from backend.app.services.chain_backed_trace_adapter import FABRIC_SMOKE_COMMAND, adapt_trace_for_calibration, detect_fabric_smoke_trace, extract_observed_records
from backend.app.services.dual_chain_replay import run_dual_chain_replay
from backend.app.services.job_manager import DEFAULT_JOBS_ROOT, JobManager
from trace.validator.cross_chain_trace_validator import validate_trace_and_meta

ROOT = Path(__file__).resolve().parents[3]
CALIBRATION_STAGE = "V2.9"
DEFAULT_CALIBRATION_CONFIG_DIR = ROOT / "configs/calibration"
CASE_CONFIG_ROOT = ROOT / ".cache/v2_9_calibration_configs"

CALIBRATION_CONFIGS = {
    "v2_synthetic_calibration_sample": DEFAULT_CALIBRATION_CONFIG_DIR / "v2_synthetic_calibration_sample.yaml",
    "v2_fabric_smoke_calibration": DEFAULT_CALIBRATION_CONFIG_DIR / "v2_fabric_smoke_calibration.yaml",
}

SUMMARY_FIELDS = [
    "calibration_id",
    "status",
    "data_truth_label",
    "backend_type",
    "calibration_truth",
    "source_type",
    "observed_record_count",
    "replay_record_count",
    "matched_record_count",
    "unmatched_observed_count",
    "unmatched_replay_count",
    "avg_abs_commit_error_ms",
    "avg_abs_finality_error_ms",
    "avg_abs_latency_error_ms",
    "max_abs_latency_error_ms",
    "suggested_block_interval_ms",
    "suggested_finality_depth",
    "warnings",
]

COMPARISON_FIELDS = [
    "record_id",
    "tx_id",
    "cross_tx_id",
    "stage_id",
    "stage",
    "chain_id",
    "observed_commit_time_ms",
    "expected_commit_time_ms",
    "commit_error_ms",
    "observed_finality_time_ms",
    "expected_finality_time_ms",
    "finality_error_ms",
    "observed_latency_ms",
    "expected_latency_ms",
    "latency_error_ms",
    "matched",
    "warning",
]


class CalibrationError(ValueError):
    """Raised when V2.9 calibration cannot run."""


class CalibrationBlocked(CalibrationError):
    """Raised when calibration is valid but blocked by missing chain-backed input."""

    def __init__(self, message: str, payload: dict[str, Any]):
        super().__init__(message)
        self.payload = payload


def _resolve_workspace_path(path_text: str, root: Path = ROOT) -> Path:
    path = Path(path_text)
    if not path.is_absolute():
        path = root / path
    resolved = path.resolve()
    try:
        resolved.relative_to(root.resolve())
    except ValueError as exc:
        raise CalibrationError(f"path must stay inside workspace: {path_text}") from exc
    return resolved


def load_calibration_config(path: Path, root: Path = ROOT) -> dict[str, Any]:
    config_path = _resolve_workspace_path(str(path), root)
    document = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise CalibrationError("calibration config must be a mapping")
    if document.get("version") != "v2" or str(document.get("stage")).lower() != "v2.9":
        raise CalibrationError("calibration config must declare version: v2 and stage: v2.9")
    calibration = document.get("calibration")
    if not isinstance(calibration, dict):
        raise CalibrationError("calibration config must include calibration mapping")
    if calibration.get("runnable") is not True:
        raise CalibrationError("planned calibration configs are not executable")
    if calibration.get("backend_type") not in {"local_virtual", "trace_replay"}:
        raise CalibrationError("V2.9 calibration can only use local_virtual or trace_replay backend type")
    if calibration.get("id") == "metaflow":
        raise CalibrationError("MetaFlow is planned and not runnable in V2.9")
    replay = document.get("replay", {})
    if replay.get("sleep_enabled") is not False:
        raise CalibrationError("V2.9 calibration must declare sleep_enabled: false")
    input_config = document.get("input", {})
    if not input_config.get("trace_file") or not input_config.get("meta_file"):
        raise CalibrationError("calibration config must declare input trace_file and meta_file")
    return document


def summarize_calibration_config(config: dict[str, Any]) -> dict[str, Any]:
    calibration = config["calibration"]
    return {
        "id": calibration["id"],
        "name": calibration.get("name", calibration["id"]),
        "status": "runnable" if calibration.get("runnable") else "planned",
        "stage": CALIBRATION_STAGE,
        "data_truth_label": calibration.get("data_truth_label", ""),
        "backend_type": calibration.get("backend_type", ""),
        "calibration_truth": calibration.get("calibration_truth", ""),
        "description": calibration.get("description", ""),
        "source_type": config.get("input", {}).get("source_type", ""),
        "limitations": config.get("limitations", []),
    }


def list_calibration_configs(root: Path = ROOT) -> list[dict[str, Any]]:
    return [summarize_calibration_config(load_calibration_config(path, root)) for path in CALIBRATION_CONFIGS.values()]


def get_calibration_config(config_id: str, root: Path = ROOT) -> dict[str, Any]:
    if config_id not in CALIBRATION_CONFIGS:
        raise CalibrationError(f"unknown V2.9 calibration config_id: {config_id}")
    return load_calibration_config(CALIBRATION_CONFIGS[config_id], root)


def _case_config_dir(output_dir: Path) -> Path:
    safe_name = output_dir.resolve().name.replace("/", "_").replace("\\", "_")
    path = CASE_CONFIG_ROOT / safe_name
    path.mkdir(parents=True, exist_ok=True)
    return path


def _dual_chain_config(config: dict[str, Any]) -> dict[str, Any]:
    input_config = config["input"]
    calibration = config["calibration"]
    return {
        "version": "v2",
        "stage": "V2.5",
        "experiment_name": f"{calibration['id']}_local_replay_model",
        "topology": "dual_chain",
        "status": "runnable",
        "runnable": True,
        "data_truth_label": calibration.get("data_truth_label", "synthetic_replay"),
        "reason": "V2.9 calibration invokes V2.5 local virtual-time replay.",
        "trace": {"path": input_config["trace_file"], "meta_path": input_config["meta_file"]},
        "chains": {
            "chain_a": {
                "chain_id": "chain_a",
                "role": "source",
                "backend": "mock_chain",
                "backend_type": "local_virtual",
                "block_interval_ms": int(config.get("replay", {}).get("source_block_interval_ms", 100)),
                "finality_depth": int(config.get("replay", {}).get("source_finality_depth", 3)),
            },
            "chain_b": {
                "chain_id": "chain_b",
                "role": "target",
                "backend": "mock_chain",
                "backend_type": "local_virtual",
                "block_interval_ms": int(config.get("replay", {}).get("target_block_interval_ms", 300)),
                "finality_depth": int(config.get("replay", {}).get("target_finality_depth", 5)),
            },
        },
        "notes": [
            "Generated by V2.9 calibration runner.",
            "Local virtual-time replay only; not a live backend.",
        ],
    }


def _key(record: dict[str, Any]) -> str:
    for field in ("stage_id", "tx_id", "cross_tx_id", "record_id"):
        if record.get(field):
            return str(record[field])
    return ""


def _number(value: Any) -> float | None:
    if value in {"", None}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _error(expected: Any, observed: Any) -> float | str:
    expected_number = _number(expected)
    observed_number = _number(observed)
    if expected_number is None or observed_number is None:
        return ""
    return expected_number - observed_number


def compare_observed_vs_replay(observed_records: list[dict[str, Any]], replay_records: list[dict[str, Any]]) -> dict[str, Any]:
    replay_by_key = {_key(record): record for record in replay_records if _key(record)}
    matched_keys: set[str] = set()
    rows: list[dict[str, Any]] = []
    warnings: list[str] = []
    for observed in observed_records:
        key = _key(observed)
        replay = replay_by_key.get(key)
        matched = replay is not None
        if matched:
            matched_keys.add(key)
        expected_commit = replay.get("expected_commit_time_ms", "") if replay else ""
        expected_finality = replay.get("expected_finality_time_ms", "") if replay else ""
        expected_latency = replay.get("stage_latency_ms", "") if replay else ""
        commit_error = _error(expected_commit, observed.get("observed_commit_time_ms"))
        finality_error = _error(expected_finality, observed.get("observed_finality_time_ms"))
        latency_error = _error(expected_latency, observed.get("observed_latency_ms"))
        warning = "" if matched else "no matching replay record"
        if commit_error == "" and finality_error == "" and latency_error == "":
            warning = (warning + "; " if warning else "") + "insufficient timing fields"
        rows.append({
            "record_id": observed.get("record_id", key),
            "tx_id": observed.get("tx_id", ""),
            "cross_tx_id": observed.get("cross_tx_id", ""),
            "stage_id": observed.get("stage_id", ""),
            "stage": observed.get("stage", ""),
            "chain_id": observed.get("chain_id", ""),
            "observed_commit_time_ms": observed.get("observed_commit_time_ms", ""),
            "expected_commit_time_ms": expected_commit,
            "commit_error_ms": commit_error,
            "observed_finality_time_ms": observed.get("observed_finality_time_ms", ""),
            "expected_finality_time_ms": expected_finality,
            "finality_error_ms": finality_error,
            "observed_latency_ms": observed.get("observed_latency_ms", ""),
            "expected_latency_ms": expected_latency,
            "latency_error_ms": latency_error,
            "matched": matched,
            "warning": warning,
        })
        if warning:
            warnings.append(warning)
    for key, replay in replay_by_key.items():
        if key in matched_keys:
            continue
        rows.append({
            "record_id": key,
            "tx_id": "",
            "cross_tx_id": replay.get("cross_tx_id", ""),
            "stage_id": replay.get("stage_id", ""),
            "stage": replay.get("stage", ""),
            "chain_id": replay.get("chain_id", ""),
            "observed_commit_time_ms": "",
            "expected_commit_time_ms": replay.get("expected_commit_time_ms", ""),
            "commit_error_ms": "",
            "observed_finality_time_ms": "",
            "expected_finality_time_ms": replay.get("expected_finality_time_ms", ""),
            "finality_error_ms": "",
            "observed_latency_ms": "",
            "expected_latency_ms": replay.get("stage_latency_ms", ""),
            "latency_error_ms": "",
            "matched": False,
            "warning": "no matching observed record",
        })
    return {"rows": rows, "warnings": sorted(set(warnings))}


def _avg_abs(rows: list[dict[str, Any]], field: str) -> float | str:
    values = [abs(float(row[field])) for row in rows if row.get(field) not in {"", None}]
    return round(mean(values), 6) if values else ""


def _max_abs(rows: list[dict[str, Any]], field: str) -> float | str:
    values = [abs(float(row[field])) for row in rows if row.get(field) not in {"", None}]
    return round(max(values), 6) if values else ""


def _suggest_block_interval(rows: list[dict[str, Any]]) -> int | str:
    latencies = [_number(row.get("observed_latency_ms")) for row in rows]
    latencies = [value for value in latencies if value is not None and value >= 0]
    return int(round(mean(latencies))) if latencies else ""


def _summary(config: dict[str, Any], rows: list[dict[str, Any]], replay_count: int, warnings: list[str]) -> dict[str, Any]:
    calibration = config["calibration"]
    input_config = config["input"]
    matched = sum(1 for row in rows if row["matched"] is True)
    observed_count = sum(1 for row in rows if row.get("observed_commit_time_ms") != "" or row.get("observed_latency_ms") != "" or row.get("observed_finality_time_ms") != "")
    return {
        "calibration_id": calibration["id"],
        "status": "completed",
        "data_truth_label": calibration.get("data_truth_label", ""),
        "backend_type": calibration.get("backend_type", ""),
        "calibration_truth": calibration.get("calibration_truth", ""),
        "source_type": input_config.get("source_type", ""),
        "observed_record_count": observed_count,
        "replay_record_count": replay_count,
        "matched_record_count": matched,
        "unmatched_observed_count": sum(1 for row in rows if row["matched"] is False and row.get("observed_latency_ms") != ""),
        "unmatched_replay_count": sum(1 for row in rows if row["matched"] is False and row.get("expected_latency_ms") != ""),
        "avg_abs_commit_error_ms": _avg_abs(rows, "commit_error_ms"),
        "avg_abs_finality_error_ms": _avg_abs(rows, "finality_error_ms"),
        "avg_abs_latency_error_ms": _avg_abs(rows, "latency_error_ms"),
        "max_abs_latency_error_ms": _max_abs(rows, "latency_error_ms"),
        "suggested_block_interval_ms": _suggest_block_interval(rows),
        "suggested_finality_depth": "",
        "warnings": "; ".join(sorted(set(warnings))),
    }


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def _write_runtime_log(path: Path, config: dict[str, Any], warnings: list[str]) -> None:
    lines = [
        "stage=V2.9",
        f"calibration_id={config['calibration']['id']}",
        f"source_type={config['input']['source_type']}",
        "mode=chain-backed trace calibration",
        "docker_fabric_network_sh_started=false",
        "public_chain_live_nodes_connected=false",
        "live_backend_started=false",
        "not FabricLiveBackend",
        "not EVMLiveBackend",
        "not production bridge",
    ]
    lines.extend(f"warning={warning}" for warning in warnings)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _run_local_replay_model(config: dict[str, Any], output_dir: Path, root: Path) -> list[dict[str, Any]]:
    config_path = _case_config_dir(output_dir) / f"{config['calibration']['id']}_v25_replay.yaml"
    config_path.write_text(yaml.safe_dump(_dual_chain_config(config), sort_keys=False), encoding="utf-8")
    replay_output = output_dir / "local_replay_model"
    result = run_dual_chain_replay(config_path, replay_output, root)
    stage_metrics_path = replay_output / "stage_metrics.csv"
    with stage_metrics_path.open(encoding="utf-8", newline="") as stream:
        return list(csv.DictReader(stream))


def _read_synthetic_observed(trace_path: Path) -> list[dict[str, Any]]:
    return extract_observed_records(trace_path)["records"]


def _check_fabric_ready(input_config: dict[str, Any], root: Path) -> tuple[Path, Path]:
    trace_path = _resolve_workspace_path(str(input_config["trace_file"]), root)
    meta_path = _resolve_workspace_path(str(input_config["meta_file"]), root)
    if not trace_path.is_file() or not meta_path.is_file():
        status = detect_fabric_smoke_trace(trace_path.parent)
        raise CalibrationBlocked("Fabric smoke trace missing", {**status, "status": "blocked", "reason": "Fabric smoke trace missing"})
    return trace_path, meta_path


def run_calibration(config_path: Path, output_dir: Path, root: Path = ROOT) -> dict[str, Any]:
    config = load_calibration_config(config_path, root)
    input_config = config["input"]
    source_type = str(input_config.get("source_type", ""))
    output_dir.mkdir(parents=True, exist_ok=True)
    warnings: list[str] = []
    if source_type == "fabric_chain_backed_trace":
        trace_path, meta_path = _check_fabric_ready(input_config, root)
        adapted = adapt_trace_for_calibration(trace_path, meta_path)
        observed_records = adapted["records"]
        replay_records: list[dict[str, Any]] = []
        warnings.extend(adapted["warnings"])
        warnings.append("Fabric smoke calibration reads existing chain-backed trace only; it does not start Fabric.")
    else:
        trace_path = _resolve_workspace_path(str(input_config["trace_file"]), root)
        meta_path = _resolve_workspace_path(str(input_config["meta_file"]), root)
        validation = validate_trace_and_meta(trace_path, meta_path)
        if not validation["valid"]:
            raise CalibrationError("schema validation failed: " + "; ".join(validation["errors"]))
        observed_records = _read_synthetic_observed(trace_path)
        replay_records = _run_local_replay_model(config, output_dir, root)
        warnings.append("Synthetic calibration sample is not real chain execution.")
    comparison = compare_observed_vs_replay(observed_records, replay_records)
    warnings.extend(comparison["warnings"])
    summary = _summary(config, comparison["rows"], len(replay_records), warnings)
    result = {"stage": CALIBRATION_STAGE, "config": config, "summary": summary, "rows": comparison["rows"], "warnings": sorted(set(warnings))}
    write_calibration_artifacts(result, output_dir)
    metadata = {
        "stage": CALIBRATION_STAGE,
        "source": "v2_realism_bridge_calibration",
        "experiment_name": summary["calibration_id"],
        "status": "completed",
        "status_message": "completed",
        "output_dir": str(output_dir),
        "data_truth_label": summary["data_truth_label"],
        "backend_type": summary["backend_type"],
        "calibration_truth": summary["calibration_truth"],
        "summary": summary,
        "artifact_count": len(list_artifacts(output_dir, "manual")),
    }
    metadata_path = output_dir / "metadata.json"
    if metadata_path.is_file():
        existing = json.loads(metadata_path.read_text(encoding="utf-8"))
        existing.update(metadata)
        metadata = existing
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    return {**metadata, "summary": summary, "artifacts": list_artifacts(output_dir, "manual"), "warnings": sorted(set(warnings))}


def write_calibration_artifacts(result: dict[str, Any], output_dir: Path) -> None:
    config = result["config"]
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "used_config.yaml").write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    (output_dir / "used_config.json").write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
    _write_csv(output_dir / "calibration_summary.csv", [result["summary"]], SUMMARY_FIELDS)
    (output_dir / "calibration_summary.json").write_text(json.dumps(result["summary"], indent=2) + "\n", encoding="utf-8")
    _write_csv(output_dir / "replay_vs_observed.csv", result["rows"], COMPARISON_FIELDS)
    write_calibration_report(output_dir / "calibration_report.md", result)
    _write_runtime_log(output_dir / "runtime.log", config, result["warnings"])


def run_calibration_job(config_path: Path, jobs_root: Path = DEFAULT_JOBS_ROOT, root: Path = ROOT) -> dict[str, Any]:
    config = load_calibration_config(config_path, root)
    manager = JobManager(jobs_root)
    run = manager.create_run(
        source="v2_realism_bridge_calibration",
        experiment_name=str(config["calibration"]["id"]),
        data_truth_label=str(config["calibration"].get("data_truth_label", "")),
        stage=CALIBRATION_STAGE,
        extra_metadata={
            "backend_type": config["calibration"].get("backend_type", ""),
            "calibration_truth": config["calibration"].get("calibration_truth", ""),
        },
    )
    run_id = run["run_id"]
    manager.mark_running(run_id)
    try:
        result = run_calibration(config_path, manager.run_dir(run_id), root)
    except CalibrationBlocked as exc:
        manager.mark_failed(run_id, str(exc))
        return {**manager.get_run(run_id), **exc.payload, "artifacts": list_artifacts(manager.run_dir(run_id), run_id)}
    except Exception as exc:
        manager.mark_failed(run_id, str(exc))
        raise
    manager.mark_completed(run_id, data_truth_label=result["data_truth_label"])
    completed = manager.update_run(run_id, summary=result["summary"], backend_type=result["backend_type"], calibration_truth=result["calibration_truth"])
    return {**completed, "summary": result["summary"], "artifacts": list_artifacts(manager.run_dir(run_id), run_id), "warnings": result["warnings"]}
