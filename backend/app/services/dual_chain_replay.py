from __future__ import annotations

import csv
import json
import shutil
from pathlib import Path
from typing import Any

import yaml

from backend.app.services.artifact_manager import list_artifacts
from backend.app.services.chain_backend import create_backend
from backend.app.services.dual_chain_metrics import summarize_dual_chain_metrics
from backend.app.services.dual_chain_profiles import DualChainConfigError, build_chain_profiles, load_dual_chain_config, resolve_workspace_path
from backend.app.services.job_manager import DEFAULT_JOBS_ROOT, JobManager
from trace.validator.cross_chain_trace_validator import validate_trace_and_meta

ROOT = Path(__file__).resolve().parents[3]
ENGINE_STAGE = "V2.5"
ENGINE_VERSION = "v2.5.local_virtual_dual_chain_replay"


def read_jsonl(path: Path):
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            if line.strip():
                yield json.loads(line)


def resolve_config_inputs(config: dict[str, Any], root: Path = ROOT) -> tuple[Path, Path]:
    trace = config.get("trace", {})
    trace_path = resolve_workspace_path(str(trace["path"]), root)
    meta_path = resolve_workspace_path(str(trace["meta_path"]), root)
    if not trace_path.is_file():
        raise DualChainConfigError(f"trace file does not exist: {trace_path}")
    if not meta_path.is_file():
        raise DualChainConfigError(f"trace meta file does not exist: {meta_path}")
    return trace_path, meta_path


def replay_stage_records(config: dict[str, Any], trace_path: Path) -> list[dict[str, Any]]:
    profiles = build_chain_profiles(config)
    backends = {chain_id: create_backend(profile) for chain_id, profile in profiles.items()}
    stage_metrics: list[dict[str, Any]] = []
    for record in read_jsonl(trace_path):
        chain_id = str(record["chain_id"])
        if chain_id not in backends:
            raise DualChainConfigError(f"trace record uses unknown chain_id: {chain_id}")
        backend = backends[chain_id]
        event = backend.submit_stage(record)
        finality = backend.observe_finality(record)
        finality_wait = finality.finality_time_ms - finality.commit_time_ms
        stage_metrics.append({
            "cross_tx_id": record["cross_tx_id"],
            "stage_id": record["stage_id"],
            "stage": record["stage"],
            "chain_id": chain_id,
            "source_chain": record["source_chain"],
            "target_chain": record["target_chain"],
            "submit_time_ms": int(record["submit_time_ms"]),
            "observed_commit_time_ms": record.get("commit_time_ms", ""),
            "observed_finality_time_ms": record.get("finality_time_ms", ""),
            "expected_commit_time_ms": finality.commit_time_ms,
            "expected_finality_time_ms": finality.finality_time_ms,
            "stage_latency_ms": finality.finality_time_ms - int(record["submit_time_ms"]),
            "finality_wait_time_ms": finality_wait,
            "status": record["status"],
            "backend_type": backend.profile.backend_type,
            "event_source": finality.source,
        })
    return stage_metrics


def write_summary_csv(path: Path, summary: dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(summary.keys()))
        writer.writeheader()
        writer.writerow(summary)


def write_stage_metrics_csv(path: Path, stage_metrics: list[dict[str, Any]]) -> None:
    fieldnames = [
        "cross_tx_id",
        "stage_id",
        "stage",
        "chain_id",
        "source_chain",
        "target_chain",
        "submit_time_ms",
        "observed_commit_time_ms",
        "observed_finality_time_ms",
        "expected_commit_time_ms",
        "expected_finality_time_ms",
        "stage_latency_ms",
        "finality_wait_time_ms",
        "status",
        "backend_type",
        "event_source",
    ]
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(stage_metrics)


def write_runtime_log(path: Path, config: dict[str, Any], trace_path: Path, meta_path: Path, validation: dict[str, Any], summary: dict[str, Any]) -> None:
    lines = [
        f"engine={ENGINE_VERSION}",
        f"stage={ENGINE_STAGE}",
        f"trace={trace_path}",
        f"meta={meta_path}",
        f"records={summary['stage_record_count']}",
        f"cross_tx_count={summary['cross_tx_count']}",
        f"source_chain={summary['source_chain_id']} backend={summary['source_backend_type']} block_interval_ms={summary['source_block_interval_ms']} finality_depth={summary['source_finality_depth']}",
        f"target_chain={summary['target_chain_id']} backend={summary['target_backend_type']} block_interval_ms={summary['target_block_interval_ms']} finality_depth={summary['target_finality_depth']}",
        "mode=local virtual-time replay only",
        "no_time_sleep=true",
        "docker_fabric_network_sh_started=false",
        "cross_chain_protocol_execution=false",
    ]
    for warning in validation.get("warnings", []):
        lines.append(f"warning={warning}")
    lines.extend(config.get("notes", []))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_report(path: Path, summary: dict[str, Any]) -> None:
    path.write_text(
        "\n".join([
            "# V2.5 Dual-chain Replay Report",
            "",
            "This report is produced by a local virtual-time replay engine. It is not real chain execution and not a production cross-chain bridge.",
            "",
            f"- cross_tx_count: {summary['cross_tx_count']}",
            f"- stage_record_count: {summary['stage_record_count']}",
            f"- avg_e2e_latency_ms: {summary['avg_e2e_latency_ms']}",
            f"- p99_e2e_latency_ms: {summary['p99_e2e_latency_ms']}",
            f"- chain_speed_imbalance: {summary['chain_speed_imbalance']}",
            f"- data_truth_label: {summary['data_truth_label']}",
            "",
            "V2.5 provides ChainBackend interfaces and LocalVirtualBackend/TraceReplayBackend only.",
            "V2.6 protocol baselines, MetaFlow, committee bridge, Pending Pool, FabricLiveBackend, and EVMLiveBackend are not implemented here.",
        ])
        + "\n",
        encoding="utf-8",
    )


def write_outputs(config_path: Path, config: dict[str, Any], output_dir: Path, trace_path: Path, meta_path: Path, validation: dict[str, Any]) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    profiles = build_chain_profiles(config)
    stage_metrics = replay_stage_records(config, trace_path)
    data_truth_label = str(config.get("data_truth_label", "synthetic_replay"))
    summary = summarize_dual_chain_metrics(stage_metrics, profiles, data_truth_label)

    shutil.copy2(config_path, output_dir / "used_config.yaml")
    (output_dir / "used_config.json").write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
    write_summary_csv(output_dir / "dual_chain_summary.csv", summary)
    (output_dir / "dual_chain_summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    write_stage_metrics_csv(output_dir / "stage_metrics.csv", stage_metrics)
    write_runtime_log(output_dir / "runtime.log", config, trace_path, meta_path, validation, summary)
    write_report(output_dir / "report.md", summary)
    return {"summary": summary, "stage_metrics": stage_metrics}


def run_dual_chain_replay(config_path: Path, output_dir: Path, root: Path = ROOT) -> dict[str, Any]:
    config_path = resolve_workspace_path(str(config_path), root)
    config = load_dual_chain_config(config_path)
    trace_path, meta_path = resolve_config_inputs(config, root)
    validation = validate_trace_and_meta(trace_path, meta_path)
    if not validation["valid"]:
        raise DualChainConfigError("schema validation failed: " + "; ".join(validation["errors"]))
    outputs = write_outputs(config_path, config, output_dir, trace_path, meta_path, validation)
    metadata = {
        "stage": ENGINE_STAGE,
        "source": "v2_dual_chain_replay",
        "experiment_name": str(config.get("experiment_name", "v2_dual_chain_sample")),
        "status": "completed",
        "status_message": "completed",
        "output_dir": str(output_dir),
        "data_truth_label": config.get("data_truth_label", "synthetic_replay"),
        "source_backend_type": outputs["summary"]["source_backend_type"],
        "target_backend_type": outputs["summary"]["target_backend_type"],
        "artifact_count": len(list_artifacts(output_dir, "manual")),
    }
    metadata_path = output_dir / "metadata.json"
    if metadata_path.is_file():
        existing = json.loads(metadata_path.read_text(encoding="utf-8"))
        existing.update(metadata)
        metadata = existing
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    return {**metadata, "summary": outputs["summary"], "artifacts": list_artifacts(output_dir, "manual")}


def run_dual_chain_replay_job(config_path: Path, jobs_root: Path = DEFAULT_JOBS_ROOT, root: Path = ROOT) -> dict[str, Any]:
    config_path = resolve_workspace_path(str(config_path), root)
    config = load_dual_chain_config(config_path)
    manager = JobManager(jobs_root)
    profiles = build_chain_profiles(config)
    backend_types = sorted({profile.backend_type for profile in profiles.values()})
    run = manager.create_run(
        source="v2_dual_chain_replay",
        experiment_name=str(config.get("experiment_name", "v2_dual_chain_sample")),
        data_truth_label=str(config.get("data_truth_label", "synthetic_replay")),
        stage=ENGINE_STAGE,
        extra_metadata={"backend_type": ",".join(backend_types)},
    )
    run_id = run["run_id"]
    manager.mark_running(run_id)
    try:
        result = run_dual_chain_replay(config_path, manager.run_dir(run_id), root)
    except Exception as exc:
        manager.mark_failed(run_id, str(exc))
        raise
    completed = manager.mark_completed(run_id, data_truth_label=str(result["data_truth_label"]))
    manager.update_run(run_id, summary=result["summary"], backend_type=",".join(backend_types))
    artifacts = list_artifacts(manager.run_dir(run_id), run_id)
    return {**completed, "summary": result["summary"], "artifacts": artifacts}
