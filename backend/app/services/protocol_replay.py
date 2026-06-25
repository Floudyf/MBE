from __future__ import annotations

import csv
import json
import shutil
from pathlib import Path
from typing import Any

import yaml

from backend.app.services.artifact_manager import list_artifacts
from backend.app.services.chain_backend import ChainBackend, create_backend
from backend.app.services.cross_chain_protocol import CrossChainProtocol, ProtocolAction, ProtocolEvent, ProtocolResult, ProtocolState, to_dict
from backend.app.services.cross_chain_protocols import ProtocolNotFound, create_protocol
from backend.app.services.dual_chain_profiles import DualChainConfigError, build_chain_profiles, resolve_workspace_path
from backend.app.services.job_manager import DEFAULT_JOBS_ROOT, JobManager
from backend.app.services.protocol_metrics import summarize_protocol_results
from trace.validator.cross_chain_trace_validator import validate_trace_and_meta

ROOT = Path(__file__).resolve().parents[3]
ENGINE_STAGE = "V2.6"
ENGINE_VERSION = "v2.6.local_cross_chain_protocol_baselines"


class ProtocolReplayError(ValueError):
    """Raised when a V2.6 protocol replay config cannot run."""


def read_jsonl(path: Path):
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            if line.strip():
                yield json.loads(line)


def load_protocol_replay_config(config_path: Path) -> dict[str, Any]:
    document = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise ProtocolReplayError("protocol replay config must be a mapping")
    if str(document.get("stage")).lower() != "v2.6":
        raise ProtocolReplayError("protocol replay config must declare stage: v2.6")
    experiment = document.get("experiment", {})
    if document.get("status") == "planned" or document.get("runnable") is False or experiment.get("runnable") is False:
        raise ProtocolReplayError("planned protocol configs are not executable")
    if not isinstance(document.get("protocols"), list) or not document["protocols"]:
        raise ProtocolReplayError("protocol replay config must enable at least one protocol")
    return document


def resolve_trace_paths(config: dict[str, Any], root: Path = ROOT) -> tuple[Path, Path]:
    trace = config.get("trace", {})
    trace_path = resolve_workspace_path(str(trace.get("trace_file") or trace.get("path")), root)
    meta_path = resolve_workspace_path(str(trace.get("meta_file") or trace.get("meta_path")), root)
    if not trace_path.is_file():
        raise ProtocolReplayError(f"trace file does not exist: {trace_path}")
    if not meta_path.is_file():
        raise ProtocolReplayError(f"trace meta file does not exist: {meta_path}")
    return trace_path, meta_path


def normalize_for_profiles(config: dict[str, Any]) -> dict[str, Any]:
    trace = config.get("trace", {})
    return {
        "data_truth_label": trace.get("data_truth_label", config.get("data_truth_label", "synthetic_replay")),
        "chains": config["chains"],
    }


def group_cross_transactions(trace_path: Path) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    terminal_priority = {"failed": 4, "refunded": 3, "timeout": 2, "completed": 1}
    for record in read_jsonl(trace_path):
        cross_tx_id = str(record["cross_tx_id"])
        item = grouped.setdefault(
            cross_tx_id,
            {
                "cross_tx_id": cross_tx_id,
                "source_chain": record["source_chain"],
                "target_chain": record["target_chain"],
                "submit_time_ms": int(record["submit_time_ms"]),
                "timeout_deadline_ms": int(record.get("timeout_deadline_ms") or 0),
                "expected_terminal_status": "completed",
                "records": 0,
            },
        )
        item["submit_time_ms"] = min(int(item["submit_time_ms"]), int(record["submit_time_ms"]))
        if record.get("timeout_deadline_ms"):
            item["timeout_deadline_ms"] = max(int(item["timeout_deadline_ms"]), int(record["timeout_deadline_ms"]))
        status = str(record.get("status", ""))
        if terminal_priority.get(status, 0) > terminal_priority.get(item["expected_terminal_status"], 0):
            item["expected_terminal_status"] = status
        item["records"] += 1
    return sorted(grouped.values(), key=lambda item: (item["submit_time_ms"], item["cross_tx_id"]))


def backend_record_for_action(action: ProtocolAction, state: ProtocolState) -> dict[str, Any]:
    status = "submitted"
    if action.action_type == "refund":
        status = "refunded"
    return {
        "schema_version": "v2.cross_chain_trace.v1",
        "cross_tx_id": action.cross_tx_id,
        "stage_id": action.action_id,
        "stage": action.stage,
        "source_chain": state.source_chain,
        "target_chain": state.target_chain,
        "chain_id": action.chain_id,
        "tx_id": action.action_id,
        "submit_time_ms": int(action.scheduled_time_ms),
        "timeout_deadline_ms": int(action.deadline_ms),
        "status": status,
        "data_truth_label": state.metadata.get("data_truth_label", "synthetic_replay"),
    }


def protocol_event_from_action(action: ProtocolAction, event_type: str, event_time_ms: int, source: str, payload: dict[str, Any] | None = None) -> ProtocolEvent:
    return ProtocolEvent(
        event_id=f"{action.action_id}_{event_type}",
        cross_tx_id=action.cross_tx_id,
        event_type=event_type,
        chain_id=action.chain_id,
        stage=action.stage,
        event_time_ms=int(event_time_ms),
        source=source,
        payload=payload or {},
    )


def protocol_event_source(backend_source: str) -> str:
    if backend_source == "local_virtual":
        return "virtual"
    if backend_source == "trace_replay":
        return "trace"
    return "backend"


def execute_action(action: ProtocolAction, state: ProtocolState, backends: dict[str, ChainBackend]) -> list[ProtocolEvent]:
    if action.action_type in {"submit_source_lock", "submit_target_mint"}:
        record = backend_record_for_action(action, state)
        backend = backends[action.chain_id]
        commit = backend.submit_stage(record)
        finality = backend.observe_finality(record)
        finality_wait = finality.finality_time_ms - finality.commit_time_ms
        return [
            protocol_event_from_action(action, "submit", action.scheduled_time_ms, "protocol", {"action_type": action.action_type}),
            protocol_event_from_action(action, "commit", commit.event_time_ms, "backend", {"backend_type": backend.profile.backend_type}),
            protocol_event_from_action(action, "finality", finality.finality_time_ms, protocol_event_source(finality.source), {"backend_type": backend.profile.backend_type, "finality_wait_time_ms": finality_wait}),
        ]
    if action.action_type == "generate_certificate":
        return [protocol_event_from_action(action, "certificate", action.scheduled_time_ms, "protocol", {"local_certificate_model": True})]
    if action.action_type == "mark_timeout":
        return [protocol_event_from_action(action, "timeout", action.scheduled_time_ms, "protocol", {"local_timeout_model": True})]
    if action.action_type == "refund":
        return [protocol_event_from_action(action, "refund", action.scheduled_time_ms, "protocol", {"local_refund_model": True})]
    if action.action_type == "complete":
        return [protocol_event_from_action(action, "certificate", action.scheduled_time_ms, "protocol", {"local_completion_model": True})]
    return [protocol_event_from_action(action, "failure", action.scheduled_time_ms, "protocol", {"reason": "unsupported action"})]


def run_one_cross_tx(protocol: CrossChainProtocol, cross_tx: dict[str, Any], start_time_ms: int, backends: dict[str, ChainBackend], data_truth_label: str) -> tuple[ProtocolResult, list[dict[str, Any]], list[dict[str, Any]]]:
    tx_input = dict(cross_tx)
    tx_input["submit_time_ms"] = start_time_ms
    state = protocol.get_initial_state(tx_input)
    state.metadata["data_truth_label"] = data_truth_label
    actions = protocol.plan_initial_actions(state)
    action_rows: list[dict[str, Any]] = []
    event_rows: list[dict[str, Any]] = []
    while actions and not protocol.is_terminal(state):
        action = actions.pop(0)
        action_rows.append(to_dict(action))
        events = execute_action(action, state, backends)
        for event in events:
            event_rows.append({**to_dict(event), "protocol_name": protocol.name, "status": state.status})
            step = protocol.handle_event(state, event)
            state = step.state
            actions.extend(step.actions)
    if not protocol.is_terminal(state):
        state.status = "failed"
        state.metadata["finished_at_ms"] = state.updated_at_ms
    return protocol.finalize_result(state), action_rows, event_rows


def run_protocol(protocol: CrossChainProtocol, cross_txs: list[dict[str, Any]], backends: dict[str, ChainBackend], data_truth_label: str) -> tuple[list[ProtocolResult], list[dict[str, Any]], list[dict[str, Any]]]:
    results: list[ProtocolResult] = []
    actions: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []
    active_finishes: list[int] = []
    next_available = 0
    window_size = int(getattr(protocol, "window_size", 0) or len(cross_txs) or 1)
    for cross_tx in cross_txs:
        base_start = int(cross_tx["submit_time_ms"])
        if getattr(protocol, "concurrency_mode", "pipeline") == "serial":
            start = max(base_start, next_available)
        elif getattr(protocol, "concurrency_mode", "pipeline") == "fixed_window":
            active_finishes = sorted(active_finishes)
            while len(active_finishes) >= window_size:
                base_start = max(base_start, active_finishes.pop(0))
            start = base_start
        else:
            start = base_start
        result, action_rows, event_rows = run_one_cross_tx(protocol, cross_tx, start, backends, data_truth_label)
        result.metadata["backend_type"] = ",".join(sorted({backend.profile.backend_type for backend in backends.values()}))
        results.append(result)
        actions.extend(action_rows)
        events.extend(event_rows)
        finish = int(result.metadata.get("finished_at_ms", start + result.e2e_latency_ms))
        if getattr(protocol, "concurrency_mode", "pipeline") == "serial":
            next_available = finish
        if getattr(protocol, "concurrency_mode", "pipeline") == "fixed_window":
            active_finishes.append(finish)
    return results, actions, events


def enabled_protocols(config: dict[str, Any]) -> list[CrossChainProtocol]:
    protocols = []
    for entry in config["protocols"]:
        if not entry.get("enabled", True):
            continue
        name = str(entry["name"])
        settings = {key: value for key, value in entry.items() if key not in {"name", "enabled"}}
        try:
            protocols.append(create_protocol(name, settings))
        except ProtocolNotFound as exc:
            raise ProtocolReplayError(str(exc)) from exc
    if not protocols:
        raise ProtocolReplayError("no runnable protocols enabled")
    return protocols


def row_from_result(result: ProtocolResult, data_truth_label: str) -> dict[str, Any]:
    return {
        "protocol_name": result.protocol_name,
        "cross_tx_id": result.cross_tx_id,
        "status": result.status,
        "success": result.success,
        "timeout": result.timeout,
        "refunded": result.refunded,
        "failed": result.failed,
        "e2e_latency_ms": result.e2e_latency_ms,
        "source_wait_time_ms": result.source_wait_time_ms,
        "target_wait_time_ms": result.target_wait_time_ms,
        "finality_wait_time_ms": result.finality_wait_time_ms,
        "action_count": result.action_count,
        "event_count": result.event_count,
        "backend_type": result.metadata.get("backend_type", ""),
        "data_truth_label": data_truth_label,
        "metadata": result.metadata,
    }


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: json.dumps(row[key], sort_keys=True) if isinstance(row.get(key), (dict, list)) else row.get(key, "") for key in fieldnames})


def write_protocol_outputs(config_path: Path, config: dict[str, Any], output_dir: Path, trace_path: Path, meta_path: Path, validation: dict[str, Any]) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    data_truth_label = str(config.get("trace", {}).get("data_truth_label", "synthetic_replay"))
    profiles = build_chain_profiles(normalize_for_profiles(config))
    backends = {chain_id: create_backend(profile) for chain_id, profile in profiles.items()}
    cross_txs = group_cross_transactions(trace_path)

    all_result_rows: list[dict[str, Any]] = []
    all_event_rows: list[dict[str, Any]] = []
    for protocol in enabled_protocols(config):
        results, _action_rows, event_rows = run_protocol(protocol, cross_txs, backends, data_truth_label)
        all_result_rows.extend(row_from_result(result, data_truth_label) for result in results)
        all_event_rows.extend(event_rows)

    summaries = summarize_protocol_results(all_result_rows, all_event_rows, profiles, data_truth_label)
    shutil.copy2(config_path, output_dir / "used_config.yaml")
    (output_dir / "used_config.json").write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
    write_csv(output_dir / "protocol_summary.csv", summaries, list(summaries[0].keys()) if summaries else [])
    (output_dir / "protocol_summary.json").write_text(json.dumps({"items": summaries}, indent=2) + "\n", encoding="utf-8")
    write_csv(output_dir / "protocol_results.csv", all_result_rows, ["protocol_name", "cross_tx_id", "status", "success", "timeout", "refunded", "failed", "e2e_latency_ms", "source_wait_time_ms", "target_wait_time_ms", "finality_wait_time_ms", "action_count", "event_count", "backend_type", "data_truth_label", "metadata"])
    write_csv(output_dir / "protocol_events.csv", all_event_rows, ["protocol_name", "cross_tx_id", "event_id", "event_type", "chain_id", "stage", "event_time_ms", "source", "status"])
    write_runtime_log(output_dir / "runtime.log", config, trace_path, meta_path, validation, summaries)
    write_report(output_dir / "report.md", summaries)
    return {"summary": {"items": summaries}, "results": all_result_rows, "events": all_event_rows}


def write_runtime_log(path: Path, config: dict[str, Any], trace_path: Path, meta_path: Path, validation: dict[str, Any], summaries: list[dict[str, Any]]) -> None:
    protocols = [summary["protocol_name"] for summary in summaries]
    backend_types = sorted({summary["source_backend_type"] for summary in summaries} | {summary["target_backend_type"] for summary in summaries})
    lines = [
        f"engine={ENGINE_VERSION}",
        f"stage: {ENGINE_STAGE}",
        f"input_trace_path={trace_path}",
        f"input_meta_path={meta_path}",
        f"protocols_enabled={','.join(protocols)}",
        f"chain_backend_types={','.join(backend_types)}",
        f"record_count={validation['stats']['records']}",
        f"cross_tx_count={validation['stats']['cross_tx_count']}",
        "no Docker/Fabric/network.sh started",
        "no time.Sleep used",
        "local protocol baseline replay only",
        "not MetaFlow",
        "not production bridge",
    ]
    for warning in validation.get("warnings", []):
        lines.append(f"warning={warning}")
    lines.extend(config.get("notes", []))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_report(path: Path, summaries: list[dict[str, Any]]) -> None:
    lines = [
        "# V2.6 Cross-chain Protocol Baseline Replay",
        "",
        "This is local cross-chain protocol baseline replay.",
        "This uses the CrossChainProtocol interface and the ChainBackend interface with LocalVirtualBackend.",
        "This is not real chain execution and not a production cross-chain bridge.",
        "This does not implement MetaFlow, real committee signatures, MintCert, RefundCert, or FinalityProof.",
        "Future V3 may replace LocalVirtualBackend with FabricLiveBackend or EVMLiveBackend.",
        "",
    ]
    for summary in summaries:
        lines.append(f"- {summary['protocol_name']}: success={summary['success_count']} timeout={summary['timeout_count']} refund={summary['refund_count']} avg_e2e_latency_ms={summary['avg_e2e_latency_ms']} max_pending={summary['max_pending_count']}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_protocol_replay(config_path: Path, output_dir: Path, root: Path = ROOT) -> dict[str, Any]:
    config_path = resolve_workspace_path(str(config_path), root)
    config = load_protocol_replay_config(config_path)
    trace_path, meta_path = resolve_trace_paths(config, root)
    validation = validate_trace_and_meta(trace_path, meta_path)
    if not validation["valid"]:
        raise ProtocolReplayError("schema validation failed: " + "; ".join(validation["errors"]))
    outputs = write_protocol_outputs(config_path, config, output_dir, trace_path, meta_path, validation)
    metadata = {
        "stage": ENGINE_STAGE,
        "source": "v2_cross_chain_protocol_replay",
        "experiment_name": str(config.get("experiment", {}).get("name", "v2_cross_chain_protocol_sample")),
        "status": "completed",
        "status_message": "completed",
        "output_dir": str(output_dir),
        "data_truth_label": config.get("trace", {}).get("data_truth_label", "synthetic_replay"),
        "protocol_truth": "local_baseline_model",
        "artifact_count": len(list_artifacts(output_dir, "manual")),
    }
    metadata_path = output_dir / "metadata.json"
    if metadata_path.is_file():
        existing = json.loads(metadata_path.read_text(encoding="utf-8"))
        existing.update(metadata)
        metadata = existing
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    return {**metadata, "summary": outputs["summary"], "artifacts": list_artifacts(output_dir, "manual")}


def run_protocol_replay_job(config_path: Path, jobs_root: Path = DEFAULT_JOBS_ROOT, root: Path = ROOT) -> dict[str, Any]:
    config_path = resolve_workspace_path(str(config_path), root)
    config = load_protocol_replay_config(config_path)
    data_truth_label = str(config.get("trace", {}).get("data_truth_label", "synthetic_replay"))
    manager = JobManager(jobs_root)
    run = manager.create_run(
        source="v2_cross_chain_protocol_replay",
        experiment_name=str(config.get("experiment", {}).get("name", "v2_cross_chain_protocol_sample")),
        data_truth_label=data_truth_label,
        stage=ENGINE_STAGE,
        extra_metadata={"protocol_truth": "local_baseline_model"},
    )
    run_id = run["run_id"]
    manager.mark_running(run_id)
    try:
        result = run_protocol_replay(config_path, manager.run_dir(run_id), root)
    except Exception as exc:
        manager.mark_failed(run_id, str(exc))
        raise
    completed = manager.mark_completed(run_id, data_truth_label=data_truth_label)
    manager.update_run(run_id, summary=result["summary"], protocol_truth="local_baseline_model")
    artifacts = list_artifacts(manager.run_dir(run_id), run_id)
    return {**completed, "summary": result["summary"], "artifacts": artifacts}
