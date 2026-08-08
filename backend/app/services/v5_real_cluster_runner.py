from __future__ import annotations


import csv
import hashlib
import json
import os
import signal
import subprocess
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from backend.app.core.paths import ROOT, V5_REAL_CLUSTER_RUNS_ROOT
from backend.app.models.v5_experiment_spec import V5ExperimentSpec
from backend.app.services.v5_experiment_compiler import compile_plan
from backend.app.services.v5_compatibility_engine import V5CompatibilityError
from backend.app.services import v5_real_cluster_artifacts
from backend.app.services.v5_artifact_contract import evaluate_expected_artifacts, write_run_artifact_catalog


RUNS_ROOT = V5_REAL_CLUSTER_RUNS_ROOT


def status() -> dict:
    return {
        "runtime_stage": "v5_1_real_plugin_driven_multi_process_multishard_runtime",
        "runtime_truth": "v5_real_cluster_candidate",
        "execution_backend": "real_cluster",
        "implemented": True,
        "one_node_one_os_process": True,
        "automatic_fallback": False,
        "production_blockchain": False,
        "production_pbft": False,
        "full_byzantine_security": False,
        "runtime_root": str(RUNS_ROOT),
    }


def compile_only(spec: V5ExperimentSpec) -> dict:
    run_dir = RUNS_ROOT / "preview"
    plan = compile_plan(spec, run_dir, source_saved_config_id=spec.saved_config_id)
    return {"compatibility": True, "plan": plan.model_dump()}


def _read_log_tail(path: Path, max_bytes: int = 1024 * 1024) -> str:
    if not path.is_file():
        return ""
    try:
        with path.open("rb") as handle:
            size = path.stat().st_size
            if size > max_bytes:
                handle.seek(size - max_bytes)
            payload = handle.read()
    except OSError:
        return ""
    text = payload.decode("utf-8", errors="replace")
    return ("[tail only]\n" + text) if size > max_bytes else text


def _terminate_process_tree(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    if os.name == "nt":
        try:
            subprocess.run(
                ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=15,
                check=False,
            )
        except (OSError, subprocess.SubprocessError):
            pass
    else:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except (OSError, ProcessLookupError):
            try:
                process.terminate()
            except OSError:
                pass
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except (OSError, ProcessLookupError):
                try:
                    process.kill()
                except OSError:
                    pass
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        try:
            process.kill()
        except OSError:
            pass


def _run_supervisor_process(
    command: list[str],
    run_dir: Path,
    timeout_seconds: int,
    cancel_check: Callable[[], bool] | None,
) -> tuple[int, str, str, bool, bool]:
    stdout_path = run_dir / "supervisor_stdout.log"
    stderr_path = run_dir / "supervisor_stderr.log"
    popen_kwargs: dict = {}
    if os.name == "nt":
        popen_kwargs["creationflags"] = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    else:
        popen_kwargs["start_new_session"] = True

    cancelled = False
    timed_out = False
    started = time.monotonic()
    with stdout_path.open("w", encoding="utf-8", buffering=1) as stdout_handle, stderr_path.open(
        "w", encoding="utf-8", buffering=1
    ) as stderr_handle:
        process = subprocess.Popen(
            command,
            cwd=ROOT / "executor",
            text=True,
            stdout=stdout_handle,
            stderr=stderr_handle,
            **popen_kwargs,
        )
        while process.poll() is None:
            if cancel_check is not None:
                try:
                    cancelled = bool(cancel_check())
                except Exception:
                    cancelled = False
                if cancelled:
                    stderr_handle.write("\nformal RunGroup cancellation requested; terminating supervisor process tree\n")
                    stderr_handle.flush()
                    _terminate_process_tree(process)
                    break
            if time.monotonic() - started >= timeout_seconds:
                timed_out = True
                stderr_handle.write(f"\nreal cluster timed out after {timeout_seconds} seconds\n")
                stderr_handle.flush()
                _terminate_process_tree(process)
                break
            time.sleep(0.25)
        returncode = process.poll()
        if returncode is None:
            _terminate_process_tree(process)
            returncode = process.poll()
        if returncode is None:
            returncode = 125

    return int(returncode), _read_log_tail(stdout_path), _read_log_tail(stderr_path), cancelled, timed_out


def _cancelled_run_result(run_id: str, run_dir: Path, stdout: str, stderr: str) -> dict:
    blocker = "cancelled_by_user"
    summary = v5_real_cluster_artifacts.read_summary(run_dir)
    summary.update(
        {
            "execution_status": "cancelled",
            "artifact_status": "incomplete",
            "formal_eligibility": False,
            "execution_gate": {"passed": False, "blockers": [blocker]},
            "artifact_gate": {"passed": False, "blockers": ["cancelled_before_artifact_validation"]},
            "completion_gate": {"passed": False, "blockers": [blocker]},
            "cancelled": True,
            "cancelled_at": datetime.now(UTC).isoformat(),
        }
    )
    (run_dir / "real_cluster_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    write_run_artifact_catalog(run_dir, run_id=run_id)
    return {
        "run_id": run_id,
        "status": "cancelled",
        "output_dir": _logical_output_dir(run_dir),
        "summary": summary,
        "artifacts": v5_real_cluster_artifacts.list_artifacts(run_dir, run_id),
        "stdout": stdout,
        "stderr": stderr,
        "error": blocker,
        "no_fallback": True,
    }


def run(spec: V5ExperimentSpec, *, cancel_check: Callable[[], bool] | None = None) -> dict:
    if spec.execution_backend != "real_cluster":
        raise ValueError("real cluster endpoint requires execution_backend=real_cluster; no fallback is available")
    run_id = "v5_" + datetime.now(UTC).strftime("%Y%m%d_%H%M%S_") + uuid4().hex[:8]
    run_dir = RUNS_ROOT / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    try:
        plan = compile_plan(spec, run_dir, source_saved_config_id=spec.saved_config_id)
    except ValueError:
        raise
    plan_path = run_dir / "compiled_run_plan.json"
    plan_path.write_text(json.dumps(plan.model_dump(), indent=2) + "\n", encoding="utf-8")
    command = ["go", "run", "./cmd/mbe-supervisor", "--mode", "v5-real-cluster", "--plan", str(plan_path), "--data-dir", str(run_dir)]
    configured_timeout_seconds = int(os.environ.get("MBE_V5_REAL_CLUSTER_TIMEOUT_SECONDS", "7200"))
    runner_timeout_seconds = max(configured_timeout_seconds, (spec.duration_ms // 1000) + 90)

    returncode, stdout, stderr, cancelled, timed_out = _run_supervisor_process(
        command, run_dir, runner_timeout_seconds, cancel_check
    )
    if cancelled:
        return _cancelled_run_result(run_id, run_dir, stdout, stderr)
    if timed_out:
        returncode = 124

    compatibility_prefix = "V5_COMPATIBILITY_BLOCKED:"
    combined_output = "\n".join([stdout or "", stderr or ""])
    if compatibility_prefix in combined_output:
        message = combined_output.split(compatibility_prefix, 1)[1].strip().splitlines()[0]
        preflight_path = run_dir / "workload_capability_preflight.json"
        details = {}
        if preflight_path.is_file():
            try:
                details = json.loads(preflight_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                details = {}
        blocker = str(details.get("blocker") or message).strip()
        raise V5CompatibilityError([blocker], code="v5_groundhog_cross_shard_preflight_blocked")

    write_run_artifact_catalog(run_dir, run_id=run_id)
    summary = v5_real_cluster_artifacts.read_summary(run_dir)
    summary["initial_state_digest"] = _initial_state_digest(run_dir)
    summary["state_home_mapping_digest"] = _state_home_mapping_digest(run_dir)
    summary["global_final_state_digest"] = _global_final_state_digest(summary)
    # Diagnostic oracle only. Full global_final_state_digest remains the
    # correctness gate; this digest helps distinguish business state from
    # protocol/system keys when investigating a mismatch.
    summary["global_business_state_digest"] = _global_business_state_digest(summary)
    summary.update(_metatrack_control_plane_evidence(run_dir, summary))
    artifact_contract = evaluate_expected_artifacts(run_dir, plan.artifact_contract or plan.expected_artifacts)
    summary["artifact_contract"] = artifact_contract
    summary["artifact_contract_version"] = artifact_contract["artifact_contract_version"]
    summary["artifact_contract_status"] = artifact_contract["artifact_contract_status"]
    summary["missing_expected_artifacts"] = artifact_contract["missing_expected_artifacts"]
    summary["missing_artifacts"] = artifact_contract["missing_expected_artifacts"]
    summary["unexpected_artifacts"] = artifact_contract["unexpected_artifacts"]
    execution_gate = _completion_gate(run_dir, {**summary, "missing_expected_artifacts": []})
    artifact_gate = {
        "passed": artifact_contract["artifact_contract_status"] == "complete",
        "blockers": [f"artifact_contract:missing:{item}" for item in artifact_contract["missing_expected_artifacts"]],
    }
    completion_gate = {
        "passed": execution_gate["passed"] and artifact_gate["passed"],
        "blockers": execution_gate["blockers"] + artifact_gate["blockers"],
    }
    summary["execution_gate"] = execution_gate
    summary["artifact_gate"] = artifact_gate
    summary["completion_gate"] = completion_gate
    summary.update(_status_fields(returncode, execution_gate, artifact_gate))
    status_value = _run_status_from_completion(returncode, execution_gate)
    root_failure = _root_failure(run_dir, stderr) if status_value == "failed" else ""
    if status_value == "failed" and not root_failure:
        root_failure = "real cluster execution failed without a reported root cause"
    if root_failure:
        summary["root_failure"] = root_failure

    (run_dir / "real_cluster_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    write_run_artifact_catalog(run_dir, run_id=run_id)
    return {
        "run_id": run_id,
        "status": status_value,
        "output_dir": _logical_output_dir(run_dir),
        "summary": summary,
        "artifacts": v5_real_cluster_artifacts.list_artifacts(run_dir, run_id),
        "stdout": stdout,
        "stderr": stderr,
        "error": root_failure if status_value == "failed" else "",
        "no_fallback": True,
    }


def run_dir(run_id: str) -> Path:
    if not run_id.startswith("v5_") or "/" in run_id or "\\" in run_id:
        raise ValueError("invalid V5 run id")
    return RUNS_ROOT / run_id



def _canonical_digest(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _initial_state_digest(run_dir: Path) -> str:
    roots: dict[str, set[str]] = {}
    for path in sorted((run_dir / "nodes").glob("*/committed_chain.csv")):
        try:
            with path.open(newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                first = next(reader, None)
        except (OSError, csv.Error, UnicodeError):
            return ""
        if not first:
            continue
        shard = str(first.get("shard_id") or "").strip()
        root = str(first.get("state_root_before") or "").strip()
        if shard and root:
            roots.setdefault(shard, set()).add(root)
    if not roots or any(len(values) != 1 for values in roots.values()):
        return ""
    return _canonical_digest({shard: next(iter(values)) for shard, values in sorted(roots.items())})


def _state_home_mapping_digest(run_dir: Path) -> str:
    path = run_dir / "client" / "placement_plan.csv"
    if not path.is_file():
        return ""
    pairs: set[tuple[str, str]] = set()
    try:
        with path.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                key = str(row.get("state_key") or "").strip()
                home = str(row.get("home_shard") or "").strip()
                if key and home:
                    pairs.add((key, home))
    except (OSError, csv.Error, UnicodeError):
        return ""
    if not pairs:
        return ""
    return _canonical_digest(sorted(pairs))


def _global_final_state_digest(summary: dict) -> str:
    roots: dict[str, set[str]] = {}
    for item in summary.get("node_summaries") or []:
        if not isinstance(item, dict):
            continue
        shard = str(item.get("shard_id") or "").strip()
        root = str(item.get("state_root") or "").strip()
        if shard and root:
            roots.setdefault(shard, set()).add(root)
    if not roots or any(len(values) != 1 for values in roots.values()):
        return ""
    return _canonical_digest({shard: next(iter(values)) for shard, values in sorted(roots.items())})


def _global_business_state_digest(summary: dict) -> str:
    roots: dict[str, set[str]] = {}
    for item in summary.get("node_summaries") or []:
        if not isinstance(item, dict):
            continue
        shard = str(item.get("shard_id") or "").strip()
        digest = str(item.get("business_state_digest") or "").strip()
        if shard and digest:
            roots.setdefault(shard, set()).add(digest)
    if not roots or any(len(values) != 1 for values in roots.values()):
        return ""
    return _canonical_digest({shard: next(iter(values)) for shard, values in sorted(roots.items())})


def _metatrack_control_plane_evidence(run_dir: Path, summary: dict) -> dict:
    out: dict = {}
    placement_path = run_dir / "client" / "transaction_placement.csv"
    shard_counts: dict[str, int] = {}
    reasons: dict[str, int] = {}
    predicted_reads = 0
    predicted_writes = 0
    if placement_path.is_file():
        try:
            with placement_path.open(newline="", encoding="utf-8") as handle:
                for row in csv.DictReader(handle):
                    shard = str(row.get("execution_shard") or "").strip()
                    reason = str(row.get("reason") or "").strip()
                    if shard:
                        shard_counts[shard] = shard_counts.get(shard, 0) + 1
                    if reason:
                        reasons[reason] = reasons.get(reason, 0) + 1
                    try:
                        predicted_reads += int(row.get("predicted_remote_reads") or 0)
                        predicted_writes += int(row.get("predicted_remote_writes") or 0)
                    except (TypeError, ValueError):
                        pass
        except (OSError, csv.Error, UnicodeError):
            shard_counts = {}
            reasons = {}
    total = sum(shard_counts.values())
    out["metatrack_execution_shard_transaction_counts"] = dict(sorted(shard_counts.items()))
    out["metatrack_execution_shard_count"] = len(shard_counts)
    out["metatrack_max_execution_shard_share"] = (max(shard_counts.values()) / total) if total else None
    out["metatrack_placement_reason_counts"] = dict(sorted(reasons.items()))
    out["metatrack_predicted_remote_read_count"] = predicted_reads
    out["metatrack_predicted_remote_write_count"] = predicted_writes

    score_path = run_dir / "client" / "placement_score.csv"
    score_rows = 0
    if score_path.is_file():
        try:
            with score_path.open(newline="", encoding="utf-8") as handle:
                score_rows = sum(1 for _ in csv.DictReader(handle))
        except (OSError, csv.Error, UnicodeError):
            score_rows = 0
    out["metatrack_placement_score_row_count"] = score_rows

    # One deterministic representative per shard prevents PBFT replica
    # amplification in mechanism counters while preserving replica-level node
    # summaries for consistency checks.
    representatives: dict[str, dict] = {}
    for item in sorted((summary.get("node_summaries") or []), key=lambda row: str((row or {}).get("node_id") or "")):
        if not isinstance(item, dict):
            continue
        shard = str(item.get("shard_id") or "").strip()
        if shard and shard not in representatives:
            representatives[shard] = item

    native_fields = {
        "state_ready_wait_count": "metatrack_state_ready_wait_count",
        "state_ready_resume_count": "metatrack_state_ready_resume_count",
        "state_prefetch_wait_ms": "metatrack_state_prefetch_wait_ms",
        "remote_state_fetch_count": "metatrack_remote_state_fetch_count",
        "remote_state_fetch_completed_count": "metatrack_remote_state_fetch_completed_count",
    }
    versioned_fields = {
        "versioned_state_ready_wave_count": "versioned_state_ready_wave_count",
        "versioned_state_ready_wait_observation_count": "versioned_state_ready_wait_observation_count",
        "versioned_state_ready_resolved_token_count": "versioned_state_ready_resolved_token_count",
        "versioned_state_probe_count": "versioned_state_probe_count",
        "versioned_state_probe_latency_ms": "versioned_state_probe_latency_ms",
    }
    for source, target in {**native_fields, **versioned_fields}.items():
        total_value = 0
        for item in representatives.values():
            try:
                total_value += int(item.get(source) or 0)
            except (TypeError, ValueError):
                pass
        out[target] = total_value

    max_width = 0
    for item in representatives.values():
        try:
            max_width = max(max_width, int(item.get("versioned_state_ready_max_wave_width") or 0))
        except (TypeError, ValueError):
            pass
    out["versioned_state_ready_max_wave_width"] = max_width

    modes = sorted({str(item.get("state_ready_scheduler_mode") or "").strip() for item in representatives.values() if str(item.get("state_ready_scheduler_mode") or "").strip()})
    out["metatrack_state_ready_scheduler_modes"] = modes
    out["metatrack_state_ready_scheduler_mode"] = modes[0] if len(modes) == 1 else ("mixed" if modes else "")
    versioned_modes = sorted({str(item.get("versioned_state_ready_scheduler_mode") or "").strip() for item in representatives.values() if str(item.get("versioned_state_ready_scheduler_mode") or "").strip()})
    out["versioned_state_ready_scheduler_modes"] = versioned_modes
    out["versioned_state_ready_scheduler_mode"] = versioned_modes[0] if len(versioned_modes) == 1 else ("mixed" if versioned_modes else "")
    return out


def _root_failure(run_dir: Path, stderr: str = "") -> str:
    stalled = _read_json(run_dir / "stalled_runtime_report.json")
    for key in ("reason", "fatal_execution_error", "fatal_persistence_error"):
        value = str(stalled.get(key) or "").strip()
        if value:
            return value
    drain = _read_json(run_dir / "drain_status.json")
    for key in ("fatal_execution_error", "fatal_persistence_error"):
        value = str(drain.get(key) or "").strip()
        if value:
            return value
    for status in _latest_node_statuses(run_dir):
        for key in ("fatal_execution_error", "fatal_persistence_error"):
            value = str(status.get(key) or "").strip()
            if value:
                return value
        failure = status.get("last_commit_failure")
        if isinstance(failure, dict):
            value = str(failure.get("error") or "").strip()
            if value:
                return value
    completion = str(drain.get("completion_reason") or "").strip()
    if completion and completion not in {"drain_quiescent", "in_progress"}:
        return completion
    for line in reversed(str(stderr or "").splitlines()):
        value = line.strip()
        if value:
            return value
    return ""

def _logical_output_dir(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        try:
            return str(Path("$MBE_RUNTIME_ROOT") / path.relative_to(RUNS_ROOT.parent))
        except ValueError:
            return "$MBE_RUNTIME_ROOT"


def _run_status_from_completion(returncode: int, execution_gate: dict) -> str:
    return "completed" if returncode == 0 and execution_gate.get("passed") is True else "failed"


def _status_fields(returncode: int, execution_gate: dict, artifact_gate: dict) -> dict:
    """Separate execution truth from artifact completeness."""
    execution_status = _run_status_from_completion(returncode, execution_gate)
    artifact_status = "complete" if artifact_gate.get("passed") else "incomplete"
    return {
        "execution_status": execution_status,
        "artifact_status": artifact_status,
        "formal_eligibility": bool(execution_status == "completed" and artifact_gate.get("passed")),
    }


def _completion_gate(run_dir: Path, summary: dict) -> dict:
    blockers: list[str] = []
    drain = _read_json(run_dir / "drain_status.json")
    finality = _read_json(run_dir / "finality_summary.json")
    if drain.get("completion_reason") != "drain_quiescent":
        blockers.append("drain_status.json:completion_reason_not_drain_quiescent")
    if not _positive_number(drain.get("drain_finished_at")):
        blockers.append("drain_status.json:missing_drain_finished_at")
    required_finality = [
        "logical_window_start_ms",
        "logical_window_end_ms",
        "logical_finality_duration_ms",
        "logical_finality_tps",
        "drain_finished_at_ms",
        "drain_duration_ms",
        "completion_window_start_ms",
        "completion_window_end_ms",
        "completion_duration_ms",
        "end_to_end_tps",
        "throughput_tps",
    ]
    for field in required_finality:
        if field not in finality:
            blockers.append(f"finality_summary.json:missing_{field}")
    if _number(finality.get("completion_window_end_ms")) < _number(finality.get("logical_window_end_ms")):
        blockers.append("finality_summary.json:completion_ends_before_logical_finality")
    if _number(finality.get("completion_duration_ms")) < _number(finality.get("logical_finality_duration_ms")):
        blockers.append("finality_summary.json:completion_duration_less_than_logical_duration")
    if finality.get("throughput_tps") != finality.get("end_to_end_tps"):
        blockers.append("finality_summary.json:throughput_tps_not_end_to_end")
    if summary.get("finality_evidence", {}).get("incomplete_unique_tx_count") not in {0, 0.0, None}:
        blockers.append("real_cluster_summary.json:incomplete_transactions")
    if not str(summary.get("initial_state_digest") or ""):
        blockers.append("real_cluster_summary.json:missing_initial_state_digest")
    if not str(summary.get("global_final_state_digest") or ""):
        blockers.append("real_cluster_summary.json:missing_global_final_state_digest")
    if (
        summary.get("cross_shard_execution_mode") == "stateless_direct_execution"
        and not str(summary.get("state_home_mapping_digest") or "")
    ):
        blockers.append("real_cluster_summary.json:missing_state_home_mapping_digest")
    for missing in summary.get("missing_expected_artifacts", []) or []:
        blockers.append(f"artifact_contract:missing:{missing}")
    latest_status = _latest_node_statuses(run_dir)
    for status in latest_status:
        node_id = str(status.get("node_id", "unknown"))
        for field in ("pending_state_delta_count", "pending_state_delta_key_count", "ready_state_delta_count", "pending_commit_count"):
            if _number(status.get(field)) != 0:
                blockers.append(f"node_runtime_status:{node_id}:{field}_not_zero")
        if _proposal_has_pending_work(status):
            blockers.append(f"node_runtime_status:{node_id}:proposal_in_flight_with_pending_work")
        if str(status.get("fatal_execution_error") or "").strip():
            blockers.append(f"node_runtime_status:{node_id}:fatal_execution_error")
        if str(status.get("fatal_persistence_error") or "").strip():
            blockers.append(f"node_runtime_status:{node_id}:fatal_persistence_error")
    return {"passed": not blockers, "blockers": blockers}


def _proposal_has_pending_work(status: dict) -> bool:
    if status.get("proposal_in_flight") is not True:
        return False
    if status.get("proposal_work_details_available") is not True:
        # Preserve the old conservative behavior for statuses written by an
        # older or incomplete runtime.
        return True
    if _number(status.get("proposal_system_state_delta_count")) > 0:
        return True
    terminal = {str(item).strip() for item in status.get("terminal_logical_tx_ids") or [] if str(item).strip()}
    proposal = {str(item).strip() for item in status.get("proposal_logical_tx_ids") or [] if str(item).strip()}
    return any(logical_id not in terminal for logical_id in proposal)


def _latest_node_statuses(run_dir: Path) -> list[dict]:
    statuses: list[dict] = []
    status_paths = {
        path
        for pattern in ("nodes/*/node_runtime_status.json", "node_*/node_runtime_status.json")
        for path in run_dir.glob(pattern)
    }
    for path in sorted(status_paths):
        status = _read_json(path)
        if status:
            statuses.append(status)
    return statuses


def _read_json(path: Path) -> dict:
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def _number(value: object) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _positive_number(value: object) -> bool:
    return _number(value) > 0
