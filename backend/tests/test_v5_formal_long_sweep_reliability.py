from types import SimpleNamespace

from backend.app.services import v5_real_cluster_runner
from backend.app.services.v5_formal_scheduler import (
    DEFAULT_FORMAL_EXECUTION_POLICY,
    _is_system_resource_error,
    _ordered_execution_rows,
    _refresh_child_counts,
    reconcile_stale_group,
    resume_unfinished,
)


def test_default_policy_is_bounded_and_fixed_workload_strict() -> None:
    assert DEFAULT_FORMAL_EXECUTION_POLICY["child_wall_timeout_seconds"] == 1800
    assert DEFAULT_FORMAL_EXECUTION_POLICY["worker_heartbeat_seconds"] == 5
    assert DEFAULT_FORMAL_EXECUTION_POLICY["stale_worker_timeout_seconds"] == 30
    assert DEFAULT_FORMAL_EXECUTION_POLICY["fixed_workload_completion_required_for_formal_tps"] is True
    assert DEFAULT_FORMAL_EXECUTION_POLICY["partial_timeout_metrics_diagnostic_only"] is True


def test_theta_major_repeat_then_method_order() -> None:
    methods = [SimpleNamespace(method_id="serial"), SimpleNamespace(method_id="aria")]
    plan = SimpleNamespace(suites=["workload_sensitivity"], methods=methods)
    rows = [
        {"method_config_id": "aria", "repeat_index": 1, "workload_point": {"target_theta": 0.8}},
        {"method_config_id": "serial", "repeat_index": 0, "workload_point": {"target_theta": 0.2}},
        {"method_config_id": "aria", "repeat_index": 0, "workload_point": {"target_theta": 0.2}},
        {"method_config_id": "serial", "repeat_index": 1, "workload_point": {"target_theta": 0.2}},
        {"method_config_id": "serial", "repeat_index": 0, "workload_point": {"target_theta": 0.8}},
    ]
    ordered = _ordered_execution_rows(rows, plan)
    assert [(r["workload_point"]["target_theta"], r["repeat_index"], r["method_config_id"]) for r in ordered] == [
        (0.2, 0, "serial"),
        (0.2, 0, "aria"),
        (0.2, 1, "serial"),
        (0.8, 0, "serial"),
        (0.8, 1, "aria"),
    ]


def test_complete_child_accounting_includes_not_started_timeout_interrupted() -> None:
    group = {"total_child_runs": 312}
    items = ([{"child_run_id": f"c{i}", "status": "completed"} for i in range(196)]
             + [{"child_run_id": f"f{i}", "status": "failed"} for i in range(8)]
             + [{"child_run_id": "t0", "status": "timed_out"}]
             + [{"child_run_id": "i0", "status": "interrupted"}])
    _refresh_child_counts(group, items)
    assert group["completed_child_runs"] == 196
    assert group["failed_child_runs"] == 8
    assert group["timed_out_child_runs"] == 1
    assert group["interrupted_child_runs"] == 1
    assert group["not_started_child_runs"] == 106


def test_windows_system_resource_errors_are_isolated() -> None:
    exc = OSError("[WinError 1450] Insufficient system resources exist to complete the requested service")
    exc.winerror = 1450
    assert _is_system_resource_error(exc) is True
    assert _is_system_resource_error(OSError("ordinary file error")) is False


def test_explicit_formal_timeout_overrides_legacy_two_hour_default() -> None:
    spec = SimpleNamespace(duration_ms=3_600_000)
    assert v5_real_cluster_runner._resolve_runner_timeout_seconds(spec, 1800) == 1800


def test_resume_unfinished_selects_missing_and_interrupted_only_by_default(monkeypatch) -> None:
    import backend.app.services.v5_formal_scheduler as scheduler
    group = {
        "run_group_id": "g1",
        "status": "cancelled",
        "matrix": [{"child_run_id": item} for item in ("done", "interrupted", "missing", "failed", "timeout")],
        "cancel_requested": True,
    }
    items = [
        {"child_run_id": "done", "status": "completed"},
        {"child_run_id": "interrupted", "status": "interrupted"},
        {"child_run_id": "failed", "status": "failed"},
        {"child_run_id": "timeout", "status": "timed_out"},
    ]
    monkeypatch.setattr(scheduler, "reconcile_stale_group", lambda group_id: group)
    monkeypatch.setattr(scheduler, "worker_active", lambda group_id: False)
    monkeypatch.setattr(scheduler, "children", lambda group_id: items)
    monkeypatch.setattr(scheduler, "write_group", lambda value: None)
    monkeypatch.setattr(scheduler, "start", lambda group_id: None)
    monkeypatch.setattr(scheduler, "read_group", lambda group_id: group)
    result = resume_unfinished("g1")
    assert set(group["resume_requested_child_ids"]) == {"interrupted", "missing"}
    assert group["cancel_requested"] is False
    assert result["status"] == "queued"


def test_reconcile_stale_group_marks_running_child_interrupted_and_reaps(monkeypatch) -> None:
    import backend.app.services.v5_formal_scheduler as scheduler
    group = {
        "run_group_id": "g2",
        "status": "running",
        "total_child_runs": 3,
        "worker_heartbeat_at": "2000-01-01T00:00:00+00:00",
    }
    items = [{"child_run_id": "c1", "status": "running"}]
    written_children = []
    monkeypatch.setattr(scheduler, "read_group", lambda group_id: group)
    monkeypatch.setattr(scheduler, "worker_active", lambda group_id: False)
    monkeypatch.setattr(scheduler, "children", lambda group_id: items)
    monkeypatch.setattr(scheduler, "write_child", lambda group_id, item: written_children.append(dict(item)))
    monkeypatch.setattr(scheduler, "write_group", lambda value: None)
    monkeypatch.setattr(v5_real_cluster_runner, "reap_persisted_supervisors", lambda group_id: [{"pid": 123, "termination_requested": True}])
    result = reconcile_stale_group("g2")
    assert result["status"] == "interrupted"
    assert written_children[0]["status"] == "interrupted"
    assert result["not_started_child_runs"] == 2
    assert result["stale_supervisor_reap"][0]["termination_requested"] is True
