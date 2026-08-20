from __future__ import annotations
import json
import sys
import threading
import time
import zipfile
from pathlib import Path

from backend.app.services import v5_formal_run_store
from backend.app.services import v5_formal_scheduler
from backend.app.services import v5_real_cluster_runner


def test_wall_watchdog_survives_blocked_heartbeat(tmp_path: Path):
    blocker = threading.Event()

    def stuck_heartbeat():
        blocker.wait(30)

    started = time.monotonic()
    _rc, _stdout, _stderr, cancelled, timed_out = v5_real_cluster_runner._run_supervisor_process(
        [sys.executable, "-c", "import time; time.sleep(30)"],
        tmp_path,
        1,
        None,
        heartbeat_callback=stuck_heartbeat,
        heartbeat_interval_seconds=0.1,
        runtime_context={"run_group_id": "v5grp_test", "child_run_id": "v5child_test"},
    )
    elapsed = time.monotonic() - started
    assert timed_out is True
    assert cancelled is False
    assert elapsed < 8
    evidence = json.loads((tmp_path / "supervisor_process.json").read_text(encoding="utf-8"))
    assert evidence["timed_out"] is True
    assert evidence["termination_reason"] == "formal_child_wall_timeout"
    assert evidence["watchdog_independent_of_heartbeat"] is True




def test_worker_heartbeat_uses_sidecar_without_rewriting_run_group(tmp_path: Path, monkeypatch):
    directory = tmp_path / "v5grp_sidecar"
    directory.mkdir(parents=True)
    group_path = directory / "run_group.json"
    original = {
        "run_group_id": "v5grp_sidecar",
        "status": "running",
        "cancel_requested": False,
    }
    group_path.write_text(json.dumps(original), encoding="utf-8")

    monkeypatch.setattr(v5_formal_scheduler, "group_dir", lambda _group_id: directory)
    monkeypatch.setattr(
        v5_formal_scheduler,
        "read_group",
        lambda _group_id: (_ for _ in ()).throw(
            AssertionError("heartbeat must not read run_group.json")
        ),
    )
    monkeypatch.setattr(
        v5_formal_scheduler,
        "write_group",
        lambda _group: (_ for _ in ()).throw(
            AssertionError("heartbeat must not rewrite run_group.json")
        ),
    )

    v5_formal_scheduler._write_worker_heartbeat(
        "v5grp_sidecar",
        child_id="v5child_sidecar",
        child_started_at="2026-08-20T00:00:00+00:00",
    )

    heartbeat = json.loads(
        (directory / "worker_heartbeat.json").read_text(encoding="utf-8")
    )
    assert heartbeat["run_group_id"] == "v5grp_sidecar"
    assert heartbeat["active_child_run_id"] == "v5child_sidecar"
    assert heartbeat["active_child_started_at"] == "2026-08-20T00:00:00+00:00"
    assert json.loads(group_path.read_text(encoding="utf-8")) == original


def test_reconcile_overlays_live_sidecar_truth(tmp_path: Path, monkeypatch):
    directory = tmp_path / "v5grp_live"
    directory.mkdir(parents=True)
    heartbeat = {
        "worker_heartbeat_at": "2026-08-20T12:00:00+00:00",
        "active_child_run_id": "v5child_live",
        "active_child_started_at": "2026-08-20T11:59:00+00:00",
    }
    (directory / "worker_heartbeat.json").write_text(
        json.dumps(heartbeat), encoding="utf-8"
    )

    monkeypatch.setattr(v5_formal_scheduler, "group_dir", lambda _group_id: directory)
    monkeypatch.setattr(
        v5_formal_scheduler,
        "read_group",
        lambda _group_id: {
            "run_group_id": "v5grp_live",
            "status": "running",
            "worker_heartbeat_at": "2020-01-01T00:00:00+00:00",
        },
    )
    monkeypatch.setattr(v5_formal_scheduler, "worker_active", lambda _group_id: True)

    observed = v5_formal_scheduler.reconcile_stale_group("v5grp_live")
    assert observed["worker_heartbeat_at"] == heartbeat["worker_heartbeat_at"]
    assert observed["active_child_run_id"] == "v5child_live"
    assert observed["active_child_started_at"] == heartbeat["active_child_started_at"]


def test_sidecar_lookup_is_optional_for_legacy_mock_group_ids(monkeypatch):
    monkeypatch.setattr(
        v5_formal_scheduler,
        "read_group",
        lambda _group_id: {
            "run_group_id": "g2",
            "status": "running",
            "worker_heartbeat_at": "2000-01-01T00:00:00+00:00",
        },
    )
    monkeypatch.setattr(v5_formal_scheduler, "worker_active", lambda _group_id: False)
    monkeypatch.setattr(v5_formal_scheduler, "children", lambda _group_id: [])
    monkeypatch.setattr(v5_formal_scheduler, "write_group", lambda _group: None)
    monkeypatch.setattr(
        v5_formal_scheduler.v5_real_cluster_runner,
        "reap_persisted_supervisors",
        lambda _group_id: [],
    )

    # The optional heartbeat sidecar must not make legacy/mock scheduler unit
    # tests depend on the public v5grp_* identifier validator.
    observed = v5_formal_scheduler.reconcile_stale_group("g2")
    assert observed["status"] == "interrupted"


def test_timeout_summary_backfills_drain_counts_without_inventing_finalized(tmp_path: Path, monkeypatch):
    (tmp_path / "drain_status.json").write_text(
        json.dumps({
            "submitted": 10000,
            "terminal": 8123,
            "incomplete": 1877,
            "phase": "DRAINING",
            "completion_reason": "wall_timeout",
        }),
        encoding="utf-8",
    )
    monkeypatch.setattr(v5_real_cluster_runner.v5_observability_metrics, "write_observability_summaries", lambda _d: None)
    monkeypatch.setattr(v5_real_cluster_runner.v5_real_cluster_artifacts, "read_summary", lambda _d: {})
    monkeypatch.setattr(v5_real_cluster_runner.v5_real_cluster_artifacts, "list_artifacts", lambda _d, _r: [])
    monkeypatch.setattr(v5_real_cluster_runner.v5_artifact_storage, "finalize_online_storage", lambda _d, run_id: {})
    monkeypatch.setattr(v5_real_cluster_runner, "write_run_artifact_catalog", lambda _d, run_id: None)
    monkeypatch.setattr(v5_real_cluster_runner, "_logical_output_dir", lambda _d: "logical")

    result = v5_real_cluster_runner._timed_out_run_result("v5_test", tmp_path, "", "", 1800)
    evidence = result["summary"]["formal_timeout_evidence"]
    assert evidence["submitted_unique_tx_count"] == 10000
    assert evidence["terminal_unique_tx_count"] == 8123
    assert evidence["incomplete_unique_tx_count"] == 1877
    assert evidence["finalized_unique_logical_tx_count"] is None
    assert evidence["terminal_is_not_assumed_finalized"] is True
    assert evidence["evidence_sources"]["terminal_unique_tx_count"] == "drain_status"


def test_stale_reconcile_clears_active_child_truth(monkeypatch):
    group = {
        "run_group_id": "v5grp_stale",
        "status": "running",
        "worker_heartbeat_at": "2020-01-01T00:00:00+00:00",
        "active_child_run_id": "v5child_stale",
        "active_child_started_at": "2020-01-01T00:00:00+00:00",
        "total_child_runs": 1,
    }
    child = {
        "child_run_id": "v5child_stale",
        "status": "running",
        "execution_status": "running",
    }
    writes = []
    monkeypatch.setattr(v5_formal_scheduler, "read_group", lambda _g: dict(group))
    monkeypatch.setattr(v5_formal_scheduler, "worker_active", lambda _g: False)
    monkeypatch.setattr(v5_formal_scheduler, "children", lambda _g: [dict(child)])
    monkeypatch.setattr(v5_formal_scheduler, "write_child", lambda _g, c: None)
    monkeypatch.setattr(v5_formal_scheduler, "write_group", lambda g: writes.append(dict(g)))
    monkeypatch.setattr(v5_formal_scheduler.v5_real_cluster_runner, "reap_persisted_supervisors", lambda _g: [])

    observed = v5_formal_scheduler.reconcile_stale_group("v5grp_stale")
    assert observed["status"] == "interrupted"
    assert "active_child_run_id" not in observed
    assert "active_child_started_at" not in observed
    assert writes[-1]["status"] == "interrupted"


def test_bundle_status_ready_only_after_verified_zip(tmp_path: Path, monkeypatch):
    group = {"run_group_id": "v5grp_bundle"}
    states = []
    monkeypatch.setattr(v5_formal_scheduler, "write_group", lambda g: states.append(g.get("bundle_status")))

    def fake_build(_directory, _group, output_path=None):
        assert output_path is not None
        with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("run_group.json", "{}")
            zf.writestr("reproducibility_manifest.json", "{}")
            zf.writestr("artifact_manifest.json", "{}")
        return output_path

    monkeypatch.setattr(v5_formal_scheduler, "build_bundle", fake_build)
    assert v5_formal_scheduler._try_build_bundle(tmp_path, group) is True
    assert states[0] == "building"
    assert states[-1] == "ready"
    bundle = tmp_path / "artifacts.zip"
    assert bundle.is_file()
    with zipfile.ZipFile(bundle) as zf:
        assert zf.testzip() is None
