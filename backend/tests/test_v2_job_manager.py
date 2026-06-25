from pathlib import Path

from backend.app.services.job_manager import JobManager


def test_create_run_generates_unique_ids_and_persists_metadata(tmp_path: Path) -> None:
    manager = JobManager(tmp_path)

    first = manager.create_run("v1_custom_run", "v1_custom_interactive", "synthetic_replay")
    second = manager.create_run("v1_custom_run", "v1_custom_interactive", "existing_trace_replay")

    assert first["run_id"] != second["run_id"]
    assert first["run_id"].startswith("v2run_")
    assert "/" not in first["run_id"]
    assert "\\" not in first["run_id"]
    assert manager.get_run(first["run_id"])["status"] == "created"
    assert (tmp_path / first["run_id"] / "metadata.json").is_file()


def test_run_status_moves_from_created_to_running_to_completed(tmp_path: Path) -> None:
    manager = JobManager(tmp_path)
    run = manager.create_run("v1_custom_run", "v1_custom_interactive", "synthetic_replay")
    run_id = run["run_id"]

    running = manager.mark_running(run_id)
    (tmp_path / run_id / "summary.csv").write_text("tx_count\n1\n", encoding="utf-8")
    (tmp_path / run_id / "report.md").write_text("# report\n", encoding="utf-8")
    completed = manager.mark_completed(run_id)

    assert running["status"] == "running"
    assert completed["status"] == "completed"
    assert completed["summary_available"] is True
    assert completed["report_available"] is True
    assert completed["artifact_count"] == 2


def test_failed_run_saves_error_message(tmp_path: Path) -> None:
    manager = JobManager(tmp_path)
    run = manager.create_run("v1_custom_run", "v1_custom_interactive", "synthetic_replay")

    failed = manager.mark_failed(run["run_id"], "executor replay failed")

    assert failed["status"] == "failed"
    assert failed["status_message"] == "executor replay failed"
    assert manager.get_run(run["run_id"])["status"] == "failed"


def test_list_runs_and_latest_return_newest_first(tmp_path: Path) -> None:
    manager = JobManager(tmp_path)
    older = manager.create_run("v1_custom_run", "older", "synthetic_replay")
    newer = manager.create_run("v1_custom_run", "newer", "synthetic_replay")

    runs = manager.list_runs()

    assert runs[0]["run_id"] == newer["run_id"]
    assert runs[1]["run_id"] == older["run_id"]
    assert manager.get_latest_run()["run_id"] == newer["run_id"]
