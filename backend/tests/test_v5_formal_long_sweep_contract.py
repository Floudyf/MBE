from pathlib import Path


def test_long_sweep_source_contract() -> None:
    root = Path(__file__).resolve().parents[2]
    scheduler = (root / "backend/app/services/v5_formal_scheduler.py").read_text(encoding="utf-8")
    runner = (root / "backend/app/services/v5_real_cluster_runner.py").read_text(encoding="utf-8")
    api = (root / "backend/app/api/v5_formal_experiments.py").read_text(encoding="utf-8")
    dto = (root / "backend/app/services/v5_formal_dto.py").read_text(encoding="utf-8")
    frontend_api = (root / "frontend/src/api.ts").read_text(encoding="utf-8")
    page = (root / "frontend/src/pages/V5FormalRunPage.tsx").read_text(encoding="utf-8")

    assert '"child_wall_timeout_seconds": 1800' in scheduler
    assert '"fixed_workload_completion_required_for_formal_tps": True' in scheduler
    assert 'return _timed_out_run_result' in runner
    assert '"formal_eligibility": False' in runner
    assert '"diagnostic_only": True' in runner
    assert 'supervisor_process.json' in runner
    assert 'reap_persisted_supervisors' in runner
    assert '/resume-unfinished' in api
    assert 'reconcile_stale_group' in api
    assert '"not_started_child_runs"' in dto
    assert 'resumeV5FormalRunGroup' in frontend_api
    assert '继续未完成实验' in page
    assert '超时保留诊断但不进入正式 TPS' in page
    assert '"cancelled", "interrupted"' in page
