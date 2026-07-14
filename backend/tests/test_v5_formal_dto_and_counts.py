from backend.app.services.v5_formal_dto import child_detail, child_summary, group_detail


def _child():
    return {"child_run_id": "child", "run_group_id": "group", "status": "completed", "attempt": 2, "comparison_group_id": "cmp", "formal_plan_config_id": "plan", "method_config_id": "method", "changed_plugin_categories": ["routing"], "result": {"run_id": "run", "status": "completed", "output_dir": "C:/secret", "stdout": "secret", "stderr": "secret", "summary": {"finality_evidence": {"terminal_unique_tx_count": 1}, "path": "C:/secret", "command": "secret", "environment": "secret"}, "artifacts": [{"name": "finality.json", "size_bytes": 1, "truth_category": "runtime", "download_url": "/safe", "path": "C:/secret"}]}}


def test_child_summary_preserves_identity_and_finality_but_not_process_secrets():
    body = child_summary(_child())
    assert {"attempt", "comparison_group_id", "formal_plan_config_id", "method_config_id", "changed_plugin_categories"} <= body.keys()
    assert body["result"]["summary"]["finality_evidence"]["terminal_unique_tx_count"] == 1
    assert "path" not in body["result"]["summary"]
    assert "output_dir" not in body["result"] and "stdout" not in body["result"]


def test_child_detail_keeps_only_safe_artifact_fields():
    artifact = child_detail(_child())["result"]["artifacts"][0]
    assert artifact == {"name": "finality.json", "size_bytes": 1, "truth_category": "runtime", "download_url": "/safe"}


def test_group_detail_never_exposes_scheduler_internals():
    body = group_detail({"run_group_id": "group", "worker_pid": 1, "bundle_path": "C:/secret", "retry_attempt": 2, "plan": {"name": "safe"}}, [_child()])
    assert not ({"worker_pid", "bundle_path", "retry_attempt"} & body["group"].keys())
