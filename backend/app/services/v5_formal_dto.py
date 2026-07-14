from __future__ import annotations

from copy import deepcopy


_SUMMARY_KEYS = ("run_group_id", "status", "execution_backend", "runtime_truth", "created_at", "updated_at", "finished_at", "total_child_runs", "completed_child_runs", "failed_child_runs")
_CHILD_KEYS = ("child_run_id", "run_group_id", "suite_type", "method", "method_id", "method_name", "method_config_id", "formal_plan_config_id", "method_role", "changed_plugin_categories", "topology_point", "workload_point", "fault_point", "seed", "repeat_index", "attempt", "comparison_group_id", "scan_variable", "scan_value", "estimated_processes", "estimated_transactions", "execution_backend", "status", "error", "paper_candidate", "metrics")


def group_summary(group: dict, *, children: list[dict] | None = None) -> dict:
    plan = group.get("plan") if isinstance(group.get("plan"), dict) else {}
    methods = plan.get("methods") if isinstance(plan.get("methods"), list) else []
    body = {key: deepcopy(group.get(key)) for key in _SUMMARY_KEYS}
    if children is not None:
        body["failed_child_runs"] = sum(item.get("status") in {"failed", "blocked"} for item in children)
    body.update({"plan_name": plan.get("name", ""), "suite_names": list(plan.get("suites") or []), "method_names": [item.get("display_name", item.get("method_id", "")) for item in methods if isinstance(item, dict)], "method_ids": [item.get("method_id", "") for item in methods if isinstance(item, dict)], "aggregate": deepcopy(group.get("aggregate")), "source_label": plan.get("source_label", "user"), "tags": list(plan.get("tags") or []), "is_test": plan.get("source_label") == "e2e" or "e2e" in (plan.get("tags") or [])})
    return body


def group_detail(group: dict, children: list[dict]) -> dict:
    body = group_summary(group, children=children)
    body.update({"plan_config_id": group.get("plan_config_id"), "plan": deepcopy(group.get("plan")), "cancel_requested": bool(group.get("cancel_requested", False))})
    return {"group": body, "children": [child_summary(item) for item in children]}


def child_summary(child: dict) -> dict:
    body = {key: deepcopy(child.get(key)) for key in _CHILD_KEYS}
    result = child.get("result") if isinstance(child.get("result"), dict) else {}
    body["result"] = {"run_id": result.get("run_id"), "status": result.get("status"), "summary": _safe_summary(result.get("summary")), "no_fallback": result.get("no_fallback")}
    return body


def child_detail(child: dict) -> dict:
    body = child_summary(child)
    result = child.get("result") if isinstance(child.get("result"), dict) else {}
    body["result"]["artifacts"] = [{key: item.get(key) for key in ("name", "size_bytes", "truth_category", "download_url")} for item in result.get("artifacts", []) if isinstance(item, dict)]
    return body


def _safe_summary(value):
    if isinstance(value, dict):
        return {key: _safe_summary(item) for key, item in value.items() if key not in {"output_dir", "stdout", "stderr", "bundle_path", "worker_pid", "path", "command", "environment"}}
    if isinstance(value, list):
        return [_safe_summary(item) for item in value]
    return deepcopy(value)
