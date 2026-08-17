from __future__ import annotations

from copy import deepcopy
from typing import Any

from pydantic import BaseModel, ConfigDict


# MBE_V5_RESULTS_UI_TRUTH_CN_FINAL_20260814_V5
_SUMMARY_KEYS = (
    "run_group_id", "status", "execution_backend", "runtime_truth", "created_at", "updated_at", "finished_at",
    "total_child_runs", "completed_child_runs", "failed_child_runs", "blocked_child_runs", "cancelled_child_runs",
    "timed_out_child_runs", "interrupted_child_runs", "not_started_child_runs",
    "worker_heartbeat_at", "active_child_run_id", "active_child_started_at", "interrupted_at", "interrupted_reason", "stale_supervisor_reap",
    "resume_requested_count", "resume_attempt", "resumed_at", "execution_policy",
)
_GROUP_TRUTH_KEYS = (
    "performance_comparison_valid",
    "direct_cross_semantic_performance_comparison_valid",
    "within_semantic_cohort_state_equivalence_valid",
    "pairwise_logical_state_equivalent",
    "fairness_validation",
    "state_equivalence_validation",
)
_CHILD_KEYS = (
    "child_run_id", "run_group_id", "suite_type", "method", "method_id", "method_name", "method_config_id",
    "formal_plan_config_id", "method_role", "changed_plugin_categories", "topology_point", "workload_point",
    "fault_point", "seed", "repeat_index", "attempt", "comparison_group_id", "scan_variable", "scan_value",
    "estimated_processes", "estimated_transactions", "execution_backend", "status", "execution_status",
    "artifact_status", "formal_eligibility", "execution_gate", "artifact_gate", "completion_gate",
    "artifact_contract_version", "missing_artifacts", "unexpected_artifacts", "error", "paper_candidate", "metrics",
    "comparison_semantics_class", "state_access_semantics", "performance_comparison_valid",
    "direct_cross_semantic_performance_comparison_valid", "within_semantic_cohort_state_equivalence_valid",
    "pairwise_logical_state_equivalent", "comparison_semantic_classes", "comparison_warning", "initial_state_digest",
    "state_home_mapping_digest", "global_final_state_digest", "individual_result_valid",
    "individual_result_validity_reasons", "comparison_eligibility_status",
)
_ARTIFACT_KEYS = (
    "name", "size_bytes", "truth_category", "artifact_role", "truth_scope", "producer", "schema_version", "sha256",
    "download_url",
)
_EVIDENCE_NAME_TOKENS = (
    "workload",
    "mapping",
    "finality",
    "state_root",
    "receipt",
    "fairness",
    "equivalence",
    "drain",
    "serial_equivalence",
    "plan_digest",
    "network_log",
    "network_metrics",
    "network_message_summary",
    "resource_usage",
    "resource_sampler",
    "mechanism",
    "block_execution",
    "proposal_selection",
    "scheduler_trace",
    "dependency_graph",
    "aggregation",
    "execution_plan",
    "aria",
    "batch_si",
    "groundhog",
    "block_stm",
)


class V5FormalChildResponse(BaseModel):
    """Public child result shape; extra historical fields remain compatible."""

    model_config = ConfigDict(extra="allow")
    child_run_id: str
    run_group_id: str
    status: str
    execution_status: str | None = None
    artifact_status: str | None = None
    formal_eligibility: bool | None = None
    execution_gate: dict[str, Any] | None = None
    artifact_gate: dict[str, Any] | None = None
    completion_gate: dict[str, Any] | None = None
    artifact_contract_version: int | None = None
    missing_artifacts: list[str] | None = None
    unexpected_artifacts: list[str] | None = None


class V5FormalRunGroupDetailResponse(BaseModel):
    model_config = ConfigDict(extra="allow")
    group: dict[str, Any]
    children: list[V5FormalChildResponse]


def group_summary(group: dict, *, children: list[dict] | None = None) -> dict:
    plan = group.get("plan") if isinstance(group.get("plan"), dict) else {}
    methods = plan.get("methods") if isinstance(plan.get("methods"), list) else []
    body = {key: deepcopy(group.get(key)) for key in _SUMMARY_KEYS}
    if children is not None:
        body["failed_child_runs"] = sum(item.get("status") == "failed" for item in children)
        body["blocked_child_runs"] = sum(item.get("status") == "blocked" for item in children)
        body["cancelled_child_runs"] = sum(item.get("status") == "cancelled" for item in children)
        body["timed_out_child_runs"] = sum(item.get("status") == "timed_out" for item in children)
        body["interrupted_child_runs"] = sum(item.get("status") == "interrupted" for item in children)
        materialized = {str(item.get("child_run_id") or "") for item in children if item.get("child_run_id")}
        body["not_started_child_runs"] = max(0, int(group.get("total_child_runs") or 0) - len(materialized))
    body.update(
        {
            "plan_name": plan.get("name", ""),
            "suite_names": list(plan.get("suites") or []),
            "method_names": [
                item.get("display_name", item.get("method_id", ""))
                for item in methods
                if isinstance(item, dict)
            ],
            "method_ids": [item.get("method_id", "") for item in methods if isinstance(item, dict)],
            "aggregate": deepcopy(group.get("aggregate")),
            "source_label": plan.get("source_label", "user"),
            "tags": list(plan.get("tags") or []),
            "is_test": plan.get("source_label") == "e2e" or "e2e" in (plan.get("tags") or []),
        }
    )
    return body


def group_detail(group: dict, children: list[dict]) -> dict:
    body = group_summary(group, children=children)
    body.update(
        {
            "plan_config_id": group.get("plan_config_id"),
            "formal_experiment_profile": deepcopy(group.get("formal_experiment_profile")),
            "plan": deepcopy(group.get("plan")),
            "cancel_requested": bool(group.get("cancel_requested", False)),
        }
    )
    for key in _GROUP_TRUTH_KEYS:
        body[key] = deepcopy(group.get(key))
    return {"group": body, "children": [child_summary(item) for item in children]}


def child_summary(child: dict) -> dict:
    body = {key: deepcopy(child.get(key)) for key in _CHILD_KEYS}
    result = child.get("result") if isinstance(child.get("result"), dict) else {}
    artifacts = [item for item in result.get("artifacts", []) if isinstance(item, dict)]
    body["runtime_artifact_count"] = len(artifacts)
    body["runtime_artifact_bytes"] = sum(_artifact_size(item) for item in artifacts)
    body["evidence_artifacts"] = _evidence_artifacts(child, result, artifacts)
    body["result"] = {
        "run_id": result.get("run_id"),
        "status": result.get("status"),
        "summary": _safe_summary(result.get("summary")),
        "no_fallback": result.get("no_fallback"),
    }
    return body


def child_detail(child: dict) -> dict:
    body = child_summary(child)
    result = child.get("result") if isinstance(child.get("result"), dict) else {}
    body["result"]["artifacts"] = [
        _public_artifact(item)
        for item in result.get("artifacts", [])
        if isinstance(item, dict)
    ]
    return body


def _public_artifact(item: dict) -> dict:
    return {key: deepcopy(item.get(key)) for key in _ARTIFACT_KEYS if key in item}


def _evidence_artifacts(child: dict, result: dict, artifacts: list[dict]) -> list[dict]:
    output: list[dict] = []
    method = child.get("method") if isinstance(child.get("method"), dict) else {}
    method_name = method.get("display_name") or child.get("method_name") or child.get("method_config_id")
    for item in artifacts:
        name = str(item.get("name") or "")
        lowered = name.lower()
        if not name or not any(token in lowered for token in _EVIDENCE_NAME_TOKENS):
            continue
        public = _public_artifact(item)
        public.update(
            {
                "child_run_id": child.get("child_run_id"),
                "method_id": child.get("method_config_id") or method.get("method_id"),
                "method_name": method_name,
                "run_id": result.get("run_id"),
            }
        )
        output.append(public)
    return output


def _artifact_size(item: dict) -> int:
    value = item.get("size_bytes")
    if isinstance(value, bool):
        return 0
    if isinstance(value, (int, float)):
        return max(0, int(value))
    return 0


def _safe_summary(value):
    if isinstance(value, dict):
        return {
            key: _safe_summary(item)
            for key, item in value.items()
            if key not in {"output_dir", "stdout", "stderr", "bundle_path", "worker_pid", "path", "command", "environment"}
        }
    if isinstance(value, list):
        return [_safe_summary(item) for item in value]
    return deepcopy(value)
