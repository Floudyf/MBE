from __future__ import annotations

import csv
import json
from pathlib import Path


_FIXED = (
    "seed",
    "repeat_index",
    "execution_backend",
    "estimated_transactions",
    "workload_snapshot_digest",
    "topology_snapshot_digest",
    "fault_snapshot_digest",
    "fairness_key",
    "block_size",
    "block_interval_ms",
)

_SEMANTIC_FIXED = (
    "state_access_semantics",
    "state_home_mapping_policy",
    "remote_fetch_policy",
    "remote_writeback_policy",
    "proof_policy",
    "legacy_cross_shard_protocol",
    "measurement_boundary",
)


_SINGLE_SHARD_STATEFUL_PERFORMANCE_CLASSES = {
    "stateful_local_legacy_v1",
    "nezha_cg_johnson_retryable_v4",
    "nezha_acg_hs_retryable_v2",
    "bsx_deterministic_coloring_serializable_v1",
    "batch_si_common_batch_snapshot_v1",
    "groundhog_typed_commutative_snapshot_v1",
}


def _performance_contract_class(row: dict) -> str:
    topology = row.get("topology_point") if isinstance(row.get("topology_point"), dict) else {}
    shards = topology.get("shards")
    try:
        shards = int(shards)
    except (TypeError, ValueError):
        shards = 0
    semantic_class = str(row.get("comparison_semantics_class") or "custom_unknown")
    if (
        shards == 1
        and semantic_class in _SINGLE_SHARD_STATEFUL_PERFORMANCE_CLASSES
        and row.get("state_home_mapping_policy") == "execution_shard_local_namespace"
        and row.get("remote_fetch_policy") == "none"
        and row.get("remote_writeback_policy") == "none"
    ):
        return "single_shard_stateful_eventual_completion_v1"
    if semantic_class != "custom_unknown":
        return f"within_semantic:{semantic_class}"
    return "unsupported"


def validate(rows: list[dict]) -> tuple[list[dict], dict]:
    checked: list[dict] = []
    groups: dict[str, list[dict]] = {}
    for row in rows:
        groups.setdefault(row["comparison_group_id"], []).append(row)
    failures: list[str] = []
    incomparable_groups: list[dict] = []
    for group_id, items in groups.items():
        baseline = items[0]
        semantic_classes = sorted({str(item.get("comparison_semantics_class") or "custom_unknown") for item in items})
        semantic_baselines: dict[str, dict] = {}
        for item in items:
            semantic_class = str(item.get("comparison_semantics_class") or "custom_unknown")
            semantic_baselines.setdefault(semantic_class, item)
        contract_classes = sorted({_performance_contract_class(item) for item in items})
        same_internal_semantics = len(semantic_classes) == 1 and semantic_classes[0] != "custom_unknown"
        common_external_contract = (
            len(contract_classes) == 1
            and contract_classes[0] == "single_shard_stateful_eventual_completion_v1"
        )
        performance_valid = same_internal_semantics or common_external_contract
        if not performance_valid:
            incomparable_groups.append({
                "comparison_group_id": group_id,
                "semantic_classes": semantic_classes,
                "performance_contract_classes": contract_classes,
            })
        for row in items:
            blockers = list(row.get("blockers", []))
            for field in _FIXED:
                if row.get(field) != baseline.get(field):
                    blockers.append(f"fairness mismatch: {field}")
            semantic_class = str(row.get("comparison_semantics_class") or "custom_unknown")
            semantic_baseline = semantic_baselines[semantic_class]
            for field in _SEMANTIC_FIXED:
                if row.get(field) != semantic_baseline.get(field):
                    blockers.append(f"semantic fairness mismatch: {field}")
            performance_contract_class = _performance_contract_class(row)
            internal_semantics_differ = len(semantic_classes) > 1
            warning = (
                "internal execution semantics differ; end-to-end performance comparison is allowed under the common single-shard eventual-completion contract"
                if performance_valid and internal_semantics_differ
                else "" if performance_valid
                else "execution semantics differ; direct performance uplift is invalid"
            )
            if blockers:
                row = {**row, "runnable": False, "blockers": blockers, "status": "blocked"}
                failures.append(f"{group_id}:{row.get('method_config_id')}")
            else:
                row = {**row, "status": "queued"}
            row = {
                **row,
                "performance_comparison_valid": performance_valid,
                "direct_cross_semantic_performance_comparison_valid": performance_valid,
                "performance_contract_class": performance_contract_class,
                "comparison_internal_semantics_differ": internal_semantics_differ,
                "comparison_semantic_classes": semantic_classes,
                "comparison_warning": warning,
            }
            checked.append(row)
    return checked, {
        "passed": not failures,
        "failures": failures,
        "row_count": len(checked),
        "performance_comparison_valid": not incomparable_groups,
        "direct_cross_semantic_performance_comparison_valid": not incomparable_groups,
        "incomparable_groups": incomparable_groups,
        "semantic_classes": sorted({str(row.get("comparison_semantics_class") or "custom_unknown") for row in checked}),
        "performance_contract_classes": sorted({str(row.get("performance_contract_class") or "unsupported") for row in checked}),
    }


def write_artifacts(root: Path, rows: list[dict], result: dict) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "fairness_validation.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    fields = [
        "child_run_id",
        "suite_type",
        "method_config_id",
        "fairness_key",
        "comparison_group_id",
        "seed",
        "repeat_index",
        "execution_backend",
        "estimated_transactions",
        "block_size",
        "block_interval_ms",
        "estimated_block_count",
        "workload_snapshot_digest",
        "topology_snapshot_digest",
        "fault_snapshot_digest",
        "method_snapshot_digest",
        "method_config_snapshot_digest",
        "comparison_semantics_class",
        "state_access_semantics",
        "state_home_mapping_policy",
        "remote_fetch_policy",
        "remote_writeback_policy",
        "proof_policy",
        "legacy_cross_shard_protocol",
        "measurement_boundary",
        "performance_contract_class",
        "comparison_internal_semantics_differ",
        "performance_comparison_valid",
        "direct_cross_semantic_performance_comparison_valid",
        "comparison_semantic_classes",
        "comparison_warning",
        "runnable",
        "status",
        "blockers",
    ]
    with (root / "fairness_matrix.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields); writer.writeheader()
        for row in rows: writer.writerow({key: json.dumps(row.get(key)) if isinstance(row.get(key), (list, dict)) else row.get(key, "") for key in fields})
    with (root / "formal_matrix.csv").open("w", newline="", encoding="utf-8") as handle:
        fields = list(rows[0].keys()) if rows else ["child_run_id"]
        writer = csv.DictWriter(handle, fieldnames=fields); writer.writeheader()
        for row in rows: writer.writerow({key: json.dumps(value) if isinstance(value, (list, dict)) else value for key, value in row.items()})
