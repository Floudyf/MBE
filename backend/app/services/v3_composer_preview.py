from __future__ import annotations

from typing import Any

from backend.app.services.v3_experiment_templates import DEFAULT_TEMPLATE_ID, STANDARD_MODULE_ORDER, get_template
from backend.app.services.v3_profile_loader import V3ProfileStore

MODULE_EDGES = [
    ("Workload", "TxPool"),
    ("TxPool", "BlockProducer"),
    ("BlockProducer", "Consensus"),
    ("Consensus", "CommitteeEpoch"),
    ("CommitteeEpoch", "Routing"),
    ("Routing", "Execution"),
    ("Execution", "StateAccess"),
    ("StateAccess", "StateStorage"),
    ("StateStorage", "Commit"),
    ("Commit", "MetricsReport"),
]
DISPLAY_NAMES = {
    "Workload": "Workload",
    "TxPool": "TxPool",
    "BlockProducer": "Block Producer",
    "Consensus": "Consensus",
    "CommitteeEpoch": "Committee / Epoch",
    "Routing": "Sharding / Routing",
    "Execution": "Execution",
    "StateAccess": "State Access",
    "StateStorage": "State Storage",
    "Commit": "Commit",
    "MetricsReport": "Metrics / Report",
}
MODULE_CLASS_MAP = {
    "TxPool": "TxPoolPlugin",
    "BlockProducer": "BlockProducer",
    "Consensus": "ConsensusPlugin",
    "Routing": "ShardingPlugin",
    "Execution": "ExecutionSchedulerPlugin",
    "StateAccess": "StateAccessPlugin",
    "Commit": "CommitPlugin",
    "MetricsReport": "MetricsPlugin",
}
BASE_PLUGINS = {
    "TxPool": "fifo_pool",
    "BlockProducer": "time_or_count_block_producer",
    "Consensus": "simple_leader",
    "StateStorage": "hash_state_storage",
    "MetricsReport": "basic_metrics",
}
MODULE_METRICS = {
    "Routing": ["remote_state_fetch_count", "state_locality_ratio"],
    "Execution": ["fast_track_count", "conservative_track_count", "conflict_count"],
    "StateAccess": ["remote_fetch_count", "remote_state_fetch_count"],
    "Commit": ["aggregated_update_count", "aggregation_ratio"],
    "MetricsReport": ["throughput_tps", "avg_latency_ms", "p95_latency_ms", "p99_latency_ms"],
}
MODULE_ARTIFACTS = {
    "Routing": ["tx_results.csv", "metatrack_mechanism_metrics.csv"],
    "Execution": ["tx_results.csv", "metatrack_mechanism_metrics.csv"],
    "StateAccess": ["tx_results.csv", "metatrack_mechanism_metrics.csv"],
    "StateStorage": ["state_commit_log.csv"],
    "Commit": ["state_commit_log.csv", "metatrack_mechanism_metrics.csv"],
    "MetricsReport": ["summary.csv", "summary.json", "report.md"],
}


def build_composer_preview(profile: dict[str, Any], store: V3ProfileStore) -> dict[str, Any]:
    template_id = _template_id(profile)
    template = get_template(template_id)
    plugin_matrix = _plugin_matrix(profile, store)
    modules = _modules(template, plugin_matrix)
    fairness_scope = _fairness_scope(profile, template)
    runnable = bool(profile.get("experiment", {}).get("runnable")) and bool(template.get("runnable")) and not bool(template.get("preview_only"))
    return {
        "view": "single_chain",
        "template_id": template_id,
        "chain_mode": template.get("chain_mode", "single_chain"),
        "modules": modules,
        "edges": [{"source": source, "target": target} for source, target in MODULE_EDGES],
        "plugin_matrix": plugin_matrix,
        "fairness_scope": fairness_scope,
        "truth_labels": {
            "truth_label": profile.get("experiment", {}).get("truth_label", ""),
            "backend_type": profile.get("experiment", {}).get("backend_type", ""),
            "runtime_mode": profile.get("experiment", {}).get("runtime_mode", ""),
        },
        "runnable": runnable,
    }


def _template_id(profile: dict[str, Any]) -> str:
    explicit = profile.get("experiment_template")
    if explicit:
        return str(explicit)
    exp_type = profile.get("experiment", {}).get("type", "")
    if "metatrack" in str(exp_type):
        return "metatrack_ablation"
    return DEFAULT_TEMPLATE_ID


def _modules(template: dict[str, Any], plugin_matrix: list[dict[str, Any]]) -> list[dict[str, Any]]:
    module_status = template.get("module_status", {})
    allowed_plugins = template.get("allowed_plugins", {})
    modules = []
    for index, module_id in enumerate(STANDARD_MODULE_ORDER):
        status = module_status.get(module_id, "fixed")
        modules.append(
            {
                "module_id": module_id,
                "display_name": DISPLAY_NAMES.get(module_id, module_id),
                "plugin": _module_plugin(module_id, plugin_matrix),
                "status": status,
                "role": _module_role(status, module_id),
                "tags": _module_tags(module_id, status),
                "position": index + 1,
                "allowed_plugins": allowed_plugins.get(module_id, []),
                "metrics": MODULE_METRICS.get(module_id, []),
                "artifacts": MODULE_ARTIFACTS.get(module_id, []),
            }
        )
    return modules


def _plugin_matrix(profile: dict[str, Any], store: V3ProfileStore) -> list[dict[str, Any]]:
    rows = []
    sections = profile.get("plugin_profiles", {})
    for section_name in ("baselines", "proposed"):
        for plugin_id in sections.get(section_name, []):
            plugin_profile = store.plugins.get(plugin_id, {})
            module_plugins = plugin_profile.get("module_plugins") or _module_plugins_from_classes(plugin_profile.get("plugins", {}))
            tags = list(plugin_profile.get("tags", []))
            if section_name == "baselines" and "baseline" not in tags:
                tags.append("baseline")
            if section_name == "proposed" and "proposed" not in tags:
                tags.append("proposed")
            rows.append(
                {
                    "method_id": plugin_id,
                    "label": plugin_profile.get("label", _label(plugin_id)),
                    "role": section_name[:-1] if section_name.endswith("s") else section_name,
                    "module_plugins": module_plugins,
                    "tags": tags,
                }
            )
    return rows


def _module_plugins_from_classes(plugins: dict[str, Any]) -> dict[str, str]:
    result = {}
    for module_id, class_name in MODULE_CLASS_MAP.items():
        if class_name in plugins:
            result[module_id] = str(plugins[class_name])
    return result


def _module_plugin(module_id: str, plugin_matrix: list[dict[str, Any]]) -> str:
    values = {
        row.get("module_plugins", {}).get(module_id)
        for row in plugin_matrix
        if row.get("module_plugins", {}).get(module_id)
    }
    if not values:
        return BASE_PLUGINS.get(module_id, "")
    if len(values) == 1:
        return next(iter(values))
    return "varies"


def _module_role(status: str, module_id: str) -> str:
    if status == "variable":
        return "research_variable"
    if status == "planned":
        return "planned"
    if status == "disabled":
        return "disabled"
    if status == "output" or module_id == "MetricsReport":
        return "output"
    if module_id == "Workload":
        return "input"
    return "environment"


def _module_tags(module_id: str, status: str) -> list[str]:
    tags = []
    if status == "variable":
        tags.extend(["MetaTrack", "proposed"])
    if status == "planned":
        tags.append("planned")
    if module_id == "MetricsReport":
        tags.append("output")
    return tags


def _fairness_scope(profile: dict[str, Any], template: dict[str, Any]) -> dict[str, Any]:
    fairness = profile.get("fairness", {})
    return {
        "template_id": template.get("template_id"),
        "variable_modules": template.get("variable_modules", []),
        "fixed_modules": template.get("fixed_modules", []),
        "disabled_modules": template.get("disabled_modules", []),
        "planned_modules": template.get("planned_modules", []),
        "output_modules": template.get("output_modules", []),
        "only_variable_modules_may_differ": template.get("fairness", {}).get("only_variable_modules_may_differ"),
        "fixed_modules_must_match": template.get("fairness", {}).get("fixed_modules_must_match"),
        "planned_modules_not_runnable": template.get("fairness", {}).get("planned_modules_not_runnable"),
        "same_workload": fairness.get("same_workload"),
        "same_seed": fairness.get("same_seed"),
        "same_tx_count": fairness.get("same_tx_count"),
        "same_chain_profile": fairness.get("same_chain_profile"),
        "same_submit_rate": fairness.get("same_submit_rate"),
        "same_block_config": fairness.get("same_block_config"),
        "same_consensus_config": fairness.get("same_consensus_config"),
        "same_network_profile": fairness.get("same_network_profile"),
        "same_calibration_profile": fairness.get("same_calibration_profile"),
    }


def _label(method_id: str) -> str:
    return " ".join(part.capitalize() for part in method_id.split("_"))
