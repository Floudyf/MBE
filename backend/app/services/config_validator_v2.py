from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from backend.app.services.plugin_registry import PluginRegistry

ROOT = Path(__file__).resolve().parents[3]
PLANNED_TOPOLOGY = ROOT / "configs/topologies/v2_dual_chain_planned.yaml"

COMPONENT_TYPES = ("topology", "trace_source", "workload", "routing", "execution", "commit", "cross_chain_protocol")
DEFAULT_SELECTION = {
    "version": "v2",
    "topology": "single_chain",
    "trace_source": "synthetic",
    "workload": "asset_hotspot",
    "routing": "hash",
    "execution": "serial",
    "commit": "normal_commit",
    "cross_chain_protocol": "disabled",
}
TRACE_LABELS = {
    "synthetic": "synthetic_replay",
    "existing_trace": "existing_trace_replay",
    "fabric_chain_backed_trace": "fabric_chain_backed_trace_replay",
    "public_chain_imported_trace": "public_chain_imported_trace_semantic_unknown",
}


def validate_selection(payload: dict[str, Any], registry: PluginRegistry) -> dict[str, Any]:
    selection = {**DEFAULT_SELECTION, **{key: value for key, value in payload.items() if value is not None}}
    reasons: list[str] = []
    warnings: list[str] = []
    blocked_by: list[str] = []
    selected_plugins: list[dict[str, str]] = []
    resolved_components: list[dict[str, Any]] = []
    status = "runnable"

    plugins: dict[str, dict[str, Any]] = {}
    for plugin_type in COMPONENT_TYPES:
        name = str(selection.get(plugin_type, ""))
        try:
            plugin = registry.get_plugin(plugin_type, name)
        except KeyError:
            return _result(selection, "invalid", False, selected_plugins, resolved_components, "synthetic_replay", [f"Unknown plugin {plugin_type}/{name}."], warnings, [f"{plugin_type}:{name}"])
        plugins[plugin_type] = plugin
        selected_plugins.append({"type": plugin_type, "name": name, "status": plugin["status"]})
        resolved_components.append({"type": plugin_type, "name": name, "status": plugin["status"], "reason": plugin["reason"]})

    topology = str(selection["topology"])
    trace_source = str(selection["trace_source"])
    cross_chain_protocol = str(selection["cross_chain_protocol"])
    data_truth_label = TRACE_LABELS.get(trace_source, "synthetic_replay")
    if topology in {"dual_chain", "cross_chain_replay", "multi_chain"}:
        data_truth_label = "planned_cross_chain_replay"

    if payload.get("force_run") is True and (topology != "single_chain" or any(plugin["status"] == "planned" for plugin in plugins.values())):
        return _result(selection, "invalid", False, selected_plugins, resolved_components, data_truth_label, ["Planned configuration was requested as runnable."], warnings, ["planned_config_force_run"])

    if topology in {"dual_chain", "cross_chain_replay", "multi_chain"}:
        planned_blockers = [f"{plugin['type']}:{plugin['name']}" for plugin in plugins.values() if plugin["status"] == "planned"]
        if f"topology:{topology}" not in planned_blockers:
            planned_blockers.insert(0, f"topology:{topology}")
        return _result(selection, "planned", False, selected_plugins, resolved_components, data_truth_label, ["Selected topology is a valid V2 direction, but its replay substrate is planned and must not run in V2.1."], warnings, planned_blockers)

    if topology == "single_chain" and cross_chain_protocol != "disabled":
        return _result(selection, "invalid", False, selected_plugins, resolved_components, data_truth_label, ["Cross-chain protocol cannot be enabled for a single_chain runnable preview."], warnings, [f"cross_chain_protocol:{cross_chain_protocol}"])

    trace_capabilities = set(plugins["trace_source"]["provides"]["capabilities"])
    for plugin_type, plugin in plugins.items():
        if topology not in plugin["compatible_topologies"]:
            return _result(selection, "invalid", False, selected_plugins, resolved_components, data_truth_label, [f"{plugin_type}/{plugin['name']} is incompatible with topology {topology}."], warnings, [f"{plugin_type}:{plugin['name']}"])
        if trace_source not in plugin["compatible_trace_sources"]:
            if trace_source == "public_chain_imported_trace" and plugin_type in {"routing", "execution", "commit"}:
                continue
            return _result(selection, "invalid", False, selected_plugins, resolved_components, data_truth_label, [f"{plugin_type}/{plugin['name']} is incompatible with trace source {trace_source}."], warnings, [f"{plugin_type}:{plugin['name']}"])
        for forbidden in plugin["forbidden_with"]:
            if selection.get("topology") == forbidden or selection.get("trace_source") == forbidden:
                return _result(selection, "invalid", False, selected_plugins, resolved_components, data_truth_label, [f"{plugin_type}/{plugin['name']} is forbidden with {forbidden}."], warnings, [forbidden])
        missing_capabilities = [capability for capability in plugin["requires"]["capabilities"] if capability not in trace_capabilities]
        if missing_capabilities and trace_source != "public_chain_imported_trace":
            return _result(selection, "invalid", False, selected_plugins, resolved_components, data_truth_label, [f"{plugin_type}/{plugin['name']} requires trace capabilities {missing_capabilities}."], warnings, missing_capabilities)

    if trace_source == "public_chain_imported_trace":
        warnings.append("public_chain_imported_trace is semantic_unknown and lacks reliable access_list, delta, and commutative/update_type semantics by default.")
        if selection["routing"] == "co_access":
            status = "experimental"
            reasons.append("co_access needs reliable access_list semantics; public-chain imported trace cannot guarantee them by default.")
        if selection["commit"] == "hot_update_aggregation":
            status = "invalid"
            reasons.append("hot_update_aggregation requires reliable commutative_update and update_type semantics, which public-chain imported trace does not provide by default.")
            blocked_by.extend(["commutative_update", "update_type"])
        elif status != "experimental":
            status = "experimental"
            reasons.append("public-chain imported trace can be previewed only with semantic_unknown limitations.")

    planned_plugins = [f"{plugin['type']}:{plugin['name']}" for plugin in plugins.values() if plugin["status"] == "planned"]
    if planned_plugins and status != "invalid":
        status = "planned"
        reasons.append("Selected direction is valid but depends on planned V2 components.")
        blocked_by.extend(planned_plugins)

    experimental_plugins = [f"{plugin['type']}:{plugin['name']}" for plugin in plugins.values() if plugin["status"] == "experimental"]
    if experimental_plugins and status == "runnable":
        status = "experimental"
        warnings.append(f"Selected experimental plugins: {', '.join(experimental_plugins)}.")

    if status == "runnable":
        reasons.append("All selected plugins are runnable, compatible, and within V1/V2 boundaries.")

    return _result(selection, status, status == "runnable", selected_plugins, resolved_components, data_truth_label, reasons, warnings, blocked_by)


def validate_planned_topology_file(path: Path = PLANNED_TOPOLOGY) -> dict[str, Any]:
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    status = str(document.get("status", ""))
    runnable = bool(document.get("runnable", False))
    is_planned = document.get("version") == "v2" and document.get("topology") == "dual_chain" and status == "planned" and runnable is False
    return {
        "path": str(path),
        "status": "planned" if is_planned else "invalid",
        "runnable": False,
        "reason": document.get("reason", "planned topology must not be runnable"),
        "blocked_by": ["v2_dual_chain_planned"] if is_planned else ["invalid_planned_topology_declaration"],
    }


def _result(
    selection: dict[str, Any],
    status: str,
    runnable: bool,
    selected_plugins: list[dict[str, str]],
    resolved_components: list[dict[str, Any]],
    data_truth_label: str,
    reasons: list[str],
    warnings: list[str],
    blocked_by: list[str],
) -> dict[str, Any]:
    return {
        "status": status,
        "runnable": runnable,
        "stage": "V2.1",
        "topology": selection.get("topology", ""),
        "selected_plugins": selected_plugins,
        "resolved_components": resolved_components,
        "data_truth_label": data_truth_label,
        "reasons": reasons,
        "warnings": warnings,
        "blocked_by": blocked_by,
    }
