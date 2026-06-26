from __future__ import annotations

from typing import Any

from backend.app.services.v3_profile_loader import V3ProfileStore, get_profile, load_profile_store, profile_inventory
from backend.app.services.v3_profile_validator import validate_any_profile


def preview_profile(profile_type: str, profile_id: str, store: V3ProfileStore | None = None) -> dict[str, Any]:
    store = store or load_profile_store()
    profile = get_profile(profile_type, profile_id, store)
    validation = validate_any_profile(profile, store)
    return _preview_payload(profile, validation, store)


def preview_all_profiles(store: V3ProfileStore | None = None) -> dict[str, Any]:
    store = store or load_profile_store()
    items = []
    for profile_id in sorted(store.chains):
        items.append(preview_profile("chain_profile", profile_id, store))
    for profile_id in sorted(store.plugins):
        items.append(preview_profile("plugin_profile", profile_id, store))
    for profile_id in sorted(store.experiments):
        items.append(preview_profile("experiment_profile", profile_id, store))
    return {"stage": "V3.1", "inventory": profile_inventory(store), "items": items}


def _preview_payload(profile: dict[str, Any], validation: dict[str, Any], store: V3ProfileStore) -> dict[str, Any]:
    profile_type = validation["profile_type"]
    referenced = _referenced_profiles(profile)
    plugin_summary = _plugin_summary(profile, store)
    fairness_summary = _fairness_summary(profile)
    expected_outputs = profile.get("outputs", {}).get("expected", []) if isinstance(profile.get("outputs"), dict) else []
    truth_label = profile.get("chain", {}).get("truth_label") or profile.get("experiment", {}).get("truth_label") or ""
    backend_type = validation.get("backend_type") or profile.get("experiment", {}).get("backend_type") or profile.get("capability", {}).get("backend_type", "")
    return {
        "profile_id": validation["profile_id"],
        "profile_type": profile_type,
        "declared_stage": profile.get("stage", validation.get("declared_stage", "")),
        "status": validation["status"],
        "valid": validation["valid"],
        "runnable": validation["runnable"],
        "backend_type": backend_type,
        "truth_label": truth_label,
        "referenced_profiles": referenced,
        "plugin_summary": plugin_summary,
        "fairness_summary": fairness_summary,
        "blocking_reasons": validation["blocking_reasons"],
        "warnings": validation["warnings"],
        "expected_outputs": expected_outputs,
        "execution": {
            "creates_run_id": False,
            "writes_runtime_artifacts": False,
            "starts_fabric": False,
            "starts_docker": False,
            "calls_go_executor": False,
        },
    }


def _referenced_profiles(profile: dict[str, Any]) -> dict[str, Any]:
    refs: dict[str, Any] = {}
    if profile.get("chain_profile"):
        refs["chain_profile"] = profile["chain_profile"]
    if profile.get("chain_profiles"):
        refs["chain_profiles"] = profile["chain_profiles"]
    if profile.get("plugin_profiles"):
        refs["plugin_profiles"] = profile["plugin_profiles"]
    return refs


def _plugin_summary(profile: dict[str, Any], store: V3ProfileStore) -> list[dict[str, Any]]:
    if profile.get("profile_type") == "plugin_profile":
        return [{"plugin_profile_id": profile.get("plugin_profile_id"), "domain": profile.get("domain"), "plugins": profile.get("plugins", {})}]
    summary = []
    for section, ids in profile.get("plugin_profiles", {}).items():
        for plugin_id in ids:
            plugin = store.plugins.get(plugin_id, {})
            summary.append({"role": section, "plugin_profile_id": plugin_id, "domain": plugin.get("domain"), "plugins": plugin.get("plugins", {})})
    return summary


def _fairness_summary(profile: dict[str, Any]) -> dict[str, Any]:
    fairness = profile.get("fairness", {})
    if not isinstance(fairness, dict):
        return {}
    return {
        "same_workload": fairness.get("same_workload"),
        "same_seed": fairness.get("same_seed"),
        "same_tx_count": fairness.get("same_tx_count"),
        "same_chain_profile": fairness.get("same_chain_profile"),
        "same_submit_rate": fairness.get("same_submit_rate"),
        "only_plugin_diff": fairness.get("only_plugin_diff"),
        "allowed_plugin_diff_classes": fairness.get("allowed_plugin_diff_classes", []),
    }
