from copy import deepcopy

from backend.app.services.v3_profile_loader import load_profile_store
from backend.app.services.v3_profile_validator import validate_experiment_profile


def test_template_fairness_scope_matches_metatrack_template():
    store = load_profile_store()
    result = validate_experiment_profile(store.experiments["metatrack_go_backed_ablation_smoke"], store)

    assert result["valid"] is True
    assert result["runnable"] is True


def test_fixed_module_diff_is_rejected_by_template_guard():
    store = load_profile_store()
    profile = deepcopy(store.experiments["metatrack_go_backed_ablation_smoke"])
    store.plugins["baseline_hash_only"]["module_plugins"]["Consensus"] = "simple_leader"
    store.plugins["full_MetaTrack"]["module_plugins"]["Consensus"] = "other_consensus"

    result = validate_experiment_profile(profile, store)

    assert result["valid"] is False
    assert "Consensus is fixed by template metatrack_ablation and cannot differ across methods." in result["errors"]


def test_planned_module_runnable_is_rejected_by_template_guard():
    store = load_profile_store()
    profile = deepcopy(store.experiments["metatrack_go_backed_ablation_smoke"])
    profile["module_runnable"] = {"CommitteeEpoch": True}

    result = validate_experiment_profile(profile, store)

    assert result["valid"] is False
    assert "CommitteeEpoch is planned in current stage and cannot be runnable." in result["errors"]


def test_disabled_module_enabled_is_rejected_by_template_guard():
    store = load_profile_store()
    profile = deepcopy(store.experiments["metatrack_go_backed_ablation_smoke"])
    profile["experiment_template"] = "consensus_only"
    profile["variable_modules"] = ["Consensus"]
    profile["fixed_modules"] = ["Workload", "TxPool", "BlockProducer", "Execution", "StateAccess", "StateStorage", "Commit"]
    profile["disabled_modules"] = ["Routing"]
    profile["planned_modules"] = ["CommitteeEpoch"]

    result = validate_experiment_profile(profile, store)

    assert result["valid"] is False
    assert "Routing is disabled by template consensus_only and cannot be enabled." in result["errors"]


def test_fabric_and_metaflow_remain_planned_not_runnable_under_v332():
    store = load_profile_store()
    for profile_id in ("fabric_validation_profile_preview", "metaflow_dual_chain_profile_preview"):
        result = validate_experiment_profile(store.experiments[profile_id], store)
        assert result["valid"] is True
        assert result["runnable"] is False
