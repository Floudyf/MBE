from copy import deepcopy

from backend.app.services.v3_profile_loader import load_profile_store
from backend.app.services.v3_profile_validator import validate_experiment_profile


def test_v33_metatrack_smoke_remains_runnable_on_role_separated_chain():
    store = load_profile_store()
    profile = store.experiments["metatrack_go_backed_ablation_smoke"]

    result = validate_experiment_profile(profile, store)

    assert profile["chain_profile"] == "single_chain_research_default"
    assert result["valid"] is True
    assert result["runnable"] is True


def test_role_separated_metatrack_fairness_still_rejects_changed_seed():
    store = load_profile_store()
    profile = deepcopy(store.experiments["metatrack_go_backed_ablation_smoke"])
    profile["fairness"]["same_seed"] = False

    result = validate_experiment_profile(profile, store)

    assert result["valid"] is False
    assert "MetaTrack fairness requires same_seed=true" in result["errors"]


def test_fabric_and_metaflow_profiles_remain_not_runnable():
    store = load_profile_store()

    for profile_id in ("fabric_validation_profile_preview", "metaflow_dual_chain_profile_preview"):
        result = validate_experiment_profile(store.experiments[profile_id], store)
        assert result["valid"] is True
        assert result["runnable"] is False
