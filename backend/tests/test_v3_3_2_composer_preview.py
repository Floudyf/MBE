from backend.app.services.v3_profile_loader import load_profile_store
from backend.app.services.v3_profile_preview import preview_profile


def test_composer_preview_outputs_modules_edges_and_scope():
    preview = preview_profile("experiment_profile", "metatrack_go_backed_ablation_smoke")
    composer = preview["composer_preview"]

    assert composer["view"] == "single_chain"
    assert composer["template_id"] == "metatrack_ablation"
    assert composer["runnable"] is True
    assert len(composer["modules"]) == 11
    assert composer["edges"][0] == {"source": "Workload", "target": "TxPool"}
    assert composer["edges"][-1] == {"source": "Commit", "target": "MetricsReport"}
    assert preview["module_graph"]["modules"] == composer["modules"]
    assert preview["fairness_scope"]["variable_modules"] == ["Routing", "Execution", "StateAccess", "Commit"]


def test_composer_preview_module_statuses_for_metatrack():
    modules = {module["module_id"]: module for module in preview_profile("experiment_profile", "metatrack_go_backed_ablation_smoke")["composer_preview"]["modules"]}

    for module_id in ("Routing", "Execution", "StateAccess", "Commit"):
        assert modules[module_id]["status"] == "variable"
        assert modules[module_id]["role"] == "research_variable"
    for module_id in ("Consensus", "TxPool", "BlockProducer", "StateStorage"):
        assert modules[module_id]["status"] == "fixed"
    assert modules["CommitteeEpoch"]["status"] == "planned"
    assert modules["MetricsReport"]["status"] == "output"


def test_old_profiles_still_preview_with_additive_fields():
    store = load_profile_store()
    preview = preview_profile("experiment_profile", "single_chain_runtime_smoke", store)

    assert preview["valid"] is True
    assert "composer_preview" in preview
    assert "plugin_summary" in preview
    assert "role_summary" in preview
