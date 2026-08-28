from backend.app.services import v5_formal_plan_validator as validator
from backend.app.services import v5_formal_scheduler as scheduler
from backend.app.services import v5_metric_extractor as extractor
from backend.app.services.v5_plugin_manifest_store import STORE


def test_fabricpp_plugins_are_independent_and_source_locked():
    expected = {
        "fabricpp_cg_execution": "execution",
        "fabricpp_cg_scheduler": "scheduler",
        "fabricpp_cg_block_executor": "block_executor",
    }
    for plugin_id, category in expected.items():
        item = STORE.get(plugin_id)
        assert item.category == category
        assert item.truth_boundary == "fabricpp_sigmod2019_transaction_reordering_v1"
    block_executor = STORE.get("fabricpp_cg_block_executor")
    assert block_executor.default_config["worker_count"] == 1
    assert block_executor.config_schema["properties"]["worker_count"]["maximum"] == 1
    source = STORE.get("fabricpp_cg_execution").source
    assert source["doi"] == "10.1145/3299869.3319883"
    assert source["algorithm"] == "Algorithm 1 transaction reordering"


def test_fabricpp_formal_method_uses_only_fabricpp_cg_triplet():
    method = validator.LITERATURE_BUILTIN_METHODS["hash_fabricpp_cg"]
    assert method.plugin_overrides["execution"] == "fabricpp_cg_execution"
    assert method.plugin_overrides["scheduler"] == "fabricpp_cg_scheduler"
    assert method.plugin_overrides["block_executor"] == "fabricpp_cg_block_executor"
    assert "cg_execution" not in method.plugin_overrides.values()
    assert "cg_scheduler" not in method.plugin_overrides.values()
    assert "cg_block_executor" not in method.plugin_overrides.values()
    assert method.plugin_config_overrides["block_executor"]["worker_count"] == 1


def test_fabricpp_has_distinct_abort_semantics_and_required_metrics():
    semantics = scheduler._execution_semantics(
        {"block_executor": "fabricpp_cg_block_executor"}, "hash_fabricpp_cg"
    )
    assert semantics["comparison_semantics_class"] == "fabricpp_cg_cycle_abortable_v1"
    required = extractor._literature_graph_required_metrics("hash_fabricpp_cg")
    for name in (
        "fabricpp_candidate_transaction_count",
        "fabricpp_cycle_abort_count",
        "fabricpp_cycle_resolution_count",
        "fabricpp_cycle_abort_rate",
    ):
        assert name in required
