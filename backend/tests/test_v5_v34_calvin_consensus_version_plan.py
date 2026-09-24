from pathlib import Path

from backend.app.services.v5_formal_plan_validator import CALVIN_BUILTIN_METHODS
from backend.app.services.v5_plugin_manifest_store import STORE


ROOT = Path(__file__).resolve().parents[2]


def test_stateless_calvin_manifest_discloses_consensus_bound_version_plan() -> None:
    state_access = STORE.get("stateless_calvin_state_access")
    scheduler = STORE.get("stateless_calvin_deterministic_scheduler")
    block_executor = STORE.get("stateless_calvin_block_executor")
    assert "declared_state_projection" in state_access.capabilities
    assert "consensus_bound_version_plan" in state_access.capabilities
    assert "consensus_bound_version_plan" in scheduler.capabilities
    assert "consensus_bound_version_plan" in block_executor.capabilities
    assert "block_start_committed_value" in block_executor.capabilities


def test_stateless_calvin_client_no_longer_signs_source_order_state_versions() -> None:
    source = (ROOT / "executor" / "v5" / "client.go").read_text(encoding="utf-8")
    assert "MBE_CALVIN_CONSENSUS_VERSION_PLAN_V34" in source
    assert "records[index].StateVersions = calvinStateVersionDependenciesForRecord" not in source
    assert "record.StateVersions = calvinStateVersionDependenciesForAccessList" not in source


def test_calvin_consensus_version_plan_is_preconsensus_scheduler_evidence() -> None:
    source = (ROOT / "executor" / "v5" / "calvin_plugins.go").read_text(encoding="utf-8")
    assert 'calvinConsensusPlanAlgorithmID = "calvin_consensus_version_plan_v1"' in source
    assert "func (p statelessCalvinScheduler) PlanBlock" in source
    assert "func (p statelessCalvinScheduler) VerifyBlockPlan" in source
    assert "var _ ConsensusExecutionPlanner = statelessCalvinScheduler{}" in source
    assert 'BlockStartReadSemantics: "current_committed_home_state_at_block_start"' in source
    assert 'calvinStatelessSchedulerID     = "stateless_calvin_deterministic_scheduler"' in source


def test_v34_metrics_expose_version_source_and_block_boundary_reads() -> None:
    source = (ROOT / "backend" / "app" / "services" / "v5_metric_extractor.py").read_text(encoding="utf-8")
    for key in (
        "calvin_consensus_version_binding_count",
        "calvin_stateless_block_start_read_count",
        "calvin_stateless_exact_predecessor_read_count",
        "calvin_client_state_version_metadata_count",
    ):
        assert key in source


def test_stateless_calvin_uses_dedicated_consensus_version_scheduler() -> None:
    stateful = CALVIN_BUILTIN_METHODS["stateful_calvin"]
    stateless = CALVIN_BUILTIN_METHODS["stateless_calvin"]
    assert stateful.plugin_overrides["scheduler"] == "calvin_deterministic_scheduler"
    assert stateless.plugin_overrides["scheduler"] == "stateless_calvin_deterministic_scheduler"


def test_legacy_source_order_helper_is_compatibility_only() -> None:
    remote = (ROOT / "executor" / "v5" / "calvin_stateless_remote.go").read_text(encoding="utf-8")
    client = (ROOT / "executor" / "v5" / "client.go").read_text(encoding="utf-8")
    assert remote.count("calvinStateVersionDependenciesForAccessList") == 2  # comment + function definition only
    assert "calvinStateVersionDependenciesForAccessList(" not in client
    assert "buildCalvinConsensusPlan" in (ROOT / "executor" / "v5" / "calvin_plugins.go").read_text(encoding="utf-8")
