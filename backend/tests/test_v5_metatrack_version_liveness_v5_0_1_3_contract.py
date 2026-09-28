from pathlib import Path


def test_v5013_client_uses_existing_bool_helper_and_latest_only_gate() -> None:
    client = Path("executor/v5/client.go").read_text(encoding="utf-8")
    runtime = Path("executor/v5/runtime.go").read_text(encoding="utf-8")
    assert 'func boolFromAny(value any) bool' in runtime
    assert 'boolValue(' not in client
    assert 'versionLivenessEnabled = boolFromAny(config.Config["version_liveness"])' in client
    assert 'if bindExecutionRouting && versionLivenessEnabled && plugins.Routing.ID() == "metatrack_coaccess_routing"' in client
    assert 'if bindExecutionRouting && plugins.Routing.ID() == "metatrack_coaccess_routing"' not in client


def test_v5013_reseal_occurs_before_route_plan_digest_binding() -> None:
    client = Path("executor/v5/client.go").read_text(encoding="utf-8")
    implementation = Path("executor/v5/metatrack_version_liveness_v5.go").read_text(encoding="utf-8")
    assert 'resealMetaTrackVersionLivenessPlan(records, routePlan)' in client
    assert client.index('resealMetaTrackVersionLivenessPlan(records, routePlan)') < client.index('routePlanDigest = routePlan.PlanDigest')
    assert 'applyMetaTrackDeclaredAccessFrontierV2(&plan, records)' in implementation
    assert 'plan.PlanDigest = routingPlanDigest(plan)' in implementation


def test_v5013_latest_runtime_is_fail_closed_and_checks_semantics() -> None:
    implementation = Path("executor/v5/metatrack_version_liveness_v5.go").read_text(encoding="utf-8")
    publisher = implementation[
        implementation.index("func (r *NodeRuntime) metaTrackVersionLivenessPublisher"):
        implementation.index("func (r *NodeRuntime) flushMetaTrackVersionLiveness")
    ]
    assert "producer liveness metadata incomplete" in publisher
    assert "metatrack_version_liveness_fallback_count" not in publisher
    for marker in (
        "liveness digest mismatch",
        "value successor count mismatch",
        "local-transient successor semantics mismatch",
        "remote-live successor semantics mismatch",
        "dead-intermediate successor semantics mismatch",
        "points to dead intermediate",
    ):
        assert marker in implementation

def test_v5013_legacy_v5_backend_contract_tracks_new_explicit_gate() -> None:
    legacy = Path("backend/tests/test_v5_metatrack_version_liveness_v5_contract.py").read_text(encoding="utf-8")
    assert "MBE_METATRACK_VERSION_LIVENESS_V5' in client" not in legacy
    assert 'versionLivenessEnabled = boolFromAny(config.Config["version_liveness"])' in legacy
    assert 'resealMetaTrackVersionLivenessPlan(records, routePlan)' in legacy
