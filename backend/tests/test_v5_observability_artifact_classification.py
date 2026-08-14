from backend.app.services.v5_artifact_contract import classify_artifact


def test_optional_observability_artifacts_are_classified_without_becoming_contract_requirements():
    role, scope, producer, schema = classify_artifact("resource_usage_summary.json")
    assert role == "research_observability"
    assert scope == "validator_node_processes_completion_window"
    assert producer == "v5_observability_metrics"
    assert schema == "mbe_v5_resource_observability_v1"

    role, scope, producer, schema = classify_artifact("network_metrics_summary.json")
    assert role == "research_observability"
    assert scope == "successful_node_receive_completion_window"
    assert producer == "v5_observability_metrics"
    assert schema == "mbe_v5_network_observability_v1"
