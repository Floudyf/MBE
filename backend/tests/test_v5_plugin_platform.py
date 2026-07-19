from pathlib import Path

import pytest

from backend.app.models.v5_experiment_spec import V5ExperimentSpec, V5PluginSelection, V5Topology
from backend.app.services.v5_compatibility_engine import validate
from backend.app.services.v5_experiment_compiler import compile_plan
from backend.app.services.v5_metric_extractor import extract
from backend.app.services.v5_plugin_manifest_store import CATEGORIES, STORE
from backend.app.services.v5_saved_config_adapter import adapt_saved_method


def _spec() -> V5ExperimentSpec:
    selections = []
    for category in CATEGORIES:
        item = next(item for item in STORE.list() if item.category == category)
        selections.append(V5PluginSelection(category=category, plugin_id=item.plugin_id))
    return V5ExperimentSpec(execution_backend="real_cluster", plugin_selections=selections, topology=V5Topology(nodes=8, shards=2, validators_per_shard=4), tx_count=100)


def test_real_cluster_spec_compiles_deterministically(tmp_path: Path) -> None:
    spec = _spec()
    first = compile_plan(spec, tmp_path)
    second = compile_plan(spec, tmp_path)
    assert first.plan_digest == second.plan_digest
    assert len(first.node_configs) == 8
    assert first.no_fallback is True


def test_real_cluster_rejects_missing_category() -> None:
    spec = _spec()
    spec.plugin_selections.pop()
    result = validate(spec)
    assert not result.valid
    assert any("missing required plugin category" in item for item in result.blockers)


def test_saved_config_adapter_reuses_v3_payload() -> None:
    selections, warnings = adapt_saved_method({"payload": {"module_plugins": {"routing": "hash_routing_baseline", "execution": "serial_execution_baseline"}}})
    assert len(selections) == len(CATEGORIES)
    assert not warnings
    block_executor = next(item for item in selections if item.category == "block_executor")
    assert block_executor.plugin_id == "serial_block_executor"
    assert block_executor.config["migrated_default"] is True


def test_block_executor_manifest_and_compiled_plan(tmp_path: Path) -> None:
    manifest = STORE.get("serial_block_executor")
    assert manifest.category == "block_executor"
    assert manifest.supported_backends == ["real_cluster"]
    assert manifest.truth_boundary == "legacy_faithful_reference_baseline"
    plan = compile_plan(_spec(), tmp_path)
    assert "block_execution_summary.json" in plan.expected_artifacts
    assert "execution_plan.jsonl" in plan.expected_artifacts
    assert all(node.plugin_profile["block_executor"]["plugin_id"] == "serial_block_executor" for node in plan.node_configs)
    assert plan.node_configs[0].plugin_profile["block_executor"]["migrated_default"] is False


def test_metatrack_expected_artifacts_are_declared_only_for_metatrack_routing(tmp_path: Path) -> None:
    hash_plan = compile_plan(_spec(), tmp_path / "hash")
    assert "metatrack_batch_plan.jsonl" not in hash_plan.expected_artifacts

    spec = _spec()
    spec.plugin_selections = [
        item.model_copy(update={"plugin_id": "metatrack_coaccess_routing"} if item.category == "routing" else {})
        for item in spec.plugin_selections
    ]
    metatrack_plan = compile_plan(spec, tmp_path / "metatrack")
    assert "metatrack_batch_plan.jsonl" in metatrack_plan.expected_artifacts
    assert "access_matrix_summary.csv" in metatrack_plan.expected_artifacts
    assert "transaction_placement.csv" in metatrack_plan.expected_artifacts
    assert "dependency_graph.csv" in metatrack_plan.expected_artifacts
    assert "track_classification.csv" in metatrack_plan.expected_artifacts
    assert "logical_physical_update_mapping.csv" in metatrack_plan.expected_artifacts


def test_block_stm_expected_artifacts_are_declared_only_for_block_stm_executor(tmp_path: Path) -> None:
    serial_plan = compile_plan(_spec(), tmp_path / "serial")
    assert "block_stm_summary.json" not in serial_plan.expected_artifacts

    spec = _spec()
    spec.plugin_selections = [
        item.model_copy(update={"plugin_id": "block_stm_block_executor", "config": {"worker_count": 4}} if item.category == "block_executor" else {})
        for item in spec.plugin_selections
    ]
    block_stm_plan = compile_plan(spec, tmp_path / "block_stm")
    assert "block_stm_summary.json" in block_stm_plan.expected_artifacts
    assert "block_stm_validation_trace.csv" in block_stm_plan.expected_artifacts
    assert "serial_equivalence.json" in block_stm_plan.expected_artifacts


def test_metric_extractor_reads_block_stm_and_metatrack_artifact_evidence(tmp_path: Path) -> None:
    (tmp_path / "real_cluster_summary.json").write_text(
        '{"block_executor_id":"block_stm_block_executor","block_executor_consistent":true,"plan_digest_consistent":true,"state_root_consistent":true,"orphan_process_count":0,"no_fallback":true,"fast_track_count":3,"conservative_track_count":2,"aggregation_group_count":1,"logical_update_count":5,"physical_update_count":3}',
        encoding="utf-8",
    )
    (tmp_path / "finality_summary.json").write_text(
        '{"logical_transaction_count":5,"finalized_unique_logical_tx_count":5,"throughput_tps":10.5}',
        encoding="utf-8",
    )
    for name in ["transaction_lifecycle.jsonl", "transaction_finality.csv", "client_receipt_log.csv", "metatrack_batch_plan.jsonl", "dependency_graph.csv", "track_classification.csv", "aggregation_plan.csv", "logical_physical_update_mapping.csv"]:
        (tmp_path / name).write_text("", encoding="utf-8")
    (tmp_path / "metatrack_scheduler_trace.csv").write_text(
        "timestamp,node_id,shard_id,height,scheduler_plugin,tx_id,track,queue_name,decision_reason,local_execution,stolen_work,blocked,wakeup,ready_queue_depth,fast_queue_depth,conservative_queue_depth,dependency_wait_ms,scheduler_idle_ms\n"
        "1,n1,s0,1,fast_first_scheduler,tx1,fast,fast_queue,enqueue,true,false,false,false,3,1,2,0,0\n"
        "2,n1,s0,1,fast_first_scheduler,tx2,conservative,blocked_waiting,wait,true,false,true,false,2,0,2,4,0\n"
        "3,n1,s0,1,fast_first_scheduler,tx2,conservative,conservative_queue,wakeup,true,false,false,true,1,0,1,4,1\n",
        encoding="utf-8",
    )
    (tmp_path / "remote_state_access.csv").write_text(
        "timestamp,node_id,execution_shard,height,block_hash,tx_id,state_key,qualified_home_key,home_shard,response_execution_shard,access_kind,latency_ms,witness_digest,home_state_root,success,error\n"
        "1,n1,s1,1,b1,tx1,k,s0::k,s0,s1,read,3,w1,r1,true,\n"
        "2,n1,s1,1,b1,tx2,k2,s0::k2,s0,s1,read,7,w2,r1,false,timeout\n"
        "3,n1,s1,1,b1,tx3,k3,s0::k3,s0,s1,write_apply,5,w3,r2,true,\n",
        encoding="utf-8",
    )
    (tmp_path / "block_stm_summary.json").write_text(
        '{"serial_equivalent":true,"block_stm_metrics":{"worker_count":4,"maximum_parallel_width":3,"abort_count":2,"reexecution_count":2,"dependency_wait_count":1,"dependency_resume_count":1,"validation_failure_count":2}}',
        encoding="utf-8",
    )

    metrics = extract(tmp_path)

    assert metrics["worker_count"] == 4
    assert metrics["abort_count"] == 2
    assert metrics["serial_equivalent"] is True
    assert metrics["track_classification_available"] is True
    assert metrics["metatrack_scheduler_trace_available"] is True
    assert metrics["logical_physical_update_mapping_available"] is True
    assert metrics["scheduler_event_count"] == 3
    assert metrics["scheduler_blocked_count"] == 1
    assert metrics["scheduler_wakeup_count"] == 1
    assert metrics["scheduler_local_execution_count"] == 3
    assert metrics["scheduler_fast_queue_event_count"] == 1
    assert metrics["scheduler_conservative_queue_event_count"] == 1
    assert metrics["scheduler_ready_queue_max_depth"] == 3
    assert metrics["scheduler_fast_queue_max_depth"] == 1
    assert metrics["scheduler_conservative_queue_max_depth"] == 2
    assert metrics["scheduler_dependency_wait_ms"] == 8
    assert metrics["scheduler_idle_ms"] == 1
    assert metrics["scheduler_idle_ratio"] == pytest.approx(1 / 3)
    assert metrics["remote_state_access_count"] == 2
    assert metrics["remote_state_access_failed_count"] == 1
    assert metrics["remote_state_read_count"] == 1
    assert metrics["remote_state_write_apply_count"] == 1
    assert metrics["remote_state_access_avg_latency_ms"] == 4
    assert metrics["missing"] == []


def test_metric_extractor_does_not_treat_planning_remote_state_csv_as_runtime_metrics(tmp_path: Path) -> None:
    (tmp_path / "real_cluster_summary.json").write_text(
        '{"remote_state_access_count":4,"remote_state_read_count":2,"remote_state_write_apply_count":2,"remote_state_access_failed_count":0,"remote_state_access_avg_latency_ms":6}',
        encoding="utf-8",
    )
    (tmp_path / "finality_summary.json").write_text(
        '{"logical_transaction_count":1,"finalized_unique_logical_tx_count":1}',
        encoding="utf-8",
    )
    for name in ["transaction_lifecycle.jsonl", "transaction_finality.csv", "client_receipt_log.csv"]:
        (tmp_path / name).write_text("", encoding="utf-8")
    (tmp_path / "remote_state_access.csv").write_text(
        "batch_index,logical_id,tx_index,state_key,home_shard,execution_shard,access_kind,witness_digest\n"
        "0,tx1,0,k,s0,s1,read,witness\n",
        encoding="utf-8",
    )

    metrics = extract(tmp_path)

    assert metrics["remote_state_access_count"] == 4
    assert metrics["remote_state_read_count"] == 2
    assert metrics["remote_state_write_apply_count"] == 2
    assert metrics["remote_state_access_avg_latency_ms"] == 6


def test_compiler_rejects_non_committee_topology(tmp_path: Path) -> None:
    spec = _spec()
    spec.topology.nodes = 7
    with pytest.raises(ValueError, match="nodes must equal"):
        compile_plan(spec, tmp_path)
