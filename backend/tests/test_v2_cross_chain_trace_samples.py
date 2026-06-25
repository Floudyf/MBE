from pathlib import Path

import yaml

from trace.validator.cross_chain_trace_validator import validate_cross_chain_trace_file, validate_trace_and_meta


ROOT = Path(__file__).resolve().parents[2]
SAMPLE_TRACE = ROOT / "trace/samples/v2_cross_trace_small.jsonl"
SAMPLE_META = ROOT / "trace/samples/v2_multi_chain_trace_meta.json"
PLANNED_TOPOLOGY = ROOT / "configs/topologies/v2_dual_chain_planned.yaml"


def test_v2_cross_chain_sample_files_exist_and_are_small() -> None:
    assert SAMPLE_TRACE.is_file()
    assert SAMPLE_META.is_file()
    assert SAMPLE_TRACE.stat().st_size < 20_000
    assert SAMPLE_META.stat().st_size < 10_000


def test_schema_sample_is_not_runnable_experiment_output() -> None:
    meta_text = SAMPLE_META.read_text(encoding="utf-8")
    assert "Schema sample only; not a replay result." in meta_text
    assert "completed runnable experiment" not in meta_text
    assert validate_trace_and_meta(SAMPLE_TRACE, SAMPLE_META)["valid"] is True


def test_sample_contains_completed_and_timeout_or_refund_paths() -> None:
    text = SAMPLE_TRACE.read_text(encoding="utf-8")
    assert '"cross_tx_id":"ctx_000001"' in text
    assert '"cross_tx_id":"ctx_000002"' in text
    assert '"status":"completed"' in text
    assert '"status":"timeout"' in text or '"status":"refunded"' in text
    assert validate_cross_chain_trace_file(SAMPLE_TRACE)["valid"] is True


def test_v2_planned_topology_guard_still_not_runnable() -> None:
    document = yaml.safe_load(PLANNED_TOPOLOGY.read_text(encoding="utf-8"))

    assert document["status"] == "planned"
    assert document["runnable"] is False
