from pathlib import Path

from trace.validator.cross_chain_trace_validator import load_json_schema, validate_cross_chain_record


ROOT = Path(__file__).resolve().parents[2]
CROSS_SCHEMA = ROOT / "trace/schema/v2_cross_chain_trace.schema.json"


def test_cross_chain_trace_schema_file_exists_and_declares_required_fields() -> None:
    schema = load_json_schema(CROSS_SCHEMA)

    assert CROSS_SCHEMA.is_file()
    assert schema["properties"]["schema_version"]["const"] == "v2.cross_chain_trace.v1"
    assert {"cross_tx_id", "stage_id", "stage", "source_chain", "target_chain", "chain_id", "submit_time_ms", "status", "data_truth_label"} <= set(schema["required"])


def test_cross_chain_record_rejects_missing_required_fields_and_bad_enums() -> None:
    missing = validate_cross_chain_record({"schema_version": "v2.cross_chain_trace.v1"})
    bad_stage = validate_cross_chain_record({
        "schema_version": "v2.cross_chain_trace.v1",
        "cross_tx_id": "ctx",
        "stage_id": "stage",
        "stage": "not_a_stage",
        "source_chain": "a",
        "target_chain": "b",
        "chain_id": "a",
        "submit_time_ms": 0,
        "status": "created",
        "data_truth_label": "synthetic_replay",
    })
    bad_status = validate_cross_chain_record({
        "schema_version": "v2.cross_chain_trace.v1",
        "cross_tx_id": "ctx",
        "stage_id": "stage",
        "stage": "created",
        "source_chain": "a",
        "target_chain": "b",
        "chain_id": "a",
        "submit_time_ms": 0,
        "status": "not_a_status",
        "data_truth_label": "synthetic_replay",
    })
    bad_truth = validate_cross_chain_record({
        "schema_version": "v2.cross_chain_trace.v1",
        "cross_tx_id": "ctx",
        "stage_id": "stage",
        "stage": "created",
        "source_chain": "a",
        "target_chain": "b",
        "chain_id": "a",
        "submit_time_ms": 0,
        "status": "created",
        "data_truth_label": "real_chain_execution",
    })

    assert missing["valid"] is False
    assert any("missing required fields" in error for error in missing["errors"])
    assert bad_stage["valid"] is False
    assert any("invalid stage" in error for error in bad_stage["errors"])
    assert bad_status["valid"] is False
    assert any("invalid status" in error for error in bad_status["errors"])
    assert bad_truth["valid"] is False
    assert any("invalid data_truth_label" in error for error in bad_truth["errors"])


def test_cross_chain_record_rejects_invalid_time_ordering() -> None:
    commit_before_submit = validate_cross_chain_record({
        "schema_version": "v2.cross_chain_trace.v1",
        "cross_tx_id": "ctx",
        "stage_id": "stage",
        "stage": "source_lock",
        "source_chain": "a",
        "target_chain": "b",
        "chain_id": "a",
        "submit_time_ms": 10,
        "commit_time_ms": 5,
        "status": "committed",
        "data_truth_label": "synthetic_replay",
    })
    finality_before_commit = validate_cross_chain_record({
        "schema_version": "v2.cross_chain_trace.v1",
        "cross_tx_id": "ctx",
        "stage_id": "stage",
        "stage": "source_lock",
        "source_chain": "a",
        "target_chain": "b",
        "chain_id": "a",
        "submit_time_ms": 10,
        "commit_time_ms": 20,
        "finality_time_ms": 15,
        "status": "finalized",
        "data_truth_label": "synthetic_replay",
    })

    assert commit_before_submit["valid"] is False
    assert any("commit_time_ms" in error for error in commit_before_submit["errors"])
    assert finality_before_commit["valid"] is False
    assert any("finality_time_ms" in error for error in finality_before_commit["errors"])


def test_public_chain_semantic_unknown_record_does_not_require_access_sets() -> None:
    result = validate_cross_chain_record({
        "schema_version": "v2.cross_chain_trace.v1",
        "cross_tx_id": "ctx_public",
        "stage_id": "ctx_public_created",
        "stage": "created",
        "source_chain": "chain_a",
        "target_chain": "chain_b",
        "chain_id": "chain_a",
        "submit_time_ms": 0,
        "status": "created",
        "data_truth_label": "public_chain_imported_trace_semantic_unknown",
    })

    assert result["valid"] is True
