from pathlib import Path

from trace.validator.cross_chain_trace_validator import load_json_schema, validate_multi_chain_trace_meta


ROOT = Path(__file__).resolve().parents[2]
META_SCHEMA = ROOT / "trace/schema/v2_multi_chain_trace_meta.schema.json"
SAMPLE_META = ROOT / "trace/samples/v2_multi_chain_trace_meta.json"


def test_multi_chain_trace_meta_schema_file_exists_and_declares_required_fields() -> None:
    schema = load_json_schema(META_SCHEMA)

    assert META_SCHEMA.is_file()
    assert schema["properties"]["schema_version"]["const"] == "v2.multi_chain_trace_meta.v1"
    assert {"schema_version", "trace_format", "data_truth_label", "chain_count", "chains", "cross_tx_count", "stage_record_count", "limitations"} <= set(schema["required"])


def test_sample_multi_chain_trace_meta_validates() -> None:
    result = validate_multi_chain_trace_meta(SAMPLE_META)

    assert result["valid"] is True
    assert result["meta"]["chain_count"] == 2
    assert {chain["role"] for chain in result["meta"]["chains"]} == {"source", "target"}
    assert "Schema sample only; not a replay result." in result["meta"]["limitations"]


def test_multi_chain_trace_meta_rejects_chain_count_mismatch(tmp_path: Path) -> None:
    meta = SAMPLE_META.read_text(encoding="utf-8").replace('"chain_count": 2', '"chain_count": 3')
    path = tmp_path / "meta.json"
    path.write_text(meta, encoding="utf-8")

    result = validate_multi_chain_trace_meta(path)

    assert result["valid"] is False
    assert any("chain_count" in error for error in result["errors"])
