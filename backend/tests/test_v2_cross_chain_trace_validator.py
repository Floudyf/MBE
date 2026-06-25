from pathlib import Path

from trace.validator.cross_chain_trace_validator import validate_cross_chain_trace_file, validate_trace_and_meta


ROOT = Path(__file__).resolve().parents[2]
SAMPLE_TRACE = ROOT / "trace/samples/v2_cross_trace_small.jsonl"
SAMPLE_META = ROOT / "trace/samples/v2_multi_chain_trace_meta.json"


def test_sample_cross_chain_trace_file_validates_streaming() -> None:
    result = validate_cross_chain_trace_file(SAMPLE_TRACE)

    assert result["valid"] is True
    assert result["stats"]["records"] == 9
    assert result["stats"]["cross_tx_count"] == 2
    assert result["stats"]["chains"] == ["chain_a", "chain_b"]


def test_sample_trace_and_meta_joint_validation_passes() -> None:
    result = validate_trace_and_meta(SAMPLE_TRACE, SAMPLE_META)

    assert result["valid"] is True
    assert result["stats"]["records"] == 9
    assert result["stats"]["cross_tx_count"] == 2


def test_trace_and_meta_reject_stage_record_count_mismatch(tmp_path: Path) -> None:
    meta = SAMPLE_META.read_text(encoding="utf-8").replace('"stage_record_count": 9', '"stage_record_count": 8')
    meta_path = tmp_path / "meta.json"
    meta_path.write_text(meta, encoding="utf-8")

    result = validate_trace_and_meta(SAMPLE_TRACE, meta_path)

    assert result["valid"] is False
    assert any("stage_record_count" in error for error in result["errors"])


def test_trace_and_meta_reject_cross_tx_count_mismatch(tmp_path: Path) -> None:
    meta = SAMPLE_META.read_text(encoding="utf-8").replace('"cross_tx_count": 2', '"cross_tx_count": 3')
    meta_path = tmp_path / "meta.json"
    meta_path.write_text(meta, encoding="utf-8")

    result = validate_trace_and_meta(SAMPLE_TRACE, meta_path)

    assert result["valid"] is False
    assert any("cross_tx_count" in error for error in result["errors"])


def test_invalid_jsonl_line_is_reported(tmp_path: Path) -> None:
    path = tmp_path / "bad.jsonl"
    path.write_text('{"schema_version":"v2.cross_chain_trace.v1"}\nnot-json\n', encoding="utf-8")

    result = validate_cross_chain_trace_file(path)

    assert result["valid"] is False
    assert any("invalid JSON" in error for error in result["errors"])
