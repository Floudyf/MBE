from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest
import yaml

from trace.converter.fabric_to_unified_trace import convert_raw_fabric_log
from trace.writer.gzip_jsonl_writer import REQUIRED_FIELDS

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "chain/fabric/samples/raw_chain_log_sample.jsonl"
SCHEMA = ROOT / "chain/fabric/access_schema.yaml"


def test_fabric_sample_converts_to_streaming_unified_trace(tmp_path: Path) -> None:
    assert yaml.safe_load(SCHEMA.read_text(encoding="utf-8"))["contracts"]
    result = convert_raw_fabric_log(RAW, SCHEMA, tmp_path)
    with gzip.open(result["trace_path"], "rt", encoding="utf-8") as stream:
        records = [json.loads(line) for line in stream]
    assert len(records) == 5
    assert all(set(REQUIRED_FIELDS) <= record.keys() and record["access_list"] for record in records)
    rewards = [record for record in records if record["contract"] == "reward"]
    assert all(record["commutative"] and record["update_type"] == "delta" and record["delta_value"] is not None for record in rewards)
    meta = json.loads(result["meta_path"].read_text(encoding="utf-8"))
    assert meta["source"] == "fabric_raw_log" and meta["tx_count"] == 5 and meta["avg_access_set_size"] > 0
    assert meta["actual_commutative_update_ratio"] > 0


def test_fabric_converter_reports_schema_and_argument_errors(tmp_path: Path) -> None:
    bad = tmp_path / "bad.jsonl"
    bad.write_text(json.dumps({"tx_id":"bad","tx_type":"x","submit_time":0,"commit_time":1,"status":"success","contract":"unknown","function":"Nope","args":{},"block_number":1,"event":"x"}) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="no access schema"):
        convert_raw_fabric_log(bad, SCHEMA, tmp_path / "unknown")
    missing = tmp_path / "missing.jsonl"
    missing.write_text(json.dumps({"tx_id":"missing","tx_type":"x","submit_time":0,"commit_time":1,"status":"success","contract":"asset","function":"TransferAsset","args":{},"block_number":1,"event":"x"}) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="missing argument"):
        convert_raw_fabric_log(missing, SCHEMA, tmp_path / "missing-out")
