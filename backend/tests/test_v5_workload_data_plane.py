from __future__ import annotations

import csv
import hashlib
import gzip
import json
import os
import subprocess
import sys
import tracemalloc
from pathlib import Path

import pytest

from backend.app.services import v5_workload_data_plane as plane
from backend.app.services.v5_workload_data_plane import WorkloadDataError, build_canonical, materialize, validate_csv


FIELDS = ["id", "tx_hash", "buyer", "seller", "price", "timestamp", "category", "raw_contract_candidates"]


def _write_source(path: Path, count: int = 10_000, *, contracts: int = 8) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        writer.writeheader()
        for index in range(count):
            category = "wearable" if index % 4 else "emote"
            contract = "0x" + f"{(index % contracts) + 1:040x}"
            writer.writerow({"id": f"sale-{index}", "tx_hash": "0x" + f"{index // 2:064x}", "buyer": "0x" + f"{index % 31 + 1:040x}", "seller": "0x" + f"{index % 17 + 101:040x}", "price": "999999999999999999999999999999999999999999999999" if index == 0 else str(index + 1), "timestamp": str(1_700_000_000_000 + index), "category": category, "raw_contract_candidates": contract})


def _manifest(path: Path) -> dict:
    return {"dataset_id": "dcl_sales_polygon_271868", "source_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def _records(path: Path) -> list[dict]:
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        return [json.loads(line) for line in stream]


def test_csv_accepts_repeated_tx_hash_and_large_decimal_and_rejects_duplicate_sale_id(tmp_path: Path) -> None:
    source = tmp_path / "source.csv"
    _write_source(source, 4)
    summary = validate_csv(source)
    assert summary.row_count == 4 and summary.unique_source_tx_hash_count == 2
    with source.open("a", encoding="utf-8", newline="") as stream:
        csv.writer(stream).writerow(["sale-0", "0x" + "f" * 64, "0x" + "1" * 40, "0x" + "2" * 40, "1", "1700000000010", "wearable", "0x" + "3" * 40])
    with pytest.raises(WorkloadDataError, match="duplicate sale id"):
        validate_csv(source)


@pytest.mark.parametrize("field,value,error", [
    ("buyer", "not-an-address", "invalid buyer"),
    ("seller", "0x1234", "invalid seller"),
    ("category", "land", "unsupported category"),
    ("raw_contract_candidates", "none", "exactly one"),
    ("raw_contract_candidates", "0x" + "1" * 40 + ",0x" + "2" * 40, "exactly one"),
])
def test_csv_rejects_invalid_required_source_values(tmp_path: Path, field: str, value: str, error: str) -> None:
    source = tmp_path / "invalid.csv"
    _write_source(source, 2)
    rows = list(csv.DictReader(source.open(encoding="utf-8")))
    rows[0][field] = value
    with source.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS); writer.writeheader(); writer.writerows(rows)
    with pytest.raises(WorkloadDataError, match=error):
        validate_csv(source)


def test_csv_rejects_missing_header_and_empty_required_value_without_mutating_source(tmp_path: Path) -> None:
    source = tmp_path / "missing.csv"
    source.write_text("id,tx_hash\na,b\n", encoding="utf-8")
    before = source.read_bytes()
    with pytest.raises(WorkloadDataError, match="header"):
        validate_csv(source)
    assert source.read_bytes() == before
    _write_source(source, 2)
    rows = list(csv.DictReader(source.open(encoding="utf-8"))); rows[0]["price"] = ""
    with source.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS); writer.writeheader(); writer.writerows(rows)
    with pytest.raises(WorkloadDataError, match="required source field is empty"):
        validate_csv(source)


def test_canonical_output_is_byte_identical_and_preserves_source_traceability(tmp_path: Path) -> None:
    source = tmp_path / "source.csv"
    _write_source(source, 8)
    manifest = _manifest(source)
    first = build_canonical(source, tmp_path / "one", manifest)
    second = build_canonical(source, tmp_path / "two", manifest)
    first_path = tmp_path / "one" / first["canonical_relative_path"]
    second_path = tmp_path / "two" / second["canonical_relative_path"]
    assert first_path.read_bytes() == second_path.read_bytes()
    records = _records(first_path)
    assert len(records) == 8
    assert records[0]["state_keys"][0].startswith("account:sender:")
    assert records[0]["source_event_id"] == "sale-0"
    assert records[0]["runtime_value"] == 1 and isinstance(records[0]["metadata"]["price_raw"], str)
    with gzip.open(first_path, "rb") as stream:
        assert stream.read(1)
        assert stream.mtime == 0


def test_original_materialization_is_reproducible_and_cache_is_atomic(tmp_path: Path) -> None:
    source = tmp_path / "source.csv"
    _write_source(source)
    canonical = build_canonical(source, tmp_path / "cache", _manifest(source))
    canonical_path = tmp_path / "cache" / canonical["canonical_relative_path"]
    kwargs = {"dataset_id": "dcl_sales_polygon_271868", "source_sha256": canonical["source_sha256"], "requested_tx_count": 10_000, "seed": 11}
    first = materialize(canonical_path, tmp_path / "cache", **kwargs)
    second = materialize(canonical_path, tmp_path / "cache", **kwargs)
    assert first["actual_tx_count"] == 10_000 and second["cache_hit"] is True
    assert not list((tmp_path / "cache" / "materialized").glob(".*"))


@pytest.mark.parametrize("label,count", [("10K", 3), ("50K", 4), ("100K", 5), ("250K", 6)])
def test_original_window_boundary_modes_are_deterministic_on_small_fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, label: str, count: int) -> None:
    source = tmp_path / "source.csv"; _write_source(source, 8)
    canonical = build_canonical(source, tmp_path / "cache", _manifest(source))
    monkeypatch.setattr(plane, "SUPPORTED_COUNTS", frozenset({3, 4, 5, 6}))
    path = tmp_path / "cache" / canonical["canonical_relative_path"]
    first = materialize(path, tmp_path / "cache", dataset_id="dcl_sales_polygon_271868", source_sha256=canonical["source_sha256"], requested_tx_count=count, seed=7)
    second = materialize(path, tmp_path / "second", dataset_id="dcl_sales_polygon_271868", source_sha256=canonical["source_sha256"], requested_tx_count=count, seed=7)
    assert label and first["actual_tx_count"] == count and first["materialized_sha256"] == second["materialized_sha256"]
    records = _records(tmp_path / "cache" / first["materialized_relative_path"])
    assert all(row["occurrence_index"] == 0 for row in records)
    assert [row["source_row_index"] for row in records] == list(range(first["start_offset"], first["end_offset"] + 1))


def test_full_materialization_streams_all_records_and_seed_changes_window(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = tmp_path / "source.csv"; _write_source(source, 13)
    canonical = build_canonical(source, tmp_path / "cache", _manifest(source)); path = tmp_path / "cache" / canonical["canonical_relative_path"]
    monkeypatch.setattr(plane, "SUPPORTED_COUNTS", frozenset({5}))
    full = materialize(path, tmp_path / "cache", dataset_id="dcl_sales_polygon_271868", source_sha256=canonical["source_sha256"], requested_tx_count=13, seed=1)
    one = materialize(path, tmp_path / "cache", dataset_id="dcl_sales_polygon_271868", source_sha256=canonical["source_sha256"], requested_tx_count=5, seed=1)
    two = materialize(path, tmp_path / "cache", dataset_id="dcl_sales_polygon_271868", source_sha256=canonical["source_sha256"], requested_tx_count=5, seed=2)
    assert full["actual_tx_count"] == 13 and full["start_offset"] == 0 and full["end_offset"] == 12
    assert one["base_window_sha256"] != two["base_window_sha256"]


def test_contract_zipf_preserves_categories_and_reuses_real_source_rows(tmp_path: Path) -> None:
    source = tmp_path / "source.csv"
    _write_source(source)
    canonical = build_canonical(source, tmp_path / "cache", _manifest(source))
    path = tmp_path / "cache" / canonical["canonical_relative_path"]
    original = materialize(path, tmp_path / "cache", dataset_id="dcl_sales_polygon_271868", source_sha256=canonical["source_sha256"], requested_tx_count=10_000, seed=17)
    derived = materialize(path, tmp_path / "cache", dataset_id="dcl_sales_polygon_271868", source_sha256=canonical["source_sha256"], requested_tx_count=10_000, seed=17, variant_mode="contract_zipf", target_alpha=1.4, skew_axis="contract")
    original_records = _records(tmp_path / "cache" / original["materialized_relative_path"])
    derived_records = _records(tmp_path / "cache" / derived["materialized_relative_path"])
    assert derived["category_counts"] == original["category_counts"]
    assert {row["source_event_id"] for row in derived_records} <= {row["source_event_id"] for row in original_records}
    assert derived["duplicate_source_row_count"] > 0


def test_zipf_supported_alphas_preserve_real_rows_and_raise_concentration(tmp_path: Path) -> None:
    source = tmp_path / "source.csv"; _write_source(source, 10_000, contracts=24)
    canonical = build_canonical(source, tmp_path / "cache", _manifest(source)); path = tmp_path / "cache" / canonical["canonical_relative_path"]
    summaries = {}
    for alpha in sorted(plane.SUPPORTED_ALPHAS):
        summaries[alpha] = materialize(path, tmp_path / "cache", dataset_id="dcl_sales_polygon_271868", source_sha256=canonical["source_sha256"], requested_tx_count=10_000, seed=17, variant_mode="contract_zipf", target_alpha=alpha, skew_axis="contract")
    assert summaries[1.0]["hhi"] > summaries[0.0]["hhi"]
    assert summaries[1.4]["gini"] > summaries[0.0]["gini"]
    assert summaries[0.0]["materialized_sha256"] != materialize(path, tmp_path / "original", dataset_id="dcl_sales_polygon_271868", source_sha256=canonical["source_sha256"], requested_tx_count=10_000, seed=17)["materialized_sha256"]
    with pytest.raises(WorkloadDataError, match="alpha"):
        materialize(path, tmp_path / "cache", dataset_id="dcl_sales_polygon_271868", source_sha256=canonical["source_sha256"], requested_tx_count=10_000, seed=17, variant_mode="contract_zipf", target_alpha=0.1, skew_axis="contract")


def test_canonical_hash_mismatch_is_not_reused(tmp_path: Path) -> None:
    source = tmp_path / "source.csv"
    _write_source(source, 4)
    manifest = _manifest(source)
    built = build_canonical(source, tmp_path / "cache", manifest)
    canonical_path = tmp_path / "cache" / built["canonical_relative_path"]
    canonical_path.write_bytes(b"not a canonical gzip")
    with pytest.raises(WorkloadDataError, match="canonical cache hash mismatch"):
        build_canonical(source, tmp_path / "cache", manifest)


def test_cache_rejects_tampered_materialization_and_writer_has_record_size_guard(tmp_path: Path) -> None:
    source = tmp_path / "source.csv"; _write_source(source)
    canonical = build_canonical(source, tmp_path / "cache", _manifest(source)); path = tmp_path / "cache" / canonical["canonical_relative_path"]
    result = materialize(path, tmp_path / "cache", dataset_id="dcl_sales_polygon_271868", source_sha256=canonical["source_sha256"], requested_tx_count=10_000, seed=1)
    (tmp_path / "cache" / result["materialized_relative_path"]).write_bytes(b"tampered")
    with pytest.raises(WorkloadDataError, match="cache hash mismatch"):
        materialize(path, tmp_path / "cache", dataset_id="dcl_sales_polygon_271868", source_sha256=canonical["source_sha256"], requested_tx_count=10_000, seed=1)
    oversized = tmp_path / "oversized.jsonl.gz"
    with gzip.open(oversized, "wt", encoding="utf-8") as stream:
        stream.write(json.dumps({"schema_version": "mbe_workload_record_v1", "padding": "x" * (plane.MAX_JSONL_RECORD_BYTES + 1)}) + "\n")
    with pytest.raises(WorkloadDataError, match="maximum size"):
        list(plane._iter_canonical(oversized))


def test_canonical_and_original_paths_remain_bounded_streams(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = tmp_path / "source.csv"; _write_source(source, 2_000)
    tracemalloc.start()
    canonical = build_canonical(source, tmp_path / "cache", _manifest(source))
    _, canonical_peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    path = tmp_path / "cache" / canonical["canonical_relative_path"]
    monkeypatch.setattr(plane, "SUPPORTED_COUNTS", frozenset({500}))
    tracemalloc.start()
    result = materialize(path, tmp_path / "cache", dataset_id="dcl_sales_polygon_271868", source_sha256=canonical["source_sha256"], requested_tx_count=500, seed=3)
    _, materialized_peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert result["actual_tx_count"] == 500
    # The test fixture itself is ~0.6 MiB.  Peaks well below a complete decoded
    # row list protect the streaming canonical and original-window paths.
    assert canonical_peak < 8 * 1024 * 1024 and materialized_peak < 8 * 1024 * 1024


def test_cli_help_success_and_invalid_path_do_not_expose_source_path(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    command = [sys.executable, str(root / "scripts/workloads/validate_dcl_sales.py")]
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(root)
    assert subprocess.run(command + ["--help"], cwd=root, capture_output=True, text=True, env=environment).returncode == 0
    source = tmp_path / "source.csv"; _write_source(source, 2)
    passed = subprocess.run(command + ["--input", str(source), "--reports-root", str(tmp_path / "reports")], cwd=root, capture_output=True, text=True, env=environment)
    assert passed.returncode == 0 and str(tmp_path) not in passed.stdout
    failed = subprocess.run(command + ["--input", str(tmp_path / "missing.csv")], cwd=root, capture_output=True, text=True, env=environment)
    assert failed.returncode != 0


def test_generic_canonical_csv_adapter_materializes_without_decentraland_fields(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    manifest = plane.load_manifest("ethereum_like_sample_for_test_only")
    canonical = build_canonical(plane.raw_source_path(manifest), tmp_path / "cache", manifest)
    path = tmp_path / "cache" / canonical["canonical_relative_path"]
    materialized = materialize(path, tmp_path / "cache", dataset_id=manifest["dataset_id"], source_sha256=canonical["source_sha256"], requested_tx_count=4, seed=11, variant_mode="key_zipf", skew_axis="contract", target_alpha=1.0)
    records = _records(tmp_path / "cache" / materialized["materialized_relative_path"])
    assert root and materialized["actual_tx_count"] == 4
    assert all("sender_id" in row and "routing_source_key" in row for row in records)
    assert all("buyer_address" not in row and "contract_address" not in row for row in records)
    assert materialized["operation_counts"] == {"asset_transfer": 2, "contract_call": 1, "mint": 1}


def test_unknown_adapter_and_missing_generic_required_fields_fail(tmp_path: Path) -> None:
    source = tmp_path / "generic.csv"
    source.write_text((Path(__file__).resolve().parents[2] / "data/workloads/samples/ethereum_like_sample_for_test_only.csv").read_text(encoding="utf-8"), encoding="utf-8")
    manifest = {"dataset_id": "ethereum_like_sample_for_test_only", "adapter_id": "missing_adapter", "source_sha256": hashlib.sha256(source.read_bytes()).hexdigest()}
    with pytest.raises(WorkloadDataError, match="unknown dataset adapter_id"):
        build_canonical(source, tmp_path / "cache", manifest)
    rows = list(csv.DictReader(source.open(encoding="utf-8")))
    rows[0]["sender_id"] = ""
    with source.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    manifest["adapter_id"] = "canonical_csv_v1"
    manifest["source_sha256"] = hashlib.sha256(source.read_bytes()).hexdigest()
    with pytest.raises(WorkloadDataError, match="missing sender_id"):
        build_canonical(source, tmp_path / "cache", manifest)
    rows[0]["sender_id"] = "sender"
    rows[0]["state_keys"] = ""
    with source.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    manifest["source_sha256"] = hashlib.sha256(source.read_bytes()).hexdigest()
    with pytest.raises(WorkloadDataError, match="state_keys"):
        build_canonical(source, tmp_path / "cache2", manifest)


def test_key_zipf_alpha_changes_materialized_identity(tmp_path: Path) -> None:
    manifest = plane.load_manifest("ethereum_like_sample_for_test_only")
    canonical = build_canonical(plane.raw_source_path(manifest), tmp_path / "cache", manifest)
    path = tmp_path / "cache" / canonical["canonical_relative_path"]
    first = materialize(path, tmp_path / "cache", dataset_id=manifest["dataset_id"], source_sha256=canonical["source_sha256"], requested_tx_count=4, seed=11, variant_mode="key_zipf", skew_axis="contract", target_alpha=0.0)
    second = materialize(path, tmp_path / "cache", dataset_id=manifest["dataset_id"], source_sha256=canonical["source_sha256"], requested_tx_count=4, seed=11, variant_mode="key_zipf", skew_axis="contract", target_alpha=1.4)
    repeat = materialize(path, tmp_path / "cache", dataset_id=manifest["dataset_id"], source_sha256=canonical["source_sha256"], requested_tx_count=4, seed=11, variant_mode="key_zipf", skew_axis="contract", target_alpha=1.4)
    assert first["materialized_id"] != second["materialized_id"]
    assert second["materialized_sha256"] == repeat["materialized_sha256"]
