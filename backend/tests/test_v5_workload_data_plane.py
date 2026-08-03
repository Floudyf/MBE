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
from backend.app.models.v5_experiment_spec import V5ExperimentSpec, V5PluginSelection, V5Topology
from backend.app.services.v5_workload_data_plane import WorkloadDataError, WorkloadPreviewRequest, build_canonical, materialize, preview_workload, validate_csv


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


def _write_preview_manifest(root: Path, source: Path, *, count: int) -> Path:
    manifest_root = root / "data" / "workloads" / "manifests"
    manifest_root.mkdir(parents=True)
    manifest = {
        "schema_version": "mbe_dataset_manifest_v1",
        "dataset_id": "dcl_sales_polygon_271868",
        "display_name": "DCL preview fixture",
        "description": "fixture",
        "source_platform": "decentraland_marketplace",
        "source_chain": "polygon_mainnet",
        "dataset_type": "marketplace_sales",
        "included_categories": ["wearable", "emote"],
        "excluded_categories": ["land"],
        "local_raw_relative_path": source.name,
        "source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
        "row_count": count,
        "unique_source_tx_hash_count": (count + 1) // 2,
        "time_start_ms": 1_700_000_000_000,
        "time_end_ms": 1_700_000_000_000 + count - 1,
        "category_counts": {"emote": (count + 3) // 4, "wearable": count - ((count + 3) // 4)},
        "operation_counts": {"emote": (count + 3) // 4, "wearable": count - ((count + 3) // 4)},
        "truth_label": "real_observed",
        "verification_method": "fixture",
        "verification_sample_count": 1,
        "verification_results": "fixture",
        "usage_note": "fixture",
        "generator_version": plane.GENERATOR_VERSION,
        "supported_variants": ["original_window", "key_zipf", "contract_zipf"],
        "supported_skew_axes": ["contract"],
        "default_skew_axis": "contract",
    }
    (manifest_root / "dcl_sales_polygon_271868.json").write_text(json.dumps(manifest), encoding="utf-8")
    return manifest_root


def _records(path: Path) -> list[dict]:
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        return [json.loads(line) for line in stream]


def _preview_request(**overrides: object) -> WorkloadPreviewRequest:
    payload = {
        "source_type": "dataset",
        "plugin_id": "canonical_trace_replay",
        "dataset_id": "dcl_sales_polygon_271868",
        "requested_tx_count": 4,
        "seed": 11,
        "variant_mode": "original_window",
    }
    payload.update(overrides)
    return WorkloadPreviewRequest(**payload)


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
    assert records[0]["schema_version"] == "mbe_workload_record_v2"
    assert records[0]["access_list_schema"] == "dcl_sale_access_template_v1"
    assert records[0]["access_list_source"] == "semantics_derived"
    assert {item["role"] for item in records[0]["access_template"]} == {
        "sender_balance",
        "sender_nonce",
        "receiver_balance",
        "receiver_nonce",
        "market_contract",
        "category_metadata",
    }
    assert records[0]["state_keys"][0].startswith("market:")
    assert not any(key.startswith(("account:sender:", "account:receiver:", "contract:")) for key in records[0]["state_keys"])
    assert records[0]["source_event_id"] == "sale-0"
    assert records[0]["contract"].startswith("0x") and records[0]["category"] in {"wearable", "emote"}
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


def test_1k_is_formally_supported_without_local_smoke_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MBE_V5_LOCAL_SMOKE_COUNTS", "123")
    assert 100 in plane.supported_workload_counts()
    assert 1_000 in plane.supported_workload_counts()
    assert 123 not in plane.supported_workload_counts()

    source = tmp_path / "source.csv"
    _write_source(source, 1_200)
    canonical = build_canonical(source, tmp_path / "cache", _manifest(source))
    path = tmp_path / "cache" / canonical["canonical_relative_path"]
    result = materialize(path, tmp_path / "cache", dataset_id="dcl_sales_polygon_271868", source_sha256=canonical["source_sha256"], requested_tx_count=1_000, seed=73)

    assert result["actual_tx_count"] == 1_000
    assert result["requested_tx_count"] == 1_000


def test_v5_experiment_spec_default_uses_formal_1k_count() -> None:
    spec = V5ExperimentSpec(
        execution_backend="real_cluster",
        plugin_selections=[V5PluginSelection(category="workload", plugin_id="deterministic_signed_synthetic")],
        topology=V5Topology(nodes=8, shards=2, validators_per_shard=4),
    )

    assert spec.tx_count == 1_000
    assert spec.workload_source is not None
    assert spec.workload_source.requested_tx_count == 1_000


def test_preview_selection_digest_is_stable_and_does_not_materialize(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = tmp_path / "source.csv"
    _write_source(source, 6)
    manifest_root = _write_preview_manifest(tmp_path, source, count=6)
    cache_root = tmp_path / ".cache" / "workloads"
    monkeypatch.setattr(plane, "ROOT", tmp_path)
    monkeypatch.setattr(plane, "MANIFEST_ROOT", manifest_root)
    monkeypatch.setattr(plane, "WORKLOAD_CACHE_ROOT", cache_root)
    monkeypatch.setattr(plane, "SUPPORTED_COUNTS", frozenset({4, 5}))

    first = preview_workload(_preview_request(requested_tx_count=4), shards=2).model_dump()
    second = preview_workload(_preview_request(requested_tx_count=4), shards=2).model_dump()

    assert first["blockers"] == []
    assert first["selected_window_preview"]["selection_digest"] == second["selected_window_preview"]["selection_digest"]
    assert first["selected_window_preview"]["actual_selected_count"] == 4
    assert first["selected_window_preview"]["requested_tx_count"] == 4
    assert first["selected_window_preview"]["operation_counts"] == first["selected_window_preview"]["category_counts"]
    assert all(0 <= value <= 1 for value in first["selected_window_preview"]["category_percentages"].values())
    assert not (cache_root / "materialized").exists()
    assert not (cache_root / "canonical").exists()


def test_preview_and_materialize_share_selection_digest_for_original_and_key_zipf(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = tmp_path / "source.csv"
    _write_source(source, 6)
    manifest_root = _write_preview_manifest(tmp_path, source, count=6)
    cache_root = tmp_path / ".cache" / "workloads"
    monkeypatch.setattr(plane, "ROOT", tmp_path)
    monkeypatch.setattr(plane, "MANIFEST_ROOT", manifest_root)
    monkeypatch.setattr(plane, "WORKLOAD_CACHE_ROOT", cache_root)
    monkeypatch.setattr(plane, "SUPPORTED_COUNTS", frozenset({4, 5}))

    original_preview = preview_workload(_preview_request(requested_tx_count=4), shards=2).model_dump()["selected_window_preview"]
    canonical = build_canonical(source, cache_root, _manifest(source))
    original_materialized = materialize(cache_root / canonical["canonical_relative_path"], cache_root, dataset_id="dcl_sales_polygon_271868", source_sha256=canonical["source_sha256"], requested_tx_count=4, seed=11)
    assert original_materialized["selection_digest"] == original_preview["selection_digest"]
    assert original_materialized["selected_window_preview"]["actual_selected_count"] == original_preview["actual_selected_count"]
    assert len(_records(cache_root / original_materialized["materialized_relative_path"])) == original_preview["actual_selected_count"]

    key_zipf_request = _preview_request(requested_tx_count=4, variant_mode="key_zipf", skew_axis="contract", target_alpha=1.0)
    key_zipf_preview = preview_workload(key_zipf_request, shards=2).model_dump()["selected_window_preview"]
    key_zipf_materialized = materialize(cache_root / canonical["canonical_relative_path"], cache_root, dataset_id="dcl_sales_polygon_271868", source_sha256=canonical["source_sha256"], requested_tx_count=4, seed=11, variant_mode="key_zipf", skew_axis="contract", target_alpha=1.0)
    assert key_zipf_materialized["selection_digest"] == key_zipf_preview["selection_digest"]
    assert len(_records(cache_root / key_zipf_materialized["materialized_relative_path"])) == key_zipf_preview["actual_selected_count"]


def test_selection_digest_changes_with_seed_count_and_alpha(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = tmp_path / "source.csv"
    _write_source(source, 6)
    manifest_root = _write_preview_manifest(tmp_path, source, count=6)
    monkeypatch.setattr(plane, "ROOT", tmp_path)
    monkeypatch.setattr(plane, "MANIFEST_ROOT", manifest_root)
    monkeypatch.setattr(plane, "WORKLOAD_CACHE_ROOT", tmp_path / ".cache" / "workloads")
    monkeypatch.setattr(plane, "SUPPORTED_COUNTS", frozenset({4, 5}))

    base = preview_workload(_preview_request(requested_tx_count=4, seed=11), shards=2).selected_window_preview["selection_digest"]
    different_seed = preview_workload(_preview_request(requested_tx_count=4, seed=12), shards=2).selected_window_preview["selection_digest"]
    different_count = preview_workload(_preview_request(requested_tx_count=5, seed=11), shards=2).selected_window_preview["selection_digest"]
    alpha_low = preview_workload(_preview_request(requested_tx_count=4, variant_mode="key_zipf", skew_axis="contract", target_alpha=0.0), shards=2).selected_window_preview["selection_digest"]
    alpha_high = preview_workload(_preview_request(requested_tx_count=4, variant_mode="key_zipf", skew_axis="contract", target_alpha=1.4), shards=2).selected_window_preview["selection_digest"]

    assert base != different_seed
    assert base != different_count
    assert alpha_low != alpha_high


def test_full_dataset_preview_uses_dataset_row_count(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = tmp_path / "source.csv"
    _write_source(source, 6)
    manifest_root = _write_preview_manifest(tmp_path, source, count=6)
    monkeypatch.setattr(plane, "ROOT", tmp_path)
    monkeypatch.setattr(plane, "MANIFEST_ROOT", manifest_root)
    monkeypatch.setattr(plane, "WORKLOAD_CACHE_ROOT", tmp_path / ".cache" / "workloads")
    monkeypatch.setattr(plane, "SUPPORTED_COUNTS", frozenset({4, 5}))

    preview = preview_workload(_preview_request(requested_tx_count=4, use_full_dataset=True), shards=2)

    assert preview.tx_count == 6
    assert preview.selected_window_preview["actual_selected_count"] == 6


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
