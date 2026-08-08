from __future__ import annotations

import gzip
import json
import re
import shutil
from pathlib import Path

import pytest
from backend.app.services import v5_workload_data_plane as plane
from backend.app.services.v5_workload_data_plane import WorkloadDataError, WorkloadPreviewRequest
from backend.app.services.workload_adapters.alien_worlds_rmw_v1 import AlienWorldsRMWAdapter
from backend.app.services.workload_adapters.axie_full_day_v1 import AxieFullDayAdapter
from backend.app.services.workload_adapters.common import sha256_file
from backend.app.services.workload_adapters.registry import get_adapter
from backend.app.services.workload_adapters.tapos_exact_write_set_v1 import TaposExactWriteSetAdapter


SAMPLES = Path(__file__).resolve().parents[2] / "data" / "workloads" / "samples"
MANIFESTS = Path(__file__).resolve().parents[2] / "data" / "workloads" / "manifests"


def validate_json_schema(instance: object, schema: dict, *, path: str = "$") -> None:
    """Validate the JSON-Schema subset used by MBE workload contracts.

    The test stays dependency-free so a clean MBE development environment only
    needs the dependencies already declared in requirements-dev.txt.
    """

    def fail(message: str) -> None:
        raise AssertionError(f"JSON schema validation failed at {path}: {message}")

    expected_type = schema.get("type")
    if expected_type is not None:
        allowed = expected_type if isinstance(expected_type, list) else [expected_type]

        def matches(value: object, type_name: str) -> bool:
            if type_name == "null":
                return value is None
            if type_name == "object":
                return isinstance(value, dict)
            if type_name == "array":
                return isinstance(value, list)
            if type_name == "string":
                return isinstance(value, str)
            if type_name == "integer":
                return isinstance(value, int) and not isinstance(value, bool)
            if type_name == "number":
                return isinstance(value, (int, float)) and not isinstance(value, bool)
            if type_name == "boolean":
                return isinstance(value, bool)
            raise AssertionError(f"Unsupported JSON schema type in test helper: {type_name}")

        if not any(matches(instance, name) for name in allowed):
            fail(f"expected type {allowed}, got {type(instance).__name__}")

    if "const" in schema and instance != schema["const"]:
        fail(f"expected const {schema['const']!r}, got {instance!r}")
    if "enum" in schema and instance not in schema["enum"]:
        fail(f"expected one of {schema['enum']!r}, got {instance!r}")

    if isinstance(instance, str):
        if len(instance) < int(schema.get("minLength", 0)):
            fail(f"string is shorter than minLength={schema['minLength']}")
        pattern = schema.get("pattern")
        if pattern is not None and re.search(pattern, instance) is None:
            fail(f"string {instance!r} does not match pattern {pattern!r}")

    if isinstance(instance, (int, float)) and not isinstance(instance, bool):
        if "minimum" in schema and instance < schema["minimum"]:
            fail(f"value {instance!r} is below minimum={schema['minimum']}")

    if isinstance(instance, list):
        if len(instance) < int(schema.get("minItems", 0)):
            fail(f"array has fewer than minItems={schema['minItems']}")
        if schema.get("uniqueItems"):
            canonical = [json.dumps(item, ensure_ascii=False, sort_keys=True) for item in instance]
            if len(canonical) != len(set(canonical)):
                fail("array items are not unique")
        item_schema = schema.get("items")
        if isinstance(item_schema, dict):
            for index, item in enumerate(instance):
                validate_json_schema(item, item_schema, path=f"{path}[{index}]")

    if isinstance(instance, dict):
        required = schema.get("required", [])
        missing = [name for name in required if name not in instance]
        if missing:
            fail(f"missing required properties: {missing}")

        properties = schema.get("properties", {})
        additional = schema.get("additionalProperties", True)
        for key, value in instance.items():
            if key in properties:
                validate_json_schema(value, properties[key], path=f"{path}.{key}")
            elif additional is False:
                fail(f"additional property {key!r} is not allowed")
            elif isinstance(additional, dict):
                validate_json_schema(value, additional, path=f"{path}.{key}")


def _gzip_copy(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    with source.open("rb") as input_stream, target.open("wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, compresslevel=9, mtime=0) as output_stream:
            shutil.copyfileobj(input_stream, output_stream)


def _sample_manifest(*, dataset_id: str, adapter_id: str, source: Path, truth_label: str = "real_observed") -> dict:
    return {
        "schema_version": "mbe_dataset_manifest_v2",
        "dataset_id": dataset_id,
        "adapter_id": adapter_id,
        "source_sha256": sha256_file(source),
        "row_count": 100,
        "truth_label": truth_label,
    }


def _materialize_sample(tmp_path: Path, source: Path, manifest: dict, *, selection_mode: str = "contiguous_window") -> tuple[dict, list[dict]]:
    cache = tmp_path / "cache"
    canonical = plane.build_canonical(source, cache, manifest)
    result = plane.materialize(
        cache / canonical["canonical_relative_path"],
        cache,
        dataset_id=manifest["dataset_id"],
        source_sha256=canonical["source_sha256"],
        source_file_sha256=canonical["source_file_sha256"],
        requested_tx_count=100,
        seed=17,
        variant_mode="validated_prefix" if selection_mode == "validated_prefix" else "original_window",
        selection_mode=selection_mode,
        supported_counts={100},
        variant_parameters={"access_profile": "balanced", "target_theta": 0.8} if selection_mode == "validated_prefix" else {},
        truth_label=manifest["truth_label"],
    )
    records: list[dict] = []
    with gzip.open(cache / result["materialized_relative_path"], "rt", encoding="utf-8") as stream:
        records = [json.loads(line) for line in stream]
    return result, records



def test_universal_dataset_manifests_and_direct_record_schema_are_machine_validated() -> None:
    manifest_schema = json.loads((MANIFESTS.parent / "schemas" / "dataset_manifest_v2.schema.json").read_text(encoding="utf-8"))
    record_schema = json.loads((MANIFESTS.parent / "schemas" / "mbe_workload_record_v3.schema.json").read_text(encoding="utf-8"))
    for name in (
        "dcl_sales_polygon_271868.json",
        "alien_worlds_wax_reconstructed_prefix_stable_rmw_v2.json",
        "axie_2021_10_01_full_day.json",
        "tapos_peak_exact_write_set.json",
    ):
        validate_json_schema(json.loads((MANIFESTS / name).read_text(encoding="utf-8")), manifest_schema)

    source = SAMPLES / "tapos_peak_exact_write_set_sample.csv"
    manifest = _sample_manifest(dataset_id="schema_sample", adapter_id="tapos_exact_write_set_v1", source=source)
    record = next(TaposExactWriteSetAdapter().iter_canonical_records(source, manifest))
    validate_json_schema(record, record_schema)

def test_registry_exposes_all_universal_dataset_adapters() -> None:
    assert isinstance(get_adapter("alien_worlds_rmw_v1"), AlienWorldsRMWAdapter)
    assert isinstance(get_adapter("axie_full_day_v1"), AxieFullDayAdapter)
    assert isinstance(get_adapter("tapos_exact_write_set_v1"), TaposExactWriteSetAdapter)


@pytest.mark.parametrize(
    ("adapter_id", "sample_name", "expected_schema", "expected_modes"),
    [
        ("axie_full_day_v1", "axie_2021_10_01_full_day_sample.csv", "axie_transfer_semantic_access_v1", {"read", "read_write"}),
        ("tapos_exact_write_set_v1", "tapos_peak_exact_write_set_sample.csv", "tapos_exact_write_set_v1", {"write"}),
    ],
)
def test_single_file_samples_materialize_as_direct_access_v3(
    tmp_path: Path,
    adapter_id: str,
    sample_name: str,
    expected_schema: str,
    expected_modes: set[str],
) -> None:
    source = SAMPLES / sample_name
    manifest = _sample_manifest(dataset_id=f"sample_{adapter_id}", adapter_id=adapter_id, source=source)
    result, records = _materialize_sample(tmp_path, source, manifest)

    assert result["actual_tx_count"] == 100
    assert result["truth_label"] == "real_observed"
    assert result["routing_source_basis"] == "logical_routing_key"
    assert result["expected_cross_shard_count"] == result["selected_window_preview"]["cross_shard_count"]
    assert result["expected_cross_shard_ratio"] == result["selected_window_preview"]["cross_shard_ratio"]
    assert len(records) == 100
    assert {record["schema_version"] for record in records} == {"mbe_workload_record_v3"}
    assert {record["access_list_schema"] for record in records} == {expected_schema}
    assert {item["mode"] for record in records for item in record["access_list"]} == expected_modes
    assert all(sorted(record["state_keys"]) == [item["key"] for item in record["access_list"]] for record in records)
    assert all(record["access_list_digest"] for record in records)


def test_alien_world_sample_preserves_rmw_and_validated_prefix(tmp_path: Path) -> None:
    source = tmp_path / "theta_0.8_10k.csv.gz"
    _gzip_copy(SAMPLES / "alien_worlds_balanced_theta_0.8_sample.csv", source)
    manifest = _sample_manifest(
        dataset_id="alien_worlds_sample",
        adapter_id="alien_worlds_rmw_v1",
        source=source,
        truth_label="real_derived_controlled",
    )
    result, records = _materialize_sample(tmp_path, source, manifest, selection_mode="validated_prefix")

    assert result["start_offset"] == 0
    assert result["selection_mode"] == "validated_prefix"
    assert result["truth_label"] == "real_derived_controlled"
    assert result["routing_source_basis"] == "logical_routing_key"
    assert result["expected_cross_shard_count"] == result["selected_window_preview"]["cross_shard_count"]
    assert len(records) == 100
    for record in records:
        read_write = [item for item in record["access_list"] if item["mode"] == "read_write"]
        assert len(read_write) == 1
        assert record["metadata"]["read_write_key_count"] == 1
        assert read_write[0]["key"] in record["state_keys"]


def test_alien_manifest_resolves_profile_theta_and_prefix_without_method_ownership(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    manifest = json.loads((MANIFESTS / "alien_worlds_wax_reconstructed_prefix_stable_rmw_v2.json").read_text(encoding="utf-8"))
    monkeypatch.setattr(plane, "ROOT", tmp_path)
    expected = tmp_path / "dataset" / "Alien_World" / "datasets" / "write_heavy" / "theta_1.2_10k.csv.gz"
    expected.parent.mkdir(parents=True)
    expected.write_bytes(b"placeholder")

    request = WorkloadPreviewRequest(
        source_type="dataset",
        plugin_id="canonical_trace_replay",
        dataset_id=manifest["dataset_id"],
        requested_tx_count=6_000,
        seed=11,
        variant_mode="validated_prefix",
        selection_mode="validated_prefix",
        variant_parameters={"access_profile": "write_heavy", "target_theta": 1.2},
        source_sha256=manifest["source_sha256"],
    )
    path, selected_manifest, definition, resolved = plane.resolve_dataset_source(manifest, request)

    assert path == expected
    assert definition["variant_mode"] == "validated_prefix"
    assert resolved["variant_parameters"] == {"access_profile": "write_heavy", "target_theta": 1.2}
    assert resolved["prefix_audit"]["tx_count"] == 6_000
    assert resolved["prefix_audit"]["read_modify_write_topology_preserved"] is True
    assert selected_manifest["source_sha256"] == resolved["prefix_audit"]["master_file_sha256"]


def test_variant_parameters_reject_unknown_fields() -> None:
    manifest = json.loads((MANIFESTS / "alien_worlds_wax_reconstructed_prefix_stable_rmw_v2.json").read_text(encoding="utf-8"))
    definition = manifest["variant_definitions"][0]
    request = WorkloadPreviewRequest(
        source_type="dataset",
        plugin_id="canonical_trace_replay",
        dataset_id=manifest["dataset_id"],
        requested_tx_count=1_000,
        seed=1,
        variant_mode="validated_prefix",
        selection_mode="validated_prefix",
        variant_parameters={"access_profile": "balanced", "target_theta": 0.8, "execution_method": "groundhog"},
    )
    with pytest.raises(WorkloadDataError, match="unknown variant parameter"):
        plane._normalize_variant_parameters(request, definition)
