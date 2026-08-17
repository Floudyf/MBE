from __future__ import annotations

import csv
import gzip
import json
from pathlib import Path

import pytest

from backend.app.services import v5_metric_extractor
from backend.app.services import v5_workload_data_plane as plane
from backend.app.services.v5_workload_data_plane import WorkloadPreviewRequest
from backend.app.services.workload_adapters.axie_controlled_rmw_v1 import (
    AxieControlledRMWAdapter,
)
from backend.app.services.workload_adapters.common import access_list_digest
from backend.app.services.workload_adapters.registry import get_adapter


MANIFEST_PATH = (
    Path(__file__).resolve().parents[2]
    / "data"
    / "workloads"
    / "manifests"
    / "axie_infinity_controlled_prefix_rmw_v1.json"
)


def _write_sample(path: Path) -> None:
    access = [
        {"key": "axie:account:a", "mode": "read_write", "update_semantics": "account_rmw"},
        {"key": "axie:contract:market", "mode": "read", "update_semantics": "contract_read"},
        {"key": "axie:token:1", "mode": "read", "update_semantics": "token_read"},
        {"key": "axie:account:b", "mode": "read", "update_semantics": "account_read"},
    ]
    access = sorted(access, key=lambda item: item["key"])
    read_keys = sorted(item["key"] for item in access)
    write_keys = sorted(item["key"] for item in access if item["mode"] == "read_write")
    state_keys = sorted(set(read_keys) | set(write_keys))
    row = {
        "schema_version": "mbe_workload_record_v3",
        "transaction_id": "axie-controlled-1",
        "timestamp": "2021-10-01T00:00:00Z",
        "sender_id": "0xa",
        "receiver_id": "0xb",
        "operation_type": "axie_transfer",
        "state_keys": json.dumps(state_keys),
        "read_keys": json.dumps(read_keys),
        "write_keys": json.dumps(write_keys),
        "skew_keys": json.dumps(["axie:account:a", "axie:account:b"]),
        "routing_source_key": "axie:account:a",
        "routing_target_key": "axie:account:b",
        "provenance": "fixture_axie_real_skeleton",
        "metadata": json.dumps({"fixture": True}),
        "access_list_schema": "axie_controlled_access_v1",
        "access_list_source": "real_axie_transfer_skeleton_controlled_account_profile_v1",
        "access_list": json.dumps(access, separators=(",", ":")),
        "access_list_digest": access_list_digest(access),
        "segment_id": "0",
        "segment_row_index": "0",
        "template_source_row_index": "0",
        "target_access_theta": "0.8",
        "target_account_write_theta": "0.8",
        "access_profile": "read_heavy",
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(row))
        writer.writeheader()
        writer.writerow(row)


def test_axie_controlled_adapter_is_registered_and_preserves_direct_access(tmp_path: Path) -> None:
    adapter = get_adapter("axie_controlled_rmw_v1")
    assert isinstance(adapter, AxieControlledRMWAdapter)
    source = tmp_path / "sample.csv.gz"
    _write_sample(source)
    manifest = {"dataset_id": "axie_fixture", "adapter_id": "axie_controlled_rmw_v1"}
    summary = adapter.validate_source(source, manifest)
    assert summary.row_count == 1
    record = next(adapter.iter_canonical_records(source, manifest))
    assert record["schema_version"] == "mbe_workload_record_v3"
    assert record["access_list_schema"] == "axie_controlled_access_v1"
    assert record["metadata"]["target_access_theta"] == 0.8
    assert record["metadata"]["target_account_write_theta"] == 0.8
    assert record["metadata"]["read_write_key_count"] == 1
    assert record["access_list_digest"]


def test_axie_controlled_manifest_exposes_alien_style_dynamic_frontend_controls() -> None:
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    assert manifest["schema_version"] == "mbe_dataset_manifest_v2"
    assert manifest["dataset_id"] == "axie_infinity_controlled_prefix_rmw_v1"
    assert manifest["adapter_id"] == "axie_controlled_rmw_v1"
    assert manifest["source_layout"] == "variant_file_family"
    assert manifest["truth_label"] == "real_derived_controlled"
    assert manifest["supported_tx_counts"] == list(range(1000, 10001, 1000))
    assert manifest["allow_full_dataset"] is False
    assert len(manifest["prefix_variants"]) == 390
    assert len({item["master_file"] for item in manifest["prefix_variants"]}) == 39
    definition = manifest["variant_definitions"][0]
    assert definition["variant_mode"] == "validated_prefix"
    assert definition["kind"] == "derived"
    assert definition["selection_mode"] == "validated_prefix"
    params = {item["name"]: item for item in definition["parameters"]}
    assert params["access_profile"]["options"] == [
        "read_heavy",
        "balanced",
        "write_heavy",
    ]
    assert params["target_theta"]["options"] == [
        round(i / 10, 1) for i in range(13)
    ]
    assert "构造参数" in params["target_theta"]["label"]


def test_axie_controlled_manifest_resolves_profile_theta_prefix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    monkeypatch.setattr(plane, "ROOT", tmp_path)
    expected = (
        tmp_path
        / "dataset"
        / "Axie_Infinity"
        / "datasets"
        / "write_heavy"
        / "theta_1.2_10k.csv.gz"
    )
    expected.parent.mkdir(parents=True)
    expected.write_bytes(b"placeholder")

    audit_index = (
        tmp_path
        / "dataset"
        / "Axie_Infinity"
        / "axie_controlled_account_skew_v1"
        / "prefix_variant_index.json"
    )
    audit_index.parent.mkdir(parents=True, exist_ok=True)
    audit_index.write_text(
        json.dumps({
            "schema_version": "axie_controlled_prefix_index_v1",
            "variants": [{
                "access_profile": "write_heavy",
                "target_theta": 1.2,
                "target_account_write_theta": 1.2,
                "tx_count": 6000,
                "measured_account_write_theta": 1.199935386998708,
                "measured_account_access_theta": 1.199935386998708,
                "measured_read_ratio": 0.2,
                "measured_write_ratio": 0.8,
                "theta_axis": "account_write",
            }],
        }) + "\n",
        encoding="utf-8",
    )

    request = WorkloadPreviewRequest(
        source_type="dataset",
        plugin_id="canonical_trace_replay",
        dataset_id=manifest["dataset_id"],
        requested_tx_count=6000,
        seed=11,
        variant_mode="validated_prefix",
        selection_mode="validated_prefix",
        variant_parameters={"access_profile": "write_heavy", "target_theta": 1.2},
        source_sha256=manifest["source_sha256"],
    )
    path, selected_manifest, definition, resolved = plane.resolve_dataset_source(
        manifest, request
    )
    assert path == expected
    assert definition["selection_mode"] == "validated_prefix"
    assert resolved["variant_parameters"] == {
        "access_profile": "write_heavy",
        "target_theta": 1.2,
    }
    audit = resolved["prefix_audit"]
    assert audit["tx_count"] == 6000
    assert audit["master_file_sha256"] == selected_manifest["source_sha256"]
    assert audit["read_modify_write_topology_preserved"] is True
    assert audit["target_account_write_theta"] == 1.2
    assert audit["measured_account_write_theta"] == pytest.approx(1.199935386998708)
    assert audit["measured_account_access_theta"] == pytest.approx(1.199935386998708)
    assert audit["theta_axis"] == "account_write"


def test_axie_controlled_dataset_summary_becomes_selectable_when_family_files_exist(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    monkeypatch.setattr(plane, "ROOT", tmp_path)
    for relative in sorted({item["master_file"] for item in manifest["prefix_variants"]}):
        path = tmp_path / "dataset" / "Axie_Infinity" / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"catalog-presence-fixture")
    summary = plane.dataset_summary(manifest)
    assert summary.available is True
    assert summary.selectable is True
    assert summary.source_layout == "variant_file_family"
    assert summary.supported_tx_counts == list(range(1000, 10001, 1000))
    assert summary.variant_definitions[0]["parameters"][0]["name"] == "access_profile"


def test_axie_theta_audit_metrics_keep_write_and_touch_axes_distinct(tmp_path: Path) -> None:
    (tmp_path / "workload_replay_summary.json").write_text(
        json.dumps({
            "variant_parameters": {"access_profile": "balanced", "target_theta": 1.2},
            "audit_metadata": {
                "target_account_write_theta": 1.2,
                "measured_account_write_theta": 1.154,
                "measured_account_access_theta": 1.161,
                "theta_axis": "account_write",
            },
        }) + "\n",
        encoding="utf-8",
    )
    metrics = {"source_artifacts": []}
    v5_metric_extractor._apply_workload_replay_metrics(metrics, tmp_path)
    assert metrics["target_account_write_theta"] == 1.2
    assert metrics["measured_account_write_theta"] == 1.154
    assert metrics["measured_account_touch_theta"] == 1.161
    assert metrics["measured_account_access_theta"] == 1.161
    assert metrics["theta_axis"] == "account_write"
