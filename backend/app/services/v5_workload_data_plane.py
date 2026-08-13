"""Streaming, deterministic V5 workload data-plane primitives.

This module deliberately has no FastAPI or scheduler dependency.  It keeps the
raw dataset read-only and publishes only completed, content-addressed cache
directories.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import os
import shutil
import tempfile
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal, Iterator

from pydantic import BaseModel, Field, model_validator

from backend.app.core.paths import ROOT, WORKLOAD_CACHE_ROOT
from backend.app.services.workload_adapters.base import SourceValidationSummary
from backend.app.services.workload_adapters.registry import get_adapter

# 100 is the bounded four-method smoke tier retained by the V5 data-plane
# acceptance evidence.  It exercises the real dataset-derived path without
# silently falling back to synthetic workload generation.
SUPPORTED_COUNTS = frozenset({100, 1_000, 10_000, 50_000, 100_000, 250_000})
SUPPORTED_ALPHAS = frozenset({0.0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2, 1.4})
GENERATOR_VERSION = "v5_workload_data_plane_v4_universal_access_list"
SELECTOR_VERSION = "universal_selector_v1"
MAX_JSONL_RECORD_BYTES = 1024 * 1024
MANIFEST_ROOT = ROOT / "data" / "workloads" / "manifests"


class WorkloadDataError(ValueError):
    pass


class _HashingSink:
    def __init__(self) -> None:
        self.digest = hashlib.sha256()

    def write(self, data: bytes) -> int:
        self.digest.update(data)
        return len(data)

    def flush(self) -> None:
        return None


class DatasetSummaryDTO(BaseModel):
    schema_version: Literal["mbe_dataset_registry_item_v2"] = "mbe_dataset_registry_item_v2"
    dataset_id: str
    display_name: str
    description: str
    source_platform: str
    source_chain: str
    truth_label: str
    row_count: int
    operation_counts: dict[str, int]
    category_counts: dict[str, int] = Field(default_factory=dict)
    source_sha256: str
    source_layout: str = "single_file"
    supported_variants: list[str] = Field(default_factory=list)
    variant_definitions: list[dict[str, Any]] = Field(default_factory=list)
    supported_tx_counts: list[int] = Field(default_factory=list)
    allow_full_dataset: bool = True
    supported_skew_axes: list[str] = Field(default_factory=list)
    default_skew_axis: str | None = None
    available: bool
    selectable: bool
    validation_status: str
    blockers: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)

class DatasetDetailDTO(DatasetSummaryDTO):
    source_platform: str
    source_chain: str
    dataset_type: str
    included_categories: list[str]
    excluded_categories: list[str]
    unique_source_tx_hash_count: int
    time_start_ms: int
    time_end_ms: int
    verification_method: str
    verification_sample_count: int
    verification_results: str
    usage_note: str
    generator_version: str
    variants: list[dict[str, Any]]
    adapter_id: str
    supported_variants: list[str]
    supported_skew_axes: list[str]
    default_skew_axis: str | None = None


class WorkloadPreviewRequest(BaseModel):
    schema_version: Literal["mbe_workload_source_v1"] = "mbe_workload_source_v1"
    source_type: Literal["synthetic", "dataset"]
    plugin_id: Literal["deterministic_signed_synthetic", "canonical_trace_replay"]
    dataset_id: str | None = None
    requested_tx_count: int = Field(ge=1, le=1_000_000)
    seed: int
    variant_mode: str | None = None
    selection_mode: Literal["contiguous_window", "validated_prefix"] = "contiguous_window"
    replay_mode: Literal["max_throughput", "fixed_rate"] = "max_throughput"
    target_submission_tps: int | None = Field(default=None, ge=1, le=1_000_000)
    target_alpha: float | None = None
    skew_axis: str | None = None
    variant_parameters: dict[str, str | int | float | bool] = Field(default_factory=dict)
    use_full_dataset: bool = False
    source_sha256: str | None = None

    @model_validator(mode="after")
    def _validate_replay_pacing(self) -> "WorkloadPreviewRequest":
        if self.replay_mode == "fixed_rate" and self.target_submission_tps is None:
            raise ValueError("fixed_rate workload preview requires target_submission_tps")
        if self.replay_mode == "max_throughput" and self.target_submission_tps is not None:
            raise ValueError("max_throughput workload preview must not carry target_submission_tps")
        return self


class WorkloadPreviewDTO(BaseModel):
    schema_version: Literal["mbe_workload_preview_v1"] = "mbe_workload_preview_v1"
    source_type: str
    plugin_id: str
    dataset_id: str | None = None
    tx_count: int
    dataset_summary: dict[str, Any] | None = None
    selected_window_preview: dict[str, Any] = Field(default_factory=dict)
    selected_time_range: dict[str, int | None]
    operation_counts: dict[str, int]
    category_counts: dict[str, int] = Field(default_factory=dict)
    natural_skew: dict[str, Any]
    derived_skew: dict[str, Any]
    expected_cross_shard: dict[str, Any]
    shard_distribution: dict[str, int]
    materialization_cache_status: dict[str, Any]
    blockers: list[str]
    warnings: list[str]


class WorkloadMaterializeDTO(BaseModel):
    schema_version: Literal["mbe_workload_materialization_v2"] = "mbe_workload_materialization_v2"
    dataset_id: str
    materialized_id: str
    variant_id: str
    variant_mode: str
    selection_mode: str
    variant_parameters: dict[str, str | int | float | bool] = Field(default_factory=dict)
    truth_label: str
    materialized_relative_path: str
    canonical_relative_path: str
    source_sha256: str
    source_file_sha256: str | None = None
    canonical_sha256: str
    materialized_sha256: str
    requested_tx_count: int
    actual_tx_count: int
    seed: int
    target_alpha: float | None = None
    cache_hit: bool
    no_fallback: bool = True
    summary: dict[str, Any]

@dataclass(frozen=True)
class CsvValidationSummary:
    source_sha256: str
    row_count: int
    unique_source_tx_hash_count: int
    time_start_ms: int
    time_end_ms: int
    operation_counts: dict[str, int]

    @property
    def category_counts(self) -> dict[str, int]:
        return self.operation_counts


def load_manifests() -> list[dict[str, Any]]:
    manifests = []
    if not MANIFEST_ROOT.is_dir():
        return manifests
    for path in sorted(MANIFEST_ROOT.glob("*.json")):
        manifests.append(json.loads(path.read_text(encoding="utf-8")))
    return manifests


def load_manifest(dataset_id: str) -> dict[str, Any]:
    for manifest in load_manifests():
        if manifest.get("dataset_id") == dataset_id:
            return manifest
    raise WorkloadDataError("unknown dataset")


def _safe_repo_path(relative_path: str, *, base: Path | None = None) -> Path:
    rel = str(relative_path or "").replace("\\", "/").strip()
    candidate = Path(rel)
    if not rel or candidate.is_absolute() or ".." in candidate.parts:
        raise WorkloadDataError("dataset manifest has unsafe local path")
    return (base or ROOT) / candidate


def _source_layout(manifest: dict[str, Any]) -> str:
    return str(manifest.get("source_layout") or "single_file")


def _variant_definitions(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    definitions = list(manifest.get("variant_definitions") or [])
    if definitions:
        return definitions
    axes = list(manifest.get("supported_skew_axes") or [])
    result: list[dict[str, Any]] = []
    for mode in list(manifest.get("supported_variants") or ["original_window"]):
        result.append({
            "variant_mode": mode,
            "display_name": mode,
            "kind": "derived" if mode in {"contract_zipf", "key_zipf"} else "original",
            "selection_mode": "contiguous_window",
            "parameters": ([
                {"name": "skew_axis", "label": "偏斜轴", "type": "enum", "options": axes, "default": manifest.get("default_skew_axis") or (axes[0] if axes else None)},
                {"name": "target_alpha", "label": "偏斜度 alpha", "type": "number_enum", "options": sorted(SUPPORTED_ALPHAS), "default": 1.0},
            ] if mode in {"contract_zipf", "key_zipf"} else []),
        })
    return result


def _variant_definition(manifest: dict[str, Any], variant_mode: str) -> dict[str, Any]:
    for definition in _variant_definitions(manifest):
        if definition.get("variant_mode") == variant_mode:
            return definition
    raise WorkloadDataError("variant_mode is not supported by dataset")


def _normalize_variant_parameters(request: WorkloadPreviewRequest, definition: dict[str, Any]) -> dict[str, str | int | float | bool]:
    values: dict[str, str | int | float | bool] = dict(request.variant_parameters)
    if request.skew_axis is not None:
        values.setdefault("skew_axis", request.skew_axis)
    if request.target_alpha is not None:
        values.setdefault("target_alpha", request.target_alpha)
    fields = list(definition.get("parameters") or [])
    allowed_names = {str(field.get("name") or "") for field in fields if field.get("name")}
    unknown = sorted(set(values) - allowed_names)
    if unknown:
        raise WorkloadDataError("unknown variant parameter(s): " + ", ".join(unknown))
    for field in fields:
        name = str(field.get("name") or "")
        if not name:
            continue
        if name not in values and field.get("default") is not None:
            values[name] = field["default"]
        if field.get("required", True) and name not in values:
            raise WorkloadDataError(f"variant parameter {name} is required")
        if name not in values:
            continue
        options = field.get("options")
        if options is not None and values[name] not in options:
            # JSON may decode integral theta values as int while the request uses float.
            if not any(str(item) == str(values[name]) for item in options):
                raise WorkloadDataError(f"variant parameter {name} is not supported")
    return values


def _match_variant_value(left: Any, right: Any) -> bool:
    if isinstance(left, (int, float)) or isinstance(right, (int, float)):
        try:
            return abs(float(left) - float(right)) < 1e-12
        except (TypeError, ValueError):
            pass
    return str(left) == str(right)


def _resolve_family_entry(manifest: dict[str, Any], *, requested_tx_count: int, parameters: dict[str, Any]) -> dict[str, Any]:
    for entry in manifest.get("prefix_variants") or []:
        if int(entry.get("tx_count") or entry.get("prefix_record_count") or 0) != requested_tx_count:
            continue
        if all(_match_variant_value(entry.get(name), value) for name, value in parameters.items()):
            return dict(entry)
    raise WorkloadDataError("requested dataset variant/prefix is not present in the manifest")


def resolve_dataset_source(manifest: dict[str, Any], request: WorkloadPreviewRequest) -> tuple[Path, dict[str, Any], dict[str, Any], dict[str, Any]]:
    variant_mode = request.variant_mode or "original_window"
    definition = _variant_definition(manifest, variant_mode)
    parameters = _normalize_variant_parameters(request, definition)
    if _source_layout(manifest) == "single_file":
        path = raw_source_path(manifest)
        selected_manifest = dict(manifest)
        return path, selected_manifest, definition, {"variant_parameters": parameters}
    if _source_layout(manifest) != "variant_file_family":
        raise WorkloadDataError("unsupported dataset source_layout")
    root = _safe_repo_path(str(manifest.get("local_raw_relative_path") or ""))
    entry = _resolve_family_entry(manifest, requested_tx_count=request.requested_tx_count, parameters=parameters)
    relative = str(entry.get("master_file") or "")
    path = _safe_repo_path(relative, base=root)
    selected_manifest = dict(manifest)
    selected_manifest["source_sha256"] = str(entry.get("master_file_sha256") or "")
    selected_manifest["row_count"] = int(entry.get("master_record_count") or 10_000)
    selected_manifest["selected_family_entry"] = entry
    return path, selected_manifest, definition, {"variant_parameters": parameters, "prefix_audit": entry}


def raw_source_path(manifest: dict[str, Any]) -> Path:
    return _safe_repo_path(str(manifest.get("local_raw_relative_path") or ""))

def adapter_for_manifest(manifest: dict[str, Any]):
    adapter_id = str(manifest.get("adapter_id") or "decentraland_sales_v1")
    try:
        return get_adapter(adapter_id)
    except ValueError as exc:
        raise WorkloadDataError(str(exc)) from exc


def dataset_status(manifest: dict[str, Any]) -> tuple[bool, str, list[str], CsvValidationSummary | None]:
    blockers: list[str] = []
    layout = _source_layout(manifest)
    if layout == "single_file":
        path = raw_source_path(manifest)
        if not path.is_file():
            return False, "unavailable", ["full dataset source is not present in this checkout"], None
        expected_size = int(manifest.get("source_size_bytes") or 0)
        if expected_size and path.stat().st_size != expected_size:
            return False, "invalid", ["source file size does not match manifest"], None
        if manifest.get("catalog_validation") == "full":
            try:
                summary = _csv_summary(adapter_for_manifest(manifest).validate_source(path, manifest, expected_sha256=manifest.get("source_sha256") or None))
            except (WorkloadDataError, ValueError) as exc:
                return False, "invalid", [str(exc)], None
            if manifest.get("row_count") and int(manifest["row_count"]) != summary.row_count:
                blockers.append("manifest row_count does not match source")
            return not blockers, "valid" if not blockers else "invalid", blockers, summary
        return True, "present_unvalidated", [], None
    if layout == "variant_file_family":
        root = raw_source_path(manifest)
        if not root.is_dir():
            return False, "unavailable", ["dataset family directory is not present in this checkout"], None
        missing: list[str] = []
        seen: set[str] = set()
        for entry in manifest.get("prefix_variants") or []:
            relative = str(entry.get("master_file") or "")
            if not relative or relative in seen:
                continue
            seen.add(relative)
            if not _safe_repo_path(relative, base=root).is_file():
                missing.append(relative)
                if len(missing) >= 3:
                    break
        if missing:
            return False, "unavailable", ["missing dataset family files: " + ", ".join(missing)], None
        if not seen:
            return False, "invalid", ["dataset family manifest contains no physical files"], None
        return True, "present_unvalidated", [], None
    return False, "invalid", ["unsupported dataset source_layout"], None

def dataset_summary(manifest: dict[str, Any]) -> DatasetSummaryDTO:
    available, status, blockers, _ = dataset_status(manifest)
    operations = dict(manifest.get("operation_counts") or manifest.get("category_counts") or {})
    definitions = _variant_definitions(manifest)
    counts = sorted({int(item) for item in manifest.get("supported_tx_counts") or supported_workload_counts()})
    warnings = list(manifest.get("warnings") or [])
    if status == "present_unvalidated":
        warnings.append("catalog checks file presence/size; full hash and schema validation runs before materialization")
    return DatasetSummaryDTO(
        dataset_id=manifest["dataset_id"],
        display_name=manifest.get("display_name", manifest["dataset_id"]),
        description=manifest.get("description", ""),
        source_platform=manifest.get("source_platform", ""),
        source_chain=manifest.get("source_chain", ""),
        truth_label=manifest.get("truth_label", "real_observed"),
        row_count=int(manifest.get("row_count") or 0),
        operation_counts=operations,
        category_counts=operations,
        source_sha256=str(manifest.get("source_sha256") or ""),
        source_layout=_source_layout(manifest),
        supported_variants=[str(item.get("variant_mode")) for item in definitions],
        variant_definitions=definitions,
        supported_tx_counts=counts,
        allow_full_dataset=bool(manifest.get("allow_full_dataset", True)),
        supported_skew_axes=list(manifest.get("supported_skew_axes") or []),
        default_skew_axis=manifest.get("default_skew_axis"),
        available=available,
        selectable=available and status in {"valid", "present_unvalidated"},
        validation_status=status,
        blockers=blockers,
        warnings=warnings,
    )

def dataset_detail(dataset_id: str) -> DatasetDetailDTO:
    manifest = load_manifest(dataset_id)
    summary = dataset_summary(manifest)
    variants = _variant_definitions(manifest)
    return DatasetDetailDTO(
        **summary.model_dump(),
        dataset_type=manifest.get("dataset_type", ""),
        included_categories=list(manifest.get("included_categories") or []),
        excluded_categories=list(manifest.get("excluded_categories") or []),
        unique_source_tx_hash_count=int(manifest.get("unique_source_tx_hash_count") or 0),
        time_start_ms=int(manifest.get("time_start_ms") or 0),
        time_end_ms=int(manifest.get("time_end_ms") or 0),
        verification_method=manifest.get("verification_method", ""),
        verification_sample_count=int(manifest.get("verification_sample_count") or 0),
        verification_results=manifest.get("verification_results", ""),
        usage_note=manifest.get("usage_note", ""),
        generator_version=manifest.get("generator_version", GENERATOR_VERSION),
        variants=variants,
        adapter_id=str(manifest.get("adapter_id") or "decentraland_sales_v1"),
    )

def preview_workload(request: WorkloadPreviewRequest, *, shards: int = 4) -> WorkloadPreviewDTO:
    if request.source_type == "synthetic":
        return WorkloadPreviewDTO(
            source_type="synthetic", plugin_id="deterministic_signed_synthetic", tx_count=request.requested_tx_count,
            selected_window_preview={"requested_tx_count": request.requested_tx_count, "actual_selected_count": request.requested_tx_count, "selection_digest": None},
            selected_time_range={"start_ms": None, "end_ms": None}, operation_counts={"synthetic": request.requested_tx_count}, category_counts={"synthetic": request.requested_tx_count},
            natural_skew={}, derived_skew={}, expected_cross_shard={"count": None, "ratio": None, "source": "synthetic_config"},
            shard_distribution={f"s{i}": 0 for i in range(max(1, shards))}, materialization_cache_status={"required": False, "cache_hit": None}, blockers=[], warnings=[],
        )
    blockers: list[str] = []
    warnings: list[str] = []
    manifest: dict[str, Any] = {}
    detail: DatasetDetailDTO | None = None
    selected_window_preview: dict[str, Any] = {}
    operation_counts: dict[str, int] = {}
    derived_skew: dict[str, Any] = {}
    tx_count = request.requested_tx_count
    try:
        manifest = load_manifest(request.dataset_id or "")
        detail = dataset_detail(manifest["dataset_id"])
        if request.plugin_id != "canonical_trace_replay":
            raise WorkloadDataError("dataset workload requires canonical_trace_replay")
        if request.source_sha256 and request.source_sha256.lower() != str(manifest.get("source_sha256") or "").lower():
            raise WorkloadDataError("workload_source source_sha256 does not match manifest")
        if not detail.selectable:
            raise WorkloadDataError("; ".join(detail.blockers or ["dataset is not selectable"]))
        definition = _variant_definition(manifest, request.variant_mode or "original_window")
        if request.use_full_dataset:
            if not manifest.get("allow_full_dataset", True):
                raise WorkloadDataError("full dataset mode is not supported")
            tx_count = int(manifest.get("row_count") or 0)
        supported_counts = {int(item) for item in manifest.get("supported_tx_counts") or supported_workload_counts()}
        if not request.use_full_dataset and tx_count not in supported_counts:
            raise WorkloadDataError("requested tx count is not supported by this dataset")
        path, selected_manifest, definition, resolved = resolve_dataset_source(manifest, request.model_copy(update={"requested_tx_count": tx_count}))
        parameters = dict(resolved.get("variant_parameters") or {})
        selected_window_preview = _selection_preview_from_source(
            path, selected_manifest, requested_tx_count=tx_count, seed=request.seed,
            variant_mode=str(definition["variant_mode"]), target_alpha=float(parameters["target_alpha"]) if "target_alpha" in parameters else request.target_alpha,
            skew_axis=str(parameters["skew_axis"]) if "skew_axis" in parameters else request.skew_axis,
            shards=shards, selection_mode=str(definition.get("selection_mode") or request.selection_mode),
            supported_counts=supported_counts, variant_parameters=parameters,
        )
        audit = resolved.get("prefix_audit")
        if audit:
            selected_window_preview["validated_prefix_audit"] = audit
            selected_window_preview["measured_access_theta"] = audit.get("measured_access_theta")
            selected_window_preview["measured_read_ratio"] = audit.get("measured_read_ratio")
            selected_window_preview["read_modify_write_topology_preserved"] = audit.get("read_modify_write_topology_preserved")
        operation_counts = dict(selected_window_preview.get("operation_counts") or manifest.get("operation_counts") or {})
        derived_skew = {"variant_mode": definition["variant_mode"], "variant_parameters": parameters}
    except (WorkloadDataError, ValueError) as exc:
        blockers.append(str(exc))
    shard_distribution = dict(selected_window_preview.get("shard_distribution") or {f"s{i}": 0 for i in range(max(1, shards))})
    return WorkloadPreviewDTO(
        source_type="dataset", plugin_id=request.plugin_id, dataset_id=request.dataset_id, tx_count=tx_count,
        dataset_summary=detail.model_dump() if detail else None, selected_window_preview=selected_window_preview,
        selected_time_range=selected_window_preview.get("selected_time_range") or {"start_ms": manifest.get("time_start_ms"), "end_ms": manifest.get("time_end_ms")},
        operation_counts=operation_counts, category_counts=operation_counts, natural_skew=dict(manifest.get("natural_skew_metrics") or {}),
        derived_skew=derived_skew, expected_cross_shard={"count": selected_window_preview.get("cross_shard_count"), "ratio": selected_window_preview.get("cross_shard_ratio"), "source": "compiled_from_routing_keys"},
        shard_distribution=shard_distribution, materialization_cache_status={"required": True, "cache_hit": None, "cache_root": ".cache/workloads"},
        blockers=blockers, warnings=warnings + (detail.warnings if detail else []),
    )

def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_csv(path: Path, *, expected_sha256: str | None = None) -> CsvValidationSummary:
    """Validate a source through its adapter in one streaming pass."""
    manifest = {"dataset_id": "legacy_adapter_validation", "adapter_id": "decentraland_sales_v1"}
    try:
        summary = adapter_for_manifest(manifest).validate_source(path, manifest, expected_sha256=expected_sha256)
    except ValueError as exc:
        raise WorkloadDataError(str(exc)) from exc
    return _csv_summary(summary)


def _csv_summary(summary: SourceValidationSummary) -> CsvValidationSummary:
    return CsvValidationSummary(
        source_sha256=summary.source_sha256,
        row_count=summary.row_count,
        unique_source_tx_hash_count=summary.unique_source_tx_hash_count,
        time_start_ms=summary.time_start_ms,
        time_end_ms=summary.time_end_ms,
        operation_counts=summary.operation_counts,
    )


def _validate_canonical_record(record: dict[str, Any], *, dataset_id: str, row_number: int) -> dict[str, Any]:
    schema_version = record.get("schema_version")
    if schema_version not in {"mbe_workload_record_v1", "mbe_workload_record_v2", "mbe_workload_record_v3"}:
        raise WorkloadDataError(f"canonical row {row_number}: unexpected schema_version")
    if record.get("dataset_id") != dataset_id:
        raise WorkloadDataError(f"canonical row {row_number}: dataset_id mismatch")
    for key in ("source_row_index", "source_event_id", "timestamp_ms", "sender_id", "operation_type", "runtime_value", "state_keys", "routing_source_key"):
        if record.get(key) in (None, "", []):
            raise WorkloadDataError(f"canonical row {row_number}: missing {key}")
    if not isinstance(record.get("state_keys"), list) or not record["state_keys"]:
        raise WorkloadDataError(f"canonical row {row_number}: state_keys must be a non-empty list")
    if len(record["state_keys"]) != len(set(record["state_keys"])):
        raise WorkloadDataError(f"canonical row {row_number}: duplicate state_keys")
    if not isinstance(record.get("skew_keys", {}), dict):
        raise WorkloadDataError(f"canonical row {row_number}: skew_keys must be an object")
    if schema_version == "mbe_workload_record_v2":
        _validate_access_template_record(record, row_number=row_number)
    elif schema_version == "mbe_workload_record_v3":
        _validate_direct_access_record(record, row_number=row_number)
    record.setdefault("source_tx_hash", None)
    record.setdefault("receiver_id", None)
    record.setdefault("routing_target_key", None)
    record.setdefault("skew_keys", {})
    record.setdefault("provenance", {})
    record.setdefault("metadata", {})
    return record

def _validate_access_template_record(record: dict[str, Any], *, row_number: int) -> None:
    if record.get("access_list_schema") != "dcl_sale_access_template_v1":
        raise WorkloadDataError(f"canonical row {row_number}: missing access_list_schema")
    if record.get("access_list_source") != "semantics_derived":
        raise WorkloadDataError(f"canonical row {row_number}: missing access_list_source")
    if not record.get("contract") or not record.get("category"):
        raise WorkloadDataError(f"canonical row {row_number}: missing contract/category for access template")
    template = record.get("access_template")
    if not isinstance(template, list) or not template:
        raise WorkloadDataError(f"canonical row {row_number}: missing access_template")
    required_roles = {
        "sender_balance",
        "sender_nonce",
        "receiver_balance",
        "receiver_nonce",
        "market_contract",
        "category_metadata",
    }
    roles = [str(item.get("role") or "") for item in template if isinstance(item, dict)]
    if set(roles) != required_roles or len(roles) != len(set(roles)):
        raise WorkloadDataError(f"canonical row {row_number}: invalid access_template roles")
    allowed_modes = {"read", "write", "read_write", "commutative_delta"}
    for item in template:
        if not isinstance(item, dict) or item.get("mode") not in allowed_modes or not item.get("semantics"):
            raise WorkloadDataError(f"canonical row {row_number}: invalid access_template item")
    by_role = {item["role"]: item for item in template}
    if by_role["market_contract"].get("mode") != "commutative_delta" or int(by_role["market_contract"].get("delta") or 0) != 1:
        raise WorkloadDataError(f"canonical row {row_number}: invalid market_contract template")
    if by_role["category_metadata"].get("mode") != "read":
        raise WorkloadDataError(f"canonical row {row_number}: invalid category_metadata template")



def _validate_direct_access_record(record: dict[str, Any], *, row_number: int) -> None:
    if not record.get("access_list_schema") or not record.get("access_list_source"):
        raise WorkloadDataError(f"canonical row {row_number}: missing direct access-list provenance")
    access_list = record.get("access_list")
    if not isinstance(access_list, list) or not access_list:
        raise WorkloadDataError(f"canonical row {row_number}: missing direct access_list")
    allowed_modes = {"read", "write", "read_write", "commutative_delta"}
    normalized: list[dict[str, Any]] = []
    keys: set[str] = set()
    for index, item in enumerate(access_list):
        if not isinstance(item, dict):
            raise WorkloadDataError(f"canonical row {row_number}: access_list[{index}] is not an object")
        key = str(item.get("key") or "").strip()
        mode = str(item.get("mode") or "")
        semantics = str(item.get("update_semantics") or "").strip()
        if not key or key in keys or mode not in allowed_modes or not semantics:
            raise WorkloadDataError(f"canonical row {row_number}: invalid direct access_list item")
        keys.add(key)
        entry: dict[str, Any] = {"key": key, "mode": mode, "update_semantics": semantics}
        if int(item.get("delta") or 0):
            entry["delta"] = int(item["delta"])
        normalized.append(entry)
    normalized.sort(key=lambda item: (item["key"], item["mode"], item["update_semantics"], int(item.get("delta") or 0)))
    if sorted(record["state_keys"]) != [item["key"] for item in normalized]:
        raise WorkloadDataError(f"canonical row {row_number}: state_keys/direct access_list mismatch")
    digest = hashlib.sha256(json.dumps(normalized, ensure_ascii=False, separators=(",", ":")).encode("utf-8")).hexdigest()
    if digest != str(record.get("access_list_digest") or "").lower():
        raise WorkloadDataError(f"canonical row {row_number}: direct access_list_digest mismatch")
    record["access_list"] = normalized


def _canonical_bytes(record: dict[str, Any]) -> bytes:
    return (json.dumps(record, ensure_ascii=False, separators=(",", ":"), sort_keys=False) + "\n").encode("utf-8")


def build_canonical(csv_path: Path, cache_root: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    """Build a deterministic canonical JSONL.GZ file and atomically publish it."""
    try:
        adapter = adapter_for_manifest(manifest)
        expected_file_hash = manifest.get("source_sha256") or None
        summary = _csv_summary(adapter.validate_source(csv_path, manifest, expected_sha256=expected_file_hash))
    except ValueError as exc:
        raise WorkloadDataError(str(exc)) from exc
    identity_source_sha256 = str(manifest.get("dataset_source_sha256") or manifest.get("source_sha256") or summary.source_sha256)
    content_id = hashlib.sha256(json.dumps({"dataset_id": manifest["dataset_id"], "adapter_id": manifest.get("adapter_id") or "decentraland_sales_v1", "source_file_sha256": summary.source_sha256, "generator_version": GENERATOR_VERSION}, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    target = cache_root / "canonical" / content_id
    output = target / "workload.jsonl.gz"
    if output.is_file() and (target / "canonical_summary.json").is_file():
        existing = json.loads((target / "canonical_summary.json").read_text(encoding="utf-8"))
        if existing.get("canonical_sha256") == sha256_file(output):
            existing = dict(existing); existing["cache_hit"] = True; return existing
        raise WorkloadDataError("canonical cache hash mismatch")
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{content_id}.", dir=target.parent))
    try:
        canonical_path = temporary / "workload.jsonl.gz"
        with canonical_path.open("wb") as raw:
            with gzip.GzipFile(filename="", mode="wb", fileobj=raw, compresslevel=9, mtime=0) as compressed:
                previous_key: tuple[int, int] | None = None
                for index, item in enumerate(adapter.iter_canonical_records(csv_path, manifest)):
                    record = _validate_canonical_record(item, dataset_id=manifest["dataset_id"], row_number=index)
                    key = (record["timestamp_ms"], int(record["source_row_index"]))
                    if previous_key is not None and key < previous_key:
                        raise WorkloadDataError("source order violates the canonical (timestamp_ms, source_row_index) contract")
                    previous_key = key
                    compressed.write(_canonical_bytes(record))
        result = {
            "dataset_id": manifest["dataset_id"], "source_sha256": identity_source_sha256, "source_file_sha256": summary.source_sha256,
            "canonical_sha256": sha256_file(canonical_path), "row_count": summary.row_count,
            "canonical_relative_path": f"canonical/{content_id}/workload.jsonl.gz", "generator_version": GENERATOR_VERSION,
            "operation_counts": summary.operation_counts, "category_counts": summary.operation_counts, "cache_hit": False,
        }
        (temporary / "canonical_summary.json").write_text(json.dumps(result, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        os.replace(temporary, target)
        return result
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise

def _iter_canonical(path: Path) -> Iterator[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8", newline="") as stream:
        for line_number, line in enumerate(stream, 1):
            if len(line.encode("utf-8")) > MAX_JSONL_RECORD_BYTES:
                raise WorkloadDataError(f"canonical line {line_number}: record exceeds maximum size")
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise WorkloadDataError(f"canonical line {line_number}: invalid JSON") from exc
            yield _validate_canonical_record(record, dataset_id=str(record.get("dataset_id") or ""), row_number=line_number)


def _canonical_count(path: Path) -> int:
    return sum(1 for _ in _iter_canonical(path))


def _window(path: Path, start: int, count: int) -> Iterator[dict[str, Any]]:
    end = start + count
    for index, record in enumerate(_iter_canonical(path)):
        if index >= end:
            break
        if index >= start:
            yield record


def _selection_start(spec: dict[str, Any], total: int, count: int) -> int:
    if spec.get("selection_mode") == "validated_prefix" or count == total:
        return 0
    span = total - count + 1
    normalized = {key: spec[key] for key in ("dataset_id", "source_sha256", "canonical_sha256", "requested_tx_count", "selection_mode", "selector_version", "generator_version", "variant_mode", "variant_parameters")}
    base = int.from_bytes(hashlib.sha256(json.dumps(normalized, sort_keys=True, separators=(",", ":")).encode()).digest()[:8], "big") % span
    return (base + int(spec.get("seed") or 0)) % span

def _logical_event_id(record: dict[str, Any], variant_id: str, index: int, occurrence: int) -> str:
    raw = f"{record['dataset_id']}|{variant_id}|{index}|{record['source_event_id']}|{occurrence}".encode()
    return hashlib.sha256(raw).hexdigest()


def _materialized_record(record: dict[str, Any], variant_id: str, index: int, occurrence: int) -> dict[str, Any]:
    materialized = dict(record)
    materialized.update({"materialized_index": index, "logical_event_id": _logical_event_id(record, variant_id, index, occurrence), "occurrence_index": occurrence})
    return materialized


def _sample_unit(domain: str, counter: int) -> float:
    value = int.from_bytes(hashlib.sha256(f"{domain}|{counter}".encode()).digest()[:8], "big")
    return value / 2**64


def _zipf_records(base: list[dict[str, Any]], alpha: float, skew_axis: str, domain: str) -> list[dict[str, Any]]:
    by_operation: dict[str, list[dict[str, Any]]] = {}
    for record in base:
        key = record.get("skew_keys", {}).get(skew_axis)
        if not key:
            raise WorkloadDataError(f"canonical record is missing skew key for axis {skew_axis}")
        by_operation.setdefault(record["operation_type"], []).append(record)
    sampled: dict[str, list[dict[str, Any]]] = {}
    for operation, records in by_operation.items():
        buckets: dict[str, list[dict[str, Any]]] = {}
        for record in records:
            buckets.setdefault(record["skew_keys"][skew_axis], []).append(record)
        ranked = sorted(buckets.items(), key=lambda item: (-len(item[1]), item[0]))
        weights = [(rank + 1) ** (-alpha) for rank in range(len(ranked))]
        total = sum(weights)
        cumulative: list[float] = []
        running = 0.0
        for weight in weights:
            running += weight / total
            cumulative.append(running)
        chosen: list[dict[str, Any]] = []
        for draw in range(len(records)):
            unit = _sample_unit(f"{domain}|{operation}|{skew_axis}", draw)
            bucket_index = next((i for i, value in enumerate(cumulative) if unit < value), len(cumulative) - 1)
            choices = ranked[bucket_index][1]
            chosen.append(choices[min(int(_sample_unit(f"{domain}|{operation}|row", draw) * len(choices)), len(choices) - 1)])
        sampled[operation] = chosen
    cursors = {operation: 0 for operation in sampled}
    interleaved: list[dict[str, Any]] = []
    for record in base:
        operation = record["operation_type"]
        interleaved.append(sampled[operation][cursors[operation]])
        cursors[operation] += 1
    return interleaved


def _skew_statistics(skew_keys: Counter[str], senders: set[str], receivers: set[str], count: int, occurrences: Counter[int], skew_axis: str | None) -> dict[str, Any]:
    values = sorted(skew_keys.values())
    if not values:
        return {"gini": 0.0, "hhi": 0.0, "top_1_ratio": 0.0, "top_10_ratio": 0.0, "top_100_ratio": 0.0, "maximum_reuse": 0}
    weighted = sum((index + 1) * value for index, value in enumerate(values))
    gini = (2 * weighted / (len(values) * sum(values))) - (len(values) + 1) / len(values)
    ordered = sorted(values, reverse=True)
    return {
        "gini": gini,
        "hhi": sum((value / count) ** 2 for value in values),
        "top_1_ratio": sum(ordered[:1]) / count,
        "top_10_ratio": sum(ordered[:10]) / count,
        "top_100_ratio": sum(ordered[:100]) / count,
        "maximum_reuse": max(occurrences.values(), default=0),
        "unique_sender_count": len(senders),
        "unique_receiver_count": len(receivers),
        "unique_skew_key_count": len(skew_keys),
        "unique_buyer_count": len(senders),
        "unique_seller_count": len(receivers),
        "unique_contract_count": len(skew_keys),
        "skew_axis": skew_axis,
        "duplicate_source_row_count": sum(value - 1 for value in occurrences.values() if value > 1),
        "duplicate_source_row_ratio": sum(value - 1 for value in occurrences.values() if value > 1) / count,
    }


def supported_workload_counts() -> tuple[int, ...]:
    return tuple(sorted(SUPPORTED_COUNTS))


def _supported_counts() -> frozenset[int]:
    return SUPPORTED_COUNTS


def _is_derived_variant(variant_mode: str) -> bool:
    return variant_mode in {"contract_zipf", "key_zipf"}


def _selection_spec(*, dataset_id: str, source_sha256: str, canonical_sha256: str, requested_tx_count: int, seed: int, total: int, variant_mode: str = "original_window", target_alpha: float | None = None, skew_axis: str | None = None, selection_mode: str = "contiguous_window", supported_counts: set[int] | frozenset[int] | None = None, variant_parameters: dict[str, Any] | None = None) -> tuple[dict[str, Any], int, str, str | None]:
    count = total if requested_tx_count == total else requested_tx_count
    allowed_counts = set(supported_counts or _supported_counts())
    if count <= 0 or count > total or (count != total and count not in allowed_counts):
        raise WorkloadDataError("requested tx count is not supported by this dataset")
    if selection_mode not in {"contiguous_window", "validated_prefix"}:
        raise WorkloadDataError("unsupported selection_mode")
    if variant_mode == "contract_zipf" and not skew_axis:
        skew_axis = "contract"
    if _is_derived_variant(variant_mode):
        if target_alpha not in SUPPORTED_ALPHAS:
            raise WorkloadDataError("unsupported key Zipf alpha")
        if not skew_axis:
            raise WorkloadDataError("derived workload requires skew_axis")
    elif variant_mode == "original_window" and target_alpha is not None:
        raise WorkloadDataError("original_window does not allow target_alpha")
    effective_parameters = dict(variant_parameters or {})
    if target_alpha is not None:
        effective_parameters.setdefault("target_alpha", target_alpha)
    if skew_axis is not None:
        effective_parameters.setdefault("skew_axis", skew_axis)
    spec = {
        "dataset_id": dataset_id, "source_sha256": source_sha256, "canonical_sha256": canonical_sha256,
        "requested_tx_count": count, "seed": seed, "selection_mode": selection_mode,
        "selector_version": SELECTOR_VERSION, "generator_version": GENERATOR_VERSION,
        "variant_mode": variant_mode, "target_alpha": target_alpha, "skew_axis": skew_axis,
        "variant_parameters": dict(sorted(effective_parameters.items())),
    }
    return spec, count, variant_mode, skew_axis

def _selection_digest(spec: dict[str, Any], *, start: int, count: int, base_window_sha256: str) -> str:
    payload = {**spec, "start_offset": start, "end_offset": start + count - 1, "base_window_sha256": base_window_sha256}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _operation_percentages(operation_counts: dict[str, int], count: int) -> dict[str, float]:
    if count <= 0:
        return {key: 0.0 for key in sorted(operation_counts)}
    return {key: value / count for key, value in sorted(operation_counts.items())}


def _stable_shard(value: object, shards: int) -> int:
    shard_count = max(1, shards)
    digest = hashlib.sha256(str(value or "").encode()).digest()
    return int.from_bytes(digest[:8], "big") % shard_count


def _canonical_sha256_from_source(csv_path: Path, manifest: dict[str, Any]) -> str:
    adapter = adapter_for_manifest(manifest)
    sink = _HashingSink()
    with gzip.GzipFile(filename="", mode="wb", fileobj=sink, compresslevel=9, mtime=0) as compressed:
        previous_key: tuple[int, int] | None = None
        for index, item in enumerate(adapter.iter_canonical_records(csv_path, manifest)):
            record = _validate_canonical_record(item, dataset_id=manifest["dataset_id"], row_number=index)
            key = (record["timestamp_ms"], int(record["source_row_index"]))
            if previous_key is not None and key < previous_key:
                raise WorkloadDataError("source order violates the canonical (timestamp_ms, source_row_index) contract")
            previous_key = key
            compressed.write(_canonical_bytes(record))
    return sink.digest.hexdigest()


def _selection_preview(canonical_path: Path, *, dataset_id: str, source_sha256: str, requested_tx_count: int, seed: int, variant_mode: str = "original_window", target_alpha: float | None = None, skew_axis: str | None = None, shards: int = 4, selection_mode: str = "contiguous_window", supported_counts: set[int] | frozenset[int] | None = None, variant_parameters: dict[str, Any] | None = None) -> dict[str, Any]:
    total = _canonical_count(canonical_path)
    spec, count, variant_mode, skew_axis = _selection_spec(dataset_id=dataset_id, source_sha256=source_sha256, canonical_sha256=sha256_file(canonical_path), requested_tx_count=requested_tx_count, seed=seed, total=total, variant_mode=variant_mode, target_alpha=target_alpha, skew_axis=skew_axis, selection_mode=selection_mode, supported_counts=supported_counts, variant_parameters=variant_parameters)
    start = _selection_start(spec, total, count)
    base_hash_builder = hashlib.sha256(); base_records: list[dict[str, Any]] = []
    selected_start_ms: int | None = None; selected_end_ms: int | None = None
    for record in _window(canonical_path, start, count):
        base_hash_builder.update(_canonical_bytes(record)); selected_start_ms = record["timestamp_ms"] if selected_start_ms is None else selected_start_ms; selected_end_ms = record["timestamp_ms"]; base_records.append(record)
    base_hash = base_hash_builder.hexdigest()
    selected = _zipf_records(base_records, float(target_alpha), str(skew_axis), f"{dataset_id}|{source_sha256}|{base_hash}|{skew_axis}|{target_alpha}|{seed}|{GENERATOR_VERSION}") if _is_derived_variant(variant_mode) else base_records
    return _selected_window_preview(spec, selected, start=start, count=count, selected_start_ms=selected_start_ms, selected_end_ms=selected_end_ms, base_window_sha256=base_hash, shards=shards)

def _selection_preview_from_source(csv_path: Path, manifest: dict[str, Any], *, requested_tx_count: int, seed: int, variant_mode: str = "original_window", target_alpha: float | None = None, skew_axis: str | None = None, shards: int = 4, selection_mode: str = "contiguous_window", supported_counts: set[int] | frozenset[int] | None = None, variant_parameters: dict[str, Any] | None = None) -> dict[str, Any]:
    adapter = adapter_for_manifest(manifest)
    summary = _csv_summary(adapter.validate_source(csv_path, manifest, expected_sha256=manifest.get("source_sha256") or None))
    canonical_sha256 = _canonical_sha256_from_source(csv_path, manifest)
    identity_hash = str(manifest.get("dataset_source_sha256") or summary.source_sha256)
    spec, count, variant_mode, skew_axis = _selection_spec(dataset_id=manifest["dataset_id"], source_sha256=identity_hash, canonical_sha256=canonical_sha256, requested_tx_count=requested_tx_count, seed=seed, total=summary.row_count, variant_mode=variant_mode, target_alpha=target_alpha, skew_axis=skew_axis, selection_mode=selection_mode, supported_counts=supported_counts, variant_parameters=variant_parameters)
    start = _selection_start(spec, summary.row_count, count)
    base_hash_builder = hashlib.sha256(); base_records: list[dict[str, Any]] = []
    selected_start_ms: int | None = None; selected_end_ms: int | None = None; previous_key: tuple[int, int] | None = None
    end = start + count
    for index, item in enumerate(adapter.iter_canonical_records(csv_path, manifest)):
        record = _validate_canonical_record(item, dataset_id=manifest["dataset_id"], row_number=index)
        key = (record["timestamp_ms"], int(record["source_row_index"]))
        if previous_key is not None and key < previous_key: raise WorkloadDataError("source order violates the canonical (timestamp_ms, source_row_index) contract")
        previous_key = key
        if index >= end: break
        if index < start: continue
        base_hash_builder.update(_canonical_bytes(record)); selected_start_ms = record["timestamp_ms"] if selected_start_ms is None else selected_start_ms; selected_end_ms = record["timestamp_ms"]; base_records.append(record)
    base_hash = base_hash_builder.hexdigest()
    selected = _zipf_records(base_records, float(target_alpha), str(skew_axis), f"{manifest['dataset_id']}|{identity_hash}|{base_hash}|{skew_axis}|{target_alpha}|{seed}|{GENERATOR_VERSION}") if _is_derived_variant(variant_mode) else base_records
    return _selected_window_preview(spec, selected, start=start, count=count, selected_start_ms=selected_start_ms, selected_end_ms=selected_end_ms, base_window_sha256=base_hash, shards=shards)

def _selected_window_preview(spec: dict[str, Any], selected: list[dict[str, Any]], *, start: int, count: int, selected_start_ms: int | None, selected_end_ms: int | None, base_window_sha256: str, shards: int) -> dict[str, Any]:
    operation_counts: Counter[str] = Counter()
    shard_distribution: Counter[str] = Counter({f"s{i}": 0 for i in range(max(1, shards))})
    cross_shard_count = 0
    skew_keys: Counter[str] = Counter()
    senders: set[str] = set()
    receivers: set[str] = set()
    occurrences: Counter[int] = Counter()
    direct_access_count = 0
    skew_axis = spec.get("skew_axis")
    for index, record in enumerate(selected):
        if record.get("schema_version") == "mbe_workload_record_v3":
            direct_access_count += 1
        operation_counts[str(record.get("operation_type") or "unknown")] += 1
        source_shard = _stable_shard(record.get("routing_source_key") or record.get("sender_id"), shards)
        target_shard = _stable_shard(record.get("routing_target_key") or record.get("receiver_id") or record.get("routing_source_key"), shards)
        shard_distribution[f"s{source_shard}"] += 1
        if source_shard != target_shard:
            cross_shard_count += 1
        if skew_axis and record.get("skew_keys", {}).get(str(skew_axis)):
            skew_keys[record["skew_keys"][str(skew_axis)]] += 1
        elif record.get("routing_target_key"):
            skew_keys[str(record["routing_target_key"])] += 1
        senders.add(str(record.get("sender_id") or ""))
        if record.get("receiver_id"):
            receivers.add(str(record["receiver_id"]))
        occurrences[int(record.get("source_row_index", index))] += 1
    operation_data = dict(operation_counts)
    selection_digest = _selection_digest(spec, start=start, count=count, base_window_sha256=base_window_sha256)
    routing_source_basis = "logical_routing_key" if selected and direct_access_count == len(selected) else ("runtime_identity" if direct_access_count == 0 else "mixed")
    return {
        "requested_tx_count": spec["requested_tx_count"],
        "actual_selected_count": len(selected),
        "selected_time_range": {"start_ms": selected_start_ms, "end_ms": selected_end_ms},
        "category_counts": operation_data,
        "operation_counts": operation_data,
        "category_percentages": _operation_percentages(operation_data, len(selected)),
        "operation_percentages": _operation_percentages(operation_data, len(selected)),
        "realized_skew": _skew_statistics(skew_keys, senders, receivers, max(1, len(selected)), occurrences, str(skew_axis) if skew_axis else None),
        "cross_shard_count": cross_shard_count,
        "cross_shard_ratio": cross_shard_count / len(selected) if selected else 0,
        "routing_source_basis": routing_source_basis,
        "shard_distribution": dict(sorted(shard_distribution.items())),
        "selection_digest": selection_digest,
        "selection_mode": spec["selection_mode"],
        "selector_version": spec["selector_version"],
        "start_offset": start,
        "end_offset": start + count - 1,
        "base_window_sha256": base_window_sha256,
    }


def materialize(canonical_path: Path, cache_root: Path, *, dataset_id: str, source_sha256: str, requested_tx_count: int, seed: int, variant_mode: str = "original_window", target_alpha: float | None = None, skew_axis: str | None = None, selection_mode: str = "contiguous_window", supported_counts: set[int] | frozenset[int] | None = None, variant_parameters: dict[str, Any] | None = None, truth_label: str = "real_observed", source_file_sha256: str | None = None, audit_metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    total = _canonical_count(canonical_path)
    canonical_hash = sha256_file(canonical_path)
    spec, count, variant_mode, skew_axis = _selection_spec(
        dataset_id=dataset_id,
        source_sha256=source_sha256,
        canonical_sha256=canonical_hash,
        requested_tx_count=requested_tx_count,
        seed=seed,
        total=total,
        variant_mode=variant_mode,
        target_alpha=target_alpha,
        skew_axis=skew_axis,
        selection_mode=selection_mode,
        supported_counts=supported_counts,
        variant_parameters=variant_parameters,
    )
    materialized_id = hashlib.sha256(json.dumps(spec, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    target = cache_root / "materialized" / materialized_id
    output = target / "workload.jsonl.gz"
    if output.is_file() and (target / "materialization_summary.json").is_file():
        summary = json.loads((target / "materialization_summary.json").read_text(encoding="utf-8"))
        if summary.get("materialized_sha256") == sha256_file(output):
            summary = dict(summary)
            summary["cache_hit"] = True
            if not summary.get("selection_digest"):
                selected_preview = _selection_preview(
                    canonical_path,
                    dataset_id=dataset_id,
                    source_sha256=source_sha256,
                    requested_tx_count=requested_tx_count,
                    seed=seed,
                    variant_mode=variant_mode,
                    target_alpha=target_alpha,
                    skew_axis=skew_axis,
                    selection_mode=selection_mode,
                    supported_counts=supported_counts,
                    variant_parameters=variant_parameters,
                )
                summary["selection_digest"] = selected_preview["selection_digest"]
                summary["selected_window_preview"] = selected_preview
            return summary
        raise WorkloadDataError("materialization cache hash mismatch")
    start = _selection_start(spec, total, count)
    base_hash_builder = hashlib.sha256()
    base_records: list[dict[str, Any]] | None = [] if _is_derived_variant(variant_mode) else None
    selected_start_ms: int | None = None
    selected_end_ms: int | None = None
    for record in _window(canonical_path, start, count):
        base_hash_builder.update(_canonical_bytes(record))
        selected_start_ms = record["timestamp_ms"] if selected_start_ms is None else selected_start_ms
        selected_end_ms = record["timestamp_ms"]
        if base_records is not None:
            base_records.append(record)
    base_hash = base_hash_builder.hexdigest()
    selected: Iterator[dict[str, Any]] | list[dict[str, Any]]
    if base_records is None:
        selected = _window(canonical_path, start, count)
    else:
        selected = _zipf_records(base_records, float(target_alpha), str(skew_axis), f"{dataset_id}|{source_sha256}|{base_hash}|{skew_axis}|{target_alpha}|{seed}|{GENERATOR_VERSION}")
    selected_preview = _selection_preview(
        canonical_path,
        dataset_id=dataset_id,
        source_sha256=source_sha256,
        requested_tx_count=requested_tx_count,
        seed=seed,
        variant_mode=variant_mode,
        target_alpha=target_alpha,
        skew_axis=skew_axis,
        selection_mode=selection_mode,
        supported_counts=supported_counts,
        variant_parameters=variant_parameters,
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{materialized_id}.", dir=target.parent))
    try:
        occurrences: Counter[int] = Counter()
        skew_keys: Counter[str] = Counter()
        senders: set[str] = set()
        receivers: set[str] = set()
        operation_counts: Counter[str] = Counter()
        total_count = 0
        with (temporary / "workload.jsonl.gz").open("wb") as raw:
            with gzip.GzipFile(filename="", mode="wb", fileobj=raw, compresslevel=9, mtime=0) as compressed:
                for index, record in enumerate(selected):
                    occurrence = occurrences[record["source_row_index"]]
                    occurrences[record["source_row_index"]] += 1
                    if skew_axis and record.get("skew_keys", {}).get(skew_axis):
                        skew_keys[record["skew_keys"][skew_axis]] += 1
                    elif record.get("routing_target_key"):
                        skew_keys[str(record["routing_target_key"])] += 1
                    senders.add(str(record["sender_id"]))
                    if record.get("receiver_id"):
                        receivers.add(str(record["receiver_id"]))
                    operation_counts[str(record["operation_type"])] += 1
                    total_count += 1
                    compressed.write(_canonical_bytes(_materialized_record(record, materialized_id, index, occurrence)))
        skew = _skew_statistics(skew_keys, senders, receivers, total_count, occurrences, skew_axis)
        summary = dict(spec)
        routing_source_basis = str(selected_preview.get("routing_source_basis") or "runtime_identity")
        expected_cross_shard_count = int(selected_preview.get("cross_shard_count") or 0) if routing_source_basis == "logical_routing_key" else 0
        expected_cross_shard_ratio = float(selected_preview.get("cross_shard_ratio") or 0.0) if routing_source_basis == "logical_routing_key" else 0.0
        summary.update({"materialized_id": materialized_id, "actual_tx_count": total_count, "truth_label": truth_label, "source_file_sha256": source_file_sha256, "audit_metadata": audit_metadata or {}, "start_offset": start, "end_offset": start + total_count - 1, "selected_time_start_ms": selected_start_ms, "selected_time_end_ms": selected_end_ms, "base_window_sha256": base_hash, "selection_digest": selected_preview["selection_digest"], "selected_window_preview": selected_preview, "routing_source_basis": routing_source_basis, "expected_cross_shard_count": expected_cross_shard_count, "expected_cross_shard_ratio": expected_cross_shard_ratio, "materialized_sha256": sha256_file(temporary / "workload.jsonl.gz"), "materialized_relative_path": f"materialized/{materialized_id}/workload.jsonl.gz", "operation_counts": dict(operation_counts), "category_counts": dict(operation_counts), "cache_hit": False, **skew})
        (temporary / "materialization_summary.json").write_text(json.dumps(summary, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        (temporary / ".ready").write_text("ready\n", encoding="utf-8")
        os.replace(temporary, target)
        return summary
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise


def materialize_request(request: WorkloadPreviewRequest) -> WorkloadMaterializeDTO:
    if request.source_type != "dataset" or request.plugin_id != "canonical_trace_replay":
        raise WorkloadDataError("dataset materialization requires canonical_trace_replay")
    manifest = load_manifest(request.dataset_id or "")
    available, _, blockers, _ = dataset_status(manifest)
    if not available:
        raise WorkloadDataError("; ".join(blockers or ["dataset is not selectable"]))
    if request.source_sha256 and request.source_sha256.lower() != str(manifest.get("source_sha256", "")).lower():
        raise WorkloadDataError("workload_source source_sha256 does not match manifest")
    definition = _variant_definition(manifest, request.variant_mode or "original_window")
    requested = int(manifest["row_count"]) if request.use_full_dataset else request.requested_tx_count
    if request.use_full_dataset and not manifest.get("allow_full_dataset", True):
        raise WorkloadDataError("full dataset mode is not supported")
    effective_request = request.model_copy(update={"requested_tx_count": requested})
    source_path, selected_manifest, definition, resolved = resolve_dataset_source(manifest, effective_request)
    selected_manifest["dataset_source_sha256"] = str(manifest.get("source_sha256") or "")
    parameters = dict(resolved.get("variant_parameters") or {})
    target_alpha = float(parameters["target_alpha"]) if "target_alpha" in parameters else request.target_alpha
    skew_axis = str(parameters["skew_axis"]) if "skew_axis" in parameters else request.skew_axis
    selection_mode = str(definition.get("selection_mode") or request.selection_mode)
    supported_counts = {int(item) for item in manifest.get("supported_tx_counts") or supported_workload_counts()}
    canonical = build_canonical(source_path, WORKLOAD_CACHE_ROOT, selected_manifest)
    summary = materialize(
        WORKLOAD_CACHE_ROOT / canonical["canonical_relative_path"], WORKLOAD_CACHE_ROOT,
        dataset_id=manifest["dataset_id"], source_sha256=str(manifest.get("source_sha256") or canonical["source_sha256"]),
        requested_tx_count=requested, seed=request.seed, variant_mode=str(definition["variant_mode"]),
        target_alpha=target_alpha, skew_axis=skew_axis, selection_mode=selection_mode,
        supported_counts=supported_counts, variant_parameters=parameters, truth_label=str(manifest.get("truth_label") or "real_observed"),
        source_file_sha256=canonical.get("source_file_sha256"), audit_metadata=resolved.get("prefix_audit"),
    )
    parameter_text = ",".join(f"{key}={parameters[key]}" for key in sorted(parameters))
    variant_id = f"{definition['variant_mode']}:count={requested}:seed={request.seed}:{parameter_text}"
    summary.update({"variant_id": variant_id, "variant_parameters": parameters, "selection_mode": selection_mode})
    return WorkloadMaterializeDTO(
        dataset_id=manifest["dataset_id"], materialized_id=summary["materialized_id"], variant_id=variant_id,
        variant_mode=str(definition["variant_mode"]), selection_mode=selection_mode, variant_parameters=parameters,
        truth_label=str(manifest.get("truth_label") or "real_observed"), materialized_relative_path=summary["materialized_relative_path"],
        canonical_relative_path=canonical["canonical_relative_path"], source_sha256=str(manifest.get("source_sha256") or canonical["source_sha256"]),
        source_file_sha256=canonical.get("source_file_sha256"), canonical_sha256=canonical["canonical_sha256"], materialized_sha256=summary["materialized_sha256"],
        requested_tx_count=requested, actual_tx_count=summary["actual_tx_count"], seed=request.seed, target_alpha=target_alpha,
        cache_hit=bool(summary.get("cache_hit")), summary=summary,
    )

def materialized_absolute_path(relative_path: str) -> Path:
    candidate = (WORKLOAD_CACHE_ROOT / relative_path).resolve()
    root = WORKLOAD_CACHE_ROOT.resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise WorkloadDataError("materialized path escapes workload cache") from exc
    return candidate


def workload_artifact_snapshots(source: dict[str, Any], materialized: dict[str, Any], manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    redacted_manifest = {
        key: value for key, value in manifest.items()
        if key not in {"local_raw_path", "local_raw_relative_path"}
    }
    selection = {
        key: materialized.get(key)
        for key in ("materialized_id", "variant_id", "variant_mode", "selection_mode", "variant_parameters", "requested_tx_count", "actual_tx_count", "seed", "start_offset", "end_offset", "selected_time_start_ms", "selected_time_end_ms", "base_window_sha256", "source_file_sha256", "audit_metadata")
        if key in materialized
    }
    skew = {
        key: materialized.get(key)
        for key in ("target_alpha", "skew_axis", "gini", "hhi", "top_1_ratio", "top_10_ratio", "top_100_ratio", "duplicate_source_row_count", "duplicate_source_row_ratio", "unique_sender_count", "unique_receiver_count", "unique_skew_key_count", "unique_buyer_count", "unique_seller_count", "unique_contract_count")
        if key in materialized
    }
    return {
        "workload_manifest_snapshot.json": redacted_manifest,
        "workload_source_spec.json": source,
        "workload_selection.json": selection,
        "workload_skew_report.json": skew,
        "workload_materialization_summary.json": materialized,
    }


def write_validation_report(summary: CsvValidationSummary, reports_root: Path) -> Path:
    reports_root.mkdir(parents=True, exist_ok=True)
    target = reports_root / f"validation-{summary.source_sha256[:16]}.json"
    target.write_text(json.dumps(asdict(summary), sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return target
