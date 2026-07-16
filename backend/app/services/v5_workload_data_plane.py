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

from pydantic import BaseModel, Field

from backend.app.core.paths import ROOT
from backend.app.services.workload_adapters.base import SourceValidationSummary
from backend.app.services.workload_adapters.registry import get_adapter

SUPPORTED_COUNTS = frozenset({10_000, 50_000, 100_000, 250_000})
SUPPORTED_ALPHAS = frozenset({0.0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2, 1.4})
GENERATOR_VERSION = "v5_workload_data_plane_v2_generic_record"
SELECTOR_VERSION = "contiguous_window_v1"
MAX_JSONL_RECORD_BYTES = 1024 * 1024
WORKLOAD_CACHE_ROOT = ROOT / ".cache" / "workloads"
MANIFEST_ROOT = ROOT / "data" / "workloads" / "manifests"


class WorkloadDataError(ValueError):
    pass


class DatasetSummaryDTO(BaseModel):
    schema_version: Literal["mbe_dataset_registry_item_v1"] = "mbe_dataset_registry_item_v1"
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
    requested_tx_count: int = Field(ge=1)
    seed: int
    variant_mode: Literal["original_window", "contract_zipf", "key_zipf"] | None = None
    target_alpha: float | None = None
    skew_axis: str | None = None
    use_full_dataset: bool = False
    source_sha256: str | None = None


class WorkloadPreviewDTO(BaseModel):
    schema_version: Literal["mbe_workload_preview_v1"] = "mbe_workload_preview_v1"
    source_type: str
    plugin_id: str
    dataset_id: str | None = None
    tx_count: int
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
    schema_version: Literal["mbe_workload_materialization_v1"] = "mbe_workload_materialization_v1"
    dataset_id: str
    materialized_id: str
    variant_id: str
    variant_mode: str
    materialized_relative_path: str
    canonical_relative_path: str
    source_sha256: str
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


def raw_source_path(manifest: dict[str, Any]) -> Path:
    rel = str(manifest.get("local_raw_relative_path") or "")
    if not rel or Path(rel).is_absolute() or ".." in Path(rel).parts:
        raise WorkloadDataError("dataset manifest has unsafe raw source path")
    return ROOT / rel


def adapter_for_manifest(manifest: dict[str, Any]):
    adapter_id = str(manifest.get("adapter_id") or "decentraland_sales_v1")
    try:
        return get_adapter(adapter_id)
    except ValueError as exc:
        raise WorkloadDataError(str(exc)) from exc


def dataset_status(manifest: dict[str, Any]) -> tuple[bool, str, list[str], CsvValidationSummary | None]:
    path = raw_source_path(manifest)
    blockers: list[str] = []
    if not path.is_file():
        return False, "unavailable", ["full CSV source is not present in this checkout"], None
    try:
        summary = _csv_summary(adapter_for_manifest(manifest).validate_source(path, manifest, expected_sha256=manifest.get("source_sha256") or None))
    except (WorkloadDataError, ValueError) as exc:
        return False, "invalid", [str(exc)], None
    if manifest.get("row_count") and int(manifest["row_count"]) != summary.row_count:
        blockers.append("manifest row_count does not match source")
    if manifest.get("unique_source_tx_hash_count") and int(manifest["unique_source_tx_hash_count"]) != summary.unique_source_tx_hash_count:
        blockers.append("manifest unique_source_tx_hash_count does not match source")
    return not blockers, "valid" if not blockers else "invalid", blockers, summary


def dataset_summary(manifest: dict[str, Any]) -> DatasetSummaryDTO:
    available, status, blockers, _ = dataset_status(manifest)
    operations = dict(manifest.get("operation_counts") or manifest.get("category_counts") or {})
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
        supported_skew_axes=list(manifest.get("supported_skew_axes") or []),
        default_skew_axis=manifest.get("default_skew_axis"),
        available=available,
        selectable=available and status == "valid",
        validation_status=status,
        blockers=blockers,
        warnings=["representative receipt verification only; not Polygon EVM replay"],
    )


def dataset_detail(dataset_id: str) -> DatasetDetailDTO:
    manifest = load_manifest(dataset_id)
    summary = dataset_summary(manifest)
    variants = list(manifest.get("supported_variants") or ["original_window", "contract_zipf"])
    skew_axes = list(manifest.get("supported_skew_axes") or [])
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
        variants=[
            {"variant_mode": mode, "selection_mode": "contiguous_window", "target_alpha_values": sorted(SUPPORTED_ALPHAS) if mode in {"contract_zipf", "key_zipf"} else [], "skew_axes": skew_axes}
            for mode in variants
        ],
        adapter_id=str(manifest.get("adapter_id") or "decentraland_sales_v1"),
        supported_variants=variants,
    )


def preview_workload(request: WorkloadPreviewRequest, *, shards: int = 4) -> WorkloadPreviewDTO:
    blockers: list[str] = []
    warnings: list[str] = []
    if request.source_type == "synthetic":
        return WorkloadPreviewDTO(
            source_type="synthetic",
            plugin_id="deterministic_signed_synthetic",
            tx_count=request.requested_tx_count,
            selected_time_range={"start_ms": None, "end_ms": None},
            operation_counts={"synthetic": request.requested_tx_count},
            category_counts={"synthetic": request.requested_tx_count},
            natural_skew={},
            derived_skew={},
            expected_cross_shard={"count": None, "ratio": None, "source": "synthetic_config"},
            shard_distribution={f"s{i}": 0 for i in range(max(1, shards))},
            materialization_cache_status={"required": False, "cache_hit": None},
            blockers=[],
            warnings=[],
        )
    try:
        manifest = load_manifest(request.dataset_id or "")
        detail = dataset_detail(manifest["dataset_id"])
    except WorkloadDataError as exc:
        manifest = {}
        detail = None
        blockers.append(str(exc))
    if request.plugin_id != "canonical_trace_replay":
        blockers.append("dataset workload requires canonical_trace_replay")
    if request.variant_mode == "original_window" and request.target_alpha is not None:
        blockers.append("original_window does not allow target_alpha")
    derived = request.variant_mode in {"contract_zipf", "key_zipf"}
    if derived and request.target_alpha not in SUPPORTED_ALPHAS:
        blockers.append("derived workload requires a supported target_alpha")
    if derived:
        supported_axes = set(manifest.get("supported_skew_axes") or [])
        axis = request.skew_axis or manifest.get("default_skew_axis")
        if not axis:
            blockers.append("derived workload requires skew_axis")
        elif supported_axes and axis not in supported_axes:
            blockers.append("skew_axis is not supported by dataset")
    if detail and not detail.selectable:
        blockers.extend(detail.blockers)
    tx_count = int(manifest.get("row_count") or request.requested_tx_count) if request.use_full_dataset else request.requested_tx_count
    if manifest and tx_count > int(manifest.get("row_count") or 0):
        blockers.append("requested_tx_count exceeds dataset row_count")
    operation_counts = dict(manifest.get("operation_counts") or manifest.get("category_counts") or {})
    shard_distribution = {f"s{i}": 0 for i in range(max(1, shards))}
    for index in range(tx_count):
        shard_distribution[f"s{index % max(1, shards)}"] += 1
    return WorkloadPreviewDTO(
        source_type="dataset",
        plugin_id=request.plugin_id,
        dataset_id=request.dataset_id,
        tx_count=tx_count,
        selected_time_range={"start_ms": manifest.get("time_start_ms"), "end_ms": manifest.get("time_end_ms")},
        operation_counts=operation_counts,
        category_counts=operation_counts,
        natural_skew=dict(manifest.get("natural_skew_metrics") or {}),
        derived_skew={"target_alpha": request.target_alpha, "skew_axis": request.skew_axis or manifest.get("default_skew_axis")} if derived else {},
        expected_cross_shard={"count": None, "ratio": None, "source": "compiled_from_state_keys"},
        shard_distribution=shard_distribution,
        materialization_cache_status={"required": True, "cache_hit": None, "cache_root": ".cache/workloads"},
        blockers=blockers,
        warnings=warnings + (detail.warnings if detail else []),
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
    if record.get("schema_version") != "mbe_workload_record_v1":
        raise WorkloadDataError(f"canonical row {row_number}: unexpected schema_version")
    if record.get("dataset_id") != dataset_id:
        raise WorkloadDataError(f"canonical row {row_number}: dataset_id mismatch")
    for key in ("source_row_index", "source_event_id", "timestamp_ms", "sender_id", "operation_type", "runtime_value", "state_keys", "routing_source_key"):
        if record.get(key) in (None, "", []):
            raise WorkloadDataError(f"canonical row {row_number}: missing {key}")
    if not isinstance(record.get("state_keys"), list) or not record["state_keys"]:
        raise WorkloadDataError(f"canonical row {row_number}: state_keys must be a non-empty list")
    if not isinstance(record.get("skew_keys", {}), dict):
        raise WorkloadDataError(f"canonical row {row_number}: skew_keys must be an object")
    record.setdefault("source_tx_hash", None)
    record.setdefault("receiver_id", None)
    record.setdefault("routing_target_key", None)
    record.setdefault("skew_keys", {})
    record.setdefault("provenance", {})
    record.setdefault("metadata", {})
    return record


def _canonical_bytes(record: dict[str, Any]) -> bytes:
    return (json.dumps(record, ensure_ascii=False, separators=(",", ":"), sort_keys=False) + "\n").encode("utf-8")


def build_canonical(csv_path: Path, cache_root: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    """Build a deterministic canonical JSONL.GZ file and atomically publish it."""
    try:
        adapter = adapter_for_manifest(manifest)
        summary = _csv_summary(adapter.validate_source(csv_path, manifest, expected_sha256=manifest.get("source_sha256") or None))
    except ValueError as exc:
        raise WorkloadDataError(str(exc)) from exc
    content_id = hashlib.sha256(json.dumps({"dataset_id": manifest["dataset_id"], "adapter_id": manifest.get("adapter_id") or "decentraland_sales_v1", "source_sha256": summary.source_sha256, "generator_version": GENERATOR_VERSION}, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    target = cache_root / "canonical" / content_id
    output = target / "workload.jsonl.gz"
    if output.is_file() and (target / "canonical_summary.json").is_file():
        existing = json.loads((target / "canonical_summary.json").read_text(encoding="utf-8"))
        if existing.get("canonical_sha256") == sha256_file(output):
            existing = dict(existing)
            existing["cache_hit"] = True
            return existing
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
            "dataset_id": manifest["dataset_id"], "source_sha256": summary.source_sha256,
            "canonical_sha256": sha256_file(canonical_path), "row_count": summary.row_count,
            "canonical_relative_path": f"canonical/{content_id}/workload.jsonl.gz",
            "generator_version": GENERATOR_VERSION, "operation_counts": summary.operation_counts, "category_counts": summary.operation_counts, "cache_hit": False,
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
    if count == total:
        return 0
    normalized = {key: spec[key] for key in ("dataset_id", "source_sha256", "canonical_sha256", "requested_tx_count", "seed", "selection_mode", "selector_version", "generator_version")}
    return int.from_bytes(hashlib.sha256(json.dumps(normalized, sort_keys=True, separators=(",", ":")).encode()).digest()[:8], "big") % (total - count + 1)


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


def _supported_counts() -> frozenset[int]:
    counts = set(SUPPORTED_COUNTS)
    extra = os.environ.get("MBE_V5_LOCAL_SMOKE_COUNTS", "")
    for item in extra.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            value = int(item)
        except ValueError:
            continue
        if value > 0:
            counts.add(value)
    return frozenset(counts)


def _is_derived_variant(variant_mode: str) -> bool:
    return variant_mode in {"contract_zipf", "key_zipf"}


def materialize(canonical_path: Path, cache_root: Path, *, dataset_id: str, source_sha256: str, requested_tx_count: int, seed: int, variant_mode: str = "original_window", target_alpha: float | None = None, skew_axis: str | None = None) -> dict[str, Any]:
    total = _canonical_count(canonical_path)
    count = total if requested_tx_count == total else requested_tx_count
    if count <= 0 or count > total or (count != total and count not in _supported_counts()):
        raise WorkloadDataError("requested tx count is not supported by this dataset")
    if variant_mode not in {"original_window", "contract_zipf", "key_zipf"}:
        raise WorkloadDataError("unsupported materialization variant")
    if variant_mode == "contract_zipf" and not skew_axis:
        skew_axis = "contract"
    if _is_derived_variant(variant_mode):
        if target_alpha not in SUPPORTED_ALPHAS:
            raise WorkloadDataError("unsupported key Zipf alpha")
        if not skew_axis:
            raise WorkloadDataError("derived workload requires skew_axis")
    elif target_alpha is not None:
        raise WorkloadDataError("original_window does not allow target_alpha")
    canonical_hash = sha256_file(canonical_path)
    spec = {"dataset_id": dataset_id, "source_sha256": source_sha256, "canonical_sha256": canonical_hash, "requested_tx_count": count, "seed": seed, "selection_mode": "contiguous_window", "selector_version": SELECTOR_VERSION, "generator_version": GENERATOR_VERSION, "variant_mode": variant_mode, "target_alpha": target_alpha, "skew_axis": skew_axis}
    materialized_id = hashlib.sha256(json.dumps(spec, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    target = cache_root / "materialized" / materialized_id
    output = target / "workload.jsonl.gz"
    if output.is_file() and (target / "materialization_summary.json").is_file():
        summary = json.loads((target / "materialization_summary.json").read_text(encoding="utf-8"))
        if summary.get("materialized_sha256") == sha256_file(output):
            summary = dict(summary)
            summary["cache_hit"] = True
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
                    compressed.write(_canonical_bytes(_materialized_record(record, variant_mode, index, occurrence)))
        skew = _skew_statistics(skew_keys, senders, receivers, total_count, occurrences, skew_axis)
        summary = dict(spec)
        summary.update({"materialized_id": materialized_id, "actual_tx_count": total_count, "start_offset": start, "end_offset": start + total_count - 1, "selected_time_start_ms": selected_start_ms, "selected_time_end_ms": selected_end_ms, "base_window_sha256": base_hash, "materialized_sha256": sha256_file(temporary / "workload.jsonl.gz"), "materialized_relative_path": f"materialized/{materialized_id}/workload.jsonl.gz", "operation_counts": dict(operation_counts), "category_counts": dict(operation_counts), "cache_hit": False, **skew})
        (temporary / "materialization_summary.json").write_text(json.dumps(summary, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        (temporary / ".ready").write_text("ready\n", encoding="utf-8")
        os.replace(temporary, target)
        return summary
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise


def materialize_request(request: WorkloadPreviewRequest) -> WorkloadMaterializeDTO:
    if request.source_type != "dataset":
        raise WorkloadDataError("materialization is only required for dataset workloads")
    if request.plugin_id != "canonical_trace_replay":
        raise WorkloadDataError("dataset materialization requires canonical_trace_replay")
    if request.variant_mode == "original_window" and request.target_alpha is not None:
        raise WorkloadDataError("original_window does not allow target_alpha")
    manifest = load_manifest(request.dataset_id or "")
    available, status, blockers, _ = dataset_status(manifest)
    if not available or status != "valid":
        raise WorkloadDataError("; ".join(blockers or ["dataset is not selectable"]))
    if request.source_sha256 and request.source_sha256.lower() != str(manifest.get("source_sha256", "")).lower():
        raise WorkloadDataError("workload_source source_sha256 does not match manifest")
    csv_path = raw_source_path(manifest)
    canonical = build_canonical(csv_path, WORKLOAD_CACHE_ROOT, manifest)
    requested = int(manifest["row_count"]) if request.use_full_dataset else request.requested_tx_count
    variant_mode = request.variant_mode or "original_window"
    skew_axis = request.skew_axis or (manifest.get("default_skew_axis") if _is_derived_variant(variant_mode) else None)
    summary = materialize(
        WORKLOAD_CACHE_ROOT / canonical["canonical_relative_path"],
        WORKLOAD_CACHE_ROOT,
        dataset_id=manifest["dataset_id"],
        source_sha256=canonical["source_sha256"],
        requested_tx_count=requested,
        seed=request.seed,
        variant_mode=variant_mode,
        target_alpha=request.target_alpha,
        skew_axis=skew_axis,
    )
    variant_id = f"{variant_mode}:count={requested}:seed={request.seed}:axis={skew_axis}:alpha={request.target_alpha}"
    return WorkloadMaterializeDTO(
        dataset_id=manifest["dataset_id"],
        materialized_id=summary["materialized_id"],
        variant_id=variant_id,
        variant_mode=variant_mode,
        materialized_relative_path=summary["materialized_relative_path"],
        canonical_relative_path=canonical["canonical_relative_path"],
        source_sha256=canonical["source_sha256"],
        canonical_sha256=canonical["canonical_sha256"],
        materialized_sha256=summary["materialized_sha256"],
        requested_tx_count=requested,
        actual_tx_count=summary["actual_tx_count"],
        seed=request.seed,
        target_alpha=request.target_alpha,
        cache_hit=bool(summary.get("cache_hit")),
        summary=summary,
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
        for key in ("materialized_id", "variant_id", "variant_mode", "requested_tx_count", "actual_tx_count", "seed", "start_offset", "end_offset", "selected_time_start_ms", "selected_time_end_ms", "base_window_sha256")
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
