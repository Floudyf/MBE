"""Streaming, deterministic V5 workload data-plane primitives.

This module deliberately has no FastAPI or scheduler dependency.  It keeps the
raw dataset read-only and publishes only completed, content-addressed cache
directories.
"""
from __future__ import annotations

import csv
import gzip
import hashlib
import json
import os
import re
import shutil
import tempfile
from collections import Counter
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Literal, Iterator

from pydantic import BaseModel, Field

from backend.app.core.paths import ROOT

REQUIRED_COLUMNS = ("id", "tx_hash", "buyer", "seller", "price", "timestamp", "category", "raw_contract_candidates")
ADDRESS = re.compile(r"^0x[a-fA-F0-9]{40}$")
TX_HASH = re.compile(r"^0x[a-fA-F0-9]{64}$")
SUPPORTED_COUNTS = frozenset({10_000, 50_000, 100_000, 250_000})
SUPPORTED_ALPHAS = frozenset({0.0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2, 1.4})
GENERATOR_VERSION = "v5_workload_data_plane_v1"
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
    truth_label: str
    row_count: int
    category_counts: dict[str, int]
    source_sha256: str
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


class WorkloadPreviewRequest(BaseModel):
    schema_version: Literal["mbe_workload_source_v1"] = "mbe_workload_source_v1"
    source_type: Literal["synthetic", "dataset"]
    plugin_id: Literal["deterministic_signed_synthetic", "canonical_trace_replay"]
    dataset_id: str | None = None
    requested_tx_count: int = Field(ge=1)
    seed: int
    variant_mode: Literal["original_window", "contract_zipf"] | None = None
    target_alpha: float | None = None
    use_full_dataset: bool = False
    source_sha256: str | None = None


class WorkloadPreviewDTO(BaseModel):
    schema_version: Literal["mbe_workload_preview_v1"] = "mbe_workload_preview_v1"
    source_type: str
    plugin_id: str
    dataset_id: str | None = None
    tx_count: int
    selected_time_range: dict[str, int | None]
    category_counts: dict[str, int]
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
    category_counts: dict[str, int]


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


def dataset_status(manifest: dict[str, Any]) -> tuple[bool, str, list[str], CsvValidationSummary | None]:
    path = raw_source_path(manifest)
    blockers: list[str] = []
    if not path.is_file():
        return False, "unavailable", ["full CSV source is not present in this checkout"], None
    try:
        summary = validate_csv(path, expected_sha256=manifest.get("source_sha256") or None)
    except WorkloadDataError as exc:
        return False, "invalid", [str(exc)], None
    if manifest.get("row_count") and int(manifest["row_count"]) != summary.row_count:
        blockers.append("manifest row_count does not match source")
    if manifest.get("unique_source_tx_hash_count") and int(manifest["unique_source_tx_hash_count"]) != summary.unique_source_tx_hash_count:
        blockers.append("manifest unique_source_tx_hash_count does not match source")
    return not blockers, "valid" if not blockers else "invalid", blockers, summary


def dataset_summary(manifest: dict[str, Any]) -> DatasetSummaryDTO:
    available, status, blockers, _ = dataset_status(manifest)
    return DatasetSummaryDTO(
        dataset_id=manifest["dataset_id"],
        display_name=manifest.get("display_name", manifest["dataset_id"]),
        description=manifest.get("description", ""),
        truth_label=manifest.get("truth_label", "real_observed"),
        row_count=int(manifest.get("row_count") or 0),
        category_counts=dict(manifest.get("category_counts") or {}),
        source_sha256=str(manifest.get("source_sha256") or ""),
        available=available,
        selectable=available and status == "valid",
        validation_status=status,
        blockers=blockers,
        warnings=["representative receipt verification only; not Polygon EVM replay"],
    )


def dataset_detail(dataset_id: str) -> DatasetDetailDTO:
    manifest = load_manifest(dataset_id)
    summary = dataset_summary(manifest)
    return DatasetDetailDTO(
        **summary.model_dump(),
        source_platform=manifest.get("source_platform", ""),
        source_chain=manifest.get("source_chain", ""),
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
            {"variant_mode": "original_window", "selection_mode": "contiguous_window", "target_alpha_values": []},
            {"variant_mode": "contract_zipf", "selection_mode": "contiguous_window", "target_alpha_values": sorted(SUPPORTED_ALPHAS)},
        ],
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
    if request.variant_mode == "contract_zipf" and request.target_alpha not in SUPPORTED_ALPHAS:
        blockers.append("contract_zipf requires a supported target_alpha")
    if detail and not detail.selectable:
        blockers.extend(detail.blockers)
    tx_count = int(manifest.get("row_count") or request.requested_tx_count) if request.use_full_dataset else request.requested_tx_count
    if manifest and tx_count > int(manifest.get("row_count") or 0):
        blockers.append("requested_tx_count exceeds dataset row_count")
    category_counts = dict(manifest.get("category_counts") or {})
    shard_distribution = {f"s{i}": 0 for i in range(max(1, shards))}
    for index in range(tx_count):
        shard_distribution[f"s{index % max(1, shards)}"] += 1
    return WorkloadPreviewDTO(
        source_type="dataset",
        plugin_id=request.plugin_id,
        dataset_id=request.dataset_id,
        tx_count=tx_count,
        selected_time_range={"start_ms": manifest.get("time_start_ms"), "end_ms": manifest.get("time_end_ms")},
        category_counts=category_counts,
        natural_skew=dict(manifest.get("natural_skew_metrics") or {}),
        derived_skew={"target_alpha": request.target_alpha, "skew_axis": "contract"} if request.variant_mode == "contract_zipf" else {},
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


def _require_address(value: str, name: str, row_index: int) -> str:
    if not ADDRESS.fullmatch(value):
        raise WorkloadDataError(f"row {row_index}: invalid {name}")
    return value.lower()


def _single_contract(value: str, row_index: int) -> str:
    candidates = re.findall(r"0x[a-fA-F0-9]{40}", value)
    if len(candidates) != 1:
        raise WorkloadDataError(f"row {row_index}: raw_contract_candidates must contain exactly one address")
    return candidates[0].lower()


def validate_csv(path: Path, *, expected_sha256: str | None = None) -> CsvValidationSummary:
    """Validate the source in one streaming pass without modifying it."""
    source_hash = sha256_file(path)
    if expected_sha256 and source_hash != expected_sha256.lower():
        raise WorkloadDataError("source SHA-256 mismatch")
    ids: set[str] = set()
    hashes: set[str] = set()
    categories: Counter[str] = Counter()
    start: int | None = None
    end: int | None = None
    with path.open("r", encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        if tuple(reader.fieldnames or ()) != REQUIRED_COLUMNS:
            raise WorkloadDataError("CSV header does not match the required workload contract")
        for source_row_index, row in enumerate(reader):
            if any(not (row.get(column) or "").strip() for column in REQUIRED_COLUMNS):
                raise WorkloadDataError(f"row {source_row_index}: required source field is empty")
            event_id = row["id"].strip()
            if event_id in ids:
                raise WorkloadDataError(f"row {source_row_index}: duplicate sale id")
            ids.add(event_id)
            tx_hash = row["tx_hash"].strip()
            if not TX_HASH.fullmatch(tx_hash):
                raise WorkloadDataError(f"row {source_row_index}: invalid tx_hash")
            hashes.add(tx_hash.lower())
            _require_address(row["buyer"].strip(), "buyer", source_row_index)
            _require_address(row["seller"].strip(), "seller", source_row_index)
            _single_contract(row["raw_contract_candidates"], source_row_index)
            if row["category"].strip() not in {"wearable", "emote"}:
                raise WorkloadDataError(f"row {source_row_index}: unsupported category")
            try:
                timestamp = int(row["timestamp"])
                if timestamp < 0:
                    raise ValueError
            except ValueError as exc:
                raise WorkloadDataError(f"row {source_row_index}: timestamp must be a millisecond integer") from exc
            try:
                price = Decimal(row["price"])
            except InvalidOperation as exc:
                raise WorkloadDataError(f"row {source_row_index}: price must be a decimal string") from exc
            if not price.is_finite() or price < 0:
                raise WorkloadDataError(f"row {source_row_index}: price must be a non-negative decimal")
            categories[row["category"].strip()] += 1
            start = timestamp if start is None else min(start, timestamp)
            end = timestamp if end is None else max(end, timestamp)
    if not ids:
        raise WorkloadDataError("CSV contains no records")
    return CsvValidationSummary(source_hash, len(ids), len(hashes), start or 0, end or 0, dict(sorted(categories.items())))


def _price_bucket(raw: str) -> int:
    digits = raw.split(".", 1)[0].lstrip("0")
    return len(digits) - 1 if digits else 0


def _canonical_record(row: dict[str, str], source_row_index: int, dataset_id: str) -> dict[str, Any]:
    buyer = row["buyer"].strip().lower()
    seller = row["seller"].strip().lower()
    contract = _single_contract(row["raw_contract_candidates"], source_row_index)
    return {
        "schema_version": "mbe_workload_record_v1",
        "dataset_id": dataset_id,
        "source_row_index": source_row_index,
        "source_event_id": row["id"].strip(),
        "source_tx_hash": row["tx_hash"].strip().lower(),
        "timestamp_ms": int(row["timestamp"]),
        "category": row["category"].strip(),
        "buyer_address": buyer,
        "seller_address": seller,
        "contract_address": contract,
        "price_raw": row["price"].strip(),
        "price_bucket": _price_bucket(row["price"].strip()),
        "runtime_value": 1,
        "state_keys": [f"account:buyer:{buyer}", f"account:seller:{seller}", f"contract:{contract}"],
        "provenance": {"source": "decentraland_marketplace_api"},
    }


def _canonical_bytes(record: dict[str, Any]) -> bytes:
    return (json.dumps(record, ensure_ascii=False, separators=(",", ":"), sort_keys=False) + "\n").encode("utf-8")


def build_canonical(csv_path: Path, cache_root: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    """Build a deterministic canonical JSONL.GZ file and atomically publish it."""
    summary = validate_csv(csv_path, expected_sha256=manifest.get("source_sha256") or None)
    content_id = hashlib.sha256(json.dumps({"dataset_id": manifest["dataset_id"], "source_sha256": summary.source_sha256, "generator_version": GENERATOR_VERSION}, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
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
                with csv_path.open("r", encoding="utf-8", newline="") as stream:
                    for index, row in enumerate(csv.DictReader(stream)):
                        record = _canonical_record(row, index, manifest["dataset_id"])
                        key = (record["timestamp_ms"], index)
                        if previous_key is not None and key < previous_key:
                            raise WorkloadDataError("source order violates the canonical (timestamp_ms, source_row_index) contract")
                        previous_key = key
                        compressed.write(_canonical_bytes(record))
        result = {
            "dataset_id": manifest["dataset_id"], "source_sha256": summary.source_sha256,
            "canonical_sha256": sha256_file(canonical_path), "row_count": summary.row_count,
            "canonical_relative_path": f"canonical/{content_id}/workload.jsonl.gz",
            "generator_version": GENERATOR_VERSION, "cache_hit": False,
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
            if record.get("schema_version") != "mbe_workload_record_v1":
                raise WorkloadDataError(f"canonical line {line_number}: unexpected schema")
            yield record


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


def _zipf_records(base: list[dict[str, Any]], alpha: float, domain: str) -> list[dict[str, Any]]:
    by_category: dict[str, list[dict[str, Any]]] = {"wearable": [], "emote": []}
    for record in base:
        by_category[record["category"]].append(record)
    sampled: dict[str, list[dict[str, Any]]] = {}
    for category, records in by_category.items():
        contracts: dict[str, list[dict[str, Any]]] = {}
        for record in records:
            contracts.setdefault(record["contract_address"], []).append(record)
        ranked = sorted(contracts.items(), key=lambda item: (-len(item[1]), item[0]))
        weights = [(rank + 1) ** (-alpha) for rank in range(len(ranked))]
        total = sum(weights)
        cumulative: list[float] = []
        running = 0.0
        for weight in weights:
            running += weight / total
            cumulative.append(running)
        chosen: list[dict[str, Any]] = []
        for draw in range(len(records)):
            unit = _sample_unit(f"{domain}|{category}|contract", draw)
            contract_index = next((i for i, value in enumerate(cumulative) if unit < value), len(cumulative) - 1)
            choices = ranked[contract_index][1]
            chosen.append(choices[min(int(_sample_unit(f"{domain}|{category}|row", draw) * len(choices)), len(choices) - 1)])
        sampled[category] = chosen
    cursors = {"wearable": 0, "emote": 0}
    interleaved: list[dict[str, Any]] = []
    for record in base:
        category = record["category"]
        interleaved.append(sampled[category][cursors[category]])
        cursors[category] += 1
    return interleaved


def _skew_statistics(contracts: Counter[str], buyers: set[str], sellers: set[str], count: int, occurrences: Counter[int]) -> dict[str, Any]:
    values = sorted(contracts.values())
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
        "unique_buyer_count": len(buyers),
        "unique_seller_count": len(sellers),
        "unique_contract_count": len(contracts),
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


def materialize(canonical_path: Path, cache_root: Path, *, dataset_id: str, source_sha256: str, requested_tx_count: int, seed: int, variant_mode: str = "original_window", target_alpha: float | None = None) -> dict[str, Any]:
    total = _canonical_count(canonical_path)
    count = total if requested_tx_count == total else requested_tx_count
    if count <= 0 or count > total or (count != total and count not in _supported_counts()):
        raise WorkloadDataError("requested tx count is not supported by this dataset")
    if variant_mode not in {"original_window", "contract_zipf"}:
        raise WorkloadDataError("unsupported materialization variant")
    if variant_mode == "contract_zipf" and target_alpha not in SUPPORTED_ALPHAS:
        raise WorkloadDataError("unsupported contract Zipf alpha")
    canonical_hash = sha256_file(canonical_path)
    spec = {"dataset_id": dataset_id, "source_sha256": source_sha256, "canonical_sha256": canonical_hash, "requested_tx_count": count, "seed": seed, "selection_mode": "contiguous_window", "selector_version": SELECTOR_VERSION, "generator_version": GENERATOR_VERSION, "variant_mode": variant_mode, "target_alpha": target_alpha}
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
    base_records: list[dict[str, Any]] | None = [] if variant_mode == "contract_zipf" else None
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
        selected = _zipf_records(base_records, float(target_alpha), f"{dataset_id}|{source_sha256}|{base_hash}|{target_alpha}|{seed}|{GENERATOR_VERSION}")
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{materialized_id}.", dir=target.parent))
    try:
        occurrences: Counter[int] = Counter()
        contracts: Counter[str] = Counter()
        buyers: set[str] = set()
        sellers: set[str] = set()
        category_counts: Counter[str] = Counter()
        total_count = 0
        with (temporary / "workload.jsonl.gz").open("wb") as raw:
            with gzip.GzipFile(filename="", mode="wb", fileobj=raw, compresslevel=9, mtime=0) as compressed:
                for index, record in enumerate(selected):
                    occurrence = occurrences[record["source_row_index"]]
                    occurrences[record["source_row_index"]] += 1
                    contracts[record["contract_address"]] += 1
                    buyers.add(record["buyer_address"])
                    sellers.add(record["seller_address"])
                    category_counts[record["category"]] += 1
                    total_count += 1
                    compressed.write(_canonical_bytes(_materialized_record(record, variant_mode, index, occurrence)))
        skew = _skew_statistics(contracts, buyers, sellers, total_count, occurrences)
        summary = dict(spec)
        summary.update({"materialized_id": materialized_id, "actual_tx_count": total_count, "start_offset": start, "end_offset": start + total_count - 1, "selected_time_start_ms": selected_start_ms, "selected_time_end_ms": selected_end_ms, "base_window_sha256": base_hash, "materialized_sha256": sha256_file(temporary / "workload.jsonl.gz"), "materialized_relative_path": f"materialized/{materialized_id}/workload.jsonl.gz", "category_counts": dict(category_counts), "cache_hit": False, **skew})
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
    summary = materialize(
        WORKLOAD_CACHE_ROOT / canonical["canonical_relative_path"],
        WORKLOAD_CACHE_ROOT,
        dataset_id=manifest["dataset_id"],
        source_sha256=canonical["source_sha256"],
        requested_tx_count=requested,
        seed=request.seed,
        variant_mode=variant_mode,
        target_alpha=request.target_alpha,
    )
    variant_id = f"{variant_mode}:count={requested}:seed={request.seed}:alpha={request.target_alpha}"
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
        for key in ("target_alpha", "gini", "hhi", "top_1_ratio", "top_10_ratio", "top_100_ratio", "duplicate_source_row_count", "duplicate_source_row_ratio", "unique_buyer_count", "unique_seller_count", "unique_contract_count")
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
