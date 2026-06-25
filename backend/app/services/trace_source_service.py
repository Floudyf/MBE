from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_TRACE_SOURCE_CONFIG = ROOT / "configs/trace_sources/v2_trace_sources.yaml"

REQUIRED_TRACE_SOURCE_FIELDS = {
    "id",
    "label",
    "status",
    "maturity",
    "data_truth_label",
    "description",
    "entry_mode",
    "capabilities",
    "limitations",
    "validation",
    "compatible_topologies",
    "notes",
}
REQUIRED_CAPABILITY_FIELDS = {"trace_format", "required_files", "optional_files", "provides_fields", "semantic_guarantees"}
REQUIRED_VALIDATION_FIELDS = {"workspace_bound", "requires_existing_file", "allows_live_network", "allows_docker_start"}
ALLOWED_STATUS = {"runnable", "planned", "experimental", "invalid"}
ALLOWED_MATURITY = {"stable", "experimental", "planned"}


class TraceSourceError(ValueError):
    """Raised when trace source declarations or requests are invalid."""


class TraceSourceNotFound(KeyError):
    """Raised when a trace source id is unknown."""


@dataclass(frozen=True)
class TraceSourceRegistry:
    version: str
    stage: str
    sources: list[dict[str, Any]]

    def list_sources(self) -> list[dict[str, Any]]:
        return list(self.sources)

    def get_source(self, source_id: str) -> dict[str, Any]:
        for source in self.sources:
            if source["id"] == source_id:
                return source
        raise TraceSourceNotFound(f"unknown trace source {source_id}")


def load_trace_sources(path: Path = DEFAULT_TRACE_SOURCE_CONFIG) -> TraceSourceRegistry:
    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise TraceSourceError(f"cannot load trace source config: {path}") from exc
    if not isinstance(document, dict):
        raise TraceSourceError("trace source config must be a mapping")
    sources = document.get("trace_sources")
    if not isinstance(sources, list) or not sources:
        raise TraceSourceError("trace source config must contain a non-empty trace_sources list")
    for index, source in enumerate(sources):
        validate_trace_source_declaration(source, index)
    return TraceSourceRegistry(version=str(document.get("version", "")), stage=str(document.get("stage", "")), sources=sources)


def validate_trace_source_declaration(source: Any, index: int = 0) -> None:
    if not isinstance(source, dict):
        raise TraceSourceError(f"trace source #{index} must be a mapping")
    missing = REQUIRED_TRACE_SOURCE_FIELDS - source.keys()
    if missing:
        raise TraceSourceError(f"trace source {source.get('id', index)} missing fields: {sorted(missing)}")
    if source["status"] not in ALLOWED_STATUS:
        raise TraceSourceError(f"trace source {source['id']} has invalid status {source['status']}")
    if source["maturity"] not in ALLOWED_MATURITY:
        raise TraceSourceError(f"trace source {source['id']} has invalid maturity {source['maturity']}")
    for field in ("entry_mode", "limitations", "compatible_topologies", "notes"):
        if not isinstance(source[field], list):
            raise TraceSourceError(f"trace source {source['id']} field {field} must be a list")
    capabilities = source["capabilities"]
    if not isinstance(capabilities, dict) or not REQUIRED_CAPABILITY_FIELDS <= capabilities.keys():
        raise TraceSourceError(f"trace source {source['id']} has invalid capabilities")
    for field in ("required_files", "optional_files", "provides_fields", "semantic_guarantees"):
        if not isinstance(capabilities[field], list):
            raise TraceSourceError(f"trace source {source['id']} capabilities.{field} must be a list")
    validation = source["validation"]
    if not isinstance(validation, dict) or not REQUIRED_VALIDATION_FIELDS <= validation.keys():
        raise TraceSourceError(f"trace source {source['id']} has invalid validation")
    if validation["allows_live_network"] is not False or validation["allows_docker_start"] is not False:
        raise TraceSourceError(f"trace source {source['id']} must not allow live network or Docker startup in V2.3")


def list_trace_sources(registry: TraceSourceRegistry | None = None) -> list[dict[str, Any]]:
    registry = registry or load_trace_sources()
    return [
        {
            "id": source["id"],
            "label": source["label"],
            "status": source["status"],
            "maturity": source["maturity"],
            "data_truth_label": source["data_truth_label"],
            "description": source["description"],
            "entry_mode": source["entry_mode"],
            "capabilities": source["capabilities"],
            "limitations": source["limitations"],
            "validation": source["validation"],
        }
        for source in registry.list_sources()
    ]


def infer_data_truth_label(source_id: str, registry: TraceSourceRegistry | None = None) -> str:
    registry = registry or load_trace_sources()
    return str(registry.get_source(source_id)["data_truth_label"])


def describe_capabilities(source_id: str, registry: TraceSourceRegistry | None = None) -> dict[str, Any]:
    registry = registry or load_trace_sources()
    return dict(registry.get_source(source_id)["capabilities"])
