from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


V3SavedConfigKind = Literal["module", "workload", "topology", "method", "formal_plan"]
V3SavedConfigValidationStatus = Literal["unknown", "valid", "runnable", "blocked"]
V3SavedConfigSource = Literal["user_saved", "builtin_seed", "imported"]


class V3SavedConfigCreateRequest(BaseModel):
    config_kind: V3SavedConfigKind
    name: str = Field(min_length=1, max_length=160)
    description: str = ""
    owner_label: str = ""
    tags: list[str] = Field(default_factory=list)
    payload: dict[str, Any] = Field(default_factory=dict)
    validation_status: V3SavedConfigValidationStatus = "unknown"
    last_validation: dict[str, Any] = Field(default_factory=dict)
    last_smoke_run_id: str = ""
    source: V3SavedConfigSource = "user_saved"


class V3SavedConfigUpdateRequest(BaseModel):
    config_kind: V3SavedConfigKind | None = None
    name: str | None = Field(default=None, min_length=1, max_length=160)
    description: str | None = None
    owner_label: str | None = None
    tags: list[str] | None = None
    payload: dict[str, Any] | None = None
    validation_status: V3SavedConfigValidationStatus | None = None
    last_validation: dict[str, Any] | None = None
    last_smoke_run_id: str | None = None
    source: V3SavedConfigSource | None = None


class V3SavedConfig(BaseModel):
    config_id: str
    config_kind: V3SavedConfigKind
    name: str
    description: str = ""
    owner_label: str = ""
    tags: list[str] = Field(default_factory=list)
    created_at: str
    updated_at: str
    version: int = 1
    payload: dict[str, Any] = Field(default_factory=dict)
    validation_status: V3SavedConfigValidationStatus = "unknown"
    last_validation: dict[str, Any] = Field(default_factory=dict)
    last_smoke_run_id: str = ""
    source: V3SavedConfigSource = "user_saved"
    truth_boundary: str = "local_emulator_config_not_production_chain"
