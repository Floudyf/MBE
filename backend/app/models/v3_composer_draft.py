from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


V3DraftModuleStatus = Literal["default", "fixed", "variable", "disabled", "planned", "output"]


class V3ComposerDraftModule(BaseModel):
    module_id: str
    status: V3DraftModuleStatus
    plugin: str
    params: dict[str, Any] = Field(default_factory=dict)


class V3ComposerDraftRequest(BaseModel):
    template_id: str
    preset_id: str | None = None
    modules: dict[str, V3ComposerDraftModule]


class V3DraftValidationResponse(BaseModel):
    is_valid: bool
    is_runnable: bool
    run_mode: str = "draft_smoke"
    normalized_draft: dict[str, Any] | None = None
    variable_modules: list[str] = Field(default_factory=list)
    fixed_modules: list[str] = Field(default_factory=list)
    disabled_modules: list[str] = Field(default_factory=list)
    planned_modules: list[str] = Field(default_factory=list)
    output_modules: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
