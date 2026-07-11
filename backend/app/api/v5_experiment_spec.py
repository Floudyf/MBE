from __future__ import annotations

from fastapi import APIRouter, HTTPException

from backend.app.models.v5_experiment_spec import V5ExperimentSpec
from backend.app.services.v5_compatibility_engine import validate
from backend.app.services.v5_experiment_compiler import compile_plan


router = APIRouter(prefix="/api/v5/experiment-spec", tags=["v5"])


@router.post("/validate")
def validate_spec(payload: V5ExperimentSpec) -> dict:
    return validate(payload).model_dump()


@router.post("/compile")
def compile_spec(payload: V5ExperimentSpec) -> dict:
    try:
        return compile_plan(payload, __import__("pathlib").Path(".cache") / "v5_compile_preview", source_saved_config_id=payload.saved_config_id).model_dump()
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
