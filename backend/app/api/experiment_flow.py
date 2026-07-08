from __future__ import annotations

from fastapi import APIRouter, HTTPException

from backend.app.models.experiment_flow import ExperimentRunPlanRequest, ExperimentSuiteRequest, RunSuiteExecutionRequest
from backend.app.services import experiment_flow_service

router = APIRouter(prefix="/api/experiment-flow")


@router.get("/profiles")
def experiment_flow_profiles() -> dict:
    return {"items": experiment_flow_service.list_profiles()}


@router.get("/topologies")
def experiment_flow_topologies() -> dict:
    return {"items": experiment_flow_service.list_topologies()}


@router.get("/workloads")
def experiment_flow_workloads() -> dict:
    return {"items": experiment_flow_service.list_workloads()}


@router.get("/methods")
def experiment_flow_methods() -> dict:
    return {"items": experiment_flow_service.list_default_methods()}


@router.get("/recommended-run")
def experiment_flow_recommended_run() -> dict:
    return experiment_flow_service.recommended_run().model_dump()


@router.post("/preview-run-plan")
def experiment_flow_preview_run_plan(request: ExperimentRunPlanRequest) -> dict:
    try:
        return experiment_flow_service.preview_run_plan(request).model_dump()
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/preview-run-matrix")
def experiment_flow_preview_run_matrix(request: ExperimentSuiteRequest) -> dict:
    try:
        return experiment_flow_service.preview_run_matrix(request).model_dump()
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/derive-v4-realism-request")
def experiment_flow_derive_v4_realism_request(request: ExperimentSuiteRequest) -> dict:
    try:
        return experiment_flow_service.derive_v4_realism_request(request).model_dump()
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/execute-selected-matrix")
def experiment_flow_execute_selected_matrix(request: RunSuiteExecutionRequest) -> dict:
    try:
        return experiment_flow_service.execute_selected_run_matrix(request).model_dump()
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
