from __future__ import annotations

from fastapi import APIRouter, HTTPException

from backend.app.models.v5_dataset import DatasetManifest
from backend.app.services import v5_workload_data_plane as workloads
from backend.app.services.v5_workload_data_plane import WorkloadDataError, WorkloadPreviewRequest


router = APIRouter(tags=["v5"])


@router.get("/api/v5/datasets")
def legacy_catalog() -> list[dict]:
    catalog = [
        DatasetManifest(source={"source_kind": "synthetic", "source_id": "deterministic_signed_synthetic"}, runnable=True, truth_boundary="synthetic_workload"),
    ]
    for item in workloads.load_manifests():
        summary = workloads.dataset_summary(item)
        catalog.append(
            DatasetManifest(
                source={"source_kind": "canonical_trace", "source_id": summary.dataset_id},
                runnable=summary.selectable,
                truth_boundary=summary.truth_label,
                blocking_reason="; ".join(summary.blockers) if summary.blockers else "",
            )
        )
    return [item.model_dump() for item in catalog]


@router.get("/api/v5/datasets/schema")
def legacy_schema() -> dict:
    return {"streaming_only": True, "read_all_forbidden": True, "catalog": legacy_catalog()}


@router.get("/api/v5/workloads/datasets")
def list_workload_datasets() -> list[dict]:
    return [workloads.dataset_summary(item).model_dump() for item in workloads.load_manifests()]


@router.get("/api/v5/workloads/datasets/{dataset_id}")
def get_workload_dataset(dataset_id: str) -> dict:
    try:
        return workloads.dataset_detail(dataset_id).model_dump()
    except WorkloadDataError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.post("/api/v5/workloads/preview")
def preview_workload(payload: WorkloadPreviewRequest) -> dict:
    return workloads.preview_workload(payload).model_dump()


@router.post("/api/v5/workloads/materialize")
def materialize_workload(payload: WorkloadPreviewRequest) -> dict:
    try:
        return workloads.materialize_request(payload).model_dump()
    except WorkloadDataError as exc:
        raise HTTPException(422, str(exc)) from exc
