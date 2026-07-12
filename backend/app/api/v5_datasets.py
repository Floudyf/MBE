from fastapi import APIRouter
from backend.app.models.v5_dataset import DatasetManifest

router=APIRouter(prefix="/api/v5/datasets",tags=["v5"])
_CATALOG=[DatasetManifest(source={"source_kind":"synthetic","source_id":"deterministic_signed_synthetic"},runnable=True,truth_boundary="synthetic_workload"),DatasetManifest(source={"source_kind":"canonical_trace","source_id":"planned"},runnable=False,truth_boundary="not_loaded",blocking_reason="adapter not implemented; no synthetic fallback"),DatasetManifest(source={"source_kind":"external_dataset","source_id":"planned"},runnable=False,truth_boundary="not_loaded",blocking_reason="adapter not implemented; no synthetic fallback")]
@router.get("")
def catalog(): return [item.model_dump() for item in _CATALOG]
@router.get("/schema")
def schema(): return {"streaming_only":True,"read_all_forbidden":True,"catalog":[item.model_dump() for item in _CATALOG]}
