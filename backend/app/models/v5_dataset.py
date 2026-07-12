from __future__ import annotations

from typing import Literal
from pydantic import BaseModel, Field

SourceKind = Literal["synthetic", "canonical_trace", "external_dataset", "derived_trace", "existing_trace"]

class WorkloadSourceRef(BaseModel):
    source_kind: SourceKind
    source_id: str
    cursor: str = ""

class DatasetManifest(BaseModel):
    source: WorkloadSourceRef
    runnable: bool
    truth_boundary: str
    blocking_reason: str = ""

class CanonicalTraceRecord(BaseModel):
    record_id: str
    sender: str
    receiver: str
    state_keys: list[str] = Field(default_factory=list)
    timestamp_ms: int

class WorkloadSession(BaseModel):
    source: WorkloadSourceRef
    cursor: str = ""
    checkpoint: str = ""
    max_in_flight: int = Field(default=128, ge=1)
