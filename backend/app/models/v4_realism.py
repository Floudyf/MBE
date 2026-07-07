from __future__ import annotations

from pydantic import BaseModel, Field


class V4RealismSmokeRequest(BaseModel):
    nodes: int = Field(default=4, ge=4, le=8)
    shards: int = Field(default=1, ge=1, le=4)
    tx_count: int = Field(default=10, ge=1, le=100)
    enable_cross_shard: bool = True
    enable_faults: bool = True
    fault_profile: str = Field(default="network_delay")
    blockemulator_csv: str | None = None
    blockemulator_tx_limit: int = Field(default=20, ge=1, le=1000)
    run_duration_ms: int = Field(default=1000, ge=100, le=10000)

