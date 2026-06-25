from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from math import ceil
from typing import Any


@dataclass(frozen=True)
class ChainProfile:
    chain_id: str
    role: str
    backend_type: str
    block_interval_ms: int
    finality_depth: int
    data_truth_label: str
    backend: str = "mock_chain"
    capabilities: list[str] = field(default_factory=list)

    @property
    def finality_budget_ms(self) -> int:
        return self.block_interval_ms * self.finality_depth


@dataclass(frozen=True)
class BackendCapability:
    backend_type: str
    status: str
    supports_submit: bool
    supports_finality_observation: bool
    supports_event_listener: bool
    supports_real_time: bool
    supports_replay: bool
    supports_virtual_time: bool
    data_truth_label: str
    limitations: list[str]


@dataclass(frozen=True)
class ChainEvent:
    chain_id: str
    tx_id: str
    stage_id: str
    event_type: str
    event_time_ms: int
    status: str
    block_number: int | None = None
    tx_hash: str = ""
    raw_ref: str = ""


@dataclass(frozen=True)
class FinalityObservation:
    chain_id: str
    tx_id: str
    stage_id: str
    commit_time_ms: int
    finality_time_ms: int
    finality_depth: int
    block_interval_ms: int
    observed: bool
    source: str


def expected_commit_time_ms(submit_time_ms: int, block_interval_ms: int, observed_commit_time_ms: int | None = None) -> int:
    slot_time = int(ceil(submit_time_ms / block_interval_ms) * block_interval_ms)
    if observed_commit_time_ms is None:
        return slot_time
    return max(int(observed_commit_time_ms), slot_time)


def expected_finality_time_ms(commit_time_ms: int, block_interval_ms: int, finality_depth: int) -> int:
    return int(commit_time_ms + block_interval_ms * finality_depth)


class ChainBackend(ABC):
    def __init__(self, profile: ChainProfile):
        self.profile = profile

    @abstractmethod
    def capability(self) -> BackendCapability:
        raise NotImplementedError

    @abstractmethod
    def submit_stage(self, record: dict[str, Any]) -> ChainEvent:
        raise NotImplementedError

    @abstractmethod
    def observe_finality(self, record: dict[str, Any]) -> FinalityObservation:
        raise NotImplementedError


class LocalVirtualBackend(ChainBackend):
    def capability(self) -> BackendCapability:
        return BACKEND_CAPABILITIES["local_virtual"]

    def submit_stage(self, record: dict[str, Any]) -> ChainEvent:
        submit_time = int(record["submit_time_ms"])
        commit_time = expected_commit_time_ms(submit_time, self.profile.block_interval_ms, record.get("commit_time_ms"))
        block_number = commit_time // self.profile.block_interval_ms if self.profile.block_interval_ms else 0
        return ChainEvent(
            chain_id=self.profile.chain_id,
            tx_id=str(record.get("tx_id", record["stage_id"])),
            stage_id=str(record["stage_id"]),
            event_type="virtual_commit",
            event_time_ms=commit_time,
            status=str(record["status"]),
            block_number=block_number,
            raw_ref=str(record["stage_id"]),
        )

    def observe_finality(self, record: dict[str, Any]) -> FinalityObservation:
        event = self.submit_stage(record)
        finality_time = expected_finality_time_ms(event.event_time_ms, self.profile.block_interval_ms, self.profile.finality_depth)
        return FinalityObservation(
            chain_id=self.profile.chain_id,
            tx_id=event.tx_id,
            stage_id=event.stage_id,
            commit_time_ms=event.event_time_ms,
            finality_time_ms=finality_time,
            finality_depth=self.profile.finality_depth,
            block_interval_ms=self.profile.block_interval_ms,
            observed=False,
            source="local_virtual",
        )


class TraceReplayBackend(LocalVirtualBackend):
    def capability(self) -> BackendCapability:
        return BACKEND_CAPABILITIES["trace_replay"]

    def observe_finality(self, record: dict[str, Any]) -> FinalityObservation:
        submit_time = int(record["submit_time_ms"])
        commit_time = expected_commit_time_ms(submit_time, self.profile.block_interval_ms, record.get("commit_time_ms"))
        finality_time = int(record.get("finality_time_ms") or expected_finality_time_ms(commit_time, self.profile.block_interval_ms, self.profile.finality_depth))
        return FinalityObservation(
            chain_id=self.profile.chain_id,
            tx_id=str(record.get("tx_id", record["stage_id"])),
            stage_id=str(record["stage_id"]),
            commit_time_ms=commit_time,
            finality_time_ms=max(finality_time, commit_time),
            finality_depth=self.profile.finality_depth,
            block_interval_ms=self.profile.block_interval_ms,
            observed="finality_time_ms" in record,
            source="trace_replay",
        )


class UnsupportedLiveBackend(ChainBackend):
    def __init__(self, profile: ChainProfile, backend_type: str):
        super().__init__(profile)
        self.backend_type = backend_type

    def capability(self) -> BackendCapability:
        return BACKEND_CAPABILITIES[self.backend_type]

    def submit_stage(self, record: dict[str, Any]) -> ChainEvent:
        raise NotImplementedError(f"{self.backend_type} is a V3 planned backend and is not implemented in V2.5")

    def observe_finality(self, record: dict[str, Any]) -> FinalityObservation:
        raise NotImplementedError(f"{self.backend_type} is a V3 planned backend and is not implemented in V2.5")


BACKEND_CAPABILITIES = {
    "local_virtual": BackendCapability(
        backend_type="local_virtual",
        status="runnable",
        supports_submit=True,
        supports_finality_observation=True,
        supports_event_listener=False,
        supports_real_time=False,
        supports_replay=True,
        supports_virtual_time=True,
        data_truth_label="synthetic_replay",
        limitations=["Local virtual-time replay only; not real chain execution."],
    ),
    "trace_replay": BackendCapability(
        backend_type="trace_replay",
        status="experimental",
        supports_submit=False,
        supports_finality_observation=True,
        supports_event_listener=False,
        supports_real_time=False,
        supports_replay=True,
        supports_virtual_time=True,
        data_truth_label="existing_trace_replay",
        limitations=["Replays trace timestamps; does not submit to a live chain."],
    ),
    "fabric_live": BackendCapability(
        backend_type="fabric_live",
        status="planned",
        supports_submit=False,
        supports_finality_observation=False,
        supports_event_listener=False,
        supports_real_time=False,
        supports_replay=False,
        supports_virtual_time=False,
        data_truth_label="production_deployment_planned",
        limitations=["V3 planned only; V2.5 never starts Fabric, Docker, or network.sh."],
    ),
    "evm_live": BackendCapability(
        backend_type="evm_live",
        status="planned",
        supports_submit=False,
        supports_finality_observation=False,
        supports_event_listener=False,
        supports_real_time=False,
        supports_replay=False,
        supports_virtual_time=False,
        data_truth_label="production_deployment_planned",
        limitations=["V3 planned only; V2.5 never connects to public-chain live nodes."],
    ),
}


def list_backend_capabilities() -> list[dict[str, Any]]:
    return [asdict(capability) for capability in BACKEND_CAPABILITIES.values()]


def create_backend(profile: ChainProfile) -> ChainBackend:
    if profile.backend_type == "local_virtual":
        return LocalVirtualBackend(profile)
    if profile.backend_type == "trace_replay":
        return TraceReplayBackend(profile)
    if profile.backend_type in {"fabric_live", "evm_live"}:
        return UnsupportedLiveBackend(profile, profile.backend_type)
    raise ValueError(f"unknown chain backend type: {profile.backend_type}")
