from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from typing import Any


PROTOCOL_STATUSES = {
    "created",
    "source_locked",
    "source_finalized",
    "cert_generated",
    "target_submitted",
    "target_finalized",
    "completed",
    "timeout",
    "refunded",
    "failed",
}
PROTOCOL_EVENT_TYPES = {"submit", "commit", "finality", "certificate", "timeout", "refund", "failure"}
PROTOCOL_EVENT_SOURCES = {"protocol", "backend", "virtual", "trace", "planned_live"}
PROTOCOL_ACTION_TYPES = {
    "submit_source_lock",
    "wait_source_finality",
    "generate_certificate",
    "submit_target_mint",
    "wait_target_finality",
    "complete",
    "refund",
    "mark_timeout",
    "fail",
}


@dataclass
class ProtocolState:
    cross_tx_id: str
    protocol_name: str
    status: str
    current_stage: str
    source_chain: str
    target_chain: str
    created_at_ms: int
    updated_at_ms: int
    deadline_ms: int
    attempt_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ProtocolEvent:
    event_id: str
    cross_tx_id: str
    event_type: str
    chain_id: str
    stage: str
    event_time_ms: int
    source: str
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass
class ProtocolAction:
    action_id: str
    cross_tx_id: str
    action_type: str
    chain_id: str
    stage: str
    scheduled_time_ms: int
    deadline_ms: int
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass
class ProtocolStepResult:
    state: ProtocolState
    actions: list[ProtocolAction] = field(default_factory=list)
    events: list[ProtocolEvent] = field(default_factory=list)


@dataclass
class ProtocolResult:
    protocol_name: str
    cross_tx_id: str
    status: str
    success: bool
    timeout: bool
    refunded: bool
    failed: bool
    e2e_latency_ms: int
    source_wait_time_ms: int
    target_wait_time_ms: int
    finality_wait_time_ms: int
    action_count: int
    event_count: int
    metadata: dict[str, Any] = field(default_factory=dict)


class CrossChainProtocol(ABC):
    name: str
    version: str

    @abstractmethod
    def get_initial_state(self, record_or_cross_tx: dict[str, Any]) -> ProtocolState:
        raise NotImplementedError

    @abstractmethod
    def plan_initial_actions(self, state: ProtocolState) -> list[ProtocolAction]:
        raise NotImplementedError

    @abstractmethod
    def handle_event(self, state: ProtocolState, event: ProtocolEvent) -> ProtocolStepResult:
        raise NotImplementedError

    def is_terminal(self, state: ProtocolState) -> bool:
        return state.status in {"completed", "timeout", "refunded", "failed"}

    def finalize_result(self, state: ProtocolState) -> ProtocolResult:
        started_at = int(state.metadata.get("started_at_ms", state.created_at_ms))
        finished_at = int(state.metadata.get("finished_at_ms", state.updated_at_ms))
        return ProtocolResult(
            protocol_name=self.name,
            cross_tx_id=state.cross_tx_id,
            status=state.status,
            success=state.status == "completed",
            timeout=state.status == "timeout" or bool(state.metadata.get("timeout_seen", False)),
            refunded=state.status == "refunded",
            failed=state.status == "failed",
            e2e_latency_ms=max(0, finished_at - started_at),
            source_wait_time_ms=int(state.metadata.get("source_wait_time_ms", 0)),
            target_wait_time_ms=int(state.metadata.get("target_wait_time_ms", 0)),
            finality_wait_time_ms=int(state.metadata.get("finality_wait_time_ms", 0)),
            action_count=int(state.metadata.get("action_count", 0)),
            event_count=int(state.metadata.get("event_count", 0)),
            metadata=dict(state.metadata),
        )


def to_dict(value: Any) -> dict[str, Any]:
    return asdict(value)
