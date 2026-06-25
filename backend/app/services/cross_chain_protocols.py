from __future__ import annotations

from typing import Any

from backend.app.services.cross_chain_protocol import CrossChainProtocol, ProtocolAction, ProtocolEvent, ProtocolState, ProtocolStepResult


class ProtocolNotFound(ValueError):
    """Raised when a requested protocol is not implemented as a local baseline."""


class LocalLockMintBaseline(CrossChainProtocol):
    version = "v2.6-local-baseline"
    concurrency_mode = "pipeline"
    committee_delay_ms = 0
    window_size: int | None = None

    def __init__(self, **settings: Any):
        if "window_size" in settings:
            self.window_size = int(settings["window_size"])
        if "committee_delay_ms" in settings:
            self.committee_delay_ms = int(settings["committee_delay_ms"])

    def get_initial_state(self, record_or_cross_tx: dict[str, Any]) -> ProtocolState:
        deadline = int(record_or_cross_tx.get("timeout_deadline_ms") or record_or_cross_tx.get("deadline_ms") or 0)
        created_at = int(record_or_cross_tx.get("submit_time_ms", 0))
        expected_terminal = str(record_or_cross_tx.get("expected_terminal_status", "completed"))
        return ProtocolState(
            cross_tx_id=str(record_or_cross_tx["cross_tx_id"]),
            protocol_name=self.name,
            status="created",
            current_stage="created",
            source_chain=str(record_or_cross_tx["source_chain"]),
            target_chain=str(record_or_cross_tx["target_chain"]),
            created_at_ms=created_at,
            updated_at_ms=created_at,
            deadline_ms=deadline,
            metadata={
                "started_at_ms": created_at,
                "expected_terminal_status": expected_terminal,
                "committee_delay_ms": self.committee_delay_ms,
                "window_size": self.window_size,
                "protocol_truth": "local_baseline_model",
            },
        )

    def plan_initial_actions(self, state: ProtocolState) -> list[ProtocolAction]:
        return [self._action(state, "submit_source_lock", state.source_chain, "source_lock", state.created_at_ms)]

    def handle_event(self, state: ProtocolState, event: ProtocolEvent) -> ProtocolStepResult:
        state.metadata["event_count"] = int(state.metadata.get("event_count", 0)) + 1
        state.updated_at_ms = event.event_time_ms
        actions: list[ProtocolAction] = []

        if event.event_type == "finality" and event.stage == "source_lock":
            wait = int(event.payload.get("finality_wait_time_ms", 0))
            state.metadata["source_wait_time_ms"] = int(state.metadata.get("source_wait_time_ms", 0)) + wait
            state.metadata["finality_wait_time_ms"] = int(state.metadata.get("finality_wait_time_ms", 0)) + wait
            if state.metadata.get("expected_terminal_status") in {"timeout", "refunded"} or (state.deadline_ms and event.event_time_ms > state.deadline_ms):
                state.status = "timeout"
                state.current_stage = "timeout"
                actions.append(self._action(state, "mark_timeout", state.source_chain, "timeout", max(event.event_time_ms, state.deadline_ms)))
            else:
                state.status = "source_finalized"
                state.current_stage = "source_finality"
                actions.append(self._action(state, "generate_certificate", state.source_chain, "cert_generated", event.event_time_ms + self.committee_delay_ms))
        elif event.event_type == "timeout":
            state.status = "timeout"
            state.current_stage = "timeout"
            state.metadata["timeout_seen"] = True
            actions.append(self._action(state, "refund", state.source_chain, "refunded", event.event_time_ms))
        elif event.event_type == "refund":
            state.status = "refunded"
            state.current_stage = "refunded"
            state.metadata["finished_at_ms"] = event.event_time_ms
        elif event.event_type == "certificate" and event.stage == "completed":
            state.status = "completed"
            state.current_stage = "completed"
            state.metadata["finished_at_ms"] = event.event_time_ms
        elif event.event_type == "certificate":
            state.status = "cert_generated"
            state.current_stage = "cert_generated"
            actions.append(self._action(state, "submit_target_mint", state.target_chain, "target_mint", event.event_time_ms))
        elif event.event_type == "finality" and event.stage == "target_mint":
            wait = int(event.payload.get("finality_wait_time_ms", 0))
            state.status = "target_finalized"
            state.current_stage = "target_finality"
            state.metadata["target_wait_time_ms"] = int(state.metadata.get("target_wait_time_ms", 0)) + wait
            state.metadata["finality_wait_time_ms"] = int(state.metadata.get("finality_wait_time_ms", 0)) + wait
            actions.append(self._action(state, "complete", state.target_chain, "completed", event.event_time_ms))
        elif event.event_type == "failure":
            state.status = "failed"
            state.current_stage = "failed"
            state.metadata["finished_at_ms"] = event.event_time_ms
        return ProtocolStepResult(state=state, actions=actions, events=[event])

    def _action(self, state: ProtocolState, action_type: str, chain_id: str, stage: str, scheduled_time_ms: int) -> ProtocolAction:
        state.metadata["action_count"] = int(state.metadata.get("action_count", 0)) + 1
        return ProtocolAction(
            action_id=f"{state.cross_tx_id}_{action_type}_{state.metadata['action_count']}",
            cross_tx_id=state.cross_tx_id,
            action_type=action_type,
            chain_id=chain_id,
            stage=stage,
            scheduled_time_ms=int(scheduled_time_ms),
            deadline_ms=state.deadline_ms,
            payload={"protocol_name": self.name, "protocol_truth": "local_baseline_model"},
        )


class LockMintSerial(LocalLockMintBaseline):
    name = "lock_mint_serial"
    concurrency_mode = "serial"


class LockMintPipeline(LocalLockMintBaseline):
    name = "lock_mint_pipeline"
    concurrency_mode = "pipeline"


class FixedWindowBaseline(LocalLockMintBaseline):
    name = "fixed_window_baseline"
    concurrency_mode = "fixed_window"
    window_size = 2


class CommitteeBridgeBasic(LocalLockMintBaseline):
    name = "committee_bridge_basic"
    concurrency_mode = "pipeline"
    committee_delay_ms = 50


IMPLEMENTED_PROTOCOLS = {
    "lock_mint_serial": LockMintSerial,
    "lock_mint_pipeline": LockMintPipeline,
    "fixed_window_baseline": FixedWindowBaseline,
    "committee_bridge_basic": CommitteeBridgeBasic,
}


def create_protocol(name: str, settings: dict[str, Any] | None = None) -> CrossChainProtocol:
    if name == "metaflow":
        raise ProtocolNotFound("metaflow is planned and is not runnable in V2.6")
    if name not in IMPLEMENTED_PROTOCOLS:
        raise ProtocolNotFound(f"unknown protocol baseline: {name}")
    return IMPLEMENTED_PROTOCOLS[name](**(settings or {}))


def list_cross_chain_protocols() -> list[dict[str, Any]]:
    return [
        {
            "name": "disabled",
            "status": "runnable",
            "maturity": "stable",
            "reason": "No cross-chain protocol.",
        },
        {
            "name": "lock_mint_serial",
            "status": "runnable",
            "maturity": "experimental",
            "reason": "Local baseline replay only; not a production bridge.",
        },
        {
            "name": "lock_mint_pipeline",
            "status": "runnable",
            "maturity": "experimental",
            "reason": "Local pipeline baseline replay only; not MetaFlow.",
        },
        {
            "name": "fixed_window_baseline",
            "status": "runnable",
            "maturity": "experimental",
            "reason": "Local fixed-window baseline only; not dynamic FDA.",
        },
        {
            "name": "committee_bridge_basic",
            "status": "runnable",
            "maturity": "experimental",
            "reason": "Local delay model only; no real signatures, proofs, or committee security.",
        },
        {
            "name": "metaflow",
            "status": "planned",
            "maturity": "planned",
            "reason": "MetaFlow is not implemented in V2.6.",
        },
    ]
