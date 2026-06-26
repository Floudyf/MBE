from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Transaction:
    tx_id: str
    submit_time_ms: int
    operation: str
    read_keys: list[str]
    write_deltas: dict[str, int]


@dataclass(frozen=True)
class Block:
    block_height: int
    block_id: str
    txs: list[Transaction]
    cut_time_ms: int


@dataclass(frozen=True)
class FinalizedBlock:
    block: Block
    proposer_node: str
    ordered_time_ms: int
    finalized_time_ms: int
    consensus_plugin: str
    status: str = "finalized"


@dataclass(frozen=True)
class TxResult:
    tx_id: str
    submit_time_ms: int
    admit_time_ms: int
    block_height: int
    execution_start_ms: int
    execution_end_ms: int
    commit_time_ms: int
    latency_ms: int
    status: str
    shard_id: int
    read_count: int
    write_count: int
    remote_fetch_count: int
    deltas: dict[str, tuple[int, int, int]] = field(repr=False)


@dataclass(frozen=True)
class StateCommit:
    block_height: int
    tx_id: str
    state_key: str
    old_value: int
    delta: int
    new_value: int
    commit_plugin: str
    commit_time_ms: int
    status: str


@dataclass(frozen=True)
class RuntimeSummary:
    run_id: str
    stage: str
    backend_type: str
    truth_label: str
    chain_profile_id: str
    plugin_profile_id: str
    experiment_profile_id: str
    tx_count: int
    success_count: int
    failure_count: int
    block_count: int
    throughput_tps: float
    avg_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    runtime_mode: str


@dataclass(frozen=True)
class RuntimeResult:
    run_id: str
    output_dir: Path
    summary: RuntimeSummary
    artifacts: dict[str, Path]
    block_log: list[dict[str, Any]]
    tx_results: list[TxResult]
    state_commit_log: list[StateCommit]
