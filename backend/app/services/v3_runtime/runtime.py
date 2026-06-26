from __future__ import annotations

from pathlib import Path
from typing import Any

from backend.app.services.run_id import new_run_id
from backend.app.services.v3_profile_loader import load_profile_store
from backend.app.services.v3_profile_validator import validate_experiment_profile
from backend.app.services.v3_runtime.artifacts import write_runtime_artifacts
from backend.app.services.v3_runtime.block_producer import TimeOrCountBlockProducer
from backend.app.services.v3_runtime.commit import NormalCommit
from backend.app.services.v3_runtime.consensus import SimpleLeaderConsensus
from backend.app.services.v3_runtime.execution import SerialExecution
from backend.app.services.v3_runtime.metrics import build_summary
from backend.app.services.v3_runtime.models import RuntimeResult
from backend.app.services.v3_runtime.state_access import DirectFetchState
from backend.app.services.v3_runtime.tx_pool import FifoTxPool
from backend.app.services.v3_runtime.workload import generate_synthetic_workload


SUPPORTED_PLUGIN_IDS = {
    "TxPoolPlugin": "fifo_pool",
    "BlockProducer": "time_or_count_block_producer",
    "ConsensusPlugin": "simple_leader",
    "ShardingPlugin": "hash_sharding",
    "ExecutionSchedulerPlugin": "serial_execution",
    "StateAccessPlugin": "direct_fetch",
    "CommitPlugin": "normal_commit",
    "MetricsPlugin": "basic_metrics",
}


class V3RuntimeError(ValueError):
    """Raised when a V3.2 runtime profile cannot be executed."""


def run_v3_single_chain_runtime(experiment_profile_id: str, output_root: Path | None = None) -> RuntimeResult:
    store = load_profile_store()
    experiment_profile = store.experiments[experiment_profile_id]
    validation = validate_experiment_profile(experiment_profile, store)
    if not validation["valid"] or not validation["runnable"]:
        raise V3RuntimeError(f"experiment profile is not runnable: {experiment_profile_id}")
    chain_profile_id = experiment_profile["chain_profile"]
    plugin_profile_id = experiment_profile["plugin_profiles"]["proposed"][0]
    chain_profile = store.chains[chain_profile_id]
    plugin_profile = store.plugins[plugin_profile_id]
    _assert_supported_plugins(plugin_profile)

    run_id = new_run_id().replace("v2run", "v3rt", 1)
    output_dir = (output_root or Path(".cache/v3_runtime_runs")) / run_id
    node_ids = _logical_node_ids(chain_profile)
    workload = generate_synthetic_workload(experiment_profile["workload"], int(chain_profile["state"]["key_count"]))
    tx_pool = FifoTxPool(int(chain_profile["tx_pool"]["max_pool_size"]), bool(chain_profile["tx_pool"]["dedup_enabled"]))
    for tx in workload:
        tx_pool.admit(tx, tx.submit_time_ms)

    producer = TimeOrCountBlockProducer(int(chain_profile["block"]["block_interval_ms"]), int(chain_profile["block"]["max_tx_per_block"]))
    consensus = SimpleLeaderConsensus(node_ids)
    executor = SerialExecution(int(chain_profile["sharding"]["shard_count"]))
    state = DirectFetchState(int(chain_profile["state"]["key_count"]))
    committer = NormalCommit()

    block_log: list[dict[str, Any]] = []
    tx_results = []
    state_commit_log = []
    for block in producer.cut_blocks(tx_pool):
        finalized = consensus.finalize(block)
        block_log.append(
            {
                "block_height": block.block_height,
                "block_id": block.block_id,
                "proposer_node": finalized.proposer_node,
                "tx_count": len(block.txs),
                "cut_time_ms": block.cut_time_ms,
                "ordered_time_ms": finalized.ordered_time_ms,
                "finalized_time_ms": finalized.finalized_time_ms,
                "consensus_plugin": finalized.consensus_plugin,
                "status": finalized.status,
            }
        )
        block_results = executor.execute_block(finalized, state, tx_pool.admit_times)
        tx_results.extend(block_results)
        state_commit_log.extend(committer.commit(state, block_results))

    summary = build_summary(
        run_id=run_id,
        stage=experiment_profile["experiment"]["stage"],
        backend_type=experiment_profile["experiment"]["backend_type"],
        truth_label=experiment_profile["experiment"]["truth_label"],
        chain_profile_id=chain_profile_id,
        plugin_profile_id=plugin_profile_id,
        experiment_profile_id=experiment_profile_id,
        tx_results=tx_results,
        block_count=len(block_log),
    )
    artifacts = write_runtime_artifacts(output_dir, chain_profile, plugin_profile, experiment_profile, block_log, tx_results, state_commit_log, summary)
    return RuntimeResult(run_id, output_dir, summary, artifacts, block_log, tx_results, state_commit_log)


def _logical_node_ids(chain_profile: dict[str, Any]) -> list[str]:
    prefix = str(chain_profile["node"]["node_id_prefix"])
    count = int(chain_profile["deployment"]["validator_count"] or chain_profile["deployment"]["node_count"])
    return [f"{prefix}_{index}" for index in range(count)]


def _assert_supported_plugins(plugin_profile: dict[str, Any]) -> None:
    plugins = plugin_profile.get("plugins", {})
    for plugin_class, expected_id in SUPPORTED_PLUGIN_IDS.items():
        if plugins.get(plugin_class) != expected_id:
            raise V3RuntimeError(f"V3.2 runtime only supports {plugin_class}:{expected_id}")
