from backend.app.services.v3_runtime.block_producer import TimeOrCountBlockProducer
from backend.app.services.v3_runtime.commit import NormalCommit
from backend.app.services.v3_runtime.consensus import SimpleLeaderConsensus
from backend.app.services.v3_runtime.models import Transaction, TxResult
from backend.app.services.v3_runtime.runtime import run_v3_single_chain_runtime
from backend.app.services.v3_runtime.sharding import assign_hash_shard
from backend.app.services.v3_runtime.state_access import DirectFetchState
from backend.app.services.v3_runtime.tx_pool import FifoTxPool


def test_runtime_smoke_creates_deterministic_summary_counts(tmp_path):
    first = run_v3_single_chain_runtime("single_chain_runtime_smoke", tmp_path / "first")
    second = run_v3_single_chain_runtime("single_chain_runtime_smoke", tmp_path / "second")

    assert first.output_dir.exists()
    assert first.summary.truth_label == "modular_runtime"
    assert first.summary.tx_count == 24
    assert first.summary.success_count == 24
    assert first.summary.failure_count == 0
    assert first.summary.block_count == second.summary.block_count
    assert first.summary.tx_count == second.summary.tx_count
    assert first.summary.avg_latency_ms == second.summary.avg_latency_ms


def test_tx_pool_dedup_works_for_duplicate_tx_ids():
    pool = FifoTxPool(max_pool_size=10, dedup_enabled=True)
    tx = Transaction("dup", 0, "update", ["asset_1"], {"asset_1": 1})

    assert pool.admit(tx, 0) is True
    assert pool.admit(tx, 1) is False
    assert len(pool.select_for_block(10)) == 1


def test_block_producer_respects_max_tx_per_block():
    pool = FifoTxPool(max_pool_size=10, dedup_enabled=True)
    for index in range(5):
        pool.admit(Transaction(f"tx_{index}", index, "update", [f"asset_{index}"], {f"asset_{index}": 1}), index)

    blocks = TimeOrCountBlockProducer(block_interval_ms=10, max_tx_per_block=2).cut_blocks(pool)

    assert [len(block.txs) for block in blocks] == [2, 2, 1]


def test_simple_leader_records_proposer_and_finalized_status():
    pool = FifoTxPool(max_pool_size=10)
    pool.admit(Transaction("tx_1", 0, "update", ["asset_1"], {"asset_1": 1}), 0)
    block = TimeOrCountBlockProducer(10, 10).cut_blocks(pool)[0]

    finalized = SimpleLeaderConsensus(["node_0", "node_1"]).finalize(block)

    assert finalized.proposer_node == "node_0"
    assert finalized.status == "finalized"
    assert finalized.finalized_time_ms >= finalized.ordered_time_ms


def test_hash_sharding_assigns_valid_shard_ids():
    shard_id = assign_hash_shard("asset_1", 4)

    assert 0 <= shard_id < 4


def test_normal_commit_updates_state_and_logs_old_new_values():
    state = DirectFetchState()
    result = TxResult(
        tx_id="tx_1",
        submit_time_ms=0,
        admit_time_ms=0,
        block_height=1,
        execution_start_ms=1,
        execution_end_ms=2,
        commit_time_ms=3,
        latency_ms=3,
        status="success",
        shard_id=0,
        read_count=1,
        write_count=1,
        remote_fetch_count=0,
        deltas={"asset_1": (0, 5, 5)},
    )

    commits = NormalCommit().commit(state, [result])

    assert state.values["asset_1"] == 5
    assert commits[0].old_value == 0
    assert commits[0].new_value == 5
