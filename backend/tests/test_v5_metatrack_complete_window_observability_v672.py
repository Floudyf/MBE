from __future__ import annotations

import json
from pathlib import Path

from backend.app.services.v5_observability_metrics import summarize_metatrack_consensus_windows
from backend.app.services.v5_paper_exporter import _with_metatrack_consensus_treatment_gate


def _write_jsonl(path: Path, rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def _routing(ordinal: int, shard: str, batch: int, shard_n: int, *, window: int = 1,
             start: int = 1, end: int = 2, global_n: int = 4,
             critical_path: int = 1, round_: int = 1, preds=None) -> dict:
    return {
        "routing_ordinal": ordinal,
        "execution_shard": shard,
        "route_batch_sequence": batch,
        "route_batch_transaction_count": 2,
        "route_batch_shard_transaction_count": 1,
        "consensus_execution_predecessor_ordinals": preds or [],
        "consensus_execution_round": round_,
        "state_versions": [],
        "consensus_window_sequence": window,
        "consensus_window_start_batch_sequence": start,
        "consensus_window_end_batch_sequence": end,
        "consensus_window_route_batch_count": end - start + 1,
        "consensus_window_transaction_count": global_n,
        "consensus_window_shard_transaction_count": shard_n,
        "consensus_window_critical_path": critical_path,
    }


def _tx(tx_id: str, routing: dict) -> dict:
    return {"tx_id": tx_id, "execution_routing": routing}


def _block(hash_: str, shard: str, height: int, txs: list[dict]) -> dict:
    return {"block_hash": hash_, "shard_id": shard, "height": height, "tx_list": txs}


def _write_complete_asymmetric_v669_run(root: Path) -> None:
    # Complete V669: one global window, but shard-local projection counts differ.
    # s0=3, s1=1 is intentionally asymmetric and must remain valid.
    t1 = _tx("t1", _routing(1, "s0", 1, 3, round_=1))
    t2 = _tx("t2", _routing(2, "s0", 1, 3, round_=1))
    t3 = _tx("t3", _routing(3, "s0", 2, 3, round_=1))
    t4 = _tx("t4", _routing(4, "s1", 2, 1, round_=1))

    blocks = {
        "s0": [_block("a", "s0", 1, [t1, t2, t3])],
        "s1": [_block("b", "s1", 1, [t4])],
    }
    for shard, nodes in (("s0", ("n0", "n1")), ("s1", ("n4", "n5"))):
        for node in nodes:
            _write_jsonl(root / "nodes" / node / "blocks.jsonl", blocks[shard])
            _write_jsonl(
                root / "nodes" / node / "commit_markers.jsonl",
                [{"block_hash": block["block_hash"], "committed": True} for block in blocks[shard]],
            )

    (root / "real_cluster_summary.json").write_text(
        json.dumps({
            "configured_block_size": 1000,
            "method_config_id": "metatrack_latest",
            "plugin_profile": {"block_producer": {"plugin_id": "metatrack_nl_window_v669"}},
        }),
        encoding="utf-8",
    )


def test_asymmetric_complete_v669_is_not_misclassified_as_streaming(tmp_path: Path):
    _write_complete_asymmetric_v669_run(tmp_path)
    summary = summarize_metatrack_consensus_windows(tmp_path)

    assert summary["available"] is True
    assert summary["schema_version"] == "mbe_metatrack_consensus_window_observability_v669"
    assert summary["truth_scope"] == "durable_signed_dependency_closed_adaptive_n_over_l_consensus_window_reconstruction_v669"
    assert len(summary["windows"]) == 1

    window = summary["windows"][0]
    assert window["shard_transaction_counts"] == {"s0": 3, "s1": 1}
    assert window["shard_transaction_count_match"] is True

    metrics = summary["metrics"]
    assert metrics["metatrack_consensus_window_signed_reconstruction_match"] is True
    assert metrics["metatrack_consensus_window_actual_projection_match"] is True
    assert metrics["metatrack_consensus_window_aggregation_active"] is True
    assert metrics["metatrack_consensus_window_observed_pbft_block_count"] == 2
    assert metrics["metatrack_consensus_window_expected_pbft_block_count"] == 2
    assert metrics["metatrack_consensus_window_baseline_route_batch_pbft_block_count"] == 3
    assert metrics["metatrack_consensus_window_pbft_blocks_saved"] == 1


def test_asymmetric_complete_v669_reenables_consensus_ablation_gate(tmp_path: Path):
    _write_complete_asymmetric_v669_run(tmp_path)
    metrics = summarize_metatrack_consensus_windows(tmp_path)["metrics"]

    common = {
        "suite_type": "ablation_experiment",
        "seed": 11,
        "repeat_index": 0,
        "estimated_transactions": 10000,
        "topology_point": {"nodes": 8, "shards": 2, "validators_per_shard": 4, "worker_count": 8},
        "workload_point": {"tx_count": 10000},
        "fault_point": {"mode": "disabled"},
        "block_size": 1000,
        "block_interval_ms": 100,
    }
    full = {
        **common,
        "method_config_id": "metatrack_latest",
        "method": {"display_name": "Metatrack", "plugin_overrides": {}, "plugin_config_overrides": {}},
        "metrics": {
            **metrics,
            "actual_committed_block_count": 88,
            "pbft_preprepare_count": 264,
            "pbft_message_count": 2424,
        },
    }
    no_cons = {
        **common,
        "method_config_id": "metatrack_ab_cons",
        "method": {
            "display_name": "去掉共识聚合",
            "plugin_overrides": {"block_producer": "time_or_count_block_producer"},
            "plugin_config_overrides": {"block_producer": {"dependency_closed_consensus": False}},
        },
        "metrics": {
            "actual_committed_block_count": 202,
            "pbft_preprepare_count": 606,
            "pbft_message_count": 5574,
        },
    }

    gated = _with_metatrack_consensus_treatment_gate([full, no_cons])
    item = next(row for row in gated if row["method_config_id"] == "metatrack_ab_cons")
    assert item["_metatrack_consensus_ablation_treatment_active"] is True
    evidence = item["_metatrack_consensus_ablation_treatment_evidence"]
    assert evidence["full_aggregation_active"] is True
    assert evidence["observed_more_blocks"] is True
    assert evidence["observed_more_pbft_work"] is True


def test_detector_global_identity_excludes_shard_local_count():
    source = Path("backend/app/services/v5_observability_metrics.py").read_text(encoding="utf-8")
    start = source.index("def _metatrack_v669_streaming_prefix_metadata_v671")
    end = source.index("def summarize_metatrack_consensus_windows", start)
    detector = source[start:end]
    tuple_start = detector.index("identities_by_window[window].add((")
    tuple_end = detector.index("))", tuple_start)
    identity_tuple = detector[tuple_start:tuple_end + 2]
    assert "consensus_window_end_batch_sequence" in identity_tuple
    assert "consensus_window_route_batch_count" in identity_tuple
    assert "consensus_window_transaction_count" in identity_tuple
    assert "consensus_window_critical_path" in identity_tuple
    assert "consensus_window_shard_transaction_count" not in identity_tuple
    assert "MBE_METATRACK_COMPLETE_WINDOW_OBS_V672" in source
