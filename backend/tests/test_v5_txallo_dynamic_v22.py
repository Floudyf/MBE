from __future__ import annotations

import csv
import gzip
import json
from pathlib import Path

from backend.app.services.v5_formal_plan_validator import ALL_BUILTIN_METHODS
from backend.app.services.v5_optme_txallo_fidelity_closure import _txallo_topology_diagnostics
from backend.app.services.v5_plugin_manifest_store import STORE
from backend.app.services.v5_txallo_dynamic_blocks_v222 import compile_txallo_dynamic_blocks


def test_txallo_v222_defaults_use_source_blocks() -> None:
    sharding = STORE.get("txallo_account_sharding")
    assert sharding.default_config["allocation_mode"] == "paper_g_ratio_snapshot"
    assert sharding.default_config["dynamic_a_txallo_runtime"] is True
    assert sharding.default_config["a_epoch_blocks"] == 15
    assert sharding.default_config["g_epoch_multiple"] == 20
    props = sharding.config_schema["properties"]
    assert props["a_epoch_blocks"]["default"] == 15
    assert props["g_epoch_multiple"]["default"] == 20
    for method_id in ("stateful_txallo", "stateless_txallo"):
        cfg = ALL_BUILTIN_METHODS[method_id].plugin_config_overrides["sharding"]
        assert cfg["dynamic_a_txallo_runtime"] is True
        assert cfg["a_epoch_blocks"] == 15
        assert cfg["g_epoch_multiple"] == 20


def test_txallo_v222_fidelity_accepts_block_epoch_accounting() -> None:
    item = {
        "method": {"plugin_config_overrides": {"sharding": {"lambda": 0.0, "epsilon": 0.0}}},
        "topology_point": {"shards": 2},
    }
    metrics = {
        "v16_txallo_diagnostics": {
            "allocation_mode": "paper_g_ratio_snapshot",
            "history_transaction_count": 100,
            "lambda_processing_capacity_per_shard": 550.0,
            "epsilon_convergence_threshold": 0.011,
            "initial_g_lambda": 50.0,
            "initial_g_epsilon": 0.001,
            "eta_cross_shard_workload_multiplier": 2.0,
            "adaptive_run_count": 19,
            "global_run_count": 2,
            "dynamic_a_txallo_runtime_enabled": True,
            "a_epoch_blocks": 15,
            "g_epoch_multiple": 20,
            "closed_source_epoch_count": 20,
            "committed_dynamic_transaction_count": 1000,
            "a_txallo_transaction_count": 950,
            "periodic_g_txallo_run_count": 1,
            "total_allocator_history_transaction_count": 1100,
            "mapping_epoch": 20,
            "pending_or_uncommitted_transactions_used": 0,
        }
    }
    diag = _txallo_topology_diagnostics(item, metrics)
    assert diag["adaptive_history_accounting_consistent"] is True
    assert diag["reconstructed_history_from_g_plus_a_chunks"] == 1100


def test_txallo_v222_runtime_hooks_are_block_and_commit_driven() -> None:
    root = Path(__file__).resolve().parents[2]
    dynamic = (root / "executor/v5/txallo_dynamic_v22.go").read_text(encoding="utf-8")
    client = (root / "executor/v5/client.go").read_text(encoding="utf-8")
    runtime = (root / "executor/v5/runtime.go").read_text(encoding="utf-8")
    core = (root / "executor/v5/txallo_core.go").read_text(encoding="utf-8")
    assert "txalloLoadDynamicBlockIndexV222" in client
    assert "txalloSourceBlockEpoch" in client
    assert "ApplyCommittedTxAlloEpoch" in client
    assert "txalloWaitCommittedEpoch" in client
    assert "txalloWaitMappingAcks" in client
    assert "a_epoch_seconds" not in client
    assert 'required = "sourcefinalize"' in dynamic
    assert 'required := "durable_committed"' in dynamic
    assert "stateful dynamic mapping requires state migration" in dynamic
    assert "startTxAlloMappingWatcher" in runtime
    assert "MBE_TXALLO_DYNAMIC_V222" not in core


def test_txallo_v222_dynamic_block_sidecar_compiler(tmp_path: Path, monkeypatch) -> None:
    import backend.app.services.v5_txallo_dynamic_blocks_v222 as mod

    source = tmp_path / "aw.csv"
    fields = ["transaction_id", "block_num", "global_sequence", "sender_id", "receiver_id"]
    with source.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for i in range(12):
            w.writerow({
                "transaction_id": f"tx{i}",
                "block_num": 1000 + (i // 2),
                "global_sequence": 5000 + i,
                "sender_id": f"p{i}",
                "receiver_id": "m.federation",
            })
    monkeypatch.setattr(mod, "ROOT", tmp_path)
    run = tmp_path / "run"
    manifest = {
        "txallo_history_pool": {
            "source_raw_relative_path": "aw.csv",
            "evaluation_anchor_raw_row_index": 2,
            "evaluation_anchor_transaction_id": "tx2",
        }
    }
    workload_plan = {"actual_tx_count": 5, "tx_count": 5, "selection_mode": "validated_prefix", "variant_mode": "original_window"}
    profile = {"sharding": {"plugin_id": "txallo_account_sharding", "config": {
        "dynamic_a_txallo_runtime": True, "a_epoch_blocks": 15, "g_epoch_multiple": 20,
    }}}
    summary = compile_txallo_dynamic_blocks(run_dir=run, manifest=manifest, workload_plan=workload_plan, profile=profile)
    assert summary is not None
    assert summary["selected_count"] == 5
    assert summary["first_block_num"] == 1001
    assert summary["evaluation_anchor_transaction_id"] == "tx2"
    assert summary["a_epoch_blocks"] == 15
    assert summary["paper_reference_a_epoch_blocks"] == 300
    assert summary["a_epoch_parameterization"] == "mbe_adapted_fixed_15_source_blocks"
    sidecar = run / summary["relative_path"]
    with gzip.open(sidecar, "rt", encoding="utf-8") as f:
        rows = [json.loads(line) for line in f if line.strip()]
    assert [row["materialized_index"] for row in rows] == list(range(5))
    assert [row["raw_source_row_index"] for row in rows] == [2, 3, 4, 5, 6]
