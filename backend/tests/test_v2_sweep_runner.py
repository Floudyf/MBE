from pathlib import Path

from backend.app.services import sweep_runner_v2
from backend.app.services.sweep_runner_v2 import SWEEP_CONFIGS, expand_sweep_cases, load_sweep_config, run_sweep_case


def test_v2_sweep_configs_exist_and_load() -> None:
    assert set(SWEEP_CONFIGS) == {
        "v2_baseline_sweep",
        "v2_chain_speed_imbalance_sweep",
        "v2_protocol_baseline_sweep",
        "v2_window_size_sweep",
        "v2_committee_delay_sweep",
    }
    for path in SWEEP_CONFIGS.values():
        assert path.is_file()
        config = load_sweep_config(path)
        assert config["stage"] == "v2.8"
        assert config["sweep"]["runnable"] is True
        assert config["sweep"]["data_truth_label"] == "synthetic_replay"
        assert config["sweep"]["backend_type"] == "local_virtual"
        assert config["sweep"]["protocol_truth"] == "local_baseline_model"
        assert config["runner"]["sleep_enabled"] is False


def test_expand_sweep_cases_have_stable_identity_and_truth() -> None:
    config = load_sweep_config(SWEEP_CONFIGS["v2_baseline_sweep"])
    cases = expand_sweep_cases(config)

    assert cases[0]["case_id"] == "case_000001"
    assert cases[0]["case_type"] == "dual_chain_replay"
    assert {case["protocol_name"] for case in cases} >= {"dual_chain_sample", "lock_mint_serial", "lock_mint_pipeline", "fixed_window_baseline", "committee_bridge_basic"}
    for case in cases:
        assert case["sweep_id"] == "v2_baseline_sweep"
        assert case["data_truth_label"] == "synthetic_replay"
        assert case["backend_type"] == "local_virtual"
        assert case["protocol_truth"] == "local_baseline_model"


def test_chain_speed_imbalance_sweep_expands_multiple_target_profiles() -> None:
    config = load_sweep_config(SWEEP_CONFIGS["v2_chain_speed_imbalance_sweep"])
    cases = expand_sweep_cases(config)

    assert {case["target_block_interval_ms"] for case in cases} == {100, 200, 300, 500}
    assert {case["target_finality_depth"] for case in cases} == {3, 5}
    assert {case["protocol_name"] for case in cases} == {"lock_mint_serial", "lock_mint_pipeline"}


def test_protocol_window_and_committee_sweeps_expand_parameters() -> None:
    protocol_cases = expand_sweep_cases(load_sweep_config(SWEEP_CONFIGS["v2_protocol_baseline_sweep"]))
    window_cases = expand_sweep_cases(load_sweep_config(SWEEP_CONFIGS["v2_window_size_sweep"]))
    committee_cases = expand_sweep_cases(load_sweep_config(SWEEP_CONFIGS["v2_committee_delay_sweep"]))

    assert {case["protocol_name"] for case in protocol_cases} == {"lock_mint_serial", "lock_mint_pipeline", "fixed_window_baseline", "committee_bridge_basic"}
    assert {case["window_size"] for case in window_cases} == {1, 2, 4, 8}
    assert {case["committee_delay_ms"] for case in committee_cases} == {0, 50, 100, 200, 500}


def test_run_sweep_case_reuses_existing_v25_and_v26_runners(tmp_path: Path, monkeypatch) -> None:
    calls = {"dual": 0, "protocol": 0}

    def fake_dual(_config_path: Path, _output_dir: Path, _root: Path) -> dict:
        calls["dual"] += 1
        return {
            "summary": {
                "cross_tx_count": 2,
                "stage_record_count": 12,
                "completed_cross_tx_count": 1,
                "timeout_cross_tx_count": 1,
                "refunded_cross_tx_count": 1,
                "failed_cross_tx_count": 0,
                "avg_e2e_latency_ms": 100,
                "p99_e2e_latency_ms": 200,
                "avg_stage_latency_ms": 20,
                "source_wait_time_ms": 30,
                "target_wait_time_ms": 40,
                "finality_wait_time_ms": 70,
                "chain_speed_imbalance": 3,
            }
        }

    def fake_protocol(_config_path: Path, _output_dir: Path, _root: Path) -> dict:
        calls["protocol"] += 1
        return {
            "summary": {
                "items": [
                    {
                        "protocol_name": "lock_mint_serial",
                        "cross_tx_count": 2,
                        "success_count": 1,
                        "timeout_count": 1,
                        "refund_count": 1,
                        "failed_count": 0,
                        "avg_e2e_latency_ms": 100,
                        "p99_e2e_latency_ms": 200,
                        "avg_source_wait_time_ms": 10,
                        "avg_target_wait_time_ms": 20,
                        "avg_finality_wait_time_ms": 30,
                        "max_pending_count": 1,
                        "avg_pending_count": 0.5,
                        "chain_speed_imbalance": 3,
                    }
                ]
            }
        }

    monkeypatch.setattr(sweep_runner_v2, "run_dual_chain_replay", fake_dual)
    monkeypatch.setattr(sweep_runner_v2, "run_protocol_replay", fake_protocol)

    config = load_sweep_config(SWEEP_CONFIGS["v2_baseline_sweep"])
    cases = expand_sweep_cases(config)
    dual_row = run_sweep_case(cases[0], config, tmp_path)
    protocol_row = run_sweep_case(next(case for case in cases if case["protocol_name"] == "lock_mint_serial"), config, tmp_path)

    assert calls == {"dual": 1, "protocol": 1}
    assert dual_row["case_type"] == "dual_chain_replay"
    assert protocol_row["case_type"] == "protocol_baseline"
    assert protocol_row["avg_e2e_latency_ms"] == 100
