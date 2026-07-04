from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


METAVERSE_TRUTH = "controlled_metaverse_workload_not_real_platform_trace"
PAPER_TRUTH = "paper_export_data_scaffold_not_paper_grade_conclusion"
SCENARIOS = [
    "asset_transfer",
    "avatar_update",
    "scene_hotspot",
    "item_transfer",
    "cross_scene_migration",
    "onchain_offchain_confirmation",
    "cross_metaverse_transfer",
    "mixed_metaverse",
]
MIXED_SCENARIOS = [
    "asset_transfer",
    "avatar_update",
    "scene_hotspot",
    "cross_scene_migration",
    "onchain_offchain_confirmation",
]
BASELINE_ROWS = [
    ("single_chain", "Single-chain local runtime", "implemented_local", True, False, False, False, False, "single-chain local control path", "implemented_local_controlled"),
    ("hash_sharding", "Hash sharding", "implemented_local", True, False, False, False, False, "hash-based shard placement", "implemented_local_controlled"),
    ("relay_preview", "Relay preview", "implemented_local", True, False, False, False, False, "cross-shard relay preview observability", "preview_not_atomic_commit"),
    ("relay_mvp", "Relay MVP", "implemented_local", True, True, False, False, False, "source lock/certificate/target commit/refund observability", "relay_mvp_not_production_atomic_commit"),
    ("state_auth_disabled", "State authenticity disabled", "implemented_local", True, False, False, False, False, "state proof/witness disabled control", "implemented_local_controlled"),
    ("prefetch_disabled", "Prefetch disabled", "implemented_local", True, False, False, False, False, "access-list prefetch disabled control", "implemented_local_controlled"),
    ("local_multi_process_dry_run", "Local multi-process dry run", "implemented_local", True, False, False, False, True, "managed process plan/status without long-running processes", "local_multi_process_runtime_mvp_not_production_cluster"),
    ("Broker", "Broker", "planned_external", False, False, False, False, False, "external baseline placeholder", "not_implemented_in_mbe_v3_13"),
    ("2PC", "2PC", "planned_external", False, False, False, False, False, "external baseline placeholder", "not_implemented_in_mbe_v3_13"),
    ("Monoxide", "Monoxide", "planned_external", False, False, False, False, False, "external baseline placeholder", "not_implemented_in_mbe_v3_13"),
    ("BlockEmulator", "BlockEmulator", "planned_external", False, False, False, False, False, "external backend placeholder", "not_implemented_in_mbe_v3_13"),
    ("Fabric", "Fabric", "planned_external", False, False, False, False, False, "Fabric live backend placeholder", "not_implemented_in_mbe_v3_13"),
    ("EVM", "EVM", "planned_external", False, False, False, False, False, "EVM live backend placeholder", "not_implemented_in_mbe_v3_13"),
    ("Porygon", "Porygon", "planned_external", False, False, False, False, False, "paper baseline placeholder", "not_implemented_in_mbe_v3_13"),
    ("Block-STM", "Block-STM", "planned_external", False, False, False, False, False, "parallel execution baseline placeholder", "not_implemented_in_mbe_v3_13"),
]


def maybe_write_metaverse_suite_artifacts(output_dir: Path, topology: dict[str, Any], summary: dict[str, Any]) -> dict[str, Any]:
    enabled = bool(topology.get("metaverse_suite_enabled", False))
    if not enabled:
        return {
            "metaverse_suite_enabled": False,
            "metaverse_experiment_truth": METAVERSE_TRUTH,
            "paper_table_available": False,
            "paper_figure_data_available": False,
        }

    config = _config(topology)
    rows = _generate_rows(config)
    scenario_counts = Counter(str(row["scenario"]) for row in rows)
    cross_scene_rows = [row for row in rows if row["is_cross_scene"]]
    offchain_rows = [row for row in rows if row["requires_offchain_confirmation"]]
    cross_metaverse_rows = [row for row in rows if row["requires_relay"]]
    cross_shard_count = sum(1 for row in rows if row["is_cross_shard"])
    burst_count = sum(1 for idx in range(config["tx_count"]) if _ratio_hit(idx, config["seed"] + 17, config["burst_rate"]))
    offchain_failure_count = sum(1 for row in offchain_rows if row["offchain_expected_status"] == "failed")
    hotspot_counts = Counter()
    for row in rows:
        for key in str(row["access_keys"]).split("|"):
            if key:
                hotspot_counts[key] += 1

    _write_json(output_dir / "metaverse_workload_catalog.json", _catalog())
    _write_json(output_dir / "metaverse_workload_config.json", config)
    _write_json(output_dir / "metaverse_trace_meta.json", {
        "schema_version": "v3.13",
        "trace_fields": list(rows[0].keys()) if rows else [],
        "scenario": config["metaverse_scenario"],
        "tx_count": config["tx_count"],
        "seed": config["seed"],
        "truth": METAVERSE_TRUTH,
        "boundary": "deterministic synthetic metaverse-style workload metadata, not real platform trace collection",
    })
    _write_scenario_summary(output_dir / "scenario_summary.csv", rows, scenario_counts)
    _write_hotspot_distribution(output_dir / "hotspot_distribution.csv", hotspot_counts)
    _write_transfer_log(output_dir / "cross_scene_transfer_log.csv", cross_scene_rows)
    _write_offchain_log(output_dir / "offchain_confirmation_log.csv", offchain_rows)
    _write_cross_metaverse_log(output_dir / "cross_metaverse_transfer_log.csv", cross_metaverse_rows)
    _write_json(output_dir / "metaverse_experiment_summary.json", {
        **_summary_metrics(config, rows, cross_scene_rows, cross_shard_count, burst_count, offchain_rows, offchain_failure_count, cross_metaverse_rows),
        "scenario_counts": dict(sorted(scenario_counts.items())),
        "truth_boundary": METAVERSE_TRUTH,
    })

    baseline_rows = _baseline_rows()
    if config["baseline_matrix_enabled"] or config["benchmark_suite_enabled"] or config["paper_export_enabled"]:
        _write_csv(output_dir / "baseline_matrix.csv", baseline_rows, [
            "baseline_id", "baseline_name", "baseline_type", "runnable", "uses_relay_mvp", "uses_state_authenticity", "uses_prefetch", "uses_local_multi_process", "description", "truth", "reason",
        ])
    else:
        baseline_rows = []

    sweep_rows: list[dict[str, Any]] = []
    if config["benchmark_suite_enabled"] or config["multi_seed_enabled"] or config["paper_export_enabled"]:
        sweep_rows = _sweep_rows(config, summary, baseline_rows or _baseline_rows())
        _write_csv(output_dir / "multi_seed_summary.csv", sweep_rows, [
            "experiment_id", "scenario", "baseline_id", "seed", "shard_count", "tx_count", "hotspot_ratio", "cross_scene_ratio", "cross_shard_ratio", "success_count", "failed_count", "avg_latency_ms", "p95_latency_ms", "p99_latency_ms", "throughput_tps", "offchain_confirm_count", "offchain_failure_count", "relay_success_count", "relay_refund_count", "truth",
        ])
        _write_json(output_dir / "benchmark_suite_summary.json", {
            "benchmark_suite_enabled": config["benchmark_suite_enabled"],
            "baseline_matrix_enabled": config["baseline_matrix_enabled"],
            "multi_seed_enabled": config["multi_seed_enabled"],
            "seed_count": config["sweep_seed_count"],
            "sweep_row_count": len(sweep_rows),
            "baseline_count": len({row["baseline_id"] for row in sweep_rows}),
            "truth": METAVERSE_TRUTH,
            "notes": "Controlled deterministic sweep scaffold only; not paper-grade benchmark evidence.",
        })

    paper_available = False
    if config["paper_export_enabled"]:
        paper_available = True
        _write_paper_exports(output_dir, config, summary, sweep_rows, rows, cross_scene_rows, offchain_rows, cross_metaverse_rows)

    metrics = _summary_metrics(config, rows, cross_scene_rows, cross_shard_count, burst_count, offchain_rows, offchain_failure_count, cross_metaverse_rows)
    metrics.update({
        "baseline_matrix_enabled": config["baseline_matrix_enabled"],
        "baseline_count": len(baseline_rows),
        "multi_seed_enabled": config["multi_seed_enabled"],
        "seed_count": config["sweep_seed_count"] if (config["multi_seed_enabled"] or config["benchmark_suite_enabled"]) else 0,
        "paper_export_enabled": config["paper_export_enabled"],
        "paper_table_available": paper_available,
        "paper_figure_data_available": paper_available,
        "metaverse_experiment_truth": METAVERSE_TRUTH,
    })
    return metrics


def _config(topology: dict[str, Any]) -> dict[str, Any]:
    return {
        "metaverse_suite_enabled": True,
        "metaverse_scenario": str(topology.get("metaverse_scenario", "mixed_metaverse")),
        "user_count": int(topology.get("user_count", 100)),
        "asset_count": int(topology.get("asset_count", 1000)),
        "item_count": int(topology.get("item_count", 1000)),
        "avatar_count": int(topology.get("avatar_count", 100)),
        "scene_count": int(topology.get("scene_count", 16)),
        "metaverse_count": int(topology.get("metaverse_count", 2)),
        "tx_count": int(topology.get("tx_count", 10000)),
        "seed": int(topology.get("seed", 42)),
        "shard_count": int(topology.get("shard_count", 4)),
        "hotspot_ratio": float(topology.get("hotspot_ratio", 0.2)),
        "cross_scene_ratio": float(topology.get("cross_scene_ratio", 0.15)),
        "cross_shard_ratio": float(topology.get("cross_shard_ratio", 0.2)),
        "burst_rate": float(topology.get("burst_rate", 0.0)),
        "read_write_ratio": float(topology.get("read_write_ratio", 0.3)),
        "asset_skew": float(topology.get("asset_skew", 0.2)),
        "scene_skew": float(topology.get("scene_skew", 0.2)),
        "offchain_confirmation_enabled": bool(topology.get("offchain_confirmation_enabled", True)),
        "offchain_confirm_delay_ms": int(topology.get("offchain_confirm_delay_ms", 100)),
        "offchain_failure_ratio": float(topology.get("offchain_failure_ratio", 0.0)),
        "cross_metaverse_enabled": bool(topology.get("cross_metaverse_enabled", True)),
        "benchmark_suite_enabled": bool(topology.get("benchmark_suite_enabled", False)),
        "baseline_matrix_enabled": bool(topology.get("baseline_matrix_enabled", False)),
        "multi_seed_enabled": bool(topology.get("multi_seed_enabled", False)),
        "paper_export_enabled": bool(topology.get("paper_export_enabled", False)),
        "sweep_seed_count": int(topology.get("sweep_seed_count", 3)),
        "sweep_shard_counts": list(topology.get("sweep_shard_counts", [1, 2, 4])),
        "sweep_cross_shard_ratios": list(topology.get("sweep_cross_shard_ratios", [0.0, 0.2, 0.5])),
        "sweep_hotspot_ratios": list(topology.get("sweep_hotspot_ratios", [0.0, 0.2, 0.5])),
    }


def _generate_rows(config: dict[str, Any]) -> list[dict[str, Any]]:
    scenario = config["metaverse_scenario"]
    active_scenarios = MIXED_SCENARIOS + (["cross_metaverse_transfer"] if config["cross_metaverse_enabled"] else []) if scenario == "mixed_metaverse" else [scenario]
    rows = []
    for idx in range(config["tx_count"]):
        current = active_scenarios[idx % len(active_scenarios)]
        user_id = _id("user", idx + config["seed"], config["user_count"])
        target_user_id = _id("user", idx * 7 + config["seed"] + 3, config["user_count"])
        source_scene = _scene(idx, config)
        target_scene = _scene(idx + 5, config)
        is_cross_scene = current in {"cross_scene_migration", "cross_metaverse_transfer"} or _ratio_hit(idx, config["seed"], config["cross_scene_ratio"])
        if not is_cross_scene:
            target_scene = source_scene
        source_shard = _shard(source_scene, config["shard_count"])
        target_shard = _shard(target_scene, config["shard_count"])
        forced_cross = _ratio_hit(idx, config["seed"] + 5, config["cross_shard_ratio"])
        is_cross_shard = source_shard != target_shard or forced_cross
        requires_offchain = config["offchain_confirmation_enabled"] and current == "onchain_offchain_confirmation"
        requires_relay = config["cross_metaverse_enabled"] and current == "cross_metaverse_transfer"
        offchain_failed = requires_offchain and _ratio_hit(idx, config["seed"] + 11, config["offchain_failure_ratio"])
        asset_id = _id("asset", idx * 3 + config["seed"], config["asset_count"])
        item_id = _id("item", idx * 5 + config["seed"], max(1, config["item_count"]))
        avatar_id = _id("avatar", idx * 2 + config["seed"], config["avatar_count"])
        write_keys = _write_keys(current, user_id, target_user_id, asset_id, item_id, avatar_id, source_scene, target_scene)
        read_keys = _read_keys(current, asset_id, item_id, source_scene)
        rows.append({
            "tx_id": f"mtx{idx:08d}",
            "timestamp_ms": idx * 10,
            "scenario": current,
            "user_id": user_id,
            "source_user_id": user_id,
            "target_user_id": target_user_id,
            "asset_id": asset_id,
            "item_id": item_id if current in {"item_transfer", "cross_metaverse_transfer", "mixed_metaverse"} else "",
            "avatar_id": avatar_id if current == "avatar_update" else "",
            "source_scene_id": source_scene,
            "target_scene_id": target_scene if is_cross_scene else "",
            "source_metaverse_id": _id("metaverse", idx + config["seed"], config["metaverse_count"]),
            "target_metaverse_id": _id("metaverse", idx + config["seed"] + 1, config["metaverse_count"]) if requires_relay else "",
            "access_keys": "|".join(read_keys + write_keys),
            "read_keys": "|".join(read_keys),
            "write_keys": "|".join(write_keys),
            "is_cross_scene": is_cross_scene,
            "is_cross_shard": is_cross_shard,
            "requires_offchain_confirmation": requires_offchain,
            "offchain_confirm_delay_ms": config["offchain_confirm_delay_ms"] if requires_offchain else 0,
            "offchain_expected_status": "failed" if offchain_failed else ("confirmed" if requires_offchain else ""),
            "requires_relay": requires_relay,
            "seed": config["seed"],
        })
    return rows


def _write_keys(scenario: str, user: str, target: str, asset: str, item: str, avatar: str, source_scene: str, target_scene: str) -> list[str]:
    if scenario == "avatar_update":
        return [f"avatar:{avatar}:state", f"scene:{source_scene}:presence"]
    if scenario == "item_transfer":
        return [f"item:{item}:owner", f"inventory:{user}", f"inventory:{target}"]
    if scenario == "cross_scene_migration":
        return [f"scene:{source_scene}:presence", f"scene:{target_scene}:presence", f"user:{user}:scene"]
    if scenario == "onchain_offchain_confirmation":
        return [f"asset:{asset}:confirmation", f"user:{user}:pending_confirmations"]
    if scenario == "cross_metaverse_transfer":
        return [f"asset:{asset}:owner", f"relay:{source_scene}:{target_scene}", f"inventory:{target}"]
    if scenario == "scene_hotspot":
        return [f"scene:{source_scene}:hot_state"]
    return [f"asset:{asset}:owner", f"inventory:{user}", f"inventory:{target}"]


def _read_keys(scenario: str, asset: str, item: str, scene: str) -> list[str]:
    if scenario == "item_transfer":
        return [f"item:{item}:metadata"]
    if scenario == "scene_hotspot":
        return [f"scene:{scene}:metadata", f"asset:{asset}:state"]
    return [f"asset:{asset}:metadata"]


def _catalog() -> dict[str, Any]:
    return {
        "stage": "V3.13 Metaverse Experiment Suite Closure",
        "truth": METAVERSE_TRUTH,
        "scenarios": [
            {"scenario": scenario, "status": "controlled_synthetic", "runnable": True, "not_real_platform_trace": True}
            for scenario in SCENARIOS
        ],
    }


def _summary_metrics(config: dict[str, Any], rows: list[dict[str, Any]], cross_scene_rows: list[dict[str, Any]], cross_shard_count: int, burst_count: int, offchain_rows: list[dict[str, Any]], offchain_failure_count: int, cross_metaverse_rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "metaverse_suite_enabled": True,
        "metaverse_scenario_selected": config["metaverse_scenario"],
        "metaverse_tx_count": len(rows),
        "metaverse_user_count": config["user_count"],
        "metaverse_asset_count": config["asset_count"],
        "metaverse_item_count": config["item_count"],
        "metaverse_avatar_count": config["avatar_count"],
        "metaverse_scene_count": config["scene_count"],
        "metaverse_count": config["metaverse_count"],
        "metaverse_hotspot_ratio": config["hotspot_ratio"],
        "metaverse_cross_scene_ratio": config["cross_scene_ratio"],
        "metaverse_cross_shard_ratio": config["cross_shard_ratio"],
        "metaverse_cross_scene_count": len(cross_scene_rows),
        "metaverse_cross_shard_count": cross_shard_count,
        "metaverse_burst_count": burst_count,
        "metaverse_offchain_confirmation_count": len(offchain_rows),
        "metaverse_offchain_failure_count": offchain_failure_count,
        "metaverse_cross_metaverse_count": len(cross_metaverse_rows),
    }


def _baseline_rows() -> list[dict[str, Any]]:
    rows = []
    for baseline_id, name, baseline_type, runnable, relay, state_auth, prefetch, local_mp, description, truth in BASELINE_ROWS:
        rows.append({
            "baseline_id": baseline_id,
            "baseline_name": name,
            "baseline_type": baseline_type,
            "runnable": runnable,
            "uses_relay_mvp": relay,
            "uses_state_authenticity": state_auth,
            "uses_prefetch": prefetch,
            "uses_local_multi_process": local_mp,
            "description": description,
            "truth": truth,
            "reason": "" if runnable else "not_implemented_in_mbe_v3_13",
        })
    return rows


def _sweep_rows(config: dict[str, Any], summary: dict[str, Any], baselines: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    runnable = [row for row in baselines if row.get("runnable") in {True, "True", "true"}]
    base_success = int(summary.get("success_count") or summary.get("tx_count") or min(config["tx_count"], 24))
    for seed_idx in range(config["sweep_seed_count"]):
        seed = config["seed"] + seed_idx
        for shard_count in config["sweep_shard_counts"]:
            for cross_ratio in config["sweep_cross_shard_ratios"]:
                for hotspot_ratio in config["sweep_hotspot_ratios"]:
                    for baseline in runnable:
                        latency = 10.0 + float(cross_ratio) * 12.0 + float(hotspot_ratio) * 5.0 + int(shard_count) * 0.5 + seed_idx
                        rows.append({
                            "experiment_id": f"v313_{seed}_{shard_count}_{cross_ratio}_{hotspot_ratio}_{baseline['baseline_id']}",
                            "scenario": config["metaverse_scenario"],
                            "baseline_id": baseline["baseline_id"],
                            "seed": seed,
                            "shard_count": shard_count,
                            "tx_count": config["tx_count"],
                            "hotspot_ratio": hotspot_ratio,
                            "cross_scene_ratio": config["cross_scene_ratio"],
                            "cross_shard_ratio": cross_ratio,
                            "success_count": base_success,
                            "failed_count": 0,
                            "avg_latency_ms": round(latency, 3),
                            "p95_latency_ms": round(latency * 1.4, 3),
                            "p99_latency_ms": round(latency * 1.8, 3),
                            "throughput_tps": round(max(1.0, base_success * 1000.0 / max(1.0, latency * base_success)), 3),
                            "offchain_confirm_count": int(config["tx_count"] * 0.2) if config["offchain_confirmation_enabled"] else 0,
                            "offchain_failure_count": int(config["tx_count"] * config["offchain_failure_ratio"]),
                            "relay_success_count": int(config["tx_count"] * 0.1) if baseline["baseline_id"] == "relay_mvp" else 0,
                            "relay_refund_count": 0,
                            "truth": METAVERSE_TRUTH,
                        })
    return rows


def _write_paper_exports(output_dir: Path, config: dict[str, Any], summary: dict[str, Any], sweep_rows: list[dict[str, Any]], rows: list[dict[str, Any]], cross_scene_rows: list[dict[str, Any]], offchain_rows: list[dict[str, Any]], cross_metaverse_rows: list[dict[str, Any]]) -> None:
    source_rows = sweep_rows or [{
        "scenario": config["metaverse_scenario"],
        "baseline_id": "current_run",
        "seed": config["seed"],
        "shard_count": config["shard_count"],
        "tx_count": len(rows),
        "hotspot_ratio": config["hotspot_ratio"],
        "cross_scene_ratio": config["cross_scene_ratio"],
        "cross_shard_ratio": config["cross_shard_ratio"],
        "avg_latency_ms": summary.get("avg_latency_ms", 0),
        "p95_latency_ms": summary.get("p95_latency_ms", 0),
        "p99_latency_ms": summary.get("p99_latency_ms", 0),
        "throughput_tps": summary.get("throughput_tps", 0),
        "truth": METAVERSE_TRUTH,
    }]
    _write_csv(output_dir / "paper_table_latency.csv", source_rows, ["scenario", "baseline_id", "seed", "shard_count", "avg_latency_ms", "p95_latency_ms", "p99_latency_ms", "truth"])
    _write_csv(output_dir / "paper_table_throughput.csv", source_rows, ["scenario", "baseline_id", "seed", "shard_count", "tx_count", "throughput_tps", "truth"])
    _write_csv(output_dir / "paper_table_cross_shard.csv", [{
        "scenario": config["metaverse_scenario"],
        "cross_scene_count": len(cross_scene_rows),
        "cross_shard_count": sum(1 for row in rows if row["is_cross_shard"]),
        "cross_metaverse_count": len(cross_metaverse_rows),
        "truth": METAVERSE_TRUTH,
    }], ["scenario", "cross_scene_count", "cross_shard_count", "cross_metaverse_count", "truth"])
    _write_csv(output_dir / "paper_table_offchain_confirmation.csv", [{
        "scenario": config["metaverse_scenario"],
        "offchain_confirmation_count": len(offchain_rows),
        "offchain_failure_count": sum(1 for row in offchain_rows if row["offchain_expected_status"] == "failed"),
        "offchain_confirm_delay_ms": config["offchain_confirm_delay_ms"],
        "truth": METAVERSE_TRUTH,
    }], ["scenario", "offchain_confirmation_count", "offchain_failure_count", "offchain_confirm_delay_ms", "truth"])
    _write_csv(output_dir / "paper_figure_data.csv", source_rows, ["scenario", "baseline_id", "seed", "shard_count", "hotspot_ratio", "cross_shard_ratio", "avg_latency_ms", "throughput_tps", "truth"])
    _write_json(output_dir / "paper_export_manifest.json", {
        "paper_export_enabled": True,
        "generated_tables": ["paper_table_latency.csv", "paper_table_throughput.csv", "paper_table_cross_shard.csv", "paper_table_offchain_confirmation.csv"],
        "generated_figures": ["paper_figure_data.csv"],
        "source_artifacts": ["metaverse_experiment_summary.json", "multi_seed_summary.csv"],
        "truth": PAPER_TRUTH,
        "notes": "Exported tables are scaffold data from controlled local synthetic workloads and do not assert paper-grade performance conclusions.",
    })


def _write_scenario_summary(path: Path, rows: list[dict[str, Any]], scenario_counts: Counter[str]) -> None:
    grouped: dict[str, dict[str, int]] = defaultdict(lambda: {"count": 0, "cross_scene_count": 0, "cross_shard_count": 0, "offchain_confirmation_count": 0, "cross_metaverse_count": 0})
    for row in rows:
        item = grouped[str(row["scenario"])]
        item["count"] += 1
        item["cross_scene_count"] += int(bool(row["is_cross_scene"]))
        item["cross_shard_count"] += int(bool(row["is_cross_shard"]))
        item["offchain_confirmation_count"] += int(bool(row["requires_offchain_confirmation"]))
        item["cross_metaverse_count"] += int(bool(row["requires_relay"]))
    csv_rows = [{"scenario": scenario, **grouped[scenario], "truth": METAVERSE_TRUTH} for scenario in sorted(scenario_counts)]
    _write_csv(path, csv_rows, ["scenario", "count", "cross_scene_count", "cross_shard_count", "offchain_confirmation_count", "cross_metaverse_count", "truth"])


def _write_hotspot_distribution(path: Path, counts: Counter[str]) -> None:
    rows = [
        {"hotspot_key": key, "scenario": _key_scenario(key), "access_count": count, "hotspot_rank": index + 1, "truth": METAVERSE_TRUTH}
        for index, (key, count) in enumerate(counts.most_common(25))
    ]
    _write_csv(path, rows, ["hotspot_key", "scenario", "access_count", "hotspot_rank", "truth"])


def _write_transfer_log(path: Path, rows: list[dict[str, Any]]) -> None:
    _write_csv(path, rows, ["tx_id", "timestamp_ms", "source_user_id", "source_scene_id", "target_scene_id", "is_cross_shard", "seed"])


def _write_offchain_log(path: Path, rows: list[dict[str, Any]]) -> None:
    _write_csv(path, rows, ["tx_id", "timestamp_ms", "asset_id", "requires_offchain_confirmation", "offchain_confirm_delay_ms", "offchain_expected_status", "seed"])


def _write_cross_metaverse_log(path: Path, rows: list[dict[str, Any]]) -> None:
    _write_csv(path, rows, ["tx_id", "timestamp_ms", "source_metaverse_id", "target_metaverse_id", "asset_id", "item_id", "requires_relay", "is_cross_shard", "seed"])


def _id(prefix: str, value: int, count: int) -> str:
    return f"{prefix}{value % max(1, count)}"


def _scene(value: int, config: dict[str, Any]) -> str:
    hot_count = max(1, int(config["scene_count"] * max(0.01, config["hotspot_ratio"])))
    if _ratio_hit(value, config["seed"] + 23, config["scene_skew"]):
        return _id("scene", value, hot_count)
    return _id("scene", value * 3 + config["seed"], config["scene_count"])


def _shard(scene_id: str, shard_count: int) -> int:
    digits = "".join(char for char in scene_id if char.isdigit())
    return int(digits or 0) % max(1, shard_count)


def _ratio_hit(index: int, seed: int, ratio: float) -> bool:
    if ratio <= 0:
        return False
    if ratio >= 1:
        return True
    return ((index * 1103515245 + seed * 12345) & 0x7FFFFFFF) % 10000 < int(ratio * 10000)


def _key_scenario(key: str) -> str:
    if key.startswith("scene:"):
        return "scene_hotspot"
    if key.startswith("avatar:"):
        return "avatar_update"
    if key.startswith("item:"):
        return "item_transfer"
    if key.startswith("relay:"):
        return "cross_metaverse_transfer"
    return "asset_transfer"


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: _csv_value(row.get(field, "")) for field in fieldnames})


def _csv_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (list, tuple, set)):
        return "|".join(str(item) for item in value)
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)
