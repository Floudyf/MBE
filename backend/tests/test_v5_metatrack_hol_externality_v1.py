from __future__ import annotations

import csv
import json
from pathlib import Path

from backend.app.services.v5_metatrack_hol_externality import (
    _attempt_rows,
    _paired_counterfactual_structure,
    _representative_nodes,
    analyze_run,
)


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _seed_run(root: Path, *, exact: bool) -> None:
    (root / "real_cluster_summary.json").write_text(
        json.dumps({"node_summaries": [{"node_id": "n0", "shard_id": "s0"}]}),
        encoding="utf-8",
    )
    node = root / "nodes" / "n0"

    structure_fields = [
        "timestamp","node_id","shard_id","height","block_hash","tx_id",
        "routing_ordinal","route_batch_sequence","raw_producer_ids","raw_producer_count",
        "legacy_frontier_ids","legacy_frontier_width","matrix_frontier_ids",
        "matrix_frontier_width","frontier_width_equal","frontier_ids_equal",
        "dependency_depth_l","tail_depth_h","descendant_count_d","direct_child_count",
        "critical_path","critical_path_length","current_track","current_reason",
        "matrix_analysis_valid","matrix_analysis_error","matrix_analysis_us",
    ]
    widths = {"m": 2, "f1": 1, "f2": 1, "tail": 2}
    structure = []
    for ordinal, tx in enumerate(["m", "f1", "f2", "tail"], start=1):
        structure.append({
            "timestamp":"1","node_id":"n0","shard_id":"s0","height":"1","block_hash":"b1",
            "tx_id":tx,"routing_ordinal":str(ordinal),"route_batch_sequence":"1",
            "raw_producer_ids":"","raw_producer_count":"0",
            "legacy_frontier_ids":"","legacy_frontier_width":str(widths[tx]),
            "matrix_frontier_ids":"","matrix_frontier_width":str(widths[tx]),
            "frontier_width_equal":"true","frontier_ids_equal":"true",
            "dependency_depth_l":"1","tail_depth_h":"2" if tx=="m" else "1",
            "descendant_count_d":"3" if tx=="m" else "0","direct_child_count":"0",
            "critical_path":"true" if tx=="m" else "false","critical_path_length":"2",
            "current_track":"conservative" if widths[tx]>=2 else "fast",
            "current_reason":"","matrix_analysis_valid":"true","matrix_analysis_error":"",
            "matrix_analysis_us":"1",
        })
    _write_csv(node / "metatrack_dependency_structure.csv", structure_fields, structure)

    base_fields = [
        "node_id","shard_id","block_height","block_hash","tx_id","track","attempt",
        "reason","success","final_completion","duration_us","duration_ns","sojourn_ns",
        "state_wait_ns","dependency_wait_ns","queue_wait_ns",
        "attempt_start_offset_ns","attempt_end_offset_ns",
    ]
    if exact:
        base_fields += [
            "fifo_ready_offset_ns","fifo_release_offset_ns",
            "fifo_ready_blocked_ns","fifo_initial_head_tx_id",
        ]

    # m becomes ready at 100ms. f1/f2 are already ready at 10/20ms but strict
    # canonical FIFO cannot release them until m reaches the head boundary.
    data = [
        ("m",100_000_000,100_000_000,0,0,""),
        ("f1",10_000_000,100_000_000,90_000_000,90_000_000,"m"),
        ("f2",20_000_000,100_000_000,80_000_000,80_000_000,"m"),
        ("tail",110_000_000,110_000_000,0,0,""),
    ]
    attempts=[]
    for index,(tx,ready,release,hol,qwait,head) in enumerate(data):
        start = ready + qwait
        row={
            "node_id":"n0","shard_id":"s0","block_height":"1","block_hash":"b1",
            "tx_id":tx,"track":"conservative","attempt":"1","reason":"done",
            "success":"true","final_completion":"true","duration_us":"10",
            "duration_ns":"10000","sojourn_ns":str(release+1_000_000),
            "state_wait_ns":"0","dependency_wait_ns":"0","queue_wait_ns":str(qwait),
            "attempt_start_offset_ns":str(start),"attempt_end_offset_ns":str(start+10000),
        }
        if exact:
            row.update({
                "fifo_ready_offset_ns":str(ready),
                "fifo_release_offset_ns":str(release),
                "fifo_ready_blocked_ns":str(hol),
                "fifo_initial_head_tx_id":head,
            })
        attempts.append(row)
    _write_csv(node / "business_execute_invocation_count_by_node.csv", base_fields, attempts)

    trace_fields = [
        "node_id","shard_id","block_hash","height","tx_id","logical_tx_id",
        "original_index","success","error","read_key_count","write_key_count",
        "state_root_after_tx",
    ]
    trace_rows = []
    for index, tx in enumerate(["m", "f1", "f2", "tail"]):
        trace_rows.append({
            "node_id":"n0","shard_id":"s0","block_hash":"b1","height":"1",
            "tx_id":tx,"logical_tx_id":f"logical-{tx}","original_index":str(index),
            "success":"true","error":"","read_key_count":"1","write_key_count":"1",
            "state_root_after_tx":f"r{index}",
        })
    _write_csv(node / "transaction_execution_trace.csv", trace_fields, trace_rows)

    (node / "block_execution_summary.json").write_text(
        json.dumps({"blocks":[{
            "height":1,"block_hash":"b1",
            "metatrack_single_fifo_ready_blocked_sum_ms":170.0,
        }]}),
        encoding="utf-8",
    )


def test_exact_runtime_externality_attributes_multi_head_to_fast_victims(tmp_path: Path):
    _seed_run(tmp_path, exact=True)
    result = analyze_run(tmp_path, "metatrack_ab_track")
    ext = result["fifo_externality"]
    assert ext["evidence_mode"] == "exact_runtime"
    assert ext["fifo_hol_victim_count"] == 2
    assert abs(ext["fifo_hol_reconstructed_ms"] - 170.0) < 1e-9
    assert abs(ext["fifo_hol_exact_runtime_aggregate_ms"] - 170.0) < 1e-9
    assert abs(ext["fifo_hol_calibration_abs_error_pct"]) < 1e-9
    assert ext["multi_frontier_fast_eligible_victim_count"] == 2
    assert abs(ext["multi_frontier_fast_eligible_hol_ms"] - 170.0) < 1e-9
    assert ext["multi_frontier_non_descendant_fast_victim_count"] == 2
    assert ext["multi_frontier_hol_share"] == 1.0
    assert ext["multi_frontier_hol_amplification_vs_tx_share"] == 2.0


def test_legacy_reconstruction_is_calibrated_against_exact_aggregate(tmp_path: Path):
    _seed_run(tmp_path, exact=False)
    result = analyze_run(tmp_path, "metatrack_ab_track")
    ext = result["fifo_externality"]
    assert ext["evidence_mode"] == "legacy_estimated"
    assert abs(ext["fifo_hol_reconstructed_ms"] - 170.0) < 1e-9
    assert abs(ext["fifo_hol_exact_runtime_aggregate_ms"] - 170.0) < 1e-9
    assert abs(ext["fifo_hol_calibration_abs_error_pct"]) < 1e-9
    assert ext["multi_frontier_fast_eligible_victim_count"] == 2


def test_analyzer_does_not_use_truncated_synthetic_scheduler_timestamps():
    source = Path("backend/app/services/v5_metatrack_hol_externality.py").read_text(encoding="utf-8")
    assert '_csv_rows(run_dir, "metatrack_scheduler_trace.csv")' not in source
    assert "business_execute_invocation_count_by_node.csv" in source
    assert "metatrack_dependency_structure.csv" in source
    assert "metatrack_single_fifo_ready_blocked_sum_ms" in source


def test_runtime_exact_fifo_evidence_contract_is_wired():
    registry = Path("executor/v5/registry.go").read_text(encoding="utf-8")
    runtime = Path("executor/v5/runtime.go").read_text(encoding="utf-8")
    for token in (
        "FIFOReadyOffsetNS",
        "FIFOReleaseOffsetNS",
        "FIFOReadyBlockedNS",
        "FIFOInitialHeadTxID",
        "singleFIFOReadyOffsetNSByTx",
        "singleFIFOReleaseOffsetNSByTx",
        "singleFIFOReadyBlockedNSByTx",
        "singleFIFOInitialHeadByTx",
        "MBE_METATRACK_HOL_EXTERNALITY_V1",
    ):
        assert token in registry
    for header in (
        "fifo_ready_offset_ns",
        "fifo_release_offset_ns",
        "fifo_ready_blocked_ns",
        "fifo_initial_head_tx_id",
    ):
        assert header in runtime


def test_historical_no_track_empty_structure_uses_paired_full_logical_identity(tmp_path: Path):
    full = tmp_path / "full"
    no_track = tmp_path / "no_track"
    full.mkdir()
    no_track.mkdir()
    _seed_run(full, exact=False)
    _seed_run(no_track, exact=False)

    # Prove the fallback joins by logical_tx_id rather than assuming physical
    # transaction identifiers happen to be stable across runs.
    business_path = no_track / "nodes" / "n0" / "business_execute_invocation_count_by_node.csv"
    with business_path.open("r", encoding="utf-8", newline="") as handle:
        business_rows = list(csv.DictReader(handle))
        business_fields = list(business_rows[0].keys())
    for row in business_rows:
        row["tx_id"] = "no-" + row["tx_id"]
    _write_csv(business_path, business_fields, business_rows)

    trace_path = no_track / "nodes" / "n0" / "transaction_execution_trace.csv"
    with trace_path.open("r", encoding="utf-8", newline="") as handle:
        trace_rows = list(csv.DictReader(handle))
        trace_fields = list(trace_rows[0].keys())
    for row in trace_rows:
        row["tx_id"] = "no-" + row["tx_id"]
    _write_csv(trace_path, trace_fields, trace_rows)

    # Historical metatrack_single_execution wrote the CSV header but no rows.
    structure_path = no_track / "nodes" / "n0" / "metatrack_dependency_structure.csv"
    header = structure_path.read_text(encoding="utf-8").splitlines()[0]
    structure_path.write_text(header + "\n", encoding="utf-8")

    reps_full = _representative_nodes(full)
    reps_no = _representative_nodes(no_track)
    attempts = _attempt_rows(no_track, reps_no)
    projected, diag = _paired_counterfactual_structure(
        full, no_track, reps_full, reps_no, attempts
    )
    assert len(projected) == 4
    assert diag["logical_mapping_coverage"] == 1.0
    assert diag["same_shard_height_alignment_rate"] == 1.0
    assert diag["raw_predecessor_mapping_coverage"] == 1.0
    assert projected[("s0", 1, "no-m")]["_width"] == 2
    assert projected[("s0", 1, "no-f1")]["_width"] == 1


def test_runtime_v11_captures_structure_for_no_track_without_changing_classifier():
    runtime = Path("executor/v5/runtime.go").read_text(encoding="utf-8")
    assert "MBE_METATRACK_HOL_STRUCTURE_V11" in runtime
    assert 'executionPlugin == metaTrackSingleExecutionID' in runtime
    assert 'executionPlugin == "dual_track_execution"' in runtime
    registry = Path("executor/v5/registry.go").read_text(encoding="utf-8")
    assert 'width 2+: independent value frontiers join -> Conservative' in registry
    assert 'singleSerialDispatchLimit = 1' in registry
