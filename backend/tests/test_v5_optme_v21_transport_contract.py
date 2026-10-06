from pathlib import Path


def _root() -> Path:
    return Path(__file__).resolve().parents[2]


def test_optme_v21_parallelizes_only_method_preserving_transport() -> None:
    root = _root()
    runtime = (root / "executor" / "v5" / "runtime.go").read_text(encoding="utf-8")
    for token in (
        "MBE_OPTME_V21_TRANSPORT_PARALLELISM",
        "optmeMethodPreservingPublicationWaves",
        "publishOptMEMethodPreservingStateVersions",
        "blockExecutorWorkerCountFromProfile(r.pluginSnapshot)",
        "optme_method_preserving_publish_parallel_wave_count",
        "optme_method_preserving_publish_wall_ms",
    ):
        assert token in runtime
    assert "optmeMethodPreservingPublishWorkerCap" not in runtime
    assert "return r.publishMethodPreservingStateVersionsSerial(ctx, block, deltas)" in runtime


def test_optme_v21_corrects_executor_wall_clock_boundary() -> None:
    runtime = (_root() / "executor" / "v5" / "runtime.go").read_text(encoding="utf-8")
    assert "executorReturnedAt := time.Now()" in runtime
    assert "executed.BlockExecutionMS = executorReturnedAt.Sub(executeStarted).Milliseconds()" in runtime
    assert "r.plugins.Routing.ID() == optmeStatelessRoutingID" in runtime


def test_optme_v21_transport_code_is_retained_only_as_superseded_history() -> None:
    root = _root()
    runtime = (root / "executor" / "v5" / "runtime.go").read_text(encoding="utf-8")
    plugins = (root / "executor" / "v5" / "optme_plugins.go").read_text(encoding="utf-8")
    assert "optmeMethodPreservingPublicationWaves" in runtime
    assert "StatelessVersionAdmission() bool { return false }" in plugins
    assert "case txalloStatelessRoutingID:" in runtime
    assert "case optmeStatelessRoutingID, txalloStatelessRoutingID:" not in runtime


def test_optme_v21_does_not_modify_paper_scheduler_core() -> None:
    core = (_root() / "executor" / "v5" / "optme_core.go").read_text(encoding="utf-8")
    assert "MBE_OPTME_V20_PARALLEL_ACG" in core
    assert "MBE_OPTME_V21" not in core


def test_optme_v21_exports_transport_metrics_separately_from_optme_execution() -> None:
    metrics = (_root() / "backend" / "app" / "services" / "v5_metric_extractor.py").read_text(encoding="utf-8")
    for token in (
        "MBE_OPTME_V21_TRANSPORT_METRICS",
        "optme_method_preserving_publish_block_count",
        "optme_method_preserving_publish_wave_count",
        "optme_method_preserving_publish_parallel_wave_count",
        "optme_method_preserving_publish_parallel_tx_count",
        "optme_method_preserving_publish_wall_ms",
        "optme_state_transport_truth_scope",
    ):
        assert token in metrics
