# MBE_V5_RESULTS_UI_TRUTH_CN_FINAL_20260814_V5
from pathlib import Path


def test_v5_results_dashboard_truth_fallback_evidence_and_cn_contract() -> None:
    root = Path(__file__).resolve().parents[2]
    dashboard = (root / "frontend/src/components/v5/V5ResultsDashboard.tsx").read_text(encoding="utf-8")
    evidence = (root / "frontend/src/components/v5/V5EvidencePanel.tsx").read_text(encoding="utf-8")
    child = (root / "frontend/src/components/v5/V5ChildDetail.tsx").read_text(encoding="utf-8")
    resource = (root / "frontend/src/components/v5/V5ResourceNetworkPanel.tsx").read_text(encoding="utf-8")
    mechanism = (root / "frontend/src/components/v5/V5MechanismAnalysis.tsx").read_text(encoding="utf-8")
    analysis = (root / "frontend/src/components/v5/V5AnalysisPanel.tsx").read_text(encoding="utf-8")
    skew = (root / "frontend/src/components/v5/V5SkewTpsChart.tsx").read_text(encoding="utf-8")
    api = (root / "frontend/src/api.ts").read_text(encoding="utf-8")
    dto = (root / "backend/app/services/v5_formal_dto.py").read_text(encoding="utf-8")

    for label in (
        "① 实验总览",
        "② 性能与稳定性",
        "③ 资源与通信",
        "④ 机制分析",
        "⑤ 子实验",
        "⑥ 证据与产物",
        "直接跨语义性能比较：受限",
        "直接跨语义性能比较：未判定",
        "工作负载真实性",
        "Student-t 95% 置信区间",
    ):
        assert label in dashboard

    # The real 8-method x 10K acceptance run exposed that group/child truth can
    # arrive from multiple public DTO locations.  Explicit false must win over
    # a missing field so the UI can never silently show "允许".
    for token in (
        "directComparisonStatus(group, analysis)",
        "analysis?.paper_result_analysis?.performance_comparison_valid",
        "group.fairness_validation?.direct_cross_semantic_performance_comparison_valid",
        "group.state_equivalence_validation?.performance_comparison_valid",
        "semanticClassForChild(group, child)",
        "group.state_equivalence_validation?.cohorts",
        "child_run_ids",
    ):
        assert token in dashboard

    # Evidence tab must combine RunGroup paper artifacts with indexed child-run
    # evidence.  The small RunGroup ZIP is labeled as such and cannot be
    # mistaken for all multi-GiB raw runtime artifacts.
    for token in (
        "children.flatMap",
        "child.evidence_artifacts",
        "runtime_artifact_count",
        "runtime_artifact_bytes",
        "实验组（RunGroup）汇总证据包",
        "子实验原始产物总量",
        "网络证据",
        "资源证据",
        "机制证据",
    ):
        assert token in evidence

    # Human-facing child-result labels are Chinese while machine keys remain in
    # source/artifact metadata for auditability.
    for label in (
        "工作负载真实性",
        "工作负载插件",
        "来源类型",
        "数据集 ID",
        "工作负载变体",
        "真实性标签",
        "请求 / 实际交易数",
        "期望跨分片",
        "实际跨分片",
        "运行时产物",
    ):
        assert label in child

    for token in ("ACG/Nezha", "BSX", "Block-STM", "Batch-SI"):
        assert token in dashboard
        assert token in resource or token in mechanism
    assert 'hash_serial: ["Serial"]' in analysis
    assert 'hash_acg: ["ACG/Nezha"]' in analysis
    assert "shortMethodName(methodId" in skew

    for token in ("evidence_artifacts?:", "runtime_artifact_count?:", "runtime_artifact_bytes?:"):
        assert token in api

    for token in (
        '"comparison_semantics_class"',
        '"direct_cross_semantic_performance_comparison_valid"',
        '"state_equivalence_validation"',
        'body["evidence_artifacts"]',
        'body["runtime_artifact_count"]',
        'body["runtime_artifact_bytes"]',
    ):
        assert token in dto


def test_v5_cross_method_serial_oracle_ui_and_dto_contract() -> None:
    root = Path(__file__).resolve().parents[2]
    dashboard = (root / "frontend/src/components/v5/V5ResultsDashboard.tsx").read_text(encoding="utf-8")
    group_summary = (root / "frontend/src/components/v5/V5GroupSummary.tsx").read_text(encoding="utf-8")
    api = (root / "frontend/src/api.ts").read_text(encoding="utf-8")
    dto = (root / "backend/app/services/v5_formal_dto.py").read_text(encoding="utf-8")

    for token in (
        "Serial Oracle",
        "不同合法串行化顺序不要求产生相同最终 state digest",
        "cross_method_serial_order_oracle_valid",
    ):
        assert token in dashboard
    assert "Serial Oracle" in group_summary
    assert "cross_method_serial_order_oracle_valid" in group_summary
    for token in (
        "cross_method_serial_order_oracle_valid?:",
        "serial_order_replay_equivalent?:",
        "serial_order_replay_input_digest?:",
    ):
        assert token in api
    for token in (
        '"cross_method_serial_order_oracle_valid"',
        '"serial_order_replay_equivalent"',
        '"serial_order_replay_input_digest"',
    ):
        assert token in dto
