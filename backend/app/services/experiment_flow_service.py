from __future__ import annotations

from backend.app.models.experiment_flow import (
    ExperimentProfile,
    ExperimentRunPlanPreview,
    ExperimentRunPlanRequest,
    ExperimentTopology,
    ExperimentWorkload,
)
from backend.app.models.v4_realism import V4RealismSmokeRequest


PROFILES = {
    "v4_3_realism_default": ExperimentProfile(
        profile_id="v4_3_realism_default",
        label="V4.3 真实度默认方案",
        description="V4.3 signed tx, localhost TCP P2P, PBFT-style commit, state root, cross-shard, fault, and BlockEmulator bridge validation profile.",
        runtime_target="v4.3",
        mechanism_tags=["signed_tx", "real_p2p", "pbft_style", "state_root", "cross_shard", "faults", "blockemulator_bridge"],
        default_topology_id="local_8_nodes_2_shards",
        default_workload_id="small_test",
        runnable=True,
    ),
    "metatrack_v3_mechanism_profile": ExperimentProfile(
        profile_id="metatrack_v3_mechanism_profile",
        label="MetaTrack 机制实验方案",
        description="V3-final local modular runtime mechanism profile for MetaTrack controlled experiments.",
        runtime_target="v3",
        mechanism_tags=["metatrack", "routing", "dual_track", "aggregation", "local_emulator"],
        default_topology_id="local_4_nodes_1_shard",
        default_workload_id="small_test",
        runnable=True,
    ),
    "baseline_hash_profile": ExperimentProfile(
        profile_id="baseline_hash_profile",
        label="Baseline Hash 对照方案",
        description="Baseline hash routing comparison profile for V3/V4-oriented experiment planning.",
        runtime_target="v3/v4",
        mechanism_tags=["baseline", "hash_routing", "comparison"],
        default_topology_id="local_4_nodes_1_shard",
        default_workload_id="small_test",
        runnable=True,
    ),
}

TOPOLOGIES = {
    "local_4_nodes_1_shard": ExperimentTopology(
        topology_id="local_4_nodes_1_shard",
        label="Local 4 nodes / 1 shard",
        nodes=4,
        shards=1,
        validators_per_shard=4,
        runtime_mode="v4.3",
        description="Smallest V4.3 PBFT-style smoke topology.",
        runnable=True,
    ),
    "local_8_nodes_2_shards": ExperimentTopology(
        topology_id="local_8_nodes_2_shards",
        label="Local 8 nodes / 2 shards",
        nodes=8,
        shards=2,
        validators_per_shard=4,
        runtime_mode="v4.3",
        description="Recommended small real-node validation topology.",
        runnable=True,
    ),
    "local_8_nodes_4_shards": ExperimentTopology(
        topology_id="local_8_nodes_4_shards",
        label="Local 8 nodes / 4 shards",
        nodes=8,
        shards=4,
        validators_per_shard=2,
        runtime_mode="v4.3",
        description="Runnable stress topology with fewer validators per shard.",
        runnable=True,
    ),
}

WORKLOADS = {
    "small_test": ExperimentWorkload(
        workload_id="small_test",
        label="Small test workload",
        source_type="sample",
        scale_label="small",
        skew_label="basic",
        description="Built-in small validation workload for V4.3 smoke.",
        planned=False,
        default_tx_count=20,
        default_blockemulator_tx_limit=20,
        csv_required=False,
    ),
    "blockemulator_sample": ExperimentWorkload(
        workload_id="blockemulator_sample",
        label="BlockEmulator sample subset",
        source_type="sample_csv",
        scale_label="small",
        skew_label="basic",
        description="Small BlockEmulator-style sample subset for bridge validation.",
        planned=False,
        default_tx_count=20,
        default_blockemulator_tx_limit=20,
        csv_required=False,
    ),
    "blockemulator_csv": ExperimentWorkload(
        workload_id="blockemulator_csv",
        label="BlockEmulator external CSV",
        source_type="external_csv",
        scale_label="user_selected",
        skew_label="user_selected",
        description="User-selected BlockEmulator CSV subset path.",
        planned=False,
        default_tx_count=20,
        default_blockemulator_tx_limit=20,
        csv_required=True,
    ),
    "real_skew_low": ExperimentWorkload(
        workload_id="real_skew_low",
        label="Real workload skew low",
        source_type="real_dataset",
        scale_label="small/medium/large",
        skew_label="low",
        description="Planned filtered real workload with low skew; dataset not attached yet.",
        planned=True,
        default_tx_count=20,
        default_blockemulator_tx_limit=20,
        csv_required=False,
    ),
    "real_skew_medium": ExperimentWorkload(
        workload_id="real_skew_medium",
        label="Real workload skew medium",
        source_type="real_dataset",
        scale_label="small/medium/large",
        skew_label="medium",
        description="Planned filtered real workload with medium skew; dataset not attached yet.",
        planned=True,
        default_tx_count=20,
        default_blockemulator_tx_limit=20,
        csv_required=False,
    ),
    "real_skew_high": ExperimentWorkload(
        workload_id="real_skew_high",
        label="Real workload skew high",
        source_type="real_dataset",
        scale_label="small/medium/large",
        skew_label="high",
        description="Planned filtered real workload with high skew; dataset not attached yet.",
        planned=True,
        default_tx_count=20,
        default_blockemulator_tx_limit=20,
        csv_required=False,
    ),
    "extreme_hotspot": ExperimentWorkload(
        workload_id="extreme_hotspot",
        label="Extreme hotspot real workload",
        source_type="real_dataset",
        scale_label="small/medium/large",
        skew_label="extreme_hotspot",
        description="Planned filtered real workload with extreme hotspot skew; dataset not attached yet.",
        planned=True,
        default_tx_count=20,
        default_blockemulator_tx_limit=20,
        csv_required=False,
    ),
}


def list_profiles() -> list[ExperimentProfile]:
    return list(PROFILES.values())


def list_topologies() -> list[ExperimentTopology]:
    return list(TOPOLOGIES.values())


def list_workloads() -> list[ExperimentWorkload]:
    return list(WORKLOADS.values())


def recommended_run() -> ExperimentRunPlanPreview:
    profile = PROFILES["v4_3_realism_default"]
    return preview_run_plan(
        ExperimentRunPlanRequest(
            profile_id=profile.profile_id,
            topology_id=profile.default_topology_id,
            workload_id=profile.default_workload_id,
        )
    )


def preview_run_plan(request: ExperimentRunPlanRequest) -> ExperimentRunPlanPreview:
    profile = _profile(request.profile_id)
    topology = _topology(request.topology_id)
    workload = _workload(request.workload_id)
    warnings: list[str] = []
    runnable = bool(profile.runnable and topology.runnable and not workload.planned)

    if workload.planned:
        runnable = False
        warnings.append(f"{workload.workload_id}: dataset not attached yet; workload is planned and not runnable.")
    if workload.csv_required and not request.blockemulator_csv:
        runnable = False
        warnings.append(f"{workload.workload_id}: blockemulator_csv is required before this workload can run.")
    if topology.shards > topology.nodes:
        runnable = False
        warnings.append(f"{topology.topology_id}: shard count cannot exceed node count.")
    if topology.topology_id == "local_8_nodes_4_shards":
        warnings.append("local_8_nodes_4_shards: 每片 validator 较少，不适合作为默认 PBFT-style realism 配置。")

    tx_count = request.tx_count_override or workload.default_tx_count
    fault_profile = request.fault_profile_override or "mixed_light"
    recommended = V4RealismSmokeRequest(
        nodes=topology.nodes,
        shards=topology.shards,
        tx_count=tx_count,
        enable_cross_shard=True,
        enable_faults=True,
        fault_profile=fault_profile,
        blockemulator_csv=request.blockemulator_csv,
        blockemulator_tx_limit=workload.default_blockemulator_tx_limit,
        run_duration_ms=1000,
    )
    next_step = "Apply recommended_v4_request in V4 realism mode." if runnable else "Resolve warnings before running V4 realism."
    return ExperimentRunPlanPreview(
        profile=profile,
        topology=topology,
        workload=workload,
        runtime=profile.runtime_target,
        recommended_v4_request=recommended,
        runnable=runnable,
        warnings=warnings,
        next_step=next_step,
    )


def _profile(profile_id: str) -> ExperimentProfile:
    try:
        return PROFILES[profile_id]
    except KeyError as exc:
        raise ValueError(f"unknown profile_id: {profile_id}") from exc


def _topology(topology_id: str) -> ExperimentTopology:
    try:
        return TOPOLOGIES[topology_id]
    except KeyError as exc:
        raise ValueError(f"unknown topology_id: {topology_id}") from exc


def _workload(workload_id: str) -> ExperimentWorkload:
    try:
        return WORKLOADS[workload_id]
    except KeyError as exc:
        raise ValueError(f"unknown workload_id: {workload_id}") from exc

