from __future__ import annotations

import hashlib
import json
from pathlib import Path

from backend.app.models.v5_compiled_run_plan import V5CompiledNodeConfig, V5CompiledRunPlan
from backend.app.models.v5_experiment_spec import V5ExperimentSpec
from backend.app.services.v5_compatibility_engine import validate
from backend.app.services.v5_plugin_manifest_store import STORE


EXPECTED_ARTIFACTS = ["compiled_run_plan.json", "process_manifest.json", "real_cluster_summary.json", "client_submission_log.csv", "artifact_catalog.json"]


def requested_cross_shard_count(tx_count: int, ratio: float) -> int:
    return int(tx_count * ratio + 0.5)


def compile_plan(spec: V5ExperimentSpec, run_dir: Path, *, source_saved_config_id: str | None = None) -> V5CompiledRunPlan:
    compatibility = validate(spec)
    if not compatibility.valid:
        raise ValueError("; ".join(compatibility.blockers))
    normalized = spec.model_dump()
    raw = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    profile = {selection.category: {"plugin_id": selection.plugin_id, "config": selection.config} for selection in compatibility.resolved_plugins}
    nodes: list[V5CompiledNodeConfig] = []
    for index in range(spec.topology.nodes):
        shard_index = index // spec.topology.validators_per_shard
        node_id = f"n{index}"
        validators = [f"n{shard_index * spec.topology.validators_per_shard + offset}" for offset in range(spec.topology.validators_per_shard)]
        nodes.append(V5CompiledNodeConfig(node_id=node_id, shard_id=f"s{shard_index}", role="leader" if node_id == validators[0] else "validator", leader=node_id == validators[0], listen_addr="127.0.0.1:0", data_dir=str(run_dir / "nodes" / node_id), validators=validators, plugin_profile=profile))
    snapshot = [STORE.get(item.plugin_id).model_dump() | {"selected_config": item.config} for item in compatibility.resolved_plugins]
    workload_config = profile["workload"]["config"]
    ratio = float(workload_config.get("cross_shard_ratio", 0.0))
    if not 0 <= ratio <= 1:
        raise ValueError("cross_shard_ratio must be between 0 and 1")
    if ratio > 0 and spec.topology.shards < 2:
        raise ValueError("cross_shard_ratio requires at least 2 shards")
    workload = workload_config | {"plugin_id": profile["workload"]["plugin_id"], "tx_count": spec.tx_count, "seed": spec.seed, "requested_cross_shard_ratio": ratio, "requested_cross_shard_count": requested_cross_shard_count(spec.tx_count, ratio)}
    return V5CompiledRunPlan(plan_id=f"v5plan_{digest[:16]}", plan_digest=digest, execution_backend=spec.execution_backend, duration_ms=spec.duration_ms, source_saved_config_id=source_saved_config_id, experiment_spec=normalized, plugin_snapshot=snapshot, node_configs=nodes, workload_plan=workload, fault_plan=spec.fault_policy, expected_artifacts=EXPECTED_ARTIFACTS, resource_estimate=compatibility.resource_estimate)
