from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest

from chain.fabric.network.fabric_network import FabricEnvStatus
from chain.fabric.runner.fabric_smoke import SmokeConfig, SmokeEnvironmentError, build_smoke_plan, run_smoke, write_raw_log
from trace.converter.fabric_to_unified_trace import convert_raw_fabric_log

ROOT = Path(__file__).resolve().parents[1]


def ready_status(tmp_path: Path) -> FabricEnvStatus:
    samples = tmp_path / "fabric-samples"
    network = samples / "test-network"
    network.mkdir(parents=True)
    script = network / "network.sh"
    script.write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    paths = [ROOT / "chain/fabric/chaincode" / name for name in ("asset", "scene", "reward")]
    return FabricEnvStatus(samples, network, script, True, True, True, *paths, [], [], True)


def raw_record() -> dict[str, object]:
    return {"tx_id":"mock-1", "tx_type":"asset_transfer", "submit_time":1000, "commit_time":1005, "status":"success", "contract":"asset", "function":"TransferAsset", "args":{"asset_id":"asset-1", "from":"seller", "to":"buyer"}, "block_number":1, "event":"AssetTransferred", "chain_latency_ms":5}


def test_dry_run_only_builds_network_deploy_and_invoke_plan(tmp_path: Path) -> None:
    result = run_smoke(ROOT, SmokeConfig("mbechannel", tmp_path / "out", dry_run=True), ready_status(tmp_path))
    plan = result["plan"]
    assert result["dry_run"] and not (tmp_path / "out").exists()
    assert any("network.sh" in command and "createChannel -c mbechannel" in command for command in plan)
    assert all(name in "\n".join(plan) for name in ("mbeasset", "mbescene", "mbereward", "peer chaincode invoke"))
    assert any("TradeAsset" in command for command in plan) and any("ClaimReward" in command for command in plan)


def test_raw_writer_has_required_fields_and_converts(tmp_path: Path) -> None:
    raw = tmp_path / "raw_chain_log.jsonl"
    write_raw_log([raw_record()], raw)
    saved = json.loads(raw.read_text(encoding="utf-8"))
    assert {"tx_id","tx_type","submit_time","commit_time","status","contract","function","args","block_number","event","chain_latency_ms"} <= saved.keys()
    result = convert_raw_fabric_log(raw, ROOT / "chain/fabric/access_schema.yaml", tmp_path / "converted")
    with gzip.open(result["trace_path"], "rt", encoding="utf-8") as stream:
        assert json.loads(next(stream))["tx_id"] == "mock-1"
    assert json.loads(result["meta_path"].read_text(encoding="utf-8"))["source"] == "fabric_raw_log"


def test_missing_environment_skips_or_fails_strict(tmp_path: Path) -> None:
    missing = FabricEnvStatus(None, None, None, False, False, False, *(ROOT / "chain/fabric/chaincode" / name for name in ("asset", "scene", "reward")), ["FABRIC_SAMPLES_DIR"], [], False)
    skipped = run_smoke(ROOT, SmokeConfig("mbechannel", tmp_path / "out"), missing)
    assert skipped["skipped"] and "FABRIC_SAMPLES_DIR" in skipped["reason"]
    with pytest.raises(SmokeEnvironmentError, match="FABRIC_SAMPLES_DIR"):
        run_smoke(ROOT, SmokeConfig("mbechannel", tmp_path / "out", strict=True), missing)


def test_runner_docs_and_cache_are_not_commit_artifacts() -> None:
    assert ".cache/" in (ROOT / ".gitignore").read_text(encoding="utf-8")
    readme = (ROOT / "chain/fabric/runner/README.md").read_text(encoding="utf-8")
    assert all(token in readme for token in ("chain-backed trace", "not a production", "MBE executor", "network.sh down", "--dry-run"))
