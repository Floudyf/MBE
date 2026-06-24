"""Run a small local Fabric test-network trace smoke test.

This runner deliberately targets the official single-machine test-network only.  It
does not create a production Fabric deployment and it does not implement multi-chain
or cross-chain behaviour.
"""
from __future__ import annotations

import json
import re
import shlex
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, TextIO

from chain.fabric.network.fabric_network import FabricEnvStatus, detect_fabric_environment
from trace.converter.fabric_to_unified_trace import convert_raw_fabric_log

CHAINCODES = {
    "asset": ("mbeasset", "asset"),
    "scene": ("mbescene", "scene"),
    "reward": ("mbereward", "reward"),
}
TRACE_OPERATIONS = {
    "asset": [("TransferAsset", ["asset-1", "seller", "trader"], "AssetTransferred"), ("TradeAsset", ["asset-1", "trader", "buyer", "25"], "AssetTraded")],
    "scene": [("JoinScene", ["avatar-1", "scene-1"], "SceneJoined")],
    "reward": [("AddReward", ["pool-1", "50"], "RewardAdded"), ("ClaimReward", ["user-1", "pool-1", "20"], "RewardClaimed")],
}
BOOTSTRAP_OPERATIONS = {
    "asset": [("CreateAsset", ["asset-1", "seller", "smoke"], None), ("SetBalance", ["seller", "0"], None), ("SetBalance", ["buyer", "100"], None), ("SetBalance", ["trader", "0"], None)],
    "scene": [("CreateScene", ["scene-1", "Smoke scene", "10"], None), ("CreateAvatar", ["avatar-1", "active"], None)],
    "reward": [("CreateRewardPool", ["pool-1", "0"], None), ("SetBalance", ["user-1", "0"], None)],
}


class SmokeEnvironmentError(RuntimeError):
    """Raised only when strict smoke execution cannot inspect its environment."""


@dataclass(frozen=True)
class SmokeConfig:
    channel: str
    output_dir: Path
    skip_network_up: bool = False
    skip_deploy: bool = False
    keep_network: bool = False
    strict: bool = False
    dry_run: bool = False


def _quoted(command: list[str]) -> str:
    return " ".join(shlex.quote(item) for item in command)


def _network_command(status: FabricEnvStatus, *args: str) -> list[str]:
    if status.network_sh is None:
        raise SmokeEnvironmentError("test-network/network.sh is unavailable")
    return [str(status.network_sh), *args]


def build_smoke_plan(project_root: Path, status: FabricEnvStatus, channel: str) -> list[str]:
    """Return the command plan without running Docker, network.sh, or peer."""
    plan = [_quoted(_network_command(status, "up", "createChannel", "-c", channel))]
    for name, (cc_name, directory) in CHAINCODES.items():
        plan.append(_quoted(_network_command(status, "deployCC", "-ccn", cc_name, "-ccp", str(project_root / "chain/fabric/chaincode" / directory), "-ccl", "go", "-c", channel)))
    for contract in ("asset", "scene", "reward"):
        cc_name = CHAINCODES[contract][0]
        for function, args, _ in [*BOOTSTRAP_OPERATIONS[contract], *TRACE_OPERATIONS[contract]]:
            payload = json.dumps({"function": function, "Args": args}, separators=(",", ":"))
            plan.append(f"peer chaincode invoke -C {channel} -n {cc_name} -c {shlex.quote(payload)} --waitForEvent")
    plan.append(_quoted(_network_command(status, "down")))
    return plan


def write_raw_log(records: list[dict[str, object]], raw_log_path: Path) -> None:
    """Write a streaming-compatible raw Fabric JSONL log with required fields."""
    raw_log_path.parent.mkdir(parents=True, exist_ok=True)
    with raw_log_path.open("w", encoding="utf-8") as stream:
        for record in records:
            stream.write(json.dumps(record, sort_keys=True) + "\n")


def _peer_environment(network_dir: Path) -> dict[str, str]:
    org = network_dir / "organizations"
    return {
        "PATH": f"{network_dir.parent / 'bin'}:{__import__('os').environ.get('PATH', '')}",
        "FABRIC_CFG_PATH": str(network_dir.parent / "config"),
        "CORE_PEER_TLS_ENABLED": "true",
        "CORE_PEER_LOCALMSPID": "Org1MSP",
        "CORE_PEER_TLS_ROOTCERT_FILE": str(org / "peerOrganizations/org1.example.com/peers/peer0.org1.example.com/tls/ca.crt"),
        "CORE_PEER_MSPCONFIGPATH": str(org / "peerOrganizations/org1.example.com/users/Admin@org1.example.com/msp"),
        "CORE_PEER_ADDRESS": "localhost:7051",
    }


def _invoke_command(network_dir: Path, channel: str, cc_name: str, function: str, args: list[str]) -> list[str]:
    orderer_ca = network_dir / "organizations/ordererOrganizations/example.com/orderers/orderer.example.com/msp/tlscacerts/tlsca.example.com-cert.pem"
    org1_ca = network_dir / "organizations/peerOrganizations/org1.example.com/peers/peer0.org1.example.com/tls/ca.crt"
    org2_ca = network_dir / "organizations/peerOrganizations/org2.example.com/peers/peer0.org2.example.com/tls/ca.crt"
    payload = json.dumps({"function": function, "Args": args}, separators=(",", ":"))
    return ["peer", "chaincode", "invoke", "-o", "localhost:7050", "--ordererTLSHostnameOverride", "orderer.example.com", "--tls", "--cafile", str(orderer_ca), "-C", channel, "-n", cc_name, "-c", payload, "--peerAddresses", "localhost:7051", "--tlsRootCertFiles", str(org1_ca), "--peerAddresses", "localhost:9051", "--tlsRootCertFiles", str(org2_ca), "--waitForEvent"]


def _parsed_tx_id(output: str, fallback: str) -> str:
    match = re.search(r"(?:txid|transaction\s+id)\s*[:=]\s*([A-Za-z0-9_-]+)", output, re.IGNORECASE)
    return match.group(1) if match else fallback


def _raw_record(sequence: int, contract: str, function: str, args: list[str], event: str, started: float, finished: float, output: str) -> dict[str, object]:
    names = {
        "TransferAsset": ("asset_id", "from", "to"), "TradeAsset": ("asset_id", "seller", "buyer", "price"),
        "JoinScene": ("user_id", "scene_id"), "AddReward": ("pool_id", "amount"), "ClaimReward": ("user_id", "pool_id", "amount"),
    }[function]
    typed_args: dict[str, object] = {key: int(value) if key in {"price", "amount"} else value for key, value in zip(names, args)}
    return {"tx_id": _parsed_tx_id(output, f"fabric-smoke-{sequence:04d}"), "tx_type": function, "submit_time": started * 1000, "commit_time": finished * 1000, "status": "success", "contract": contract, "function": function, "args": typed_args, "block_number": sequence, "event": event, "chain_latency_ms": (finished - started) * 1000}


def _run(command: list[str], cwd: Path, log: TextIO, env: dict[str, str] | None = None) -> str:
    log.write(f"$ {_quoted(command)}\n")
    # communicate drains stdout while the command runs; deployCC emits enough output to fill a pipe.
    process = subprocess.Popen(command, cwd=cwd, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env)
    output, _ = process.communicate()
    log.write(output)
    if process.returncode:
        raise RuntimeError(f"command failed ({process.returncode}): {_quoted(command)}")
    return output


def run_smoke(project_root: Path, config: SmokeConfig, status: FabricEnvStatus | None = None) -> dict[str, object]:
    """Execute the single-machine smoke path, or return a no-side-effect dry-run plan."""
    status = status or detect_fabric_environment(project_root, config.strict)
    if status.missing:
        message = "Fabric environment unavailable: " + ", ".join(status.missing)
        if config.strict:
            raise SmokeEnvironmentError(message)
        return {"skipped": True, "reason": message, "plan": []}
    plan = build_smoke_plan(project_root, status, config.channel)
    if config.dry_run:
        return {"dry_run": True, "plan": plan}
    assert status.test_network_dir is not None
    out = config.output_dir
    out.mkdir(parents=True, exist_ok=True)
    raw_path, runtime_path = out / "raw_chain_log.jsonl", out / "runtime.log"
    records: list[dict[str, object]] = []
    with runtime_path.open("w", encoding="utf-8") as log:
        log.write("V1.4-d local Fabric smoke runner; bootstrap calls are excluded from raw trace because access_schema.yaml covers only trace operations.\n")
        log.write("When peer output has no stable tx id, tx_id=fabric-smoke-NNNN and block_number=N are deterministic smoke fallbacks.\n")
        try:
            if not config.skip_network_up:
                _run(_network_command(status, "up", "createChannel", "-c", config.channel), status.test_network_dir, log)
            if not config.skip_deploy:
                for cc_name, directory in CHAINCODES.values():
                    _run(_network_command(status, "deployCC", "-ccn", cc_name, "-ccp", str(project_root / "chain/fabric/chaincode" / directory), "-ccl", "go", "-c", config.channel), status.test_network_dir, log)
            env = {**__import__('os').environ, **_peer_environment(status.test_network_dir)}
            sequence = 0
            for contract in ("asset", "scene", "reward"):
                cc_name = CHAINCODES[contract][0]
                for function, args, _ in BOOTSTRAP_OPERATIONS[contract]:
                    _run(_invoke_command(status.test_network_dir, config.channel, cc_name, function, args), status.test_network_dir, log, env)
                for function, args, event in TRACE_OPERATIONS[contract]:
                    started = time.time(); output = _run(_invoke_command(status.test_network_dir, config.channel, cc_name, function, args), status.test_network_dir, log, env); finished = time.time()
                    sequence += 1; records.append(_raw_record(sequence, contract, function, args, str(event), started, finished, output))
            write_raw_log(records, raw_path)
            conversion = convert_raw_fabric_log(raw_path, project_root / "chain/fabric/access_schema.yaml", out)
            summary = {"source": "fabric_smoke", "tx_count": len(records), "contracts": sorted({record["contract"] for record in records}), "raw_chain_log": str(raw_path), "trace": str(conversion["trace_path"]), "trace_meta": str(conversion["meta_path"])}
            (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
            return summary
        finally:
            if not config.keep_network:
                try:
                    _run(_network_command(status, "down"), status.test_network_dir, log)
                except Exception as error:  # Preserve the primary smoke failure in the log.
                    log.write(f"cleanup failed: {error}\n")
