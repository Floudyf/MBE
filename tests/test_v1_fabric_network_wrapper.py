from __future__ import annotations
import os, subprocess, sys
from pathlib import Path
import yaml
from chain.fabric.network.fabric_network import detect_fabric_environment, build_test_network_commands
ROOT=Path(__file__).resolve().parents[1]
def test_skip_strict_fake_and_commands(tmp_path,monkeypatch):
 monkeypatch.delenv('FABRIC_SAMPLES_DIR',raising=False); status=detect_fabric_environment(ROOT); assert 'FABRIC_SAMPLES_DIR' in status.missing and not status.ready_for_manual_network
 fake=tmp_path/'fabric-samples'; n=fake/'test-network';n.mkdir(parents=True);(n/'network.sh').write_text('');monkeypatch.setenv('FABRIC_SAMPLES_DIR',str(fake));status=detect_fabric_environment(ROOT);assert status.network_sh and all(p.is_dir() for p in [status.asset_chaincode_dir,status.scene_chaincode_dir,status.reward_chaincode_dir])
 commands=build_test_network_commands(ROOT,fake);assert 'network.sh up' in commands['up'] and 'createChannel -c mbechannel' in commands['create-channel'];assert all(x in commands['deploy-asset'] for x in ('deployCC','mbeasset','chain/fabric/chaincode/asset','-ccl go'));assert 'mbescene' in commands['deploy-scene'] and 'mbereward' in commands['deploy-reward'] and 'network.sh down' in commands['down']
def test_cli_docs_and_planned_config(monkeypatch):
 monkeypatch.delenv('FABRIC_SAMPLES_DIR',raising=False)
 for args in (['scripts/v1_fabric_env_check.py'],['scripts/v1_fabric_network.py','--check'],['scripts/v1_fabric_network.py','--dry-run','--action','deploy-asset']): assert subprocess.run([sys.executable,*args],cwd=ROOT).returncode==0
 text=(ROOT/'chain/fabric/network/README.md').read_text();assert all(x in text for x in ('FABRIC_SAMPLES_DIR','dry-run','strict','graceful skip','test-network','V1.4-d'))
 assert 'FABRIC_SAMPLES_DIR' in (ROOT/'chain/fabric/network/fabric_env.example').read_text()
 e=yaml.safe_load((ROOT/'configs/experiments/v1_fabric_chain_backed_asset.yaml').read_text())['experiment'];assert e['runnable'] is False and e['implemented'] is False
