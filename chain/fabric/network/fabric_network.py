from __future__ import annotations
import os, shutil
from dataclasses import dataclass, field
from pathlib import Path

@dataclass
class FabricEnvStatus:
    fabric_samples_dir: Path|None; test_network_dir: Path|None; network_sh: Path|None
    docker_available: bool; docker_compose_available: bool; peer_available: bool
    asset_chaincode_dir: Path; scene_chaincode_dir: Path; reward_chaincode_dir: Path
    missing: list[str]=field(default_factory=list); warnings: list[str]=field(default_factory=list)
    ready_for_manual_network: bool=False

def detect_fabric_environment(project_root: Path, strict: bool=False) -> FabricEnvStatus:
    root=Path(os.environ['FABRIC_SAMPLES_DIR']) if os.environ.get('FABRIC_SAMPLES_DIR') else None
    test=root/'test-network' if root else None; network=test/'network.sh' if test else None
    dirs=[project_root/'chain/fabric/chaincode'/x for x in ('asset','scene','reward')]
    missing=[] if root else ['FABRIC_SAMPLES_DIR']
    for label,path in [('test-network',test),('network.sh',network)]:
        if path is not None and not path.exists(): missing.append(label)
    missing += [str(path.relative_to(project_root)) for path in dirs if not path.is_dir()]
    docker=shutil.which('docker') is not None; compose=docker
    peer_path=shutil.which('peer') or (str(root/'bin/peer') if root and (root/'bin/peer').is_file() else None)
    peer=peer_path is not None
    if not docker: missing.append('docker')
    if not peer: missing.append('peer CLI')
    warnings=[]
    return FabricEnvStatus(root,test,network,docker,compose,peer,*dirs,missing,warnings,not missing)

def build_test_network_commands(project_root: Path, fabric_samples_dir: Path) -> dict[str,str]:
    network=fabric_samples_dir/'test-network'/'network.sh'; cc=project_root/'chain/fabric/chaincode'
    base=str(network); return {'up':f'{base} up','create-channel':f'{base} createChannel -c mbechannel','deploy-asset':f'{base} deployCC -ccn mbeasset -ccp "{(cc/"asset").as_posix()}" -ccl go -c mbechannel','deploy-scene':f'{base} deployCC -ccn mbescene -ccp "{(cc/"scene").as_posix()}" -ccl go -c mbechannel','deploy-reward':f'{base} deployCC -ccn mbereward -ccp "{(cc/"reward").as_posix()}" -ccl go -c mbechannel','down':f'{base} down'}
