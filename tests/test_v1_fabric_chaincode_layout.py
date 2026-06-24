from __future__ import annotations
import json
from pathlib import Path
import yaml

ROOT=Path(__file__).resolve().parents[1]
BASE=ROOT/'chain/fabric/chaincode'
SPECS={'asset':('asset_contract.go',['TransferAsset','TradeAsset'],['asset:','balance:'],['asset_transfer','asset_trade']), 'scene':('scene_contract.go',['JoinScene'],['scene:','avatar:','scene_member:'],['scene_join']), 'reward':('reward_contract.go',['AddReward','ClaimReward'],['reward_pool:','balance:'],['add_reward','reward_claim','delta_value'])}

def test_chaincode_layout_and_payloads_align_with_schema():
    for name,(contract,functions,prefixes,tokens) in SPECS.items():
        directory=BASE/name
        for file in ('go.mod','main.go','models.go','README.md',contract): assert (directory/file).is_file()
        text=(directory/contract).read_text(encoding='utf-8')
        for token in [*functions,*prefixes,*tokens,'tx_type','contract','function','args','primary_key','access_keys','event']: assert token in text
    schema=yaml.safe_load((ROOT/'chain/fabric/access_schema.yaml').read_text(encoding='utf-8'))['contracts']
    assert schema['asset']['TransferAsset']['primary_key']=='asset:{asset_id}'
    assert {'asset:{asset_id}','balance:{seller}','balance:{buyer}'} <= set(schema['asset']['TradeAsset']['access_list'])
    assert {'scene:{scene_id}','avatar:{user_id}','scene_member:{scene_id}:{user_id}'} <= set(schema['scene']['JoinScene']['access_list'])
    for name in ('AddReward','ClaimReward'):
        item=schema['reward'][name]; assert item['primary_key']=='reward_pool:{pool_id}' and item['commutative'] and item['update_type']=='delta' and item['delta_arg']=='amount'
    assert schema['reward']['ClaimReward']['delta_sign']=='negative'

def test_samples_and_fabric_config_remain_planned():
    schema=yaml.safe_load((ROOT/'chain/fabric/access_schema.yaml').read_text(encoding='utf-8'))['contracts']
    for line in (ROOT/'chain/fabric/samples/raw_chain_log_sample.jsonl').read_text(encoding='utf-8').splitlines():
        record=json.loads(line); assert record['function'] in schema[record['contract']]
    config=yaml.safe_load((ROOT/'configs/experiments/v1_fabric_chain_backed_asset.yaml').read_text(encoding='utf-8'))['experiment']
    assert config['runnable'] is False and config['implemented'] is False
    for path in [ROOT/'chain/fabric/README.md',BASE/'README.md',*(BASE/name/'README.md' for name in SPECS)]: assert path.is_file()

def test_chaincode_state_semantics_are_not_skeletons():
    asset=(BASE/'asset'/'asset_contract.go').read_text(encoding='utf-8')
    scene=(BASE/'scene'/'scene_contract.go').read_text(encoding='utf-8')
    reward=(BASE/'reward'/'reward_contract.go').read_text(encoding='utf-8')
    compact_scene=''.join(scene.split())
    compact_reward=''.join(reward.split())

    for text in (asset,scene,reward):
        assert 'GetState' in text
        assert 'PutState' in text
        assert 'json.Marshal' in text
        assert 'json.Unmarshal' in text
        assert 'SetEvent' in text
        assert 'return nil,nil' not in text

    for function in ('CreateAsset','TransferAsset','TradeAsset','ReadAsset','GetBalance','SetBalance'):
        assert function in asset
    assert 'a.Owner=to' in asset and 'a.Version++' in asset
    assert 'bb.Amount-=price' in asset and 'sb.Amount+=price' in asset

    for function in ('CreateScene','ReadScene','CreateAvatar','ReadAvatar','JoinScene'):
        assert function in scene
    assert 'sceneMemberKey(sceneID,userID)' in compact_scene
    assert 'PutState(memberKey,member)' in compact_scene
    assert 'avatar already joined scene' in scene

    for function in ('CreateRewardPool','ReadRewardPool','SetBalance','GetBalance','AddReward','ClaimReward'):
        assert function in reward
    assert 'pool.Amount+=amount' in compact_reward
    assert 'pool.Amount-=amount' in compact_reward
    assert 'balance.Amount+=amount' in compact_reward
