import csv
import json
from pathlib import Path

from backend.app.services import v5_txallo_epoch_benefit_v1 as benefit
from backend.app.services import v5_txallo_mapping_epoch_v229 as coherence


def build_fixture(tmp_path: Path, *, bad_digest=False):
    (tmp_path / 'workload').mkdir()
    (tmp_path / 'client').mkdir()
    shards = ['s0', 's1']
    # The initial mapping knows only a hot account. The first two new leaves
    # have no prior mapping; E1 learns them, and E1 includes one returning leaf.
    e0 = {'hot': 's0'}
    e1 = {'hot': 's0', 'leaf1': 's0', 'leaf2': 's0'}
    epochs = []
    for epoch, mapping in [(0, e0), (1, e1)]:
        aliases = {}
        epochs.append({'epoch': epoch, 'mapping': mapping, 'aliases': aliases,
                       'mapping_digest': coherence._digest(mapping),
                       'aliases_digest': coherence._digest(aliases),
                       'state_digest': coherence._digest({'mapping': mapping, 'aliases': aliases})})
    path = tmp_path / 'workload' / 'txallo_mapping_epochs.jsonl'
    path.write_text(''.join(json.dumps(x) + '\n' for x in epochs), encoding='utf-8')
    recs = [
        (0, 0, 'hot', 'leaf1'),
        (1, 0, 'hot', 'leaf2'),
        (2, 1, 'hot', 'leaf1'),
        (3, 1, 'hot', 'leaf3'),
    ]
    rows = []
    for idx, epoch, sender, receiver in recs:
        mapping = epochs[epoch]['mapping']
        h = mapping.get(sender, benefit._fallback(sender, shards))
        r = mapping.get(receiver, benefit._fallback(receiver, shards))
        rows.append({'tx_index': str(idx), 'routing_epoch': str(epoch),
                     'sender_account': sender, 'receiver_account': receiver,
                     'sender_mapping_source': 'history_mapping' if sender in mapping else 'fallback_hash',
                     'receiver_mapping_source': 'history_mapping' if receiver in mapping else 'fallback_hash',
                     'sender_shard': h, 'receiver_shard': r,
                     'mapping_state_digest': epochs[epoch]['state_digest'] if not bad_digest else 'bad',
                     'txallo_cross_shard': str(h != r).lower()})
    with (tmp_path/'client'/'txallo_transaction_placement.csv').open('w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0])); w.writeheader(); w.writerows(rows)
    return rows


def test_counterfactual_and_learning_coverage(tmp_path):
    rows = build_fixture(tmp_path)
    result = benefit.audit_run(tmp_path, ['s0', 's1'])
    assert result['passed'], result
    assert result['transaction_count'] == 4
    assert result['unique_evaluation_account_count'] == 4
    assert result['accounts_seen_in_multiple_mapping_epochs'] == 2
    assert result['mapping_changes'][1]['newly_mapped_account_count'] == 2
    assert result['mapping_changes'][1]['newly_mapped_used_during_epoch_count'] == 1
    assert result['actual_cross_shard_ratio'] == sum(x['txallo_cross_shard']=='true' for x in rows)/4
    assert result['evaluation_graph']['max_degree'] == 3


def test_active_mapping_state_digest_mismatch_is_fail_closed(tmp_path):
    build_fixture(tmp_path, bad_digest=True)
    result = benefit.audit_run(tmp_path, ['s0', 's1'])
    assert not result['passed']
    assert any('digest' in reason for reason in result['blockers'])


def test_missing_shard_inventory_prevents_claim(tmp_path):
    build_fixture(tmp_path)
    assert not benefit.audit_run(tmp_path)['passed']
    assert benefit.audit_run(tmp_path, allow_inferred_shards=True)['passed']


def test_fallback_matches_go_rune_sum():
    assert benefit._fallback('é', ['s0','s1','s2']) == ['s0','s1','s2'][sum(map(ord, 'txallo_account:é')) % 3]
