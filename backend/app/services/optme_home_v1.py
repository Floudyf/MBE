"""Fail-closed mapping for stateless OptME's partition-owned v17 commitments.

The single optme-global ordering domain is not a replica/storage identity.
Only signed-off local compiled plan node_configs may identify Home partitions.
No runtime algorithm, PBFT, or original commitment artifact is modified.
"""
from __future__ import annotations
import json
from collections import Counter
from pathlib import Path

GLOBAL='optme-global'


def verified_home_map(run_root:Path, files:list[Path]) -> dict[str,str]:
    plan_path=run_root/'compiled_run_plan.json'
    if not plan_path.is_file():
        raise ValueError('compiled_run_plan.json missing; cannot prove OptME Home partition')
    try:
        plan=json.loads(plan_path.read_text(encoding='utf-8'))
    except (OSError,ValueError) as exc:
        raise ValueError('compiled_run_plan.json unreadable or malformed') from exc
    if not isinstance(plan, dict):
        raise ValueError("compiled run plan is not a JSON object")
    # MBE_OPTME_HOME_PLAN_SHAPE_V2: schema string alone is not a trust anchor.
    # For a non-v1 envelope require the complete original typed node-plan contract.
    # This is NOT acceptance of arbitrary schemas: every node, Home, consensus
    # domain and plugin binding below is still verified fail-closed.
    schema=str(plan.get("schema_version") or "")
    # MBE_OPTME_GO_PLAN_PROOF_V3: Go supervisor overwrites the compiled run
    # plan using executor/v5.Plan, which intentionally omits Python-only
    # schema_version, experiment_spec and plugin_snapshot. Distinguish this
    # reviewed Go serialization from unknown/malformed formats without ever
    # skipping the node-level Home/replica/storage-plugin checks below.
    if schema not in {"v5_compiled_run_plan_v1", ""}:
        raise ValueError("unsupported compiled run plan schema: "+repr(schema))
    if schema == "":
        pd=plan.get("plan_digest")
        pi=plan.get("plan_id")
        nodes_for_go=plan.get("node_configs")
        contract=plan.get("artifact_contract")
        wp=plan.get("workload_plan")
        if (not isinstance(pd,str) or len(pd)!=64 or
                not all(c in "0123456789abcdefABCDEF" for c in pd) or
                pi!=("v5plan_"+pd[:16]) or
                plan.get("execution_backend")!="real_cluster" or
                plan.get("no_fallback") is not True or
                plan.get("artifact_contract_version")!=2 or
                not isinstance(contract,list) or not contract or
                not isinstance(wp,dict) or wp.get("no_fallback") is not True or
                not isinstance(nodes_for_go,list) or len(nodes_for_go)<2):
            raise ValueError("invalid Go-supervisor compiled run plan provenance/contract")
        node_ids=[node.get("node_id") if isinstance(node,dict) else None for node in nodes_for_go]
        if (any(not isinstance(x,str) or not x for x in node_ids) or
                len(node_ids)!=len(set(node_ids))):
            raise ValueError("invalid Go-supervisor compiled node inventory")
        expected=set(node_ids)
        if any(not isinstance(n.get("validators"),list) or
               len(n["validators"])!=len(expected) or
               set(n["validators"])!=expected for n in nodes_for_go):
            raise ValueError("Go-supervisor optme-global PBFT validator inventory invalid")
    nodes=plan.get('node_configs')
    if not isinstance(nodes,list) or len(nodes)<2:
        raise ValueError('compiled plan node_configs missing or too small')
    topo=(plan.get('experiment_spec') or {}).get('topology') or {}
    required_nodes=topo.get('nodes')
    if required_nodes is not None and (isinstance(required_nodes,bool) or int(required_nodes)!=len(nodes)):
        raise ValueError('compiled plan topology node count mismatch')
    home={}
    for node in nodes:
        if not isinstance(node,dict):raise ValueError('invalid compiled node config')
        node_id=str(node.get('node_id') or '').strip()
        execution=str(node.get('execution_shard_id') or '').strip()
        ordering=str(node.get('consensus_domain_id') or '').strip()
        declared=str(node.get('shard_id') or '').strip()
        if not node_id or '/' in node_id or '\\' in node_id or node_id in home:
            raise ValueError('compiled plan missing/duplicate node identity')
        if not execution or execution==GLOBAL or not execution.startswith('s'):
            raise ValueError('compiled plan missing concrete execution/Home partition')
        if ordering!=GLOBAL or declared!=GLOBAL:
            raise ValueError('compiled plan is not single optme-global consensus')
        profile=node.get('plugin_profile') or {}
        be=(profile.get('block_executor') or {}).get('plugin_id')
        ss=(profile.get('state_storage') or {}).get('plugin_id')
        if be!='stateless_optme_block_executor' or ss!='optme_partition_state_store':
            raise ValueError('compiled node is not stateless OptME partition-owned state')
        home[node_id]=execution
    found=[p.parent.name for p in files]
    if len(found)!=len(set(found)) or set(found)!=set(home):
        raise ValueError('OptME v17 evidence must cover every compiled node exactly once')
    counts=Counter(home.values())
    if len(counts)<2:
        raise ValueError('OptME Home proof requires at least two distinct storage partitions')
    validators=topo.get('validators_per_shard')
    if validators is not None:
        if isinstance(validators,bool) or int(validators)<1 or any(v!=int(validators) for v in counts.values()):
            raise ValueError('OptME Home replica counts do not match compiled topology')
    for p in files:
        if p.name!='business_state_commitments_v17.json' or p.parent.parent.name!='nodes':
            raise ValueError('unexpected v17 commitment evidence path')
    return home
