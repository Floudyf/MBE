from __future__ import annotations
import json, shutil
from collections import Counter
from pathlib import Path

from backend.app.services import v5_workload_data_plane as plane


def _source_row() -> dict:
    keys=["k/read","k/write","k/rmw"]
    return {
        "schema_version":"mbe_workload_record_v2","record_id":"aw:00000000:closure","source_order":0,
        "source_transaction_id":"a"*64,"sender_id":"alice","receiver_id":"m.federation","operation_type":"alien_worlds_mine",
        "static_access_envelope":{"keys":keys,"complete":True,"policy_id":"controlled","construction_method":"CONTROLLED_STATIC_WORKLOAD_SPEC"},
        "declared_scheduling_evidence":{"policy_id":"aw_controlled_declared_scheduling_v2_program_semantics","items":[
            {"key":keys[0],"access_mode":"R","update_semantic":"NONE"},
            {"key":keys[1],"access_mode":"W","update_semantic":"SET"},
            {"key":keys[2],"access_mode":"RW","update_semantic":"RMW"},
        ]},
        "routing_source_key":keys[2],"routing_target_key":keys[1],"skew_keys":keys,
        "workload_metadata":{"target_access_theta":0.6},"provenance":{"source_kind":"controlled_static_workload","package_version":"fixture"},
    }


def test_finite_zipf_theta_uses_transaction_key_touch_counts() -> None:
    uniform=plane._finite_zipf_metrics(Counter({f"k{i}":10 for i in range(20)}))
    assert abs(float(uniform["theta"])) < 1e-8
    skewed=plane._finite_zipf_metrics(Counter({"k1":100,"k2":50,"k3":25,"k4":12,"k5":6}))
    assert float(skewed["theta"]) > 0.5
    assert skewed["touches"] == 193


def test_canonical_publish_race_becomes_cache_hit(tmp_path: Path, monkeypatch) -> None:
    source=tmp_path/"input.jsonl"; source.write_text(json.dumps(_source_row())+"\n",encoding="utf-8")
    manifest={"dataset_id":"race_fixture","adapter_id":"alien_worlds_layered_v2","row_count":1,"source_sha256":""}
    original=plane.os.replace
    fired={"value":False}
    def race_replace(src, dst):
        dst=Path(dst)
        if not fired["value"] and dst.parent.name=="canonical":
            fired["value"]=True
            shutil.copytree(src,dst)
            raise PermissionError(5,"simulated competing publisher")
        return original(src,dst)
    monkeypatch.setattr(plane.os,"replace",race_replace)
    result=plane.build_canonical(source,tmp_path/"cache",manifest)
    assert fired["value"] is True
    assert result["cache_hit"] is True
    assert (tmp_path/"cache"/result["canonical_relative_path"]).is_file()


def test_materialized_publish_race_becomes_cache_hit(tmp_path: Path, monkeypatch) -> None:
    source=tmp_path/"input.jsonl"; source.write_text(json.dumps(_source_row())+"\n",encoding="utf-8")
    manifest={"dataset_id":"race_fixture","adapter_id":"alien_worlds_layered_v2","row_count":1,"source_sha256":""}
    canonical=plane.build_canonical(source,tmp_path/"cache",manifest)
    cpath=tmp_path/"cache"/canonical["canonical_relative_path"]
    original=plane.os.replace; fired={"value":False}
    def race_replace(src,dst):
        dst=Path(dst)
        if not fired["value"] and dst.parent.name=="materialized":
            fired["value"]=True; shutil.copytree(src,dst); raise PermissionError(5,"simulated competing publisher")
        return original(src,dst)
    monkeypatch.setattr(plane.os,"replace",race_replace)
    summary=plane.materialize(cpath,tmp_path/"cache",dataset_id="race_fixture",source_sha256=canonical["source_sha256"],requested_tx_count=1,seed=1,supported_counts={1},variant_parameters={"target_theta":0.6},truth_label="real_derived_controlled")
    assert fired["value"] is True
    assert summary["cache_hit"] is True


def test_preview_keeps_generic_measured_access_theta() -> None:
    import inspect
    source = inspect.getsource(plane.preview_workload)
    assert 'if selected_window_preview.get("measured_access_theta") is None:' in source
    assert 'selected_window_preview["measured_access_theta"] = measured_account_touch_theta' in source
