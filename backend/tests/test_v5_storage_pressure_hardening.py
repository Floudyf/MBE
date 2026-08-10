import json
import pytest
from backend.app.services import v5_real_cluster_runner as runner

def test_disk_pressure_evidence_uses_explicit_threshold(tmp_path, monkeypatch):
    class Usage:
        total=1000; used=900; free=100
    monkeypatch.setattr(runner.shutil,"disk_usage",lambda _path:Usage())
    e=runner._disk_pressure_evidence(tmp_path,threshold_bytes=101,phase="test")
    assert e["below_threshold"] is True and e["free_bytes"]==100 and e["threshold_bytes"]==101

def test_start_disk_guard_raises_typed_resource_pressure_without_rewriting_artifacts(tmp_path, monkeypatch):
    monkeypatch.setattr(runner,"_threshold_bytes",lambda _name,_default:101)
    monkeypatch.setattr(runner,"_disk_pressure_evidence",lambda _path,threshold_bytes,phase:{
        "schema_version":"mbe_v5_disk_pressure_guard_v1","phase":phase,"free_bytes":100,
        "threshold_bytes":threshold_bytes,"below_threshold":True,
    })
    with pytest.raises(runner.V5ResourcePressureError) as caught:
        runner._ensure_start_disk_capacity(tmp_path,"v5_disk_guard")
    assert caught.value.run_id=="v5_disk_guard"
    assert caught.value.evidence["phase"]=="before_child_start"
    assert (tmp_path/"resource_guard.json").is_file()
    assert not hasattr(runner,"_compact_runtime_artifacts")

def test_bundle_failure_removes_partial_zip(tmp_path, monkeypatch):
    from backend.app.services import v5_formal_scheduler as scheduler
    group={"run_group_id":"v5grp_bundle_partial"}
    writes=[]
    monkeypatch.setattr(scheduler,"write_group",lambda payload:writes.append(dict(payload)))
    def fail_bundle(directory, _group):
        (directory/"artifacts.zip").write_bytes(b"partial")
        raise OSError("simulated bundle failure")
    monkeypatch.setattr(scheduler,"build_bundle",fail_bundle)
    assert scheduler._try_build_bundle(tmp_path,group) is False
    assert not (tmp_path/"artifacts.zip").exists()
    assert group["bundle_status"]=="failed" and "simulated bundle failure" in group["bundle_error"]
    assert writes[-1]["bundle_status"]=="failed"

