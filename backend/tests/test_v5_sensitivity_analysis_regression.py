from backend.app.services.v5_paper_exporter import paper_result_analysis

def test_workload_sensitivity_does_not_flatten_theta_into_method_mean():
    group={"run_group_id":"v5grp_test","total_child_runs":13,"fairness_validation":{"status":"passed","performance_comparison_valid":True},"performance_comparison_valid":True}
    child={"child_run_id":"v5child_theta_0","suite_type":"workload_sensitivity","method_config_id":"hash_serial","method":{"display_name":"Serial"},"status":"failed","execution_status":"failed","workload_point":{"target_theta":0.0},"metrics":{},"result":{"summary":{}}}
    analysis=paper_result_analysis(group,[child])
    assert analysis["analysis_mode"]=="workload_sensitivity_by_scan_point"
    assert analysis["metrics"]=={} and analysis["observed_metrics"]=={}
    assert analysis["partial_run_group"] is True
    assert analysis["planned_child_count"]==13 and analysis["persisted_child_count"]==1
