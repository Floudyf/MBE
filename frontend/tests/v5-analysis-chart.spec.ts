import { readFileSync } from "node:fs";

import { expect, test, type Page } from "@playwright/test";

const group = {
  run_group_id: "v5grp_analysis_chart",
  status: "completed",
  plan_name: "analysis chart",
  execution_backend: "real_cluster",
  runtime_truth: "v5_real_cluster_candidate",
  created_at: "2026-01-01T00:00:00Z",
  updated_at: "2026-01-01T00:00:00Z",
  total_child_runs: 1,
  completed_child_runs: 1,
  failed_child_runs: 0,
  suite_names: ["comparison_experiment"],
  method_names: ["Baseline"],
  method_ids: ["hash_serial"],
  source_label: "user",
  tags: [],
  is_test: false,
};

function paperRow(metric: string, mean: number) {
  return {
    method_id: "hash_serial",
    method_name: "Baseline",
    metric,
    metric_unit: metric === "end_to_end_tps" ? "tps" : "ms",
    valid_sample_count: 1,
    excluded_sample_count: 0,
    raw_values: [mean],
    mean,
    median: mean,
    std: null,
    min: mean,
    max: mean,
    ci95_low: null,
    ci95_high: null,
    statistical_note: "single_sample_no_variance_or_ci",
    source_child_ids: ["v5child_chart"],
  };
}

async function openResults(page: Page) {
  const paper = {
    schema_version: "mbe_paper_result_analysis_v1",
    run_group_id: group.run_group_id,
    analysis_status: "complete",
    fairness_status: "passed",
    metrics: {
      end_to_end_tps: [paperRow("end_to_end_tps", 12.5)],
      p95_finality_ms: [paperRow("p95_finality_ms", 20)],
      p99_finality_ms: [paperRow("p99_finality_ms", 42)],
    },
    excluded_samples: [],
  };
  const artifactCatalog = {
    run_group_id: group.run_group_id,
    status: "ready",
    bundle_ready: true,
    bundle_size_bytes: 4096,
    file_count: 6,
    files: [
      { name: "paper_figure_data.csv", size_bytes: 100, artifact_role: "paper_analysis", truth_scope: "formal_run_group_analysis", producer: "v5_paper_exporter", schema_version: "mbe_v5_paper_analysis_v1", download_url: "/api/v5/formal/run-groups/v5grp_analysis_chart/artifacts/paper_figure_data.csv" },
      { name: "aggregate/remote_state_metrics_summary.json", size_bytes: 200, artifact_role: "aggregate_metric", truth_scope: "node_physical_and_replica_deduplicated", producer: "v5_metric_aggregator", schema_version: "mbe_remote_state_metrics_v2", download_url: "/api/v5/formal/run-groups/v5grp_analysis_chart/artifacts/aggregate/remote_state_metrics_summary.json" },
      { name: "children/v5child_chart.json", size_bytes: 300, artifact_role: "child_experiment_record", truth_scope: "child_run_metadata", producer: "v5_formal_scheduler", schema_version: "mbe_v5_child_record_v1", download_url: "/api/v5/formal/run-groups/v5grp_analysis_chart/artifacts/children/v5child_chart.json" },
      { name: "nodes/n0/block_stm_summary.json", size_bytes: 400, artifact_role: "node_mechanism_evidence", truth_scope: "node_physical", producer: "v5_real_cluster_runtime", schema_version: "mbe_v5_node_artifact_v1", download_url: "/api/v5/formal/run-groups/v5grp_analysis_chart/artifacts/nodes/n0/block_stm_summary.json" },
      { name: "workload/workload_selection.json", size_bytes: 450, artifact_role: "workload_evidence", truth_scope: "canonical_workload_selection", producer: "v5_workload_data_plane", schema_version: "mbe_workload_selection_v1", download_url: "/api/v5/formal/run-groups/v5grp_analysis_chart/artifacts/workload/workload_selection.json" },
      { name: "logs/supervisor_stdout.log", size_bytes: 500, artifact_role: "audit_log", truth_scope: "supervisor_process", producer: "mbe_supervisor", schema_version: "mbe_v5_log_v1", download_url: "/api/v5/formal/run-groups/v5grp_analysis_chart/artifacts/logs/supervisor_stdout.log" },
    ],
  };
  await page.route("**/api/v5/formal/**", async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname.endsWith("/summaries")) return route.fulfill({ json: { items: [group], total: 1, next_cursor: null } });
    if (url.pathname.endsWith(`/run-groups/${group.run_group_id}/metrics`)) return route.fulfill({ json: {} });
    if (url.pathname.endsWith(`/run-groups/${group.run_group_id}/artifacts`)) return route.fulfill({ json: artifactCatalog });
    if (url.pathname.endsWith(`/run-groups/${group.run_group_id}/analysis`)) return route.fulfill({ json: { run_group_id: group.run_group_id, charts: [], groups: [], paper_result_analysis: paper } });
    if (url.pathname.endsWith(`/run-groups/${group.run_group_id}`)) return route.fulfill({ json: { group, children: [] } });
    return route.fulfill({ status: 404, json: { detail: "unexpected V5 formal endpoint" } });
  });
  await page.goto("/");
  await page.getByTestId("primary-navigation").getByRole("button", { name: "③ 结果与产物", exact: true }).click();
  await expect(page.getByTestId("v5-results-page")).toBeVisible();
  const legacy = page.locator("details.v5-legacy-results-details");
  await legacy.locator("summary").first().click();
  await expect(legacy).toHaveAttribute("open", "");
  await expect(page.getByTestId("v5-analysis-panel")).toBeVisible();
}

test("renders exactly the two formal paper charts with n=1 samples", async ({ page }) => {
  await openResults(page);

  await expect(page.getByTestId("v5-paper-chart-end_to_end_tps")).toBeVisible();
  await expect(page.getByTestId("v5-paper-chart-p99_finality_ms")).toBeVisible();
  await expect(page.locator(".analysis-paper-grid .analysis-chart")).toHaveCount(2);
  await expect(page.getByTestId("v5-paper-chart-end_to_end_tps-svg")).toContainText("12.50");
  await expect(page.getByTestId("v5-paper-chart-p99_finality_ms-svg")).toContainText("42.00");
  await expect(page.getByTestId("v5-paper-chart-end_to_end_tps-svg")).toContainText("n=1");
  await expect(page.getByTestId("v5-analysis-panel")).not.toContainText("0.00 - 0.00");
});

test("toggles latency chart between P99 and P95 without changing transaction data table", async ({ page }) => {
  await openResults(page);

  await page.getByTestId("v5-latency-percentile-toggle").getByRole("button", { name: "P95" }).click();
  await expect(page.getByTestId("v5-paper-chart-p95_finality_ms")).toBeVisible();
  await expect(page.getByTestId("v5-paper-chart-p95_finality_ms-svg")).toContainText("20.00");
  await page.getByTestId("v5-latency-percentile-toggle").getByRole("button", { name: "P99" }).click();
  await expect(page.getByTestId("v5-paper-chart-p99_finality_ms-svg")).toContainText("42.00");

  await page.getByTestId("v5-analysis-panel").locator("details summary").click();
  await expect(page.getByTestId("v5-paper-analysis-table")).toContainText("p95_finality_ms");
  await expect(page.getByTestId("v5-paper-analysis-table")).toContainText("p99_finality_ms");
});

test("offers CSV PNG SVG and PDF downloads for each formal chart", async ({ page }) => {
  await openResults(page);

  for (const label of ["下载 CSV", "下载 SVG", "下载 PNG", "下载 PDF"]) {
    await expect(page.getByTestId("v5-paper-chart-end_to_end_tps").getByRole("button", { name: label })).toBeVisible();
    await expect(page.getByTestId("v5-paper-chart-p99_finality_ms").getByRole("button", { name: label })).toBeVisible();
  }
});

test("downloads formal chart data and image artifacts", async ({ page }) => {
  await openResults(page);
  const chart = page.getByTestId("v5-paper-chart-end_to_end_tps");
  const expectations = [
    { label: "下载 CSV", filename: "end_to_end_tps.csv", marker: "method_id,method_name,sample_status" },
    { label: "下载 SVG", filename: "end_to_end_tps.svg", marker: "<svg" },
    { label: "下载 PNG", filename: "end_to_end_tps.png", marker: "\u0089PNG" },
    { label: "下载 PDF", filename: "end_to_end_tps.pdf", marker: "%PDF" },
  ];

  for (const item of expectations) {
    const downloadPromise = page.waitForEvent("download");
    await chart.getByRole("button", { name: item.label }).click();
    const download = await downloadPromise;
    expect(download.suggestedFilename()).toBe(item.filename);
    const path = await download.path();
    expect(path).toBeTruthy();
    const body = readFileSync(path!);
    expect(body.toString("latin1")).toContain(item.marker);
  }
});

test("groups formal artifact catalog by V5.2 truth roles and opens only core analysis", async ({ page }) => {
  await openResults(page);

  await expect(page.getByTestId("v5-artifact-group-core-analysis")).toHaveAttribute("open", "");
  await expect(page.getByTestId("v5-artifact-group-core-analysis")).toContainText("论文分析");
  await expect(page.getByTestId("v5-artifact-group-core-analysis")).toContainText("下载");
  await expect(page.getByTestId("v5-artifact-group-core-analysis").getByRole("link", { name: "下载" }).first()).toHaveAttribute("href", /\/artifacts\/paper_figure_data\.csv$/);
  await expect(page.getByTestId("v5-artifact-group-core-analysis")).not.toContainText("工作负载证据");
  await expect(page.getByTestId("v5-artifact-group-aggregate-metrics")).not.toHaveAttribute("open", "");
  await expect(page.getByTestId("v5-artifact-group-client-workload-evidence")).not.toHaveAttribute("open", "");

  await page.getByTestId("v5-artifact-group-aggregate-metrics").locator("summary").click();
  await expect(page.getByTestId("v5-artifact-group-aggregate-metrics")).toContainText("聚合指标");
  await expect(page.getByTestId("v5-artifact-group-aggregate-metrics")).toContainText("node_physical_and_replica_deduplicated");
  await page.getByTestId("v5-artifact-group-node-mechanism-evidence").locator("summary").click();
  await expect(page.getByTestId("v5-artifact-group-node-mechanism-evidence")).toContainText("节点级机制证据");
  await page.getByTestId("v5-artifact-group-client-workload-evidence").locator("summary").click();
  await expect(page.getByTestId("v5-artifact-group-client-workload-evidence")).toContainText("工作负载证据");
  await expect(page.getByTestId("v5-artifact-group-client-workload-evidence")).toContainText("canonical_workload_selection");
  await page.getByTestId("v5-artifact-group-logs-audit").locator("summary").click();
  await expect(page.getByTestId("v5-artifact-group-logs-audit")).toContainText("审计日志");
});


function v2PaperRow(method_id: string, method_name: string, mean: number, sample_status: "paper_eligible" | "completed_invalid") {
  return {
    method_id,
    method_name,
    metric: "end_to_end_tps",
    metric_unit: "tps",
    valid_sample_count: sample_status === "paper_eligible" ? 1 : 0,
    observed_sample_count: 1,
    excluded_sample_count: sample_status === "paper_eligible" ? 0 : 1,
    sample_status,
    sample_status_counts: { [sample_status]: 1 },
    raw_values: [mean],
    mean,
    median: mean,
    std: null,
    min: mean,
    max: mean,
    ci95_low: null,
    ci95_high: null,
    statistical_note: "single_sample_no_variance_or_ci",
    source_child_ids: [`child-${method_id}`],
  };
}

async function openV2SevenMethodResults(page: Page) {
  const methods = [
    ["hash_serial", "Stateful Hash + Serial", 17.87, "paper_eligible"],
    ["hash_block_stm", "Stateful Hash + Block-STM", 17.90, "paper_eligible"],
    ["hash_aria", "Stateful Hash + Aria", 9.35, "paper_eligible"],
    ["stateless_hash_serial", "Stateless Hash + Serial", 45.15, "completed_invalid"],
    ["stateless_hash_block_stm", "Stateless Hash + Block-STM", 8.69, "completed_invalid"],
    ["metatrack_serial", "MetaTrack + Serial", 17.88, "paper_eligible"],
    ["metatrack_block_stm", "MetaTrack + Block-STM", 12.27, "paper_eligible"],
  ] as const;
  const observed = methods.map(([id, name, mean, status]) => v2PaperRow(id, name, mean, status));
  const eligible = observed.filter((row) => row.sample_status === "paper_eligible");
  const withMetric = (rows: typeof observed, metric: string, unit: string, multiplier: number) => rows.map((row) => ({ ...row, metric, metric_unit: unit, mean: row.mean * multiplier, median: row.mean * multiplier, min: row.mean * multiplier, max: row.mean * multiplier, raw_values: [row.mean * multiplier] }));
  const paper = {
    schema_version: "mbe_paper_result_analysis_v2",
    run_group_id: group.run_group_id,
    analysis_status: "incomplete",
    fairness_status: "passed",
    performance_comparison_valid: true,
    metrics: {
      end_to_end_tps: eligible,
      p95_finality_ms: withMetric(eligible, "p95_finality_ms", "ms", 100),
      p99_finality_ms: withMetric(eligible, "p99_finality_ms", "ms", 110),
    },
    observed_metrics: {
      end_to_end_tps: observed,
      p95_finality_ms: withMetric(observed, "p95_finality_ms", "ms", 100),
      p99_finality_ms: withMetric(observed, "p99_finality_ms", "ms", 110),
    },
    sample_statuses: [],
    status_counts: { execution_failed: 0, blocked_incompatible: 0, completed_invalid: 2, comparison_excluded: 0, paper_eligible: 5 },
    excluded_samples: [
      { child_run_id: "child-stateless_hash_serial", method_id: "stateless_hash_serial", status: "completed_invalid", reasons: ["finalized_not_equal_submitted"] },
      { child_run_id: "child-stateless_hash_block_stm", method_id: "stateless_hash_block_stm", status: "completed_invalid", reasons: ["cross_shard_failed_not_zero"] },
    ],
  };
  await page.route("**/api/v5/formal/**", async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname.endsWith("/summaries")) return route.fulfill({ json: { items: [{ ...group, total_child_runs: 7, completed_child_runs: 7, method_ids: methods.map(([id]) => id), method_names: methods.map(([, name]) => name) }], total: 1, next_cursor: null } });
    if (url.pathname.endsWith(`/run-groups/${group.run_group_id}/metrics`)) return route.fulfill({ json: {} });
    if (url.pathname.endsWith(`/run-groups/${group.run_group_id}/artifacts`)) return route.fulfill({ json: { run_group_id: group.run_group_id, status: "ready", bundle_ready: true, bundle_size_bytes: 1, file_count: 0, files: [] } });
    if (url.pathname.endsWith(`/run-groups/${group.run_group_id}/analysis`)) return route.fulfill({ json: { run_group_id: group.run_group_id, charts: [], groups: [], paper_result_analysis: paper } });
    if (url.pathname.endsWith(`/run-groups/${group.run_group_id}`)) return route.fulfill({ json: { group: { ...group, total_child_runs: 7, completed_child_runs: 7 }, children: [] } });
    return route.fulfill({ status: 404, json: { detail: "unexpected V5 formal endpoint" } });
  });
  await page.goto("/");
  await page.getByTestId("primary-navigation").getByRole("button", { name: "③ 结果与产物", exact: true }).click();
  await expect(page.getByTestId("v5-results-page")).toBeVisible();
  const legacy = page.locator("details.v5-legacy-results-details");
  await legacy.locator("summary").first().click();
  await expect(legacy).toHaveAttribute("open", "");
  await expect(page.getByTestId("v5-analysis-panel")).toBeVisible();
}

test("defaults to paper-valid samples and keeps invalid completed runs distinct in the observed view", async ({ page }) => {
  await openV2SevenMethodResults(page);

  const tpsChart = page.getByTestId("v5-paper-chart-end_to_end_tps-svg");
  await expect(page.getByTestId("v5-analysis-view-toggle").getByRole("button", { name: "论文有效样本" })).toHaveClass(/active/);
  await expect(tpsChart.locator("g[data-sample-status]")).toHaveCount(5);
  await expect(tpsChart).not.toContainText("Stateless Hash");
  await expect(page.getByTestId("v5-analysis-status-counts")).toContainText("2完成但无效");
  await expect(page.getByTestId("v5-analysis-status-counts")).toContainText("0兼容性阻止");
  await expect(page.getByTestId("v5-analysis-status-counts")).toContainText("0执行失败");

  await page.getByTestId("v5-analysis-view-toggle").getByRole("button", { name: "全部运行结果" }).click();
  await expect(page.getByTestId("v5-paper-chart-end_to_end_tps-svg").locator("g[data-sample-status]")).toHaveCount(7);
  await expect(page.getByTestId("v5-paper-chart-end_to_end_tps-svg").locator('g[data-sample-status="completed_invalid"]')).toHaveCount(2);
  await expect(page.getByTestId("v5-paper-chart-end_to_end_tps-svg")).toHaveAttribute("data-chart-width", "866");
});


async function openBatchSIComparisonResults(page: Page) {
  const batchSI = {
    ...v2PaperRow("hash_batch_si", "Batch-SI", 386.4, "paper_eligible"),
    metric: "end_to_end_tps",
    metric_unit: "tps",
  };
  const metatrack = {
    ...v2PaperRow("metatrack_serial", "MetaTrack", 488.28, "paper_eligible"),
    metric: "end_to_end_tps",
    metric_unit: "tps",
  };
  const withMetric = (rows: Array<typeof batchSI>, metric: string, multiplier: number) => rows.map((row) => ({
    ...row,
    metric,
    metric_unit: "ms",
    mean: row.mean * multiplier,
    median: row.mean * multiplier,
    min: row.mean * multiplier,
    max: row.mean * multiplier,
    raw_values: [row.mean * multiplier],
  }));
  const rows = [batchSI, metatrack];
  const paper = {
    schema_version: "mbe_paper_result_analysis_v2",
    run_group_id: group.run_group_id,
    analysis_status: "complete",
    fairness_status: "passed",
    performance_comparison_valid: false,
    metrics: {
      end_to_end_tps: rows,
      p95_finality_ms: withMetric(rows, "p95_finality_ms", 2),
      p99_finality_ms: withMetric(rows, "p99_finality_ms", 2.2),
    },
    observed_metrics: {
      end_to_end_tps: rows,
      p95_finality_ms: withMetric(rows, "p95_finality_ms", 2),
      p99_finality_ms: withMetric(rows, "p99_finality_ms", 2.2),
    },
    sample_statuses: [],
    status_counts: { execution_failed: 0, blocked_incompatible: 0, completed_invalid: 0, comparison_excluded: 0, paper_eligible: 2 },
    excluded_samples: [],
  };
  await page.route("**/api/v5/formal/**", async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname.endsWith("/summaries")) return route.fulfill({ json: { items: [{ ...group, total_child_runs: 2, completed_child_runs: 2, method_ids: ["hash_batch_si", "metatrack_serial"], method_names: ["Batch-SI", "MetaTrack"] }], total: 1, next_cursor: null } });
    if (url.pathname.endsWith(`/run-groups/${group.run_group_id}/metrics`)) return route.fulfill({ json: {} });
    if (url.pathname.endsWith(`/run-groups/${group.run_group_id}/artifacts`)) return route.fulfill({ json: { run_group_id: group.run_group_id, status: "ready", bundle_ready: true, bundle_size_bytes: 1, file_count: 0, files: [] } });
    if (url.pathname.endsWith(`/run-groups/${group.run_group_id}/analysis`)) return route.fulfill({ json: { run_group_id: group.run_group_id, charts: [], groups: [], paper_result_analysis: paper } });
    if (url.pathname.endsWith(`/run-groups/${group.run_group_id}`)) return route.fulfill({ json: { group: { ...group, total_child_runs: 2, completed_child_runs: 2 }, children: [] } });
    return route.fulfill({ status: 404, json: { detail: "unexpected V5 formal endpoint" } });
  });
  await page.goto("/");
  await page.getByTestId("primary-navigation").getByRole("button", { name: "③ 结果与产物", exact: true }).click();
  await expect(page.getByTestId("v5-results-page")).toBeVisible();
  const legacy = page.locator("details.v5-legacy-results-details");
  await legacy.locator("summary").first().click();
  await expect(legacy).toHaveAttribute("open", "");
  await expect(page.getByTestId("v5-analysis-panel")).toBeVisible();
}

test("renders a paper-valid Batch-SI bar with its registered label", async ({ page }) => {
  await openBatchSIComparisonResults(page);

  const chart = page.getByTestId("v5-paper-chart-end_to_end_tps-svg");
  await expect(chart.locator("g[data-sample-status]")).toHaveCount(2);
  await expect(chart).toContainText("Batch-SI");
  await expect(chart).toContainText("386.40");
  await expect(chart.locator('g[data-sample-status="paper_eligible"]')).toHaveCount(2);
});
