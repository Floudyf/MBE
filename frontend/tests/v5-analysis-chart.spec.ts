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
  await page.getByTestId("primary-navigation").getByRole("button").nth(2).click();
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

  for (const label of ["Download CSV", "Download SVG", "Download PNG", "Download PDF"]) {
    await expect(page.getByTestId("v5-paper-chart-end_to_end_tps").getByRole("button", { name: label })).toBeVisible();
    await expect(page.getByTestId("v5-paper-chart-p99_finality_ms").getByRole("button", { name: label })).toBeVisible();
  }
});

test("downloads formal chart data and image artifacts", async ({ page }) => {
  await openResults(page);
  const chart = page.getByTestId("v5-paper-chart-end_to_end_tps");
  const expectations = [
    { label: "Download CSV", filename: "end_to_end_tps.csv", marker: "method_id,method_name,metric" },
    { label: "Download SVG", filename: "end_to_end_tps.svg", marker: "<svg" },
    { label: "Download PNG", filename: "end_to_end_tps.png", marker: "\u0089PNG" },
    { label: "Download PDF", filename: "end_to_end_tps.pdf", marker: "%PDF" },
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
  await expect(page.getByTestId("v5-artifact-group-core-analysis")).toContainText("paper_analysis");
  await expect(page.getByTestId("v5-artifact-group-core-analysis")).toContainText("Download");
  await expect(page.getByTestId("v5-artifact-group-core-analysis").getByRole("link", { name: "Download" }).first()).toHaveAttribute("href", /\/artifacts\/paper_figure_data\.csv$/);
  await expect(page.getByTestId("v5-artifact-group-core-analysis")).not.toContainText("workload_evidence");
  await expect(page.getByTestId("v5-artifact-group-aggregate-metrics")).not.toHaveAttribute("open", "");
  await expect(page.getByTestId("v5-artifact-group-client-workload-evidence")).not.toHaveAttribute("open", "");

  await page.getByTestId("v5-artifact-group-aggregate-metrics").locator("summary").click();
  await expect(page.getByTestId("v5-artifact-group-aggregate-metrics")).toContainText("aggregate_metric");
  await expect(page.getByTestId("v5-artifact-group-aggregate-metrics")).toContainText("node_physical_and_replica_deduplicated");
  await page.getByTestId("v5-artifact-group-node-mechanism-evidence").locator("summary").click();
  await expect(page.getByTestId("v5-artifact-group-node-mechanism-evidence")).toContainText("node_mechanism_evidence");
  await page.getByTestId("v5-artifact-group-client-workload-evidence").locator("summary").click();
  await expect(page.getByTestId("v5-artifact-group-client-workload-evidence")).toContainText("workload_evidence");
  await expect(page.getByTestId("v5-artifact-group-client-workload-evidence")).toContainText("canonical_workload_selection");
  await page.getByTestId("v5-artifact-group-logs-audit").locator("summary").click();
  await expect(page.getByTestId("v5-artifact-group-logs-audit")).toContainText("audit_log");
});
