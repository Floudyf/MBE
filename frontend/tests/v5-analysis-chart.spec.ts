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
  method_names: ["Method A"],
  method_ids: ["method-a"],
  source_label: "user",
  tags: [],
  is_test: false,
};

function row(overrides: Record<string, unknown> = {}) {
  return { method_id: "method-a", method_name: "Method A", scan_value: "20", sample_count: 1, mean_tps: 12.5, mean_p99_ms: 4.5, ci95_low_tps: null, ci95_high_tps: null, ...overrides };
}

async function openResults(page: Page, charts: unknown[], groups: unknown[]) {
  await page.route("**/api/v5/formal/**", async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname.endsWith("/summaries")) return route.fulfill({ json: { items: [group], total: 1, next_cursor: null } });
    if (url.pathname.endsWith(`/run-groups/${group.run_group_id}/metrics`)) return route.fulfill({ json: {} });
    if (url.pathname.endsWith(`/run-groups/${group.run_group_id}/artifacts`)) return route.fulfill({ json: { run_group_id: group.run_group_id, status: "ready", bundle_ready: false, bundle_size_bytes: 0, file_count: 0, files: [] } });
    if (url.pathname.endsWith(`/run-groups/${group.run_group_id}/analysis`)) return route.fulfill({ json: { run_group_id: group.run_group_id, charts, groups } });
    if (url.pathname.endsWith(`/run-groups/${group.run_group_id}`)) return route.fulfill({ json: { group, children: [] } });
    return route.fulfill({ status: 404, json: { detail: "unexpected V5 formal endpoint" } });
  });
  await page.goto("/");
  await page.getByTestId("primary-navigation").getByRole("button", { name: "③ 结果与产物" }).click();
  await expect(page.getByTestId("v5-analysis-panel")).toBeVisible();
}

test("renders a bar SVG using analysis response values and preserves missing CI", async ({ page }) => {
  await openResults(page, [{ suite_type: "comparison_experiment", kind: "bar", rows: [row({ mean_tps: 12.5 })] }], [row({ mean_tps: 12.5 })]);
  const chart = page.getByTestId("v5-analysis-bar-chart");
  await expect(chart).toBeVisible();
  await expect(chart.locator("rect")).toHaveCount(1);
  await expect(chart).toContainText("12.50");
  await expect(page.getByTestId("v5-analysis-table")).toContainText("— - —");
  await expect(page.getByTestId("v5-analysis-table")).not.toContainText("0.00 - 0.00");
});

test("renders a line SVG from scan points and TPS values", async ({ page }) => {
  const rows = [row({ scan_value: "20", mean_tps: 10 }), row({ scan_value: "80", mean_tps: 25 })];
  await openResults(page, [{ suite_type: "workload_sensitivity", kind: "line", rows }], rows);
  const chart = page.getByTestId("v5-analysis-line-chart");
  await expect(chart).toBeVisible();
  await expect(chart.locator("polyline")).toHaveCount(1);
  await expect(chart.locator("circle")).toHaveCount(2);
  await expect(chart).toContainText("20");
  await expect(chart).toContainText("80");
  await expect(chart).toContainText("10.00");
  await expect(chart).toContainText("25.00");
});

test("summary analysis does not render a fabricated SVG chart", async ({ page }) => {
  await openResults(page, [{ suite_type: "main_experiment", kind: "summary", rows: [row()] }], [row()]);
  await expect(page.getByTestId("v5-analysis-panel")).toContainText("不绘制虚假趋势线");
  await expect(page.getByTestId("v5-analysis-bar-chart")).toHaveCount(0);
  await expect(page.getByTestId("v5-analysis-line-chart")).toHaveCount(0);
});
