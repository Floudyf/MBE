import { expect, test, type APIRequestContext } from "@playwright/test";

type Summary = {
  run_group_id: string;
  status: string;
  plan_name: string;
  execution_backend: string;
  runtime_truth: string;
  total_child_runs: number;
  completed_child_runs: number;
  failed_child_runs: number;
  method_names: string[];
  method_ids: string[];
  suite_names: string[];
  source_label: "user" | "e2e" | "script";
  tags: string[];
  is_test: boolean;
};

async function testRecord(request: APIRequestContext): Promise<Summary> {
  const read = async () => {
    const response = await request.get("/api/v5/formal/run-groups/summaries?limit=20&include_tests=true");
    expect(response.ok()).toBeTruthy();
    return (await response.json()).items as Summary[];
  };
  const existing = (await read()).find((item) => item.is_test);
  if (existing) return existing;

  const catalogResponse = await request.get("/api/v5/plugins?backend=real_cluster");
  expect(catalogResponse.ok()).toBeTruthy();
  const catalog = (await catalogResponse.json()).items as Array<{ category: string; plugin_id: string; default_config: Record<string, unknown> }>;
  const pluginSelections = Array.from(new Map(catalog.map((item) => [item.category, item])).values()).map((item) => ({
    category: item.category,
    plugin_id: item.plugin_id,
    config: { ...item.default_config },
  }));
  const workload = pluginSelections.find((item) => item.category === "workload");
  expect(workload).toBeTruthy();
  workload!.plugin_id = "deterministic_signed_synthetic";
  workload!.config = { ...workload!.config, cross_shard_ratio: 0, timeout_every: 0 };
  const created = await request.post("/api/v5/formal/run-groups", {
    data: {
      execution_backend: "real_cluster",
      plan: {
        name: "v5_results_history_e2e",
        base_spec: {
          name: "v5_results_history_e2e",
          execution_backend: "real_cluster",
          plugin_selections: pluginSelections,
          topology: { nodes: 4, shards: 1, validators_per_shard: 4 },
          tx_count: 20,
          seed: 11,
          duration_ms: 6000,
          fault_policy: { mode: "disabled" },
          requested_metrics: [],
        },
        suites: ["main_experiment"],
        methods: [{ method_id: "v5_catalog_default", display_name: "V5 Catalog Default", plugin_overrides: {}, role: "main" }],
        seeds: [11],
        repeats: 1,
        source_label: "e2e",
        tags: ["e2e"],
      },
    },
  });
  expect(created.ok()).toBeTruthy();
  const groupId = (await created.json()).run_group_id as string;
  await expect.poll(async () => {
    const detail = await request.get(`/api/v5/formal/run-groups/${groupId}`);
    return detail.ok() ? (await detail.json()).group.status : "";
  }, { timeout: 180_000 }).toMatch(/^(completed|completed_with_failures|failed|cancelled)$/);
  const record = (await read()).find((item) => item.run_group_id === groupId && item.is_test);
  expect(record).toBeTruthy();
  return record!;
}

function summaryUrl(url: URL, groupId: string, includeTests: string) {
  return url.pathname.endsWith("/api/v5/formal/run-groups/summaries")
    && url.searchParams.get("search") === groupId
    && url.searchParams.get("include_tests") === includeTests;
}

function summaryRequest(response: import("@playwright/test").Response, groupId: string, includeTests: string) {
  return summaryUrl(new URL(response.url()), groupId, includeTests);
}

test("results history is collapsed, filters real summaries, and keeps current detail above it", async ({ page, request }) => {
  const record = await testRecord(request);
  await page.goto("/?e2e=1");
  await page.getByTestId("primary-navigation").getByRole("button", { name: "③ 结果与产物" }).click();
  const history = page.getByTestId("v5-run-group-list");
  await expect(history).not.toHaveAttribute("open", "");
  await expect(page.getByTestId("v5-results-page")).toBeVisible();
  await history.locator("summary").click();
  const tests = history.getByRole("checkbox", { name: "显示测试记录" });
  await expect(tests).not.toBeChecked();

  const hiddenResponse = page.waitForResponse((response) => summaryRequest(response, record.run_group_id, "false"));
  await history.getByLabel("搜索").fill(record.run_group_id);
  expect((await hiddenResponse).ok()).toBeTruthy();
  await expect(history.locator("tbody tr")).toHaveCount(0);

  const visibleResponse = page.waitForResponse((response) => summaryRequest(response, record.run_group_id, "true"));
  await tests.check();
  const visiblePayload = await (await visibleResponse).json() as { items: Summary[] };
  expect(visiblePayload.items).toHaveLength(1);
  expect(visiblePayload.items[0].run_group_id).toBe(record.run_group_id);
  expect(visiblePayload.items[0].is_test).toBe(true);
  await expect(history.locator("tbody tr")).toHaveCount(1);
  await expect(history.locator("tbody tr").first().getByTestId("v5-run-group-select")).toHaveText(record.run_group_id);

  await history.getByTestId("v5-run-group-select").click();
  await expect(page.getByTestId("v5-group-rungroup").locator("dd")).toHaveText(record.run_group_id);
  await history.getByLabel("状态").selectOption(record.status);
  await expect(history.locator("tbody tr")).toHaveCount(1);
  if (record.method_ids[0]) {
    await history.getByLabel("方法 ID").fill(record.method_ids[0]);
    await expect(history.locator("tbody tr")).toHaveCount(1);
  }
  if (record.suite_names[0]) {
    await history.getByLabel("实验类型").selectOption(record.suite_names[0]);
    await expect(history.locator("tbody tr")).toHaveCount(1);
  }
  const pageOrder = await page.evaluate(() => ({
    summary: document.querySelector('[data-testid="v5-group-summary"]')?.getBoundingClientRect().top ?? -1,
    history: document.querySelector('[data-testid="v5-run-group-list"]')?.getBoundingClientRect().top ?? -1,
  }));
  expect(pageOrder.summary).toBeLessThan(pageOrder.history);
});

test("latest history response wins when include-tests changes quickly", async ({ page }) => {
  const record: Summary = {
    run_group_id: "v5grp_history_race_e2e",
    status: "completed",
    plan_name: "history race",
    execution_backend: "real_cluster",
    runtime_truth: "v5_real_cluster_candidate",
    total_child_runs: 1,
    completed_child_runs: 1,
    failed_child_runs: 0,
    method_names: ["V5 Catalog Default"],
    method_ids: ["v5_catalog_default"],
    suite_names: ["main_experiment"],
    source_label: "e2e",
    tags: ["e2e"],
    is_test: true,
  };
  await page.route("**/api/v5/formal/run-groups/summaries**", async (route) => {
    const url = new URL(route.request().url());
    if (url.searchParams.get("include_tests") === "false" && url.searchParams.get("search") === record.run_group_id) {
      await new Promise((resolve) => setTimeout(resolve, 500));
      await route.fulfill({ contentType: "application/json", body: JSON.stringify({ items: [], total: 0, next_cursor: null }) });
      return;
    }
    if (url.searchParams.get("include_tests") === "false") {
      await route.fulfill({ contentType: "application/json", body: JSON.stringify({ items: [], total: 0, next_cursor: null }) });
      return;
    }
    await route.fulfill({ contentType: "application/json", body: JSON.stringify({ items: [record], total: 1, next_cursor: null }) });
  });
  await page.goto("/");
  await page.getByTestId("primary-navigation").getByRole("button", { name: "③ 结果与产物" }).click();
  const history = page.getByTestId("v5-run-group-list");
  await history.locator("summary").click();
  const staleRequest = page.waitForRequest((request) => summaryUrl(new URL(request.url()), record.run_group_id, "false"));
  const staleResponse = page.waitForResponse((response) => summaryRequest(response, record.run_group_id, "false"));
  const visibleResponse = page.waitForResponse((response) => summaryRequest(response, record.run_group_id, "true"));
  await history.getByLabel("搜索").fill(record.run_group_id);
  await staleRequest;
  await history.getByRole("checkbox", { name: "显示测试记录" }).check();
  expect((await visibleResponse).ok()).toBeTruthy();
  await expect(history.getByTestId("v5-run-group-select")).toHaveText(record.run_group_id);
  expect((await staleResponse).ok()).toBeTruthy();
  await expect(history.getByTestId("v5-run-group-select")).toHaveText(record.run_group_id);
});
