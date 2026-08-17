import { expect, test } from "@playwright/test";
import { selectOnlyMethod, selectOnlySuite } from "./v5-formal-test-helpers";

test("saves a V5 method profile and executes the complete Formal workflow", async ({ page, request }) => {
  test.setTimeout(180_000);
  const methodName = `V5 E2E Method ${Date.now()}`;
  let configId = "";

  try {
    await page.goto("/?e2e=1");
    await expect(page.getByTestId("v5-method-design-page")).toBeVisible();
    const navigation = page.getByTestId("primary-navigation");
    await expect(navigation.getByRole("button", { name: "① 实验设计", exact: true })).toBeVisible();
    await expect(navigation.getByRole("button", { name: "② 运行实验", exact: true })).toBeVisible();
    await expect(navigation.getByRole("button", { name: "③ 结果与产物", exact: true })).toBeVisible();
    await expect(navigation.getByRole("button", { name: "负载库", exact: true })).toBeVisible();
    await expect(navigation.getByRole("button", { name: "真实性边界", exact: true })).toBeVisible();
    await expect(navigation.getByRole("button", { name: "高级功能", exact: true })).toBeVisible();
    await expect(navigation).not.toContainText("V5 Real Cluster");
    await expect(navigation).not.toContainText("V3 Composer");
    for (const label of ["nodes", "shards", "tx_count", "seed", "repeats"]) await expect(page.getByLabel(label)).toHaveCount(0);

    await page.getByTestId("v5-method-name").fill(methodName);
    await page.getByTestId("v5-method-description").fill("real V5 E2E profile");
    for (const [category, plugin] of [["routing", "metatrack_coaccess_routing"], ["execution", "dual_track_execution"], ["scheduler", "fast_first_scheduler"], ["commit", "commutative_hot_update_aggregation"]]) {
      await page.getByTestId(`v5-method-category-${category}`).locator("select").selectOption(plugin);
    }
    await page.getByTestId("v5-method-validate").click();
    await expect(page.getByTestId("v5-method-compatibility").locator("strong")).toHaveText("true");
    await expect(page.getByTestId("v5-method-compatibility").locator(".file-error")).toHaveCount(0);

    const saveResponse = page.waitForResponse((response) => response.url().endsWith("/api/v3/saved-configs") && response.request().method() === "POST");
    await page.getByTestId("v5-method-save-and-run").click();
    configId = String((await (await saveResponse).json()).config_id ?? "");
    expect(configId).toMatch(/^v3cfg_/);
    await expect(page.getByTestId("v5-formal-run-page")).toBeVisible();
    await expect(page.getByTestId("v5-run-preferred-method")).not.toContainText(configId);
    await expect(page.getByTestId("v5-run-preferred-method")).not.toContainText(methodName);
    await expect(page.getByTestId(`v5-run-method-${configId}`)).toHaveCount(0);
    await expect(page.getByTestId("v5-run-method-v5_catalog_default")).toHaveCount(0);
    await selectOnlySuite(page, "main_experiment");
    await selectOnlyMethod(page, "hash_block_stm");

    await page.getByLabel("nodes").fill("8"); await page.getByLabel("shards").fill("2"); await page.getByLabel("validators per shard").fill("4");
    await page.getByLabel("tx_count").fill("40"); await page.getByLabel("cross_shard_ratio").fill("0.25"); await page.getByLabel("seeds").fill("47"); await page.getByLabel("repeats").fill("1");
    await page.getByTestId("v5-formal-preview-button").click();
    await expect(page.getByTestId("v5-formal-preview-summary")).toContainText("矩阵行数：1");
    const previewRow = page.locator(`[data-method-config-id="hash_block_stm"]`);
    await expect(previewRow).toHaveCount(1);
    await expect(previewRow.locator("td").nth(1)).toContainText("Block-STM");
    await expect(previewRow.locator("td").nth(2)).toHaveText("synthetic");
    await expect(previewRow.locator("td").nth(5)).toHaveText("40");
    await expect(previewRow.locator("td").nth(9)).toHaveText("100");
    await expect(previewRow.locator("td").nth(10)).toHaveText("75");
    await expect(previewRow.locator("td").nth(11)).toHaveText("1");
    await expect(previewRow.locator(".matrix-compatibility-cell")).toContainText("可运行");
    await expect(page.getByTestId("v5-start-run-group-button")).toBeEnabled();
    await page.getByTestId("v5-start-run-group-button").click();
    await expect.poll(async () => page.getByTestId("v5-formal-group-summary").innerText(), { timeout: 180_000 }).toContain("已完成");
    const groupId = await page.locator("code").filter({ hasText: "v5grp_" }).innerText();

    await page.getByRole("button", { name: "查看结果与产物" }).click();
    await expect(page.getByTestId("v5-results-page")).toBeVisible();
    const legacy = page.locator("details.v5-legacy-results-details");
    await legacy.locator("summary").first().click();
    await expect(legacy).toHaveAttribute("open", "");
    await expect(page.getByTestId("v5-group-rungroup").locator("dd")).toHaveText(groupId);
    await expect(page.getByTestId("v5-group-backend").locator("dd")).toHaveText("真实集群");
    await expect(page.getByTestId("v5-group-children").locator("dd")).toHaveText("1/1");
    const child = page.getByTestId("v5-child-table").locator("tbody tr").first().locator("td");
    await expect(child.nth(2)).toContainText("Block-STM"); await expect(child.nth(7)).toHaveText("已完成");
    await child.nth(0).getByRole("button").click();
    await expect(page.getByTestId("v5-metric-method-config-id").locator("dd")).toHaveText("hash_block_stm");
    await expect(page.getByTestId("v5-metric-terminal").locator("dd")).toHaveText("40"); await expect(page.getByTestId("v5-metric-incomplete").locator("dd")).toHaveText("0");
    await expect(page.getByTestId("v5-metric-cross-requested").locator("dd")).toHaveText("10"); await expect(page.getByTestId("v5-metric-cross-finalized").locator("dd")).toHaveText("10");
    await expect(page.getByTestId("v5-metric-orphan-processes").locator("dd")).toHaveText("0"); await expect(page.getByTestId("v5-metric-no-fallback").locator("dd")).toHaveText("是"); await expect(page.getByTestId("v5-metric-state-root-consistent").locator("dd")).toHaveText("是");
    await expect(page.getByTestId("v5-bundle-ready").locator("dd")).toHaveText("是");

    await navigation.getByRole("button", { name: "高级功能", exact: true }).click();
    const advanced = page.getByTestId("advanced-navigation");
    for (const entry of ["V5 真实集群单次调试", "V3 Composer（历史兼容）", "V4 真实性验证（历史）", "V1/V2 运行记录与产物（历史）"]) await expect(advanced.getByText(entry, { exact: true })).toBeVisible();
    const workbench = advanced.locator("article").filter({ hasText: "V5 真实集群单次调试" });
    await workbench.getByRole("button", { name: "进入", exact: true }).click();
    await expect(page.getByText("V5.1 Real Cluster", { exact: true })).toBeVisible();
  } finally {
    if (configId) await request.delete(`/api/v3/saved-configs/${configId}`);
  }
});
