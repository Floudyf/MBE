import { expect, test } from "@playwright/test";

test("previews and runs a V5 real-cluster Formal RunGroup", async ({ page }) => {
  test.setTimeout(180_000);
  await page.goto("/");
  await page.locator(".final-sidebar").getByRole("button", { name: "② 运行实验", exact: true }).click();
  await expect(page.getByTestId("v5-formal-run-page")).toBeVisible();
  await expect(page.getByTestId("v5-run-method-v5_catalog_default")).toHaveCount(0);
  for (const methodId of ["hash_serial", "hash_block_stm", "metatrack_serial", "metatrack_block_stm"]) {
    await expect(page.getByTestId(`v5-run-method-${methodId}`).getByRole("checkbox")).toBeChecked();
  }
  await expect(page.getByTestId("v5-formal-preview-button")).toBeEnabled();
  await expect(page.getByLabel("nodes")).toHaveValue("8");
  await expect(page.getByLabel("shards")).toHaveValue("2");
  await expect(page.getByLabel("validators per shard")).toHaveValue("4");
  await page.getByTestId("v5-run-method-hash_block_stm").getByRole("checkbox").uncheck();
  await page.getByTestId("v5-run-method-metatrack_serial").getByRole("checkbox").uncheck();
  await page.getByTestId("v5-run-method-metatrack_block_stm").getByRole("checkbox").uncheck();
  await page.getByTestId("v5-suite-comparison_experiment").getByRole("checkbox").uncheck();
  await page.getByTestId("v5-suite-main_experiment").getByRole("checkbox").check();
  await page.getByLabel("nodes").fill("4");
  await page.getByLabel("shards").fill("1");
  await page.getByLabel("validators per shard").fill("4");
  await page.getByLabel("tx_count").fill("20");
  await page.getByLabel("cross_shard_ratio").fill("0");
  await page.getByLabel("seeds").fill("11");
  await page.getByLabel("repeats").fill("1");
  await page.getByTestId("v5-formal-preview-button").click();
  await expect(page.getByTestId("v5-formal-preview-summary")).toContainText("矩阵行数：1");
  await expect(page.getByText("可运行", { exact: true })).toBeVisible();
  await expect(page.getByTestId("v5-estimated-block-count")).toHaveText("1");
  await expect(page.getByTestId("v5-start-run-group-button")).toBeEnabled();
  await page.getByLabel("block size").fill("10");
  await expect(page.getByTestId("v5-start-run-group-button")).toBeDisabled();
  await page.getByTestId("v5-formal-preview-button").click();
  await expect(page.getByTestId("v5-formal-preview-summary")).toContainText("矩阵行数：1");
  await expect(page.getByTestId("v5-estimated-block-count")).toHaveText("2");
  await expect(page.getByTestId("v5-start-run-group-button")).toBeEnabled();
  await page.getByLabel("block size").fill("100");
  await expect(page.getByTestId("v5-start-run-group-button")).toBeDisabled();
  await page.getByTestId("v5-formal-preview-button").click();
  await expect(page.getByTestId("v5-estimated-block-count")).toHaveText("1");
  await expect(page.getByTestId("v5-start-run-group-button")).toBeEnabled();
  await page.getByLabel("tx_count").fill("21");
  await expect(page.getByTestId("v5-start-run-group-button")).toBeDisabled();
  await page.getByTestId("v5-formal-preview-button").click();
  await expect(page.getByTestId("v5-formal-preview-summary")).toContainText("矩阵行数：1");
  await expect(page.getByTestId("v5-start-run-group-button")).toBeEnabled();
  await page.getByLabel("tx_count").fill("20");
  await expect(page.getByTestId("v5-start-run-group-button")).toBeDisabled();
  await page.getByTestId("v5-formal-preview-button").click();
  await expect(page.getByTestId("v5-start-run-group-button")).toBeEnabled();
  await page.getByTestId("v5-start-run-group-button").click();
  await expect(page.getByText("run_group_id：", { exact: false })).toBeVisible();
  await expect.poll(async () => page.getByTestId("v5-formal-group-summary").innerText(), { timeout: 180_000 }).toContain("状态：已完成");
  const child = page.getByTestId("v5-formal-child-table").locator("tbody tr").first().locator("td");
  await expect(child.nth(3)).toHaveText("11");
  await expect(child.nth(4)).toHaveText("20");
  await expect(child.nth(5)).toHaveText("已完成");
  await expect(child.nth(6)).toHaveText("完整");
  await expect(child.nth(7)).toHaveText("可用");
  await expect(child.nth(8)).toHaveText("true");
});

test("selecting Groundhog preserves the user-selected multi-shard topology and keeps the real-cluster start path available", async ({ page }) => {
  await page.goto("/");
  await page.locator(".final-sidebar").getByRole("button", { name: "② 运行实验", exact: true }).click();
  await expect(page.getByTestId("v5-formal-run-page")).toBeVisible();

  await page.getByLabel("nodes").fill("32");
  await page.getByLabel("shards").fill("8");
  await expect(page.getByLabel("validators per shard")).toHaveValue("4");

  await page.getByLabel("nodes").fill("64");
  await page.getByLabel("shards").fill("2");
  await expect(page.getByLabel("validators per shard")).toHaveValue("32");

  await page.getByLabel("nodes").fill("32");
  await page.getByLabel("shards").fill("8");

  for (const methodId of ["hash_serial", "hash_block_stm", "metatrack_serial", "metatrack_block_stm"]) {
    const checkbox = page.getByTestId(`v5-run-method-${methodId}`).getByRole("checkbox");
    if (await checkbox.isChecked()) await checkbox.uncheck();
  }
  await page.getByTestId("v5-run-method-hash_groundhog").getByRole("checkbox").check();
  await expect(page.getByTestId("v5-groundhog-topology-notice")).toContainText("不会自动改写拓扑");
  await expect(page.getByLabel("nodes")).toHaveValue("32");
  await expect(page.getByLabel("shards")).toHaveValue("8");
  await expect(page.getByLabel("validators per shard")).toHaveValue("4");
  await expect(page.getByLabel("cross_shard_ratio")).toHaveValue("0");

  await page.getByTestId("v5-suite-comparison_experiment").getByRole("checkbox").uncheck();
  await page.getByTestId("v5-suite-main_experiment").getByRole("checkbox").check();
  await page.getByLabel("tx_count").fill("20");
  await page.getByTestId("v5-formal-preview-button").click();

  await expect(page.locator('[data-method-config-id="hash_groundhog"]')).toContainText("可运行");
  await expect(page.getByTestId("v5-formal-preview-blockers")).toHaveCount(0);
  await expect(page.getByTestId("v5-start-run-group-button")).toBeEnabled();
});


test("mixed Groundhog and non-Groundhog methods keep their own paired block producer and executor", async ({ page }) => {
  await page.goto("/");
  await page.locator(".final-sidebar").getByRole("button", { name: "② 运行实验", exact: true }).click();
  await expect(page.getByTestId("v5-formal-run-page")).toBeVisible();

  for (const methodId of ["hash_aria", "hash_groundhog", "stateless_hash_serial", "stateless_hash_block_stm"]) {
    const checkbox = page.getByTestId(`v5-run-method-${methodId}`).getByRole("checkbox");
    if (!(await checkbox.isChecked())) await checkbox.check();
  }
  await page.getByLabel("cross_shard_ratio").fill("0");
  await page.getByLabel("tx_count").fill("20");
  await page.getByTestId("v5-formal-preview-button").click();

  await expect(page.getByTestId("v5-formal-preview-summary")).toContainText("矩阵行数：8");
  await expect(page.getByTestId("v5-formal-preview-blockers")).toHaveCount(0);
  await expect(page.getByText("groundhog_block_producer and groundhog_block_executor must be selected together", { exact: false })).toHaveCount(0);
  await expect(page.getByTestId("v5-start-run-group-button")).toBeEnabled();
});
