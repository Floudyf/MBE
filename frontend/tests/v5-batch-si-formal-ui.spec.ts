import { expect, test } from "@playwright/test";

async function openFormalRunPage(page: import("@playwright/test").Page) {
  await page.goto("/?e2e=1");
  await page.locator(".final-sidebar").getByRole("button", { name: "② 运行实验", exact: true }).click();
  await expect(page.getByTestId("v5-formal-run-page")).toBeVisible();
}

test("formal experiment cards expose Batch-SI ablations and keep workers separate from node processes", async ({ page }) => {
  await openFormalRunPage(page);

  const comparison = page.getByTestId("v5-suite-comparison_experiment");
  await expect(comparison).toHaveAttribute("aria-pressed", "true");
  const batchSIComparison = page.getByTestId("v5-run-method-hash_batch_si");
  if ((await batchSIComparison.getAttribute("aria-pressed")) !== "true") await batchSIComparison.click();
  await expect(batchSIComparison).toHaveAttribute("aria-pressed", "true");

  await page.getByTestId("v5-suite-ablation_experiment").click();
  await expect(page.getByTestId("v5-suite-ablation_experiment")).toHaveAttribute("aria-pressed", "true");
  await expect(page.getByTestId("v5-ablation-targets").getByRole("button", { name: /MetaTrack/ })).toBeDisabled();

  for (const methodId of [
    "hash_batch_si",
    "hash_batch_si_no_wrbp",
    "hash_batch_si_no_ofas",
    "hash_batch_si_serial_batch",
    "hash_batch_si_txid_priority",
  ]) {
    await expect(page.getByTestId(`v5-run-method-${methodId}`)).toHaveAttribute("aria-pressed", "true");
  }
  await expect(page.getByTestId("v5-run-method-hash_batch_si")).toBeDisabled();

  await expect(page.getByTestId("v5-process-count")).toHaveText("8");
  await expect(page.getByTestId("v5-worker-count")).toHaveText("4");
  await page.getByTestId("v5-worker-options").getByRole("button", { name: "8", exact: true }).click();
  await expect(page.getByTestId("v5-worker-count")).toHaveText("8");
  await expect(page.getByTestId("v5-process-count")).toHaveText("8");

  await page.getByLabel("nodes").fill("4");
  await page.getByLabel("shards").fill("1");
  await expect(page.getByLabel("validators per shard")).toHaveValue("4");
  await expect(page.getByTestId("v5-process-count")).toHaveText("4");
  await expect(page.getByTestId("v5-worker-count")).toHaveText("8");
});

test("Batch-SI main experiment previews and completes through the real-cluster start path", async ({ page }) => {
  test.setTimeout(240_000);
  await openFormalRunPage(page);

  await page.getByTestId("v5-suite-main_experiment").click();
  const batchSI = page.getByTestId("v5-run-method-hash_batch_si");
  if ((await batchSI.getAttribute("aria-pressed")) !== "true") {
    await batchSI.click();
  }

  await page.getByLabel("nodes").fill("4");
  await page.getByLabel("shards").fill("1");
  await page.getByLabel("validators per shard").fill("4");
  await page.getByTestId("v5-worker-options").getByRole("button", { name: "2", exact: true }).click();
  await page.getByLabel("tx_count").fill("20");
  await page.getByLabel("cross_shard_ratio").fill("0");
  await page.getByLabel("seeds").fill("11");
  await page.getByLabel("repeats").fill("1");
  await page.getByLabel("block size").fill("10");

  await page.getByTestId("v5-formal-preview-button").click();
  await expect(page.getByTestId("v5-formal-preview-summary")).toContainText("矩阵行数：1");
  await expect(page.getByText("可运行", { exact: true })).toBeVisible();
  await expect(page.getByTestId("v5-formal-preview-blockers")).toHaveCount(0);
  await expect(page.getByTestId("v5-start-run-group-button")).toBeEnabled();

  await page.getByTestId("v5-start-run-group-button").click();
  await expect(page.getByText("run_group_id：", { exact: false })).toBeVisible();
  await expect.poll(
    async () => page.getByTestId("v5-formal-group-summary").innerText(),
    { timeout: 240_000 },
  ).toContain("状态：已完成");

  const child = page.getByTestId("v5-formal-child-table").locator("tbody tr").first().locator("td");
  await expect(child.nth(2)).toHaveText("Batch-SI");
  await expect(child.nth(3)).toHaveText("11");
  await expect(child.nth(4)).toHaveText("20");
  await expect(child.nth(5)).toHaveText("已完成");
  await expect(child.nth(6)).toHaveText("完整");
  await expect(child.nth(7)).toHaveText("可用");
  await expect(child.nth(8)).toHaveText("true");
});

test("eight-method comparison previews Batch-SI without semantic fairness blockers", async ({ page }) => {
  await openFormalRunPage(page);

  await page.getByTestId("v5-suite-comparison_experiment").click();
  const selected = new Set([
    "hash_serial",
    "hash_block_stm",
    "hash_groundhog",
    "hash_batch_si",
    "stateless_hash_serial",
    "stateless_hash_block_stm",
    "metatrack_serial",
    "metatrack_block_stm",
  ]);
  for (const methodId of [
    "hash_serial",
    "hash_block_stm",
    "hash_aria",
    "hash_groundhog",
    "hash_batch_si",
    "stateless_hash_serial",
    "stateless_hash_block_stm",
    "metatrack_serial",
    "metatrack_block_stm",
  ]) {
    const button = page.getByTestId(`v5-run-method-${methodId}`);
    const pressed = (await button.getAttribute("aria-pressed")) === "true";
    if (pressed !== selected.has(methodId)) await button.click();
  }

  await page.getByLabel("nodes").fill("8");
  await page.getByLabel("shards").fill("1");
  await page.getByLabel("validators per shard").fill("8");
  await page.getByTestId("v5-worker-options").getByRole("button", { name: "4", exact: true }).click();
  await page.getByLabel("tx_count").fill("20");
  await page.getByLabel("cross_shard_ratio").fill("0");
  await page.getByLabel("seeds").fill("11");
  await page.getByLabel("repeats").fill("1");
  await page.getByLabel("block size").fill("10");

  await page.getByTestId("v5-formal-preview-button").click();

  await expect(page.getByTestId("v5-formal-preview-summary")).toContainText("矩阵行数：8");
  await expect(page.getByTestId("v5-formal-preview-blockers")).toHaveCount(0);
  await expect(page.getByTestId("v5-start-run-group-button")).toBeEnabled();
  await expect(page.getByTestId("v5-formal-preview-summary")).not.toContainText("semantic fairness mismatch");
});
