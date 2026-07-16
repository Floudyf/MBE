import { expect, test } from "@playwright/test";
import { stat } from "node:fs/promises";

test("shows persisted V5 results, runtime evidence, and real artifacts", async ({ page, request }) => {
  test.setTimeout(180_000);
  await page.goto("/?e2e=1");
  await page.locator(".final-sidebar nav").first().getByRole("button").nth(1).click();
  await page.getByTestId("v5-run-method-v5_catalog_default").getByRole("checkbox").check();
  await page.getByLabel("nodes").fill("8");
  await page.getByLabel("shards").fill("2");
  await page.getByLabel("validators per shard").fill("4");
  await page.getByLabel("tx_count").fill("40");
  await page.getByLabel("cross_shard_ratio").fill("0.25");
  await page.getByLabel("seeds").fill("31");
  await page.getByLabel("repeats").fill("1");
  await page.getByTestId("v5-formal-preview-button").click();
  await expect(page.getByTestId("v5-formal-preview-summary")).toContainText("矩阵行数：1");
  await page.getByTestId("v5-start-run-group-button").click();
  await expect.poll(async () => page.getByTestId("v5-formal-group-summary").innerText(), { timeout: 180_000 }).toContain("已完成");
  const groupId = await page.locator("code").filter({ hasText: "v5grp_" }).innerText();
  expect(groupId).toMatch(/^v5grp_[A-Za-z0-9_]+$/);

  await page.getByRole("button", { name: "查看结果与产物" }).click();
  await expect(page.getByTestId("v5-results-page")).toBeVisible();
  await expect(page.getByTestId("v5-group-rungroup").locator("dd")).toHaveText(groupId);
  await expect(page.getByTestId("v5-group-status").locator("dd")).toHaveText("completed");
  await expect(page.getByTestId("v5-group-backend").locator("dd")).toHaveText("real_cluster");
  await expect(page.getByTestId("v5-group-children").locator("dd")).toHaveText("1/1");
  await expect(page.getByTestId("v5-run-group-list").locator("tr", { hasText: groupId })).toHaveCount(0);

  const row = page.getByTestId("v5-child-table").locator("tbody tr").first().locator("td");
  await expect(row.nth(7)).toHaveText("已完成completed");
  await expect(row.nth(10)).toHaveText("40");
  await expect(row.nth(11)).toHaveText("0");
  await row.nth(0).getByRole("button").click();
  await expect(page.getByTestId("v5-metric-submitted").locator("dd")).toHaveText("40");
  await expect(page.getByTestId("v5-metric-terminal").locator("dd")).toHaveText("40");
  await expect(page.getByTestId("v5-metric-incomplete").locator("dd")).toHaveText("0");
  await expect(page.getByTestId("v5-metric-intra-committed").locator("dd")).toHaveText("30");
  await expect(page.getByTestId("v5-metric-cross-requested").locator("dd")).toHaveText("10");
  await expect(page.getByTestId("v5-metric-cross-finalized").locator("dd")).toHaveText("10");
  await expect(page.getByTestId("v5-metric-state-root-consistent").locator("dd")).toHaveText("true");
  await expect(page.getByTestId("v5-metric-no-fallback").locator("dd")).toHaveText("true");
  await expect(page.getByTestId("v5-metric-orphan-processes").locator("dd")).toHaveText("0");

  const groupCatalog = page.getByTestId("v5-group-artifact-catalog");
  for (const filename of ["raw_summary.csv", "aggregate_summary.csv", "formal_matrix.csv", "fairness_matrix.csv"]) await expect(groupCatalog).toContainText(filename);
  const childCatalog = page.getByTestId("v5-child-artifact-catalog");
  for (const filename of ["finality_summary.json", "real_cluster_summary.json", "transaction_finality.csv", "transaction_lifecycle.csv", "throughput_windows.csv", "drain_status.json"]) await expect(childCatalog).toContainText(filename);

  const download = page.waitForEvent("download");
  await page.getByTestId("v5-bundle-download").click();
  const bundle = await download;
  expect(bundle.suggestedFilename()).toBe("artifacts.zip");
  const path = await bundle.path();
  expect(path).not.toBeNull();
  expect((await stat(path!)).size).toBeGreaterThan(0);

  const href = await childCatalog.locator("tr", { hasText: "finality_summary.json" }).getByRole("link", { name: "下载" }).getAttribute("href");
  expect(href).toBeTruthy();
  const artifact = await request.get(href!);
  expect(artifact.status()).toBe(200);
  expect((await artifact.body()).byteLength).toBeGreaterThan(0);
});
