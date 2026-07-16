import { expect, test } from "@playwright/test";
import { selectOnlySuite } from "./v5-formal-test-helpers";

test("topology scaling expands explicit topology points and blocks invalid topology", async ({ page }) => {
  await page.goto("/");
  await page.getByTestId("primary-navigation").getByRole("button", { name: "② 运行实验" }).click();
  await page.getByTestId("v5-run-method-v5_catalog_default").getByRole("checkbox").check();
  await selectOnlySuite(page, "topology_scaling");
  await page.getByLabel("tx_count").fill("20");
  const editor = page.getByTestId("v5-point-editor-拓扑扫描点");
  await editor.getByRole("button", { name: "添加扫描点" }).click(); await editor.getByRole("button", { name: "添加扫描点" }).click();
  const fields = editor.locator('input[type="number"]');
  await fields.nth(0).fill("4"); await fields.nth(1).fill("1"); await fields.nth(2).fill("4");
  await fields.nth(3).fill("8"); await fields.nth(4).fill("2"); await fields.nth(5).fill("4");
  await expect(page.getByTestId("v5-estimated-children")).toHaveText("2");
  await expect(page.getByTestId("v5-estimated-process-starts")).toHaveText("12");
  await expect(page.getByTestId("v5-estimated-transactions")).toHaveText("40");
  const preview = page.waitForResponse((response) => response.url().includes("/api/v5/formal/preview") && response.request().method() === "POST");
  await page.getByTestId("v5-formal-preview-button").click();
  const body = await (await preview).json();
  expect(body.rows).toHaveLength(2);
  expect(body.rows.map((row: { topology_point: { nodes: number; shards: number; validators_per_shard: number } }) => [row.topology_point.nodes, row.topology_point.shards, row.topology_point.validators_per_shard])).toEqual(expect.arrayContaining([[4, 1, 4], [8, 2, 4]]));
  for (const row of body.rows) { expect(row.suite_type).toBe("topology_scaling"); expect(row.scan_variable).toBe("topology"); expect(row.scan_value).not.toBe(""); expect(row.execution_backend).toBe("real_cluster"); expect(row.runnable).toBe(true); }
  await fields.nth(3).fill("8"); await fields.nth(4).fill("1"); await fields.nth(5).fill("4");
  await page.getByTestId("v5-formal-preview-button").click();
  await expect(page.getByTestId("v5-formal-run-page")).toContainText("节点数必须等于分片数乘以每片验证节点数");
});
