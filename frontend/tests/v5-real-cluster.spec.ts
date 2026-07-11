import { expect, test } from "@playwright/test";

test("renders the V5 catalog-driven real cluster workbench", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "V5 Real Cluster" }).click();
  await expect(page.getByRole("heading", { name: "插件驱动真实多进程运行" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Workload" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Routing" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Validate" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Run Real Cluster" })).toBeVisible();
});
