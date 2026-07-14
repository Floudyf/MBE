import { expect, test } from "@playwright/test";

test("renders the V5 catalog-driven real cluster workbench", async ({ page }) => {
  await page.goto("/");
  await page.getByTestId("primary-navigation").getByRole("button", { name: "高级功能", exact: true }).click();
  const workbench = page.getByTestId("advanced-navigation").locator("article").filter({ hasText: "V5 真实集群单次调试" });
  await workbench.getByRole("button", { name: "进入", exact: true }).click();
  await expect(page.getByRole("heading", { name: "插件驱动真实多进程运行" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Workload" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Routing" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Validate" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Run Real Cluster" })).toBeVisible();
});
