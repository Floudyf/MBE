import { expect, test } from "@playwright/test";

test("protects unsaved V5 method changes before entering Run", async ({ page }) => {
  await page.goto("/");
  await page.getByTestId("v5-method-name").fill("未保存方法");
  await page.getByTestId("v5-method-category-routing").locator("select").selectOption("metatrack_coaccess_routing");
  await page.getByTestId("primary-navigation").getByRole("button", { name: "② 运行实验" }).click();
  const dialog = page.getByRole("dialog", { name: "未保存方法设计" });
  await expect(dialog).toBeVisible();
  await expect(dialog.getByRole("button", { name: "保存并进入运行实验" })).toBeVisible();
  await expect(dialog.getByRole("button", { name: "放弃修改并离开" })).toBeVisible();
  await dialog.getByRole("button", { name: "继续编辑" }).click();
  await expect(page.getByTestId("v5-method-design-page")).toBeVisible();
  await page.getByTestId("primary-navigation").getByRole("button", { name: "② 运行实验" }).click();
  await dialog.getByRole("button", { name: "放弃修改并离开" }).click();
  await expect(page.getByTestId("v5-formal-run-page")).toBeVisible();
  await expect(page.getByTestId("v5-run-preferred-method")).toContainText("未选择");
  await expect(page.getByTestId("v5-run-method-v5_catalog_default").getByRole("checkbox")).not.toBeChecked();
  await expect(page.getByRole("button", { name: "预览正式实验矩阵" })).toBeDisabled();
});
