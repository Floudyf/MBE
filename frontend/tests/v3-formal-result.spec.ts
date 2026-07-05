import { expect, test } from "@playwright/test";

test("opens the latest formal result from history", async ({ page }) => {
  const diagnostics = collectBrowserDiagnostics(page);
  await page.goto("/");

  const history = page.getByTestId("v3-formal-run-history");
  await expect(history).toBeVisible({ timeout: 30_000 });

  const openButtons = page.getByTestId("v3-formal-history-open-result");
  await expect.poll(async () => {
    const buttonCount = await openButtons.count();
    const emptyCount = await page.getByText("暂无历史正式实验。").count();
    return buttonCount > 0 || emptyCount > 0;
  }, { timeout: 30_000 }).toBe(true);
  const count = await openButtons.count();
  test.skip(count === 0, "No formal run history entry is available yet.");

  await openButtons.first().click();
  await expect(page.getByTestId("v3-formal-result-panel")).toBeVisible({ timeout: 30_000 });
  await expect(page.getByTestId("v3-formal-data-file-list")).toBeVisible();

  const bodyText = await page.locator("body").innerText();
  if (/失败|failed/i.test(bodyText)) {
    await expect(page.getByTestId("v3-formal-failure-diagnostics")).toBeVisible();
  }
  const chartCount = await page.getByTestId("v3-formal-chart-dashboard").count();
  if (chartCount > 0) {
    await expect(page.getByTestId("v3-formal-chart-dashboard")).toBeVisible();
  }

  expect(diagnostics.errors).toEqual([]);
});

function collectBrowserDiagnostics(page: import("@playwright/test").Page) {
  const errors: string[] = [];
  page.on("console", (message) => {
    if (message.type() === "error") errors.push(`console: ${message.text()}`);
  });
  page.on("requestfailed", (request) => {
    errors.push(`request failed: ${request.method()} ${request.url()} ${request.failure()?.errorText || ""}`);
  });
  page.on("response", (response) => {
    const status = response.status();
    if (status >= 400 && response.url().includes("/api/")) {
      errors.push(`response ${status}: ${response.url()}`);
    }
  });
  return { errors };
}
