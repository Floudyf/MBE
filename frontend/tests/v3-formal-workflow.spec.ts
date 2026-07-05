import { expect, test } from "@playwright/test";

test("runs a minimal formal workload comparison workflow", async ({ page }) => {
  test.slow();
  const diagnostics = collectBrowserDiagnostics(page);
  await page.goto("/");

  await expect(page.getByRole("heading", { name: "MetaTrack 正式性能实验", exact: true })).toBeVisible({ timeout: 30_000 });
  await page.getByText("最真实链路确认").click();

  await page.getByTestId("v3-formal-preview-button").click();
  const matrix = page.getByTestId("v3-formal-matrix-preview");
  await expect(matrix).toBeVisible({ timeout: 60_000 });
  await expect(matrix).toContainText("6");

  await expect(page.getByTestId("v3-formal-run-button")).toBeEnabled({ timeout: 15_000 });
  await page.getByTestId("v3-formal-run-button").click();

  await expect(page.getByTestId("v3-formal-result-panel")).toBeVisible({ timeout: 240_000 });
  await expect(page.getByTestId("v3-formal-data-file-list")).toBeVisible();
  await expect(page.getByTestId("v3-formal-run-history")).toBeVisible();

  const bodyText = await page.locator("body").innerText();
  if (/失败|failed/i.test(bodyText)) {
    await expect(page.getByTestId("v3-formal-failure-diagnostics")).toBeVisible();
  } else {
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
