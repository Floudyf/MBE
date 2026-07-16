import { expect, test } from "@playwright/test";

for (const viewport of [{ width: 1440, height: 900 }, { width: 1920, height: 1080 }]) {
  test(`V5 Chinese primary pages fit ${viewport.width}x${viewport.height}`, async ({ page }) => {
    await page.setViewportSize(viewport);
    await page.goto("/");
    const nav = page.getByTestId("primary-navigation");
    const pages: Array<{ index: number; testId: string; label: string }> = [
      { index: 0, testId: "v5-method-design-page", label: "method design" },
      { index: 1, testId: "v5-formal-run-page", label: "formal run" },
      { index: 2, testId: "v5-results-page", label: "results" },
      { index: 3, testId: "v5-workload-library-page", label: "workload library" },
    ];
    for (const entry of pages) {
      await nav.getByRole("button").nth(entry.index).click();
      await expect(page.getByTestId(entry.testId)).toBeVisible();
      const overflow = await page.evaluate(() => ({ scrollWidth: document.documentElement.scrollWidth, clientWidth: document.documentElement.clientWidth }));
      expect(overflow.scrollWidth, `${entry.label} page overflow: scrollWidth=${overflow.scrollWidth}, clientWidth=${overflow.clientWidth}`).toBeLessThanOrEqual(overflow.clientWidth + 1);
    }
  });
}
