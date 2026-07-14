import { expect, test } from "@playwright/test";

for (const viewport of [{ width: 1440, height: 900 }, { width: 1920, height: 1080 }]) {
  test(`V5 Chinese primary pages fit ${viewport.width}x${viewport.height}`, async ({ page }) => {
    await page.setViewportSize(viewport); await page.goto("/");
    const nav = page.getByTestId("primary-navigation");
    const pages: Array<[string, string[]]> = [["① 实验设计", ["方法基本信息", "方法插件", "验证方法兼容性", "保存方法", "保存并进入运行实验", "已保存的 V5 方法"]], ["② 运行实验", ["当前执行方法", "实验类型", "实验方法", "负载与拓扑", "预览正式实验矩阵", "启动真实集群实验组"]], ["③ 结果与产物", ["结果与产物", "实验组历史"]], ["负载库", ["确定性签名合成负载", "尚未接入"]], ["真实性边界", ["已实现", "未声明"]], ["高级功能", ["V5 真实集群单次调试", "V3 Composer", "V4", "V1/V2"]]];
    for (const [label, texts] of pages) {
      await nav.getByRole("button", { name: label }).click();
      for (const text of texts) await expect(page.getByText(text, { exact: false }).first()).toBeVisible();
      const overflow = await page.evaluate(() => ({ scrollWidth: document.documentElement.scrollWidth, clientWidth: document.documentElement.clientWidth }));
      expect(overflow.scrollWidth).toBeLessThanOrEqual(overflow.clientWidth + 1);
    }
  });
}
