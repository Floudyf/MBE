import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./tests",
  outputDir: "./test-results",
  timeout: 180_000,
  expect: {
    timeout: 15_000,
  },
  fullyParallel: false,
  // Formal logical-runtime flows share local artifact roots and must not overlap.
  workers: 1,
  retries: 1,
  reporter: [["list"], ["html", { outputFolder: "playwright-report", open: "never" }]],
  webServer: [
    {
      command: "python -m uvicorn backend.app.main:app --host 127.0.0.1 --port 8000",
      cwd: "..",
      url: "http://127.0.0.1:8000/health",
      timeout: 120_000,
      reuseExistingServer: !process.env.CI,
      stdout: "pipe",
      stderr: "pipe",
    },
    {
      command: "npm run dev -- --host 127.0.0.1 --port 5173",
      url: "http://127.0.0.1:5173",
      timeout: 120_000,
      reuseExistingServer: !process.env.CI,
      stdout: "pipe",
      stderr: "pipe",
    },
  ],
  use: {
    baseURL: "http://127.0.0.1:5173",
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
    video: "retain-on-failure",
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
});
