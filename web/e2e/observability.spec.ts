import { expect, test } from "@playwright/test";
import { mockControlPlane, nodeConfig, record } from "./fixtures";
import { observabilityFixture } from "./observability-fixture";

test("shows smoothed rates, histories and model/window filters without layout overflow", async ({
  page,
}) => {
  await mockControlPlane(page, [record(nodeConfig("vllm-a"))]);
  await page.goto("/admin/");
  const panel = page.getByRole("region", { name: "Gateway observability" });
  await expect(panel).toBeVisible();
  await expect(panel.getByText("EWMA · 10 s half-life").first()).toBeVisible();
  await expect(panel.locator(".metric-trend")).toHaveCount(6);
  await page.screenshot({
    path: `test-results/observability-${test.info().project.name}.png`,
    fullPage: true,
  });
  await expect(panel.locator(".metric-line").first()).toHaveAttribute("d", /^M/);
  await page.getByRole("combobox", { name: "Statistics window", exact: true }).click();
  await page.getByRole("option", { name: "Last 1 minute", exact: true }).click();
  await expect(panel.getByText("Observed 60 s in this window")).toBeVisible();
  await panel.getByRole("button", { name: "gateway-chat", exact: true }).click();
  await expect(
    panel.getByRole("heading", { name: "Gateway observability · gateway-chat" }),
  ).toBeVisible();
  const input = panel.getByRole("slider").first();
  await input.fill("50");
  await expect(panel.locator(".metric-guide").first()).toHaveAttribute("y2", "132");
  await expect
    .poll(() => page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth))
    .toBe(true);
});

test("refresh failure retains statistics and a process restart replaces history", async ({
  page,
}) => {
  await mockControlPlane(page, [record(nodeConfig("vllm-a"))]);
  await page.goto("/admin/");
  const panel = page.getByRole("region", { name: "Gateway observability" });
  await expect(panel.locator(".metric-line").first()).toHaveAttribute("d", /^M/);
  await page.route("**/admin/api/observability**", (route) => route.abort());
  await page.getByRole("button", { name: "Refresh", exact: true }).click();
  await expect(panel.getByText(/These statistics are stale/)).toBeVisible();
  await expect(panel.locator(".metric-line").first()).toHaveAttribute("d", /^M/);
  await page.route("**/admin/api/observability**", (route) => {
    const data = observabilityFixture(["vllm-a"], ["gateway-chat"]);
    data.instance_id = "new-process";
    data.history = [];
    return route.fulfill({ json: data });
  });
  await page.getByRole("button", { name: "Refresh", exact: true }).click();
  await expect(panel.getByText(/These statistics are stale/)).toHaveCount(0);
  await expect(panel.locator(".metric-line").first()).toHaveAttribute("d", "");
});

test("node details request node statistics and render both languages", async ({ page }) => {
  await mockControlPlane(page, [record(nodeConfig("vllm-a"))]);
  await page.goto("/admin/");
  await page
    .locator("button:visible")
    .filter({ hasText: /^Upstreams$/ })
    .click();
  await page.getByText("vllm-a", { exact: true }).click();
  await page.getByRole("tab", { name: "Metrics", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Gateway observability · vllm-a" })).toBeVisible();
  await expect(
    page.locator(".metrics-note").filter({ hasText: /Includes traffic outside/ }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Language" }).first().click();
  await page.getByRole("menuitem", { name: "简体中文" }).click();
  await expect(page.getByRole("heading", { name: "网关运行指标 · vllm-a" })).toBeVisible();
});
