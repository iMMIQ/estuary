import { expect, test } from "@playwright/test";

import { mockControlPlane, nodeConfig, record, status } from "./fixtures";

test("keeps slow polling results without starting overlapping requests", async ({ page }) => {
  await page.clock.install({ time: new Date("2026-01-01T00:00:00Z") });
  await page.clock.pauseAt(new Date("2026-01-01T00:01:00Z"));
  await mockControlPlane(page);
  let requests = 0;
  let release!: () => void;
  const delayed = new Promise<void>((resolve) => {
    release = resolve;
  });
  await page.route("**/admin/api/nodes", async (route) => {
    requests += 1;
    await delayed;
    await route.fulfill({ json: { nodes: [record(nodeConfig("slow-node"))] } });
  });
  await page.goto("/admin/");
  await expect.poll(() => requests).toBeGreaterThan(0);
  const initialRequests = requests;
  await page.clock.runFor(10000);
  expect(requests).toBe(initialRequests);
  release();
  await page
    .locator("button:visible")
    .filter({ hasText: /^Upstreams$/ })
    .click();
  await expect(page.getByText("slow-node", { exact: true })).toBeVisible();
  await page.clock.runFor(5000);
  await expect.poll(() => requests).toBe(initialRequests + 1);
});

test("manual refresh supersedes pending data and preserves a healthy partial result", async ({
  page,
}) => {
  await page.clock.install({ time: new Date("2026-01-01T00:00:00Z") });
  await page.clock.pauseAt(new Date("2026-01-01T00:01:00Z"));
  await mockControlPlane(page);
  let pendingRequests = 0;
  let release!: () => void;
  const delayed = new Promise<void>((resolve) => {
    release = resolve;
  });
  await page.route("**/admin/api/nodes", async (route) => {
    pendingRequests += 1;
    await delayed;
    await route.fulfill({ json: { nodes: [record(nodeConfig("stale-node"))] } });
  });
  await page.goto("/admin/");
  await expect.poll(() => pendingRequests).toBeGreaterThan(0);
  await page.route("**/admin/api/nodes", (route) =>
    route.fulfill({ json: { nodes: [record(nodeConfig("fresh-node"))] } }),
  );
  await page.route("**/admin/api/status", (route) =>
    route.fulfill({ status: 503, json: { error: { message: "Status unavailable" } } }),
  );
  await page.getByRole("button", { name: "Refresh", exact: true }).click();
  await page
    .locator("button:visible")
    .filter({ hasText: /^Upstreams$/ })
    .click();
  await expect(page.getByText("fresh-node", { exact: true })).toBeVisible();
  await expect(page.getByRole("alert")).toContainText("Status unavailable");
  release();
  await page.clock.runFor(5000);
  await expect(page.getByText("stale-node", { exact: true })).toHaveCount(0);
  await page.route("**/admin/api/status", (route) => route.fulfill({ json: status(1) }));
  await page.getByRole("button", { name: "Retry", exact: true }).click();
  await expect(page.getByRole("alert")).toHaveCount(0);
});

test("creates an upstream through the management workflow", async ({ page }) => {
  const nodes = await mockControlPlane(page);
  await page.goto("/admin/");
  await expect(page.getByRole("heading", { name: "Overview" })).toBeVisible();
  await expect(page.getByText("All systems nominal")).toBeVisible();
  await expect(page.getByText("Raw Metrics")).toHaveCount(0);
  if (test.info().project.name === "mobile")
    await page.locator(".vllm-runtime-panel summary").click();
  await expect(page.getByText("Prompt throughput")).toBeVisible();

  await page
    .locator("button:visible")
    .filter({ hasText: /^Upstreams$/ })
    .click();
  await expect(page.getByText("No upstream nodes")).toBeVisible();
  await page
    .locator("button:visible")
    .filter({ hasText: /^Add Node$/ })
    .first()
    .click();
  await page.getByLabel("Node ID").fill("vllm-a");
  await page.getByLabel("Base URL").fill("http://vllm-a.internal:8000/v1");
  await page.getByLabel("Public model 1").fill("gateway-chat");
  await page.getByLabel("Upstream model 1").fill("model-a");
  await page.getByRole("combobox", { name: "Model family 1", exact: true }).click();
  await page.getByRole("option", { name: "DeepSeek (recipe)", exact: true }).click();
  await page.getByRole("button", { name: "Next" }).click();
  await page.getByRole("button", { name: "Next" }).click();
  await page.getByRole("button", { name: "Add Node" }).click();

  await expect(page.getByText("vllm-a").first()).toBeVisible();
  await expect(page.getByText("Node added")).toBeVisible();
  expect(nodes[0].config.model_capabilities).toMatchObject({
    "gateway-chat": { family: "deepseek" },
  });
});

test("shows top IPs and manages a connection limit", async ({ page }) => {
  await mockControlPlane(page);
  await page.goto("/admin/");

  await expect(page.getByText("192.0.2.10")).toBeVisible();
  await page.getByLabel("IP address").fill("192.0.2.10");
  await page.getByLabel("Connection limit").fill("2");
  await page.getByRole("button", { name: "Apply limit" }).click();
  await expect(page.getByText("Limit: 2")).toBeVisible();
  await page.getByRole("button", { name: "Remove limit for 192.0.2.10" }).click();
  await expect(page.getByText("Limit: 2")).toHaveCount(0);
});

test("drains and deletes an existing upstream", async ({ page }) => {
  await mockControlPlane(page, [record(nodeConfig())]);
  await page.goto("/admin/");
  await page
    .locator("button:visible")
    .filter({ hasText: /^Upstreams$/ })
    .click();

  await page.getByLabel("Actions for vllm-a").click();
  await page.getByRole("menuitem", { name: "Drain" }).click();
  await expect(page.getByRole("alert").getByText("Node is draining")).toBeVisible();

  await page.getByLabel("Actions for vllm-a").click();
  await expect(page.getByRole("menuitem", { name: "Resume" })).toBeVisible();
  await page.getByRole("menuitem", { name: "Delete" }).click();
  await page.getByRole("button", { name: "Delete Node" }).click();
  await expect(page.getByText("Node deleted")).toBeVisible();
  await expect(page.getByText("No upstream nodes")).toBeVisible();
});

test("surfaces a revision conflict and refreshes the node", async ({ page }) => {
  await mockControlPlane(page, [record(nodeConfig())], true);
  await page.goto("/admin/");
  await page
    .locator("button:visible")
    .filter({ hasText: /^Upstreams$/ })
    .click();
  await page.getByLabel("Actions for vllm-a").click();
  await page.getByRole("menuitem", { name: "Edit" }).click();
  await page.getByRole("button", { name: "Next" }).click();
  await page.getByRole("button", { name: "Next" }).click();
  await page.getByRole("button", { name: "Save Changes" }).click();

  await expect(page.getByText("The node changed; refresh and retry")).toBeVisible();
  await expect(page.getByRole("heading", { name: "Edit vllm-a" })).toBeVisible();
});

test("detects Chinese and persists an explicit language choice", async ({ page }) => {
  await page.addInitScript(() => {
    Object.defineProperty(navigator, "languages", {
      configurable: true,
      value: ["zh-CN", "en-US"],
    });
  });
  await mockControlPlane(page, [record(nodeConfig())]);
  await page.goto("/admin/");

  await expect(page.getByRole("heading", { name: "总览" })).toBeVisible();
  await expect(page.locator("html")).toHaveAttribute("lang", "zh-CN");
  await expect(page).toHaveTitle("Estuary 控制台");

  await page.getByRole("button", { name: "语言" }).click();
  await page.getByRole("menuitem", { name: "English" }).click();
  await expect(page.getByRole("heading", { name: "Overview" })).toBeVisible();
  await expect(page.locator("html")).toHaveAttribute("lang", "en");
  await page.reload();
  await expect(page.getByRole("heading", { name: "Overview" })).toBeVisible();

  await page.getByRole("button", { name: "Language" }).click();
  await page.getByRole("menuitem", { name: "简体中文" }).click();
  await page
    .locator("button:visible")
    .filter({ hasText: /^上游节点$/ })
    .click();
  await page.getByRole("button", { name: "添加节点" }).first().click();
  await page.getByRole("button", { name: "下一步" }).click();
  await expect(page.getByText("必须填写节点 ID")).toBeVisible();
});

test("captures the bilingual interface for visual review", async ({ page }, testInfo) => {
  test.skip(process.env.ESTUARY_I18N_SCREENSHOTS !== "1", "manual screenshot review");
  await mockControlPlane(page, [record(nodeConfig())]);

  for (const locale of ["en", "zh-CN"] as const) {
    await page.goto("/admin/");
    await page.evaluate((value) => localStorage.setItem("estuary.locale", value), locale);
    await page.reload();

    const capture = async (name: string) => {
      await expect
        .poll(() =>
          page.evaluate(
            () => document.documentElement.scrollWidth <= document.documentElement.clientWidth,
          ),
        )
        .toBe(true);
      await page.screenshot({
        path: testInfo.outputPath(`i18n-${locale}-${name}.png`),
        fullPage: true,
      });
    };

    await capture("overview");
    await page
      .locator("button:visible")
      .filter({ hasText: locale === "en" ? /^Upstreams$/ : /^上游节点$/ })
      .click();
    await capture("upstreams");
    await page.locator(".upstream-table tbody tr").click();
    await capture("details");
    await page.getByRole("tab", { name: locale === "en" ? "Models" : "模型", exact: true }).click();
    await capture("models");
    await page.getByRole("button", { name: locale === "en" ? "Edit" : "编辑" }).click();
    await page.getByRole("button", { name: locale === "en" ? "Next" : "下一步" }).click();
    await capture("editor-advanced");
    await page.getByRole("button", { name: locale === "en" ? "Next" : "下一步" }).click();
    await capture("editor-review");
  }
});
