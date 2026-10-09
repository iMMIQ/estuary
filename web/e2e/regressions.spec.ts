import { expect, type Page, test } from "@playwright/test";
import type { NodeConfig } from "../src/types";
import { mockControlPlane, nodeConfig, record } from "./fixtures";

async function openEdit(page: Page) {
  await page.goto("/admin/");
  await page
    .locator("button:visible")
    .filter({ hasText: /^Upstreams$/ })
    .click();
  await page.getByLabel("Actions for vllm-a").click();
  await page.getByRole("menuitem", { name: "Edit", exact: true }).click();
}

test("sidebar preserves dirty editor when discard is declined", async ({ page }) => {
  test.skip(test.info().project.name === "mobile", "desktop sidebar navigation");
  await mockControlPlane(page, [record(nodeConfig())]);
  await openEdit(page);
  let dialogs = 0;
  page.on("dialog", async (dialog) => {
    dialogs++;
    await dialog.dismiss();
  });
  await page.getByLabel("Base URL").fill("http://new-config.internal:8000/v1");
  await page
    .locator(".desktop-sidebar button")
    .filter({ hasText: /^Overview$/ })
    .click();
  await expect(page.getByRole("heading", { name: "Edit vllm-a", exact: true })).toBeVisible();
  await expect(page.getByLabel("Base URL")).toHaveValue("http://new-config.internal:8000/v1");
  expect(dialogs).toBe(1);
});

test("preflight result is discarded after changing configuration", async ({ page }) => {
  const initial = record(nodeConfig());
  await mockControlPlane(page, [initial]);
  let release!: () => void;
  let checkedUrl = "";
  let finished = false;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  await page.route("**/admin/api/nodes/preflight*", async (route) => {
    checkedUrl = route.request().postDataJSON().base_url;
    await gate;
    await route.fulfill({
      json: {
        ok: true,
        runtime: initial.runtime,
        admission: initial.admission,
        checks: { configuration: "passed", provider: "passed", health: "passed" },
      },
    });
    finished = true;
  });
  await openEdit(page);
  await page.getByRole("button", { name: "Test Connection", exact: true }).click();
  await expect.poll(() => checkedUrl).toBe(initial.config.base_url);
  await page.getByLabel("Base URL").fill("http://not-tested.internal:8000/v1");
  release();
  await expect.poll(() => finished).toBe(true);
  await expect(page.getByText("Connection verified", { exact: true })).toHaveCount(0);
  await expect(page.getByLabel("Base URL")).toHaveValue("http://not-tested.internal:8000/v1");
});

test("revision conflict merges local edits and saves with the current revision", async ({
  page,
}) => {
  const initial = record(nodeConfig());
  const nodes = await mockControlPlane(page, [initial]);
  const sent: number[] = [];
  let saved: NodeConfig | undefined;
  await page.route("**/admin/api/nodes/vllm-a?*", async (route) => {
    saved = route.request().postDataJSON().config;
    sent.push(route.request().postDataJSON().revision);
    if (sent.length > 1) {
      await route.fulfill({ json: nodes[0] });
      return;
    }
    nodes[0].revision = 2;
    nodes[0].config.max_concurrency = 32;
    await route.fulfill({
      status: 409,
      json: {
        error: {
          message: "The node changed; refresh and retry",
          code: "revision_conflict",
        },
      },
    });
  });
  await openEdit(page);
  await page.getByLabel("Base URL").fill("http://mine.internal:8000/v1");
  await page.getByRole("button", { name: "Next", exact: true }).click();
  await page.getByRole("button", { name: "Next", exact: true }).click();
  await page.getByRole("button", { name: "Save Changes", exact: true }).click();
  await expect(page.getByText("The node changed; refresh and retry")).toBeVisible();
  await expect(page.getByRole("button", { name: "Save Changes", exact: true })).toBeDisabled();
  await page.getByRole("button", { name: "Merge and keep my changes", exact: true }).click();
  await expect(page.getByLabel("Base URL")).toHaveValue("http://mine.internal:8000/v1");
  await expect(page.getByRole("textbox", { name: "Max concurrency", exact: true })).toHaveValue(
    "32",
  );
  await page.getByRole("button", { name: "Next", exact: true }).click();
  await page.getByRole("button", { name: "Next", exact: true }).click();
  await page.getByRole("button", { name: "Save Changes", exact: true }).click();
  await expect.poll(() => sent.length).toBe(2);
  expect(sent).toEqual([1, 2]);
  expect(saved?.base_url).toBe("http://mine.internal:8000/v1");
  expect(saved?.max_concurrency).toBe(32);
});

test("editing preserves inherited image capability", async ({ page }) => {
  const initial = record({
    ...nodeConfig(),
    model_capabilities: {
      "*": { multimodal: false },
    },
  });
  await mockControlPlane(page, [initial]);
  let saved: NodeConfig | undefined;
  await page.route("**/admin/api/nodes/vllm-a?*", async (route) => {
    saved = route.request().postDataJSON().config;
    await route.fulfill({ json: initial });
  });
  await openEdit(page);
  await expect(
    page.getByRole("combobox", { name: "Capability source 1", exact: true }),
  ).toHaveValue("Use default");
  await expect(page.getByRole("switch", { name: "Image input 1", exact: true })).not.toBeChecked();
  await expect(page.getByRole("combobox", { name: "Model family 1", exact: true })).toHaveCount(0);
  await page.getByRole("switch", { name: "Image input 1", exact: true }).check();
  await expect(
    page.getByRole("combobox", { name: "Capability source 1", exact: true }),
  ).toHaveValue("Override");
  await page.getByRole("combobox", { name: "Capability source 1", exact: true }).click();
  await page.getByRole("option", { name: "Use default", exact: true }).click();
  await expect(page.getByRole("switch", { name: "Image input 1", exact: true })).not.toBeChecked();
  await page.getByRole("button", { name: "Next", exact: true }).click();
  await page.getByRole("button", { name: "Next", exact: true }).click();
  await page.getByRole("button", { name: "Save Changes", exact: true }).click();
  await expect.poll(() => saved !== undefined).toBe(true);
  expect(saved?.model_capabilities).toEqual({ "*": { multimodal: false } });
});

test("failed IP limit retains the input for retry", async ({ page }) => {
  await mockControlPlane(page);
  await page.route("**/admin/api/ip-limits/**", (route) =>
    route.fulfill({
      status: 400,
      json: { error: { message: "Limit rejected", code: "invalid_limit" } },
    }),
  );
  await page.goto("/admin/");
  await page.getByLabel("IP address", { exact: true }).fill("192.0.2.10");
  await page.getByRole("button", { name: "Apply limit", exact: true }).click();
  await expect(page.getByText("Limit rejected", { exact: true })).toBeVisible();
  await expect(page.getByLabel("IP address", { exact: true })).toHaveValue("192.0.2.10");
});

test("keyboard menu actions keep the upstream list selected", async ({ page }) => {
  const nodes = await mockControlPlane(page, [record(nodeConfig())]);
  await page.goto("/admin/");
  await page
    .locator("button:visible")
    .filter({ hasText: /^Upstreams$/ })
    .click();
  await page.getByRole("button", { name: "Actions for vllm-a" }).click();
  const drain = page.getByRole("menuitem", { name: "Drain", exact: true });
  await drain.focus();
  await drain.press("Enter");
  await expect.poll(() => nodes[0].runtime.lifecycle).toBe("draining");
  await expect(page.getByRole("heading", { name: "Upstreams", exact: true })).toBeVisible();
});

test("editor applies backend provider-path and KV endpoint constraints", async ({ page }) => {
  const config: NodeConfig = nodeConfig();
  config.provider.kv_events = {
    endpoint: "tcp://127.0.0.1:5557",
    replay_endpoint: null,
    topic: "kv-events",
    reconnect_ms: 1000,
    max_blocks: 1000,
    max_directory_bytes: 1048576,
    max_event_bytes: 1024,
  };
  await mockControlPlane(page, [record(config)]);
  await openEdit(page);
  await page.getByRole("button", { name: "Next", exact: true }).click();
  await page.getByLabel("Metrics path", { exact: true }).fill("//evil.example/metrics");
  await page.getByLabel("Publisher endpoint", { exact: true }).fill("tcp://*:5557");
  await page.getByRole("button", { name: "Next", exact: true }).click();
  await expect(
    page.getByText("Use a path starting with / on the upstream origin, without query or fragment", {
      exact: true,
    }),
  ).toBeVisible();
  await expect(
    page.getByText("Use a connectable tcp://host:port endpoint", { exact: true }),
  ).toBeVisible();
  await page.getByLabel("Metrics path", { exact: true }).fill("/metrics");
  await page.getByLabel("Publisher endpoint", { exact: true }).fill("tcp://127.0.0.1:5557");
  await page.getByRole("button", { name: "Next", exact: true }).click();
  await expect(
    page.getByRole("heading", { name: "Review Configuration", exact: true }),
  ).toBeVisible();
});
