import { expect, test, type Page } from "@playwright/test";

function status(nodeCount: number) {
  return {
    status: nodeCount ? "ready" : "not_ready",
    live: true,
    ready: nodeCount > 0,
    version: "0.2.0-test",
    generated_at_unix_ms: Date.now(),
    process: { state: "serving" },
    fleet: {
      total_nodes: nodeCount,
      routable_nodes: nodeCount,
      accepting_nodes: nodeCount,
      models: nodeCount,
      active_requests: 0,
      total_concurrency: nodeCount * 16,
      available_concurrency: nodeCount * 16,
    },
    queue: { requests: 0, bytes: 0, max_requests: 512, max_bytes: 268435456 },
    routing: { prefix_enabled: true },
  };
}

function nodeConfig(id = "vllm-a") {
  return {
    id,
    base_url: `http://${id}.internal:8000/v1`,
    api_key: null,
    api_key_env: null,
    models: { "gateway-chat": "model-a" },
    max_concurrency: 16,
    weight: 1,
    draining: false,
    health_path: "/v1/models",
    headers: {},
    headers_from_env: {},
    provider: {
      type: "vllm",
      anthropic_protocol: "auto",
      version_path: "/version",
      metrics_path: "/metrics",
      tokenize_path: "/tokenize",
      monitor_interval_ms: 1000,
      request_timeout_ms: 2000,
      telemetry_stale_ms: 5000,
      waiting_threshold: 8,
      tokenize_cache_entries: 4096,
      kv_events: null,
    },
  };
}

function record(config: Record<string, unknown>) {
  return {
    config,
    credentials: { api_key_configured: false, api_key_source: "none", header_names: [] },
    revision: 1,
    created_at_unix_ms: Date.now(),
    updated_at_unix_ms: Date.now(),
    runtime: {
      id: config.id,
      base_url: config.base_url,
      provider: "vllm",
      health: "healthy",
      lifecycle: "serving",
      circuit: "closed",
      circuit_open_until_unix_ms: null,
      circuit_failures: 0,
      circuit_half_open_in_flight: 0,
      active: 0,
      available: 16,
      max_concurrency: 16,
      weight: 1,
      provider_state: "ready",
      provider_version: "0.25.0",
      provider_last_error: null,
      upstream_running: 0,
      upstream_waiting: 0,
      kv_cache_usage: 0,
      latency_ewma_ms: 0,
      error_ewma: 0,
      last_error: null,
      last_change_unix_ms: Date.now(),
      provider_generation: 1,
      provider_telemetry_updated_unix_ms: Date.now(),
    },
    admission: {
      state: "accepting",
      reason: "Eligible for a new assignment",
      routable: true,
      accepting_assignments: true,
      telemetry_fresh: true,
      waiting_watermark_blocked: false,
    },
    exact_kv_authoritative: false,
    exact_kv_blocks: 0,
  };
}

async function mockControlPlane(
  page: Page,
  initial: ReturnType<typeof record>[] = [],
  revisionConflict = false,
) {
  const nodes = [...initial];
  await page.route("**/admin/api/status", (route) =>
    route.fulfill({ json: status(nodes.length) }),
  );
  await page.route("**/admin/api/nodes", async (route) => {
    if (route.request().method() === "POST") {
      const created = record(route.request().postDataJSON());
      nodes.push(created);
      await route.fulfill({ status: 201, json: created });
      return;
    }
    await route.fulfill({ json: { nodes } });
  });
  await page.route("**/admin/api/nodes/**", async (route) => {
    const request = route.request();
    const path = new URL(request.url()).pathname;
    const id = decodeURIComponent(path.split("/").at(-1) ?? "");
    const nodeId = path.endsWith("/drain")
      ? decodeURIComponent(path.split("/").at(-2) ?? "")
      : id;
    const node = nodes.find((item) => item.config.id === nodeId);
    if (!node) {
      await route.fulfill({ status: 404, json: { error: { message: "Node not found", code: "node_not_found" } } });
      return;
    }
    if (path.endsWith("/drain")) {
      const draining = request.method() === "PUT";
      node.runtime.lifecycle = draining ? "draining" : "serving";
      node.admission.state = draining ? "draining" : "accepting";
      node.admission.accepting_assignments = !draining;
      node.admission.routable = !draining;
      await route.fulfill({ json: { drained: true } });
      return;
    }
    if (request.method() === "DELETE") {
      nodes.splice(nodes.indexOf(node), 1);
      await route.fulfill({ json: { deleted: true } });
      return;
    }
    if (request.method() === "PUT" && revisionConflict) {
      await route.fulfill({
        status: 409,
        json: { error: { message: "The node changed; refresh and retry", code: "revision_conflict" } },
      });
      return;
    }
    await route.fulfill({ json: node });
  });
  return nodes;
}

test("creates an upstream through the management workflow", async ({ page }) => {
  await mockControlPlane(page);
  await page.goto("/admin/");
  await expect(page.getByRole("heading", { name: "Overview" })).toBeVisible();
  await expect(page.getByText("All systems nominal")).toBeVisible();

  await page.locator("button:visible").filter({ hasText: /^Upstreams$/ }).click();
  await expect(page.getByText("No upstream nodes")).toBeVisible();
  await page.locator("button:visible").filter({ hasText: /^Add Node$/ }).first().click();
  await page.getByLabel("Node ID").fill("vllm-a");
  await page.getByLabel("Base URL").fill("http://vllm-a.internal:8000/v1");
  await page.getByLabel("Public model 1").fill("gateway-chat");
  await page.getByLabel("Upstream model 1").fill("model-a");
  await page.getByRole("button", { name: "Next" }).click();
  await page.getByRole("button", { name: "Next" }).click();
  await page.getByRole("button", { name: "Add Node" }).click();

  await expect(page.getByText("vllm-a").first()).toBeVisible();
  await expect(page.getByText("Node added")).toBeVisible();
});

test("drains and deletes an existing upstream", async ({ page }) => {
  await mockControlPlane(page, [record(nodeConfig())]);
  await page.goto("/admin/");
  await page.locator("button:visible").filter({ hasText: /^Upstreams$/ }).click();

  await page.getByLabel("Actions for vllm-a").click();
  await page.getByRole("menuitem", { name: "Drain" }).click();
  await expect(page.getByText("Node is draining")).toBeVisible();

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
  await page.locator("button:visible").filter({ hasText: /^Upstreams$/ }).click();
  await page.getByLabel("Actions for vllm-a").click();
  await page.getByRole("menuitem", { name: "Edit" }).click();
  await page.getByRole("button", { name: "Next" }).click();
  await page.getByRole("button", { name: "Next" }).click();
  await page.getByRole("button", { name: "Save Changes" }).click();

  await expect(page.getByText("The node changed; refresh and retry")).toBeVisible();
  await expect(page.getByRole("heading", { name: "Edit vllm-a" })).toBeVisible();
});
