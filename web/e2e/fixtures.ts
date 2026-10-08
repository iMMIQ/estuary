import type { Page } from "@playwright/test";
export function status(nodeCount: number) {
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
    connections: {
      public: 6,
      max_public: 2048,
      top_ips: [
        { ip: "192.0.2.10", active: 3 },
        { ip: "192.0.2.20", active: 2 },
        { ip: "2001:db8::1", active: 1 },
      ],
      ip_limits: [],
    },
    response_buffer: { used_bytes: 0, max_bytes: 268435456, waiting_responses: 0 },
    routing: { prefix_enabled: true },
  };
}

export function nodeConfig(id = "vllm-a") {
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

export function record(config: Record<string, unknown>) {
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
      kv_cache_usage: 0.62,
      prompt_tokens_per_second: 1240,
      generation_tokens_per_second: 186,
      requests_per_second: 2.4,
      prefix_cache_queries_total: 10000,
      prefix_cache_hits_total: 7400,
      prefix_cache_hit_rate: 0.74,
      preemptions_total: 2,
      latency_ewma_ms: 20,
      ttft_ewma_ms: 125,
      pending_prefill_tokens: 64,
      pending_decode_tokens: 32,
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
    exact_kv_bytes: 0,
  };
}

export async function mockControlPlane(
  page: Page,
  initial: ReturnType<typeof record>[] = [],
  revisionConflict = false,
) {
  const nodes = [...initial];
  const limits = new Map<string, number>();
  await page.route("**/admin/api/status", (route) =>
    route.fulfill({
      json: {
        ...status(nodes.length),
        connections: {
          ...status(nodes.length).connections,
          ip_limits: [...limits].map(([ip, limit]) => ({ ip, limit })),
        },
      },
    }),
  );
  await page.route("**/admin/api/ip-limits/**", (route) => {
    const ip = decodeURIComponent(new URL(route.request().url()).pathname.split("/").at(-1) ?? "");
    if (route.request().method() === "PUT") limits.set(ip, route.request().postDataJSON().limit);
    else limits.delete(ip);
    return route.fulfill({ json: { ip, limit: limits.get(ip), deleted: !limits.has(ip) } });
  });
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
    const nodeId = path.endsWith("/drain") ? decodeURIComponent(path.split("/").at(-2) ?? "") : id;
    const node = nodes.find((item) => item.config.id === nodeId);
    if (!node) {
      await route.fulfill({
        status: 404,
        json: { error: { message: "Node not found", code: "node_not_found" } },
      });
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
        json: {
          error: { message: "The node changed; refresh and retry", code: "revision_conflict" },
        },
      });
      return;
    }
    await route.fulfill({ json: node });
  });
  return nodes;
}
