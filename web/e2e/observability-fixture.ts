import type { Observability, ScopeMetrics } from "../src/observability-types";

// Deterministic example data for browser tests; never used by the application.
export function observabilityFixture(
  nodeIds: string[],
  modelIds: string[],
  scope = "fleet",
  id = "",
  window = 300,
): Observability {
  const now = Date.now();
  const rate = (value: number) => ({ raw: value, smoothed: value * 0.9, mean_60s: value * 0.8 });
  const counts = {
    started: 100,
    success: 90,
    failed: 3,
    cancelled: 5,
    unknown: 2,
    retries: 4,
    retried_requests: 3,
    generation_requests: 100,
    input_tokens: 10000,
    output_tokens: 3000,
    cached_tokens: 5000,
    cache_input_tokens: 10000,
    input_samples: 98,
    output_samples: 98,
    cache_samples: 98,
    tpot_reported: 50,
    tpot_estimated: 40,
    route_exact: 50,
    route_approximate: 40,
    route_none: 10,
  };
  const distribution = { samples: 90, p50: 0.1, p95: 0.3, mean: 0.13, overflow: 0 };
  const create = (kind: ScopeMetrics["kind"], name: string): ScopeMetrics => ({
    peaks: { output: 450, active: 8, queued: 4, waiting: 2, kv_usage: 0.7 },
    kind,
    id: name,
    instance: kind === "node" ? 1 : null,
    retired: false,
    total: { ...counts },
    window: { ...counts },
    observed_seconds: window,
    rates: { incoming: rate(10), completed: rate(9), input: rate(1000), output: rate(300) },
    latency: {
      ttft: { ...distribution },
      upstream_ttft: { ...distribution },
      duration: { ...distribution, p95: 3 },
      ingress: { ...distribution, p95: 0.01 },
      queue: { ...distribution, p95: 0.02 },
      tpot: { ...distribution, p50: 0.02, p95: 0.04 },
      generation_rate: { ...distribution, p50: 50, p95: 70 },
    },
    success_ratio: 0.9,
    failure_ratio: 0.03,
    cancellation_ratio: 0.05,
    unknown_ratio: 0.02,
    usage_coverage: 0.98,
    cache_hit_ratio: 0.5,
    pressure: {
      active: nodeIds.length ? 5 : 0,
      upstream_active: nodeIds.length ? 5 : 0,
      capacity: nodeIds.length * 16,
      queued: 2,
      admission_waiters: 0,
      buffer_waiters: 0,
      buffer_bytes: 1024,
      buffer_limit: 1048576,
      running: nodeIds.length ? 5 : null,
      waiting: nodeIds.length ? 2 : null,
      kv_usage: nodeIds.length ? 0.6 : null,
      fresh_nodes: nodeIds.length,
      provider_nodes: nodeIds.length,
      telemetry_age_ms: 500,
    },
    engine: {
      input: rate(1200),
      output: rate(400),
      requests: rate(12),
      cache_hit_ratio: 0.75,
      cache_hits_60s: 75,
      cache_queries_60s: 100,
      preemptions_per_minute: 0,
      reporting_nodes: nodeIds.length,
      expected_nodes: kind === "model" ? 0 : nodeIds.length,
      input_nodes: nodeIds.length,
      output_nodes: nodeIds.length,
      request_nodes: nodeIds.length,
      resets: 0,
    },
  });
  const fleet = create("fleet", "");
  const nodes = nodeIds.map((id) => create("node", id));
  const models = modelIds.map((id) => create("model", id));
  const selected =
    scope === "node"
      ? (nodes.find((node) => node.id === id) ?? null)
      : scope === "model"
        ? (models.find((model) => model.id === id) ?? null)
        : fleet;
  return {
    instance_id: "test-process",
    generated_at_unix_ms: now,
    retention_seconds: 900,
    step_seconds: 5,
    window_seconds: window,
    scope_limit_per_kind: 32,
    limited_events: 0,
    fleet,
    nodes,
    models,
    selected,
    history: Array.from({ length: 180 }, (_, index) => ({
      peaks: {
        output: 300 + Math.sin(index / 4) * 80,
        active: 5,
        queued: 2,
        waiting: 2,
        kv_usage: 0.6,
      },
      at_unix_ms: now - (179 - index) * 5000,
      rates: { ...fleet.rates, output: rate(300 + Math.sin(index / 4) * 80) },
      ttft_p95: 0.3 + Math.sin(index / 7) * 0.03,
      queue_p95: 0.02,
      pressure: fleet.pressure,
      engine: fleet.engine,
    })),
  };
}
