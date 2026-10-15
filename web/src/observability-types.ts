interface Rate {
  raw: number | null;
  smoothed: number | null;
  mean_60s: number | null;
}
interface Rates {
  incoming: Rate;
  completed: Rate;
  input: Rate;
  output: Rate;
}
interface Distribution {
  samples: number;
  p50: number | null;
  p95: number | null;
  mean: number | null;
  overflow: number;
}
interface Counts {
  started: number;
  success: number;
  failed: number;
  cancelled: number;
  unknown: number;
  retries: number;
  retried_requests: number;
  generation_requests: number;
  input_tokens: number;
  output_tokens: number;
  cached_tokens: number;
  cache_input_tokens: number;
  input_samples: number;
  output_samples: number;
  cache_samples: number;
  tpot_reported: number;
  tpot_estimated: number;
  route_exact: number;
  route_approximate: number;
  route_none: number;
}
interface Pressure {
  active: number;
  upstream_active: number;
  capacity: number;
  queued: number;
  admission_waiters: number;
  buffer_waiters: number;
  buffer_bytes: number;
  buffer_limit: number;
  running: number | null;
  waiting: number | null;
  kv_usage: number | null;
  fresh_nodes: number;
  provider_nodes: number;
  telemetry_age_ms: number | null;
}
interface EngineTelemetry {
  input: Rate;
  output: Rate;
  requests: Rate;
  cache_hit_ratio: number | null;
  cache_hits_60s: number | null;
  cache_queries_60s: number | null;
  preemptions_per_minute: number | null;
  reporting_nodes: number;
  expected_nodes: number;
  input_nodes: number;
  output_nodes: number;
  request_nodes: number;
  resets: number;
}
export interface ScopeMetrics {
  peaks: {
    output: number | null;
    active: number;
    queued: number;
    waiting: number | null;
    kv_usage: number | null;
  };
  kind: "fleet" | "node" | "model";
  id: string;
  instance: number | null;
  retired: boolean;
  total: Counts;
  window: Counts;
  observed_seconds: number;
  rates: Rates;
  latency: Record<
    "ttft" | "upstream_ttft" | "duration" | "ingress" | "queue" | "tpot" | "generation_rate",
    Distribution
  >;
  success_ratio: number | null;
  failure_ratio: number | null;
  cancellation_ratio: number | null;
  unknown_ratio: number | null;
  usage_coverage: number | null;
  cache_hit_ratio: number | null;
  pressure: Pressure;
  engine: EngineTelemetry;
}
export interface HistoryPoint {
  peaks: ScopeMetrics["peaks"];
  at_unix_ms: number;
  rates: Rates;
  ttft_p95: number | null;
  queue_p95: number | null;
  pressure: Pressure;
  engine: EngineTelemetry;
}
export interface Observability {
  instance_id: string;
  generated_at_unix_ms: number;
  retention_seconds: number;
  step_seconds: number;
  window_seconds: number;
  scope_limit_per_kind: number;
  limited_events: number;
  fleet: ScopeMetrics;
  nodes: ScopeMetrics[];
  models: ScopeMetrics[];
  selected: ScopeMetrics | null;
  history: HistoryPoint[];
}
