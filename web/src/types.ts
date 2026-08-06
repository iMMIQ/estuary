export type ProviderKind = "openai" | "vllm";
export type AnthropicProtocol = "auto" | "native" | "responses" | "chat";
export type HealthState = "starting" | "healthy" | "degraded" | "unhealthy";
export type LifecycleState = "serving" | "draining";
export type CircuitState = "closed" | "open" | "half_open";

export interface KvEventsConfig {
  endpoint: string;
  replay_endpoint: string | null;
  topic: string;
  reconnect_ms: number;
  max_blocks: number;
  max_directory_bytes: number;
  max_event_bytes: number;
}

export interface ProviderConfig {
  type: ProviderKind;
  anthropic_protocol: AnthropicProtocol;
  version_path: string;
  metrics_path: string;
  tokenize_path: string;
  monitor_interval_ms: number;
  request_timeout_ms: number;
  telemetry_stale_ms: number;
  waiting_threshold: number;
  tokenize_cache_entries: number;
  kv_events: KvEventsConfig | null;
}

export interface NodeConfig {
  id: string;
  base_url: string;
  api_key: string | null;
  api_key_env: string | null;
  models: Record<string, string>;
  max_concurrency: number;
  weight: number;
  draining: boolean;
  health_path: string;
  headers: Record<string, string>;
  headers_from_env: Record<string, string>;
  provider: ProviderConfig;
}

export interface NodeRuntime {
  id: string;
  base_url: string;
  provider: ProviderKind;
  health: HealthState;
  lifecycle: LifecycleState;
  circuit: CircuitState;
  circuit_open_until_unix_ms: number | null;
  circuit_failures: number;
  circuit_half_open_in_flight: number;
  active: number;
  available: number;
  max_concurrency: number;
  weight: number;
  provider_state: "generic" | "checking" | "ready" | "incompatible";
  provider_version: string | null;
  provider_last_error: string | null;
  upstream_running: number | null;
  upstream_waiting: number | null;
  kv_cache_usage: number | null;
  latency_ewma_ms: number;
  error_ewma: number;
  last_error: string | null;
  last_change_unix_ms: number;
  provider_generation: number;
  provider_telemetry_updated_unix_ms: number | null;
}

export interface AdmissionSnapshot {
  state:
    | "accepting"
    | "draining"
    | "health_blocked"
    | "provider_blocked"
    | "circuit_open"
    | "circuit_limited"
    | "waiting_watermark"
    | "at_capacity";
  reason: string;
  routable: boolean;
  accepting_assignments: boolean;
  telemetry_fresh: boolean;
  waiting_watermark_blocked: boolean;
}

export interface NodeRecord {
  config: NodeConfig;
  credentials: {
    api_key_configured: boolean;
    api_key_source: "database" | "environment" | "none";
    header_names: string[];
  };
  revision: number;
  created_at_unix_ms: number;
  updated_at_unix_ms: number;
  runtime: NodeRuntime;
  admission: AdmissionSnapshot;
  exact_kv_authoritative: boolean;
  exact_kv_blocks: number;
  exact_kv_bytes: number;
}

export interface GatewayStatus {
  status: "ready" | "not_ready";
  live: boolean;
  ready: boolean;
  version: string;
  generated_at_unix_ms: number;
  fleet: {
    total_nodes: number;
    routable_nodes: number;
    accepting_nodes: number;
    models: number;
    active_requests: number;
    total_concurrency: number;
    available_concurrency: number;
  };
  queue: {
    requests: number;
    bytes: number;
    max_requests: number;
    max_bytes: number;
  };
  response_buffer: {
    used_bytes: number;
    max_bytes: number;
    waiting_responses: number;
  };
  routing: {
    prefix_enabled: boolean;
  };
}

export interface PreflightResponse {
  ok: true;
  runtime: NodeRuntime;
  admission: AdmissionSnapshot;
  checks: {
    configuration: "passed";
    provider: "passed";
    health: "passed";
  };
}

export interface Pair {
  key: string;
  value: string;
}

export interface NodeDraft extends Omit<NodeConfig, "api_key" | "models" | "headers_from_env"> {
  api_key: string;
  preserve_api_key: boolean;
  models: Pair[];
  headers_from_env: Pair[];
}
