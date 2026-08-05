export type ProviderKind = "openai" | "vllm";
export type HealthState = "starting" | "healthy" | "degraded" | "unhealthy";
export type LifecycleState = "serving" | "draining";
export type CircuitState = "closed" | "open" | "half_open";

export interface KvEventsConfig {
  endpoint: string;
  replay_endpoint: string | null;
  topic: string;
  reconnect_ms: number;
  max_blocks: number;
  max_event_bytes: number;
}

export interface ProviderConfig {
  type: ProviderKind;
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
  health: HealthState;
  lifecycle: LifecycleState;
  circuit: CircuitState;
  active: number;
  available: number;
  max_concurrency: number;
  provider_state: "generic" | "checking" | "ready" | "incompatible";
  provider_version: string | null;
  provider_last_error: string | null;
  upstream_running: number | null;
  upstream_waiting: number | null;
  kv_cache_usage: number | null;
  latency_ewma_ms: number;
  error_ewma: number;
}

export interface NodeRecord {
  config: NodeConfig;
  revision: number;
  created_at_unix_ms: number;
  updated_at_unix_ms: number;
  runtime: NodeRuntime;
  exact_kv_authoritative: boolean;
  exact_kv_blocks: number;
}

export interface Pair {
  key: string;
  value: string;
}

export interface NodeDraft extends Omit<NodeConfig, "models" | "headers_from_env"> {
  models: Pair[];
  headers_from_env: Pair[];
}
