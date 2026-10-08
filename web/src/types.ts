import type {
  ModelCapabilityConfig,
  ModelFamily,
  NodeConfig,
  ProviderKind,
} from "./generated/config";

export type {
  AnthropicProtocol,
  ModelCapabilityConfig,
  NodeConfig,
  ProviderKind,
  VllmKvEventsConfig as KvEventsConfig,
} from "./generated/config";

type HealthState = "starting" | "healthy" | "degraded" | "unhealthy";
type LifecycleState = "serving" | "draining";
type CircuitState = "closed" | "open" | "half_open";

interface NodeRuntime {
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
  prompt_tokens_per_second: number | null;
  generation_tokens_per_second: number | null;
  requests_per_second: number | null;
  prefix_cache_queries_total: number | null;
  prefix_cache_hits_total: number | null;
  prefix_cache_hit_rate: number | null;
  preemptions_total: number | null;
  latency_ewma_ms: number;
  ttft_ewma_ms: number | null;
  pending_prefill_tokens: number;
  pending_decode_tokens: number;
  error_ewma: number;
  last_error: string | null;
  last_change_unix_ms: number;
  provider_generation: number;
  provider_telemetry_updated_unix_ms: number | null;
}

interface AdmissionSnapshot {
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
    admission_waiters: number;
    max_requests: number;
    max_bytes: number;
  };
  connections: {
    public: number;
    max_public: number;
    top_ips: { ip: string; active: number }[];
    ip_limits: { ip: string; limit: number }[];
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
  multimodal?: boolean;
  family?: ModelFamily;
  inherit_capability?: boolean;
}

export interface NodeDraft
  extends Omit<NodeConfig, "api_key" | "models" | "model_capabilities" | "headers_from_env"> {
  api_key: string;
  preserve_api_key: boolean;
  models: Pair[];
  headers_from_env: Pair[];
  wildcard_capability?: ModelCapabilityConfig;
  unmapped_capabilities?: Record<string, ModelCapabilityConfig>;
}
