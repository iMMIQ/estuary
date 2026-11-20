import type { ModelCapabilityConfig, NodeConfig, NodeSnapshot } from "./generated/config";

export type {
  ModelCapabilityConfig,
  NodeConfig,
  Point as HistoryPoint,
  ProviderConfig,
  ProviderKind,
  ScopeSnapshot as ScopeMetrics,
  Snapshot as Observability,
  VllmKvEventsConfig as KvEventsConfig,
} from "./generated/config";

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
  runtime: NodeSnapshot;
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
  runtime: NodeSnapshot;
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
  inherit_capability?: boolean;
}

export interface NodeDraft extends Omit<NodeConfig, "api_key" | "models" | "model_capabilities"> {
  api_key: string;
  preserve_api_key: boolean;
  models: Pair[];
  wildcard_capability?: ModelCapabilityConfig;
  unmapped_capabilities?: Record<string, ModelCapabilityConfig>;
}
