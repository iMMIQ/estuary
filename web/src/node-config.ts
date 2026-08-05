import type { NodeConfig, NodeDraft, NodeRecord, Pair, ProviderKind } from "./types";

export function pairsToRecord(pairs: Pair[]): Record<string, string> {
  return Object.fromEntries(
    pairs
      .map(({ key, value }) => [key.trim(), value.trim()])
      .filter(([key, value]) => key.length > 0 && value.length > 0),
  );
}

export function recordToPairs(record: Record<string, string>): Pair[] {
  const pairs = Object.entries(record).map(([key, value]) => ({ key, value }));
  return pairs.length > 0 ? pairs : [{ key: "", value: "" }];
}

export function createDraft(kind: ProviderKind = "vllm"): NodeDraft {
  return {
    id: "",
    base_url: "http://127.0.0.1:8000/v1",
    api_key_env: null,
    models: [{ key: "", value: "" }],
    max_concurrency: 16,
    weight: 1,
    draining: false,
    health_path: "/v1/models",
    headers: {},
    headers_from_env: [{ key: "", value: "" }],
    provider: {
      type: kind,
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

export function recordToDraft(node: NodeRecord): NodeDraft {
  return {
    ...structuredClone(node.config),
    models: recordToPairs(node.config.models),
    headers_from_env: recordToPairs(node.config.headers_from_env),
  };
}

export function draftToConfig(draft: NodeDraft): NodeConfig {
  return {
    ...draft,
    id: draft.id.trim(),
    base_url: draft.base_url.trim(),
    api_key_env: draft.api_key_env?.trim() || null,
    health_path: draft.health_path.trim(),
    models: pairsToRecord(draft.models),
    headers_from_env: pairsToRecord(draft.headers_from_env),
    provider: {
      ...draft.provider,
      kv_events:
        draft.provider.type === "vllm" ? draft.provider.kv_events : null,
    },
  };
}

export function formatCompactNumber(value: number): string {
  return new Intl.NumberFormat("en", { notation: "compact", maximumFractionDigits: 1 }).format(value);
}
