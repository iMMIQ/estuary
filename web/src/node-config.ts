import type { NodeConfig, NodeDraft, NodeRecord, Pair, ProviderKind } from "./types";

export type DraftErrors = Record<string, string>;

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
    api_key: "",
    preserve_api_key: false,
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

export function recordToDraft(node: NodeRecord): NodeDraft {
  return {
    ...structuredClone(node.config),
    provider: {
      ...structuredClone(node.config.provider),
      anthropic_protocol: node.config.provider.anthropic_protocol ?? "auto",
    },
    api_key: "",
    preserve_api_key: node.credentials.api_key_source === "database",
    models: recordToPairs(node.config.models),
    headers_from_env: recordToPairs(node.config.headers_from_env),
  };
}

export function draftToConfig(draft: NodeDraft): NodeConfig {
  const { preserve_api_key: _, ...config } = draft;
  const apiKey = draft.api_key.trim();
  return {
    ...config,
    id: draft.id.trim(),
    base_url: draft.base_url.trim(),
    api_key: apiKey || null,
    api_key_env: apiKey ? null : draft.api_key_env?.trim() || null,
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

export function shouldClearApiKey(draft: NodeDraft): boolean {
  return !draft.preserve_api_key && !draft.api_key.trim();
}

export function validateDraft(draft: NodeDraft): DraftErrors {
  const errors: DraftErrors = {};
  if (!draft.id.trim()) errors.id = "Node ID is required";

  try {
    const url = new URL(draft.base_url);
    if (!["http:", "https:"].includes(url.protocol) || !url.hostname) {
      errors.base_url = "Use an absolute HTTP or HTTPS URL";
    } else if (url.username || url.password || url.search || url.hash) {
      errors.base_url = "Credentials, query strings and fragments are not allowed";
    }
  } catch {
    errors.base_url = "Use an absolute HTTP or HTTPS URL";
  }

  if (!draft.health_path.trim()) errors.health_path = "Health path is required";
  if (!Number.isFinite(draft.max_concurrency) || draft.max_concurrency < 1) {
    errors.max_concurrency = "Max concurrency must be at least 1";
  }
  if (!Number.isFinite(draft.weight) || draft.weight <= 0) {
    errors.weight = "Weight must be greater than 0";
  }

  const completeModels = draft.models.filter((row) => row.key.trim() && row.value.trim());
  if (completeModels.length === 0) errors.models = "Add at least one complete model mapping";
  if (draft.models.some((row) => Boolean(row.key.trim()) !== Boolean(row.value.trim()))) {
    errors.models = "Complete or remove every model mapping row";
  }
  const modelKeys = completeModels.map((row) => row.key.trim());
  if (new Set(modelKeys).size !== modelKeys.length) errors.models = "Public model names must be unique";

  if (draft.headers_from_env.some((row) => Boolean(row.key.trim()) !== Boolean(row.value.trim()))) {
    errors.headers_from_env = "Complete or remove every header row";
  }

  if (draft.provider.type === "vllm") {
    for (const [key, value] of [
      ["version_path", draft.provider.version_path],
      ["metrics_path", draft.provider.metrics_path],
      ["tokenize_path", draft.provider.tokenize_path],
    ] as const) {
      if (!value.startsWith("/")) errors[key] = "Path must start with /";
    }
    if (draft.provider.monitor_interval_ms < 100) errors.monitor_interval_ms = "Must be at least 100 ms";
    if (draft.provider.request_timeout_ms < 1) errors.request_timeout_ms = "Must be at least 1 ms";
    if (draft.provider.telemetry_stale_ms < draft.provider.monitor_interval_ms) {
      errors.telemetry_stale_ms = "Must not be shorter than the monitor interval";
    }
    if (draft.provider.waiting_threshold < 1) errors.waiting_threshold = "Must be at least 1";
  }

  return errors;
}

export function formatCompactNumber(value: number): string {
  return new Intl.NumberFormat("en", { notation: "compact", maximumFractionDigits: 1 }).format(value);
}
