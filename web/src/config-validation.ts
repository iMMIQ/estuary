import {
  type IntegerBounds,
  NODE_INTEGER_LIMITS,
  type NodeConfig,
  type NodeIntegerLimits,
  RESERVED_UPSTREAM_HEADERS,
} from "./generated/config";

const INTEGER_LIMIT_KEYS: Record<string, keyof NodeIntegerLimits> = {
  max_concurrency: "max_concurrency",
  "provider.monitor_interval_ms": "monitor_interval_ms",
  "provider.request_timeout_ms": "request_timeout_ms",
  "provider.telemetry_stale_ms": "telemetry_stale_ms",
  "provider.waiting_threshold": "waiting_threshold",
  "provider.tokenize_cache_entries": "tokenize_cache_entries",
  "provider.kv_events.reconnect_ms": "kv_reconnect_ms",
  "provider.kv_events.max_blocks": "kv_max_blocks",
  "provider.kv_events.max_directory_bytes": "kv_max_directory_bytes",
  "provider.kv_events.max_event_bytes": "kv_max_event_bytes",
};

const notBlank = (value: string): boolean => value.trim().length > 0;

const inBounds = (value: number, bounds: IntegerBounds): boolean =>
  Number.isSafeInteger(value) && value >= bounds.min && value <= bounds.max;

const validHeader = (name: string): boolean =>
  /^[!#$%&'*+.^_`|~0-9A-Za-z-]+$/.test(name) &&
  !RESERVED_UPSTREAM_HEADERS.includes(name.toLowerCase());

const withoutUrlParts = (url: URL): boolean =>
  !url.username &&
  !url.password &&
  !url.search &&
  !url.hash &&
  !url.href.includes("?") &&
  !url.href.includes("#");

function parseBaseUrl(value: string): URL | null {
  try {
    const url = new URL(value);
    return ["http:", "https:"].includes(url.protocol) && url.hostname ? url : null;
  } catch {
    return null;
  }
}

function validProviderPath(base: URL | null, value: string): boolean {
  if (!base || !value.startsWith("/")) return false;
  try {
    const url = new URL(value, base);
    return url.origin === base.origin && !url.href.includes("?") && !url.href.includes("#");
  } catch {
    return false;
  }
}

function validTcpEndpoint(value: string): boolean {
  try {
    const url = new URL(value);
    return (
      url.protocol === "tcp:" &&
      Boolean(url.hostname) &&
      !url.hostname.includes("*") &&
      /^\d+$/.test(url.port) &&
      Number(url.port) <= 65535 &&
      withoutUrlParts(url) &&
      ["", "/"].includes(url.pathname)
    );
  } catch {
    return false;
  }
}

/** Validate a node configuration matching backend typed validation. */
export function validateNodeConfig(config: NodeConfig): Record<string, string> {
  const errors: Record<string, string> = {};
  const set = (field: string, error: string) => {
    if (!errors[field]) errors[field] = error;
  };

  if (!notBlank(config.id)) set("id", "validation.nodeIdRequired");
  const base = parseBaseUrl(config.base_url);
  if (!base) set("base_url", "validation.absoluteUrl");
  else if (!withoutUrlParts(base)) set("base_url", "validation.urlParts");
  if (config.api_key !== null && !notBlank(config.api_key)) set("api_key", "validation.notBlank");
  if (config.api_key_env !== null && !notBlank(config.api_key_env))
    set("api_key_env", "validation.notBlank");
  if (!notBlank(config.health_path)) set("health_path", "validation.healthPathRequired");
  if (!inBounds(config.max_concurrency, NODE_INTEGER_LIMITS.max_concurrency))
    set("max_concurrency", "validation.concurrency");
  if (!Number.isFinite(config.weight) || config.weight <= 0) set("weight", "validation.weight");

  const modelEntries = Object.entries(config.models);
  if (modelEntries.length === 0 || !modelEntries.every(([k, v]) => notBlank(k) && notBlank(v)))
    set("models", "validation.modelRequired");
  if (
    !Object.keys(config.model_capabilities).every(
      (k) =>
        notBlank(k) &&
        (k === "*" || Object.hasOwn(config.models, k) || Object.hasOwn(config.models, "*")),
    )
  )
    set("models", "validation.capabilityMapping");
  if (!Object.keys(config.headers).every(validHeader)) set("headers", "validation.headerInvalid");
  if (!Object.entries(config.headers_from_env).every(([k, v]) => validHeader(k) && notBlank(v)))
    set("headers_from_env", "validation.headerInvalid");

  const { provider } = config;
  if (provider.type !== "vllm") {
    if (provider.kv_events !== null) set("kv_endpoint", "validation.kvProvider");
    return errors;
  }

  for (const field of ["version_path", "metrics_path", "tokenize_path"] as const) {
    if (!validProviderPath(base, provider[field])) set(field, "validation.pathSlash");
  }
  if (!inBounds(provider.monitor_interval_ms, NODE_INTEGER_LIMITS.monitor_interval_ms))
    set("monitor_interval_ms", "validation.min100ms");
  if (!inBounds(provider.request_timeout_ms, NODE_INTEGER_LIMITS.request_timeout_ms))
    set("request_timeout_ms", "validation.min1ms");
  if (!inBounds(provider.telemetry_stale_ms, NODE_INTEGER_LIMITS.telemetry_stale_ms))
    set("telemetry_stale_ms", "validation.min1ms");
  else if (provider.telemetry_stale_ms < provider.monitor_interval_ms)
    set("telemetry_stale_ms", "validation.telemetryInterval");
  if (!inBounds(provider.waiting_threshold, NODE_INTEGER_LIMITS.waiting_threshold))
    set("waiting_threshold", "validation.min1");
  if (!inBounds(provider.tokenize_cache_entries, NODE_INTEGER_LIMITS.tokenize_cache_entries))
    set("tokenize_cache_entries", "validation.tokenizeEntries");

  const kv = provider.kv_events;
  if (kv) {
    if (!validTcpEndpoint(kv.endpoint)) set("kv_endpoint", "validation.tcpEndpoint");
    if (kv.replay_endpoint !== null && !validTcpEndpoint(kv.replay_endpoint))
      set("kv_replay_endpoint", "validation.tcpEndpoint");
    if (!inBounds(kv.reconnect_ms, NODE_INTEGER_LIMITS.kv_reconnect_ms))
      set("kv_reconnect_ms", "validation.min1ms");
    if (!inBounds(kv.max_blocks, NODE_INTEGER_LIMITS.kv_max_blocks))
      set("kv_max_blocks", "validation.min1");
    if (!inBounds(kv.max_directory_bytes, NODE_INTEGER_LIMITS.kv_max_directory_bytes))
      set("kv_max_directory_bytes", "validation.min1byte");
    if (!inBounds(kv.max_event_bytes, NODE_INTEGER_LIMITS.kv_max_event_bytes))
      set("kv_max_event_bytes", "validation.min1byte");
  }
  return errors;
}

/** Numeric controls read bounds from the same backend-generated limits. */
export function integerBounds(path: string): IntegerBounds {
  const key = INTEGER_LIMIT_KEYS[path];
  if (!key) throw new Error(`No integer limit for ${path}`);
  return NODE_INTEGER_LIMITS[key];
}
