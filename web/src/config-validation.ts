import type { NodeConfig, NodeValidationRule, RuleKind } from "./generated/config";
import rawRules from "./generated/config-rules.json";
import reservedHeaders from "./generated/reserved-headers.json";

const rules: NodeValidationRule[] = rawRules as NodeValidationRule[];

function atPath(value: unknown, path: string): unknown {
  return path.split(".").reduce<unknown>((current, key) => {
    if (current === null || typeof current !== "object") return undefined;
    return (current as Record<string, unknown>)[key];
  }, value);
}

function entries(value: unknown): [string, unknown][] {
  return Object.entries(value as Record<string, unknown>);
}

function notBlank(value: unknown): boolean {
  return typeof value === "string" && value.trim().length > 0;
}

function validHeader(name: string): boolean {
  return (
    /^[!#$%&'*+.^_`|~0-9A-Za-z-]+$/.test(name) && !reservedHeaders.includes(name.toLowerCase())
  );
}

function withoutUrlParts(url: URL): boolean {
  return (
    !url.username &&
    !url.password &&
    !url.search &&
    !url.hash &&
    !url.href.includes("?") &&
    !url.href.includes("#")
  );
}

function validRule(rule: RuleKind, value: unknown, config: NodeConfig): boolean {
  if (value === null || value === undefined)
    return ["not_blank", "tcp_endpoint", "kv_provider"].includes(rule.kind);
  switch (rule.kind) {
    case "not_blank":
      return notBlank(value);
    case "integer":
      return (
        typeof value === "number" &&
        Number.isSafeInteger(value) &&
        value >= rule.minimum &&
        value <= rule.maximum
      );
    case "positive":
      return typeof value === "number" && Number.isFinite(value) && value > 0;
    case "model_mappings":
      return (
        entries(value).length > 0 &&
        entries(value).every(([key, val]) => notBlank(key) && notBlank(val))
      );
    case "capability_mappings":
      return entries(value).every(
        ([key]) =>
          notBlank(key) &&
          (key === "*" || Object.hasOwn(config.models, key) || Object.hasOwn(config.models, "*")),
      );
    case "headers":
      return entries(value).every(([key]) => validHeader(key));
    case "environment_headers":
      return entries(value).every(([key, val]) => validHeader(key) && notBlank(val));
    case "at_least_field": {
      const other = atPath(config, rule.other);
      return typeof value === "number" && typeof other === "number" && value >= other;
    }
    case "kv_provider":
      return config.provider.type === "vllm";
    case "http_url":
    case "url_without_parts":
    case "provider_path":
    case "tcp_endpoint": {
      if (typeof value !== "string") return false;
      try {
        const url =
          rule.kind === "provider_path" ? new URL(value, config.base_url) : new URL(value);
        if (rule.kind === "http_url")
          return ["http:", "https:"].includes(url.protocol) && Boolean(url.hostname);
        if (rule.kind === "url_without_parts") return withoutUrlParts(url);
        if (rule.kind === "provider_path")
          return (
            value.startsWith("/") &&
            url.origin === new URL(config.base_url).origin &&
            !url.href.includes("?") &&
            !url.href.includes("#")
          );
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
  }
}

/** Interpret the backend's rules; UI-only checks for unfinished rows stay in the editor. */
export function validateNodeConfig(config: NodeConfig): Record<string, string> {
  const errors: Record<string, string> = {};
  for (const { condition, path, error_field, error, rule } of rules) {
    const enabled =
      condition === "always" ||
      (config.provider.type === "vllm" &&
        (condition === "vllm" || config.provider.kv_events !== null));
    if (enabled && !errors[error_field] && !validRule(rule, atPath(config, path), config)) {
      errors[error_field] = error;
    }
  }
  return errors;
}

/** Numeric controls read bounds from the same contract as the validators. */
export function integerBounds(path: string): { min: number; max: number } {
  const rule = rules.find((rule) => rule.path === path)?.rule;
  if (rule?.kind !== "integer") throw new Error(`No integer rule for ${path}`);
  return { min: rule.minimum, max: Math.min(rule.maximum, Number.MAX_SAFE_INTEGER) };
}
