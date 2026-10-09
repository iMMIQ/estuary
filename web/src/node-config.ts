import { configDefaults as defaults } from "./config-contract";
import { validateNodeConfig } from "./config-validation";
import type {
  ModelCapabilityConfig,
  NodeConfig,
  NodeDraft,
  NodeRecord,
  Pair,
  ProviderKind,
} from "./types";

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

export function effectiveCapability(config: NodeConfig, model: string): ModelCapabilityConfig {
  return (
    config.model_capabilities?.[model] ??
    config.model_capabilities?.["*"] ??
    structuredClone(defaults.capability)
  );
}

export function effectiveModelMappings(config: NodeConfig): [string, string][] {
  const entries = Object.entries(config.models);
  const fallback = config.models["*"];
  if (fallback !== undefined) {
    for (const model of Object.keys(config.model_capabilities ?? {})) {
      if (model !== "*" && !Object.hasOwn(config.models, model))
        entries.push([model, fallback === "*" ? model : fallback]);
    }
  }
  return entries;
}

export function protocolPaths(
  config: NodeConfig,
  model: string,
): { responses: string; messages: string } {
  if (effectiveCapability(config, model).family === "deepseek")
    return {
      responses: "Responses → Chat Completions → Responses",
      messages: "Messages → Chat Completions → Messages",
    };
  const protocol =
    config.provider.anthropic_protocol === "auto"
      ? config.provider.type === "vllm"
        ? "native"
        : "chat"
      : config.provider.anthropic_protocol;
  return {
    responses: "Responses → Responses",
    messages:
      protocol === "native"
        ? "Messages → Messages"
        : protocol === "responses"
          ? "Messages → Responses → Messages"
          : "Messages → Chat Completions → Messages",
  };
}

function modelPairs(node: NodeRecord): Pair[] {
  const pairs = Object.entries(node.config.models).map(([key, value]) => ({
    key,
    value,
    multimodal: effectiveCapability(node.config, key).multimodal,
    family: effectiveCapability(node.config, key).family ?? defaults.capability.family,
    inherit_capability: !Object.hasOwn(node.config.model_capabilities ?? {}, key),
  }));
  return pairs.length > 0 ? pairs : [{ key: "", value: "", ...defaults.capability }];
}

export function createDraft(kind: ProviderKind = "vllm"): NodeDraft {
  return {
    ...structuredClone(defaults.editor),
    api_key: "",
    preserve_api_key: false,
    models: [{ key: "", value: "" }],
    headers_from_env: [{ key: "", value: "" }],
    provider: { ...structuredClone(defaults.editor.provider), type: kind },
  };
}

export function recordToDraft(node: NodeRecord): NodeDraft {
  return {
    ...structuredClone(node.config),
    provider: {
      ...structuredClone(defaults.node.provider),
      ...structuredClone(node.config.provider),
      anthropic_protocol:
        node.config.provider.anthropic_protocol ?? defaults.node.provider.anthropic_protocol,
    },
    api_key: "",
    preserve_api_key: node.credentials.api_key_source === "database",
    models: modelPairs(node),
    wildcard_capability: structuredClone(node.config.model_capabilities?.["*"]),
    unmapped_capabilities: structuredClone(
      Object.fromEntries(
        Object.entries(node.config.model_capabilities ?? {}).filter(
          ([model]) => model !== "*" && !Object.hasOwn(node.config.models, model),
        ),
      ),
    ),
    headers_from_env: recordToPairs(node.config.headers_from_env),
  };
}

export function draftToConfig(draft: NodeDraft): NodeConfig {
  const { preserve_api_key: _, wildcard_capability, unmapped_capabilities, ...config } = draft;
  const apiKey = draft.api_key.trim();
  return {
    ...config,
    id: draft.id.trim(),
    base_url: draft.base_url.trim(),
    api_key: apiKey || null,
    api_key_env: apiKey ? null : draft.api_key_env?.trim() || null,
    health_path: draft.health_path.trim(),
    models: pairsToRecord(draft.models),
    model_capabilities: {
      ...(draft.models.some((row) => row.key.trim() === "*" && row.value.trim())
        ? unmapped_capabilities
        : {}),
      ...(wildcard_capability ? { "*": wildcard_capability } : {}),
      ...Object.fromEntries(
        draft.models
          .filter((row) => !row.inherit_capability)
          .map(
            (row) =>
              [
                row.key.trim(),
                {
                  multimodal: row.multimodal ?? defaults.capability.multimodal,
                  family: row.family ?? defaults.capability.family,
                },
              ] as const,
          )
          .filter(([key]) => key.length > 0),
      ),
    },
    headers_from_env: pairsToRecord(draft.headers_from_env),
    provider: {
      ...draft.provider,
      kv_events: draft.provider.type === "vllm" ? draft.provider.kv_events : null,
    },
  };
}

export function draftWildcardCapability(draft: NodeDraft): ModelCapabilityConfig | undefined {
  const row = draft.models.find((item) => item.key.trim() === "*" && !item.inherit_capability);
  return row
    ? {
        family: row.family ?? defaults.capability.family,
        multimodal: row.multimodal ?? defaults.capability.multimodal,
      }
    : draft.wildcard_capability;
}

export function shouldClearApiKey(draft: NodeDraft): boolean {
  return !draft.preserve_api_key && !draft.api_key.trim();
}

export function validateDraft(draft: NodeDraft): DraftErrors {
  const errors: DraftErrors = validateNodeConfig(draftToConfig(draft));

  const completeModels = draft.models.filter((row) => row.key.trim() && row.value.trim());
  if (completeModels.length === 0) errors.models = "validation.modelRequired";
  if (draft.models.some((row) => Boolean(row.key.trim()) !== Boolean(row.value.trim()))) {
    errors.models = "validation.modelIncomplete";
  }
  const modelKeys = completeModels.map((row) => row.key.trim());
  if (new Set(modelKeys).size !== modelKeys.length) errors.models = "validation.modelUnique";

  if (draft.headers_from_env.some((row) => Boolean(row.key.trim()) !== Boolean(row.value.trim()))) {
    errors.headers_from_env = "validation.headerIncomplete";
  }

  return errors;
}

export function formatCompactNumber(value: number, locale = "en"): string {
  return new Intl.NumberFormat(locale, { notation: "compact", maximumFractionDigits: 1 }).format(
    value,
  );
}
