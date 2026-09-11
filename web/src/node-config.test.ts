import { describe, expect, test } from "bun:test";
import {
  createDraft,
  draftToConfig,
  pairsToRecord,
  recordToPairs,
  recordToDraft,
  shouldClearApiKey,
  validateDraft,
} from "./node-config";

describe("node config mapping", () => {
  test("drops blank key-value rows", () => {
    expect(
      pairsToRecord([
        { key: " public ", value: " upstream " },
        { key: "", value: "ignored" },
      ]),
    ).toEqual({ public: "upstream" });
  });

  test("keeps an empty editable row", () => {
    expect(recordToPairs({})).toEqual([{ key: "", value: "" }]);
  });

  test("new vLLM nodes use a finite waiting watermark", () => {
    expect(createDraft("vllm").provider.waiting_threshold).toBe(8);
    expect(createDraft("vllm").provider.anthropic_protocol).toBe("auto");
  });

  test("preserves an explicit Anthropic upstream protocol", () => {
    const draft = createDraft("openai");
    draft.provider.anthropic_protocol = "responses";
    expect(draftToConfig(draft).provider.anthropic_protocol).toBe("responses");
  });

  test("keeps model families independent on a mixed node", () => {
    const draft = createDraft("openai");
    draft.models = [{ key: "ds", value: "deepseek-chat", family: "deepseek" }, { key: "qwen", value: "qwen" }];
    const config = draftToConfig(draft);
    expect(config.model_capabilities.ds.family).toBe("deepseek");
    expect(config.model_capabilities.qwen.family).toBe("generic");
  });

  test("serializes per-model image capability", () => {
    const draft = createDraft("openai");
    draft.id = "node-a";
    draft.models = [{ key: "text", value: "internal-text", multimodal: false }];
    expect(draftToConfig(draft).model_capabilities).toEqual({
      text: { multimodal: false, family: "generic" },
    });
  });

  test("defaults legacy node records to automatic Anthropic routing", () => {
    const config = draftToConfig(createDraft("openai"));
    const legacyProvider = { ...config.provider } as Partial<typeof config.provider>;
    delete legacyProvider.anthropic_protocol;
    const record = {
      config: { ...config, provider: legacyProvider },
      credentials: { api_key_source: "none" },
    };
    const draft = recordToDraft(record as Parameters<typeof recordToDraft>[0]);
    expect(draft.provider.anthropic_protocol).toBe("auto");
  });

  test("stores a directly entered Bearer key and drops the legacy fallback", () => {
    const draft = createDraft("openai");
    draft.api_key = "  upstream-secret  ";
    draft.api_key_env = "LEGACY_KEY";
    const config = draftToConfig(draft);
    expect(config.api_key).toBe("upstream-secret");
    expect(config.api_key_env).toBeNull();
    expect(shouldClearApiKey(draft)).toBeFalse();
  });

  test("distinguishes preserving and explicitly removing a stored key", () => {
    const draft = createDraft("openai");
    draft.preserve_api_key = true;
    expect(draftToConfig(draft).api_key).toBeNull();
    expect(shouldClearApiKey(draft)).toBeFalse();

    draft.preserve_api_key = false;
    expect(shouldClearApiKey(draft)).toBeTrue();
  });

  test("generic providers cannot retain KV event settings", () => {
    const draft = createDraft("openai");
    draft.id = "node-a";
    draft.models = [{ key: "chat", value: "model" }];
    draft.provider.kv_events = {
      endpoint: "tcp://127.0.0.1:5557",
      replay_endpoint: null,
      topic: "kv-events",
      reconnect_ms: 1000,
      max_blocks: 1000,
      max_directory_bytes: 1024 * 1024,
      max_event_bytes: 1024,
    };
    expect(draftToConfig(draft).provider.kv_events).toBeNull();
  });

  test("rejects non-HTTP and relative base URLs", () => {
    const draft = createDraft();
    draft.id = "node-a";
    draft.models = [{ key: "chat", value: "model" }];
    draft.base_url = "localhost:8000/v1";
    expect(validateDraft(draft).base_url).toBe("validation.absoluteUrl");

    draft.base_url = "ftp://models.example/v1";
    expect(validateDraft(draft).base_url).toBe("validation.absoluteUrl");
  });

  test("rejects incomplete and duplicate public model mappings", () => {
    const draft = createDraft();
    draft.id = "node-a";
    draft.models = [{ key: "chat", value: "" }];
    expect(validateDraft(draft).models).toBe("validation.modelIncomplete");

    draft.models = [
      { key: "chat", value: "model-a" },
      { key: " chat ", value: "model-b" },
    ];
    expect(validateDraft(draft).models).toBe("validation.modelUnique");
  });

  test("requires telemetry freshness to cover at least one monitor interval", () => {
    const draft = createDraft();
    draft.id = "node-a";
    draft.models = [{ key: "chat", value: "model" }];
    draft.provider.monitor_interval_ms = 5000;
    draft.provider.telemetry_stale_ms = 4999;
    expect(validateDraft(draft).telemetry_stale_ms).toBe("validation.telemetryInterval");
  });

  test("accepts a complete default vLLM draft", () => {
    const draft = createDraft();
    draft.id = "node-a";
    draft.models = [{ key: "chat", value: "model" }];
    expect(validateDraft(draft)).toEqual({});
  });

  test.each([NaN, Infinity, -Infinity, 1.5, Number.MAX_SAFE_INTEGER + 1])("rejects invalid integer limits: %s", (value) => {
    const draft = createDraft();
    draft.max_concurrency = value;
    draft.provider.monitor_interval_ms = value;
    draft.provider.request_timeout_ms = value;
    draft.provider.telemetry_stale_ms = value;
    draft.provider.waiting_threshold = value;
    draft.provider.tokenize_cache_entries = value;
    draft.provider.kv_events = {
      endpoint: "tcp://127.0.0.1:5557", replay_endpoint: null, topic: "",
      reconnect_ms: value, max_blocks: value, max_directory_bytes: value, max_event_bytes: value,
    };
    const errors = validateDraft(draft);
    for (const field of ["max_concurrency", "monitor_interval_ms", "request_timeout_ms", "telemetry_stale_ms", "waiting_threshold", "tokenize_cache_entries", "kv_reconnect_ms", "kv_max_blocks", "kv_max_directory_bytes", "kv_max_event_bytes"]) {
      expect(errors[field]).toBeDefined();
    }
  });

  test("bounds tokenization cache entries to the server limit", () => {
    const draft = createDraft();
    for (const value of [0, 65537]) {
      draft.provider.tokenize_cache_entries = value;
      expect(validateDraft(draft).tokenize_cache_entries).toBeDefined();
    }
    for (const value of [1, 65536]) {
      draft.provider.tokenize_cache_entries = value;
      expect(validateDraft(draft).tokenize_cache_entries).toBeUndefined();
    }
  });
});
