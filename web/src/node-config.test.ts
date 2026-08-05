import { describe, expect, test } from "bun:test";
import {
  createDraft,
  draftToConfig,
  pairsToRecord,
  recordToPairs,
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
      max_event_bytes: 1024,
    };
    expect(draftToConfig(draft).provider.kv_events).toBeNull();
  });

  test("rejects non-HTTP and relative base URLs", () => {
    const draft = createDraft();
    draft.id = "node-a";
    draft.models = [{ key: "chat", value: "model" }];
    draft.base_url = "localhost:8000/v1";
    expect(validateDraft(draft).base_url).toBe("Use an absolute HTTP or HTTPS URL");

    draft.base_url = "ftp://models.example/v1";
    expect(validateDraft(draft).base_url).toBe("Use an absolute HTTP or HTTPS URL");
  });

  test("rejects incomplete and duplicate public model mappings", () => {
    const draft = createDraft();
    draft.id = "node-a";
    draft.models = [{ key: "chat", value: "" }];
    expect(validateDraft(draft).models).toBe("Complete or remove every model mapping row");

    draft.models = [
      { key: "chat", value: "model-a" },
      { key: " chat ", value: "model-b" },
    ];
    expect(validateDraft(draft).models).toBe("Public model names must be unique");
  });

  test("requires telemetry freshness to cover at least one monitor interval", () => {
    const draft = createDraft();
    draft.id = "node-a";
    draft.models = [{ key: "chat", value: "model" }];
    draft.provider.monitor_interval_ms = 5000;
    draft.provider.telemetry_stale_ms = 4999;
    expect(validateDraft(draft).telemetry_stale_ms).toBe(
      "Must not be shorter than the monitor interval",
    );
  });

  test("accepts a complete default vLLM draft", () => {
    const draft = createDraft();
    draft.id = "node-a";
    draft.models = [{ key: "chat", value: "model" }];
    expect(validateDraft(draft)).toEqual({});
  });
});
