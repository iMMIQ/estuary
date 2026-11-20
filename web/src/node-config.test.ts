import { describe, expect, test } from "bun:test";
import {
  createDraft,
  draftToConfig,
  effectiveCapability,
  pairsToRecord,
  protocolPaths,
  recordToDraft,
  shouldClearApiKey,
  validateDraft,
} from "./node-config";
import type { NodeRecord } from "./types";

describe("node config mapping", () => {
  test("round trips wildcard and explicit model capabilities without materializing inherited values", () => {
    const config = draftToConfig(createDraft("openai"));
    config.models = { text: "text-model", plain: "other" };
    config.model_capabilities = {
      "*": { multimodal: false },
      plain: { multimodal: true },
    };
    const draft = recordToDraft({ config, credentials: { api_key_source: "none" } } as NodeRecord);
    expect(draft.models[0]).toMatchObject({ multimodal: false, inherit_capability: true });
    expect(draftToConfig(draft).model_capabilities).toEqual(config.model_capabilities);
    expect(effectiveCapability(config, "text").multimodal).toBeFalse();
    draft.models[0] = { ...draft.models[0], inherit_capability: false, multimodal: true };
    expect(draftToConfig(draft).model_capabilities.text.multimodal).toBeTrue();
    expect(draftToConfig(draft).model_capabilities["*"].multimodal).toBeFalse();
  });

  test("all models use Chat protocol paths", () => {
    expect(protocolPaths.messages).toBe("Messages → Chat Completions → Messages");
    expect(protocolPaths.responses).toBe("Responses → Chat Completions → Responses");
  });

  test("catch-all model mapping retains capabilities for individual public aliases", () => {
    const config = draftToConfig(createDraft("openai"));
    config.models = { "*": "*" };
    config.model_capabilities = { text: { multimodal: false } };
    const draft = recordToDraft({ config, credentials: { api_key_source: "none" } } as NodeRecord);
    expect(draftToConfig(draft).model_capabilities).toEqual(config.model_capabilities);
    draft.models = [{ key: "text", value: "upstream" }];
    expect(draftToConfig(draft).model_capabilities.text.multimodal).toBeTrue();
  });

  test("drops blank key-value rows", () => {
    expect(
      pairsToRecord([
        { key: " public ", value: " upstream " },
        { key: "", value: "ignored" },
      ]),
    ).toEqual({ public: "upstream" });
  });

  test("new vLLM nodes use a finite waiting watermark", () => {
    expect(createDraft("vllm").provider.waiting_threshold).toBe(8);
  });

  test("keeps image capabilities independent on a mixed node", () => {
    const draft = createDraft("openai");
    draft.models = [
      { key: "text", value: "text-model", multimodal: false },
      { key: "vision", value: "vision-model" },
    ];
    const config = draftToConfig(draft);
    expect(config.model_capabilities.text.multimodal).toBeFalse();
    expect(config.model_capabilities.vision.multimodal).toBeTrue();
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

  test("accepts a complete default vLLM draft", () => {
    const draft = createDraft();
    draft.id = "node-a";
    draft.models = [{ key: "chat", value: "model" }];
    expect(validateDraft(draft)).toEqual({});
  });
});
