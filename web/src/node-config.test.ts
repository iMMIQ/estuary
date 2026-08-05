import { describe, expect, test } from "bun:test";
import { createDraft, draftToConfig, pairsToRecord, recordToPairs } from "./node-config";

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
});
