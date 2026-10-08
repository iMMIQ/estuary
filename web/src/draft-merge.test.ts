import { expect, test } from "bun:test";
import { mergeDraft } from "./draft-merge";
import { createDraft } from "./node-config";

test("merges disjoint local and remote edits including nested provider fields", () => {
  const base = createDraft();
  const local = structuredClone(base);
  const remote = structuredClone(base);
  local.base_url = "http://mine/v1";
  local.provider.request_timeout_ms = 7000;
  remote.max_concurrency = 32;
  remote.provider.waiting_threshold = 12;
  const result = mergeDraft(base, local, remote);
  expect(result.conflicts).toEqual([]);
  expect(result.draft).toMatchObject({
    base_url: "http://mine/v1",
    max_concurrency: 32,
    provider: { request_timeout_ms: 7000, waiting_threshold: 12 },
  });
});

test("reports simultaneous edits and keeps local values only after the user chooses to merge", () => {
  const base = createDraft();
  const local = structuredClone(base);
  const remote = structuredClone(base);
  local.base_url = "http://mine/v1";
  remote.base_url = "http://theirs/v1";
  local.models = [{ key: "local", value: "a" }];
  remote.models = [{ key: "remote", value: "b" }];
  const result = mergeDraft(base, local, remote);
  expect(result.conflicts).toEqual(["base_url", "models"]);
  expect(result.draft.base_url).toBe(local.base_url);
  expect(result.draft.models).toEqual(local.models);
});
