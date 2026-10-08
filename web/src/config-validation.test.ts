import { expect, test } from "bun:test";
import fixtures from "../../tests/fixtures/node-config-contract.json";
import { configDefaults as defaults } from "./config-contract";
import { integerBounds, validateNodeConfig } from "./config-validation";
import type { NodeConfig } from "./generated/config";
import { createDraft } from "./node-config";

function apply(target: Record<string, unknown>, path: string, value: unknown): void {
  const keys = path.split(".");
  const last = keys.pop();
  if (!last) throw new Error("Empty fixture path");
  let current = target;
  for (const key of keys) current = current[key] as Record<string, unknown>;
  current[last] = value;
}

for (const { name, changes, valid } of fixtures.cases) {
  test(`shared config contract: ${name}`, () => {
    const config = {
      ...structuredClone(defaults.node),
      ...structuredClone(fixtures.base),
      provider: { ...structuredClone(defaults.node.provider), ...fixtures.base.provider },
    };
    for (const [path, value] of Object.entries(changes)) {
      apply(
        config,
        path,
        path === "provider.kv_events" && value !== null
          ? { ...structuredClone(defaults.kv_events), ...(value as object) }
          : value,
      );
    }
    expect(Object.keys(validateNodeConfig(config as NodeConfig)).length === 0).toBe(valid);
  });
}

test("draft presets and numeric controls use the backend contract", () => {
  const draft = createDraft();
  expect(draft.provider).toEqual(defaults.editor.provider);
  expect(draft.max_concurrency).toBe(defaults.editor.max_concurrency);
  expect(integerBounds("provider.tokenize_cache_entries")).toEqual({ min: 1, max: 65536 });
  draft.provider.version_path = "/changed";
  expect(createDraft().provider.version_path).toBe(defaults.editor.provider.version_path);
});
