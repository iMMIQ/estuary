import { expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { gunzipSync } from "node:zlib";

test("compressed assets match the final published files including lazy import references", () => {
  const directory = new URL("../dist/assets/", import.meta.url);
  const assets = readdirSync(directory).filter(name => /\.(js|css)$/.test(name));
  expect(assets.length).toBeGreaterThan(0);
  for (const name of assets) {
    const original = readFileSync(new URL(name, directory));
    const compressed = readFileSync(new URL(`${name}.gz`, directory));
    expect(gunzipSync(compressed).equals(original)).toBe(true);
  }
});
