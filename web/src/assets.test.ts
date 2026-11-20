import { expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { gunzipSync } from "node:zlib";

test("published assets are stored precompressed without uncompressed duplicates", () => {
  const dist = new URL("../dist/", import.meta.url);
  const directory = new URL("assets/", dist);
  const entries = readdirSync(directory);
  const compressed = entries.filter((name) => /\.(js|css)\.gz$/.test(name));
  const raw = entries.filter((name) => /\.(js|css)$/.test(name));
  expect(compressed.length).toBeGreaterThan(0);
  expect(raw).toEqual([]);
  for (const name of compressed) {
    const content = gunzipSync(readFileSync(new URL(name, directory)));
    expect(content.byteLength).toBeGreaterThan(0);
  }
  const indexHtml = readFileSync(new URL("index.html", dist), "utf8");
  const referenced = [...indexHtml.matchAll(/\/admin\/assets\/([^"'\s>]+)/g)].map((m) => m[1]);
  expect(referenced.length).toBeGreaterThan(0);
  for (const name of referenced) {
    expect(compressed).toContain(`${name}.gz`);
  }
});
