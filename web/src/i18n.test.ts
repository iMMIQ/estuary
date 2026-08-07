import { describe, expect, test } from "bun:test";
import { resolveLocale, translationResources } from "./i18n";

describe("i18n", () => {
  test("keeps locale resources in sync", () => {
    expect(Object.keys(translationResources["zh-CN"]).sort()).toEqual(
      Object.keys(translationResources.en).sort(),
    );
  });

  test("prefers a saved locale and otherwise detects Chinese", () => {
    expect(resolveLocale("en", ["zh-CN"])).toBe("en");
    expect(resolveLocale(null, ["zh-Hans-CN", "en-US"])).toBe("zh-CN");
    expect(resolveLocale("invalid", ["en-US"])).toBe("en");
  });
});
