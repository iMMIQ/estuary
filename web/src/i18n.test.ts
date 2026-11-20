import { describe, expect, test } from "bun:test";
import { resolveLocale, translate, translationResources } from "./i18n";

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

  test("interpolates variables and falls back to defaultValue", () => {
    expect(translate("en", "sync.seconds", { count: 12 })).toBe("Synced 12s ago");
    expect(translate("zh-CN", "sync.seconds", { count: 12 })).toBe("12 秒前同步");
    expect(translate("en", "unknown.key", { defaultValue: "Fallback label" })).toBe(
      "Fallback label",
    );
  });
});
