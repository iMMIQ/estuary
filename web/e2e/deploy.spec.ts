import { readFileSync } from "node:fs";
import { expect, test } from "@playwright/test";

const source = readFileSync(new URL("../../src/supervisor/deploy.rs", import.meta.url), "utf8");
const html = source.match(/const DEPLOY_HTML: &str = r#"([\s\S]*?)"#;/)?.[1];
if (!html) throw new Error("The embedded deployment page was not found");

test("the deployment page loads releases and switches the active version", async ({ page }) => {
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  let active = "0.3.2";
  await page.route("**/deploy/", (route) =>
    route.fulfill({ contentType: "text/html", body: html }),
  );
  await page.route("**/deploy/api/releases**", (route) => {
    if (route.request().method() === "PUT") {
      active = new URL(route.request().url()).pathname.split("/").at(-1) ?? "";
      return route.fulfill({ json: { active_version: active } });
    }
    return route.fulfill({
      json: {
        releases: ["0.3.2", "0.3.3"].map((version) => ({
          version,
          active: version === active,
          current: version === active,
          size_bytes: 1024 * 1024,
        })),
      },
    });
  });
  await page.goto("/deploy/");
  const previous = page.locator(".row").filter({ hasText: "0.3.2" });
  const candidate = page.locator(".row").filter({ hasText: "0.3.3" });
  await expect(previous.getByRole("button", { name: "切换" })).toBeDisabled();
  await candidate.getByRole("button", { name: "切换" }).click();
  await expect(candidate.getByText("当前", { exact: true })).toBeVisible();
  await expect(candidate.getByRole("button", { name: "切换" })).toBeDisabled();
  await expect(previous.getByRole("button", { name: "切换" })).toBeEnabled();
  expect(errors).toEqual([]);
});
