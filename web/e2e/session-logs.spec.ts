import { expect, test } from "@playwright/test";
import { mockControlPlane } from "./fixtures";

const request = {
  id: "request-a",
  external_request_id: "external",
  session_id: "agent-1",
  session_source: "explicit_header",
  model: "deepseek",
  endpoint: "/v1/responses",
  protocol: "openai_responses",
  streaming: true,
  started_at_ms: Date.now(),
  ended_at_ms: Date.now(),
  http_status: 200,
  outcome: "success",
  delivery: "body_consumed",
  timings_us: { total: 20000, first_output: 10000 },
  request_bytes: 100,
  response_bytes: 50,
  error_phase: null,
  error_class: null,
  usage: { input_tokens: 10, output_tokens: 2 },
  capture_state: "captured",
  events: [],
  attempts: [],
};

test("explores session logs, details, payloads and pagination", async ({ page }) => {
  await mockControlPlane(page);
  await page.route("**/admin/api/logs/status", (route) =>
    route.fulfill({
      json: {
        enabled: true,
        available: true,
        capture_content: true,
        committed: 2,
        dropped: 0,
        truncated: 1,
      },
    }),
  );
  const queried: URL[] = [];
  await page.route("**/admin/api/logs/requests?*", (route) => {
    const url = new URL(route.request().url());
    queried.push(url);
    return route.fulfill({
      json: {
        requests: [
          url.searchParams.has("cursor")
            ? { ...request, id: "request-b", model: "second-page" }
            : request,
        ],
        next_cursor: url.searchParams.has("cursor") ? null : "next-cursor",
      },
    });
  });
  await page.route("**/admin/api/logs/sessions?*", (route) =>
    route.fulfill({ json: { sessions: [{ id: "agent-1", requests: 2 }] } }),
  );
  await page.route("**/admin/api/logs/requests/request-a", (route) =>
    route.fulfill({
      json: {
        request: {
          ...request,
          attempts: [
            {
              number: 1,
              node: "upstream-a",
              endpoint: "chat/completions",
              model: "ds-chat",
              adapter: "deepseek_recipe",
              outcome: "success",
              http_status: 200,
              timings_us: { total: 15000 },
              route: { score: 0.5 },
            },
          ],
        },
        payloads: [
          {
            stage: "client_input",
            attempt: 0,
            state: "complete",
            bytes_seen: 100,
            content: { input: "investigate this code" },
          },
        ],
      },
    }),
  );
  await page.goto("/admin/");
  await page
    .locator("button:visible")
    .filter({ hasText: /^Session logs$/ })
    .click();
  await expect(page.getByRole("heading", { name: "Session logs", exact: true })).toBeVisible();
  await expect(page.getByRole("cell", { name: "deepseek openai_responses" })).toBeVisible();
  await page.getByLabel("Request details request-a").click();
  await expect(page.getByText("#1 · upstream-a · success")).toBeVisible();
  await expect(page.getByText("investigate this code")).toHaveCount(0);
  await page.getByRole("button", { name: "Expand", exact: true }).click();
  await expect(page.locator("pre").filter({ hasText: "investigate this code" })).toBeVisible();
  await page.getByRole("button", { name: "Back", exact: true }).click();
  await page.getByRole("button", { name: "Next", exact: true }).click();
  await expect(page.getByText("second-page")).toBeVisible();
  await page.getByRole("button", { name: "First page", exact: true }).click();
  await page.getByRole("combobox", { name: "Session", exact: true }).click();
  await page.getByRole("option", { name: "agent-1 (2)", exact: true }).click();
  await expect.poll(() => queried.at(-1)?.searchParams.get("session")).toBe("agent-1");
  expect(queried.at(-1)?.searchParams.has("cursor")).toBe(false);
  expect(queried.every((url) => Number(url.searchParams.get("since")) > 0)).toBe(true);
});

test("disabled logging explains setup and does not fetch content", async ({ page }) => {
  await mockControlPlane(page);
  await page.route("**/admin/api/logs/status", (route) =>
    route.fulfill({ json: { enabled: false } }),
  );
  let contentReads = 0;
  await page.route("**/admin/api/logs/requests**", (route) => {
    contentReads++;
    return route.fulfill({ json: {} });
  });
  await page.goto("/admin/");
  await page
    .locator("button:visible")
    .filter({ hasText: /^Session logs$/ })
    .click();
  await expect(page.getByRole("alert")).toContainText("Session logging is disabled");
  expect(contentReads).toBe(0);
});
