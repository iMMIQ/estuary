import { expect, test } from "bun:test";
import { chartPath, finiteMetric, metricNumber, metricPercent } from "./metric-series";
import type { HistoryPoint } from "./observability-types";

test("metric formatting preserves missing values and genuine zero", () => {
  expect(metricNumber(null, "en")).toBe("—");
  expect(metricNumber(Number.NaN, "en")).toBe("—");
  expect(metricNumber(0, "en")).toBe("0");
  expect(metricNumber(0.017, "en")).toBe("0.017");
  expect(metricNumber(0.0001, "en")).toBe("<0.001");
  expect(metricPercent(0.0001, "en")).toBe("0.01%");
  expect(finiteMetric(Number.POSITIVE_INFINITY)).toBe(false);
});

test("charts keep telemetry gaps instead of connecting or zero-filling them", () => {
  const points = [
    { at_unix_ms: 0, ttft_p95: 1 },
    { at_unix_ms: 5000, ttft_p95: 2 },
    { at_unix_ms: 10000, ttft_p95: null },
    { at_unix_ms: 15000, ttft_p95: 3 },
    { at_unix_ms: 50000, ttft_p95: 4 },
  ] as HistoryPoint[];
  expect(
    chartPath(
      points,
      (point) => point.ttft_p95,
      (x) => x,
      (y) => y,
      10000,
    ),
  ).toBe("M0.00,1.00 L5000.00,2.00  M15000.00,3.00 M50000.00,4.00");
});
