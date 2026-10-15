import type { HistoryPoint } from "./observability-types";

export function nearestSample(points: HistoryPoint[], time: number): number {
  let nearest = 0;
  points.forEach((point, index) => {
    if (Math.abs(point.at_unix_ms - time) < Math.abs((points[nearest]?.at_unix_ms ?? 0) - time))
      nearest = index;
  });
  return nearest;
}

export function finiteMetric(value: number | null | undefined): value is number {
  return value != null && Number.isFinite(value);
}

export function metricNumber(value: number | null | undefined, locale?: string): string {
  if (!finiteMetric(value)) return "—";
  if (value > 0 && value < 0.001) return "<0.001";
  return new Intl.NumberFormat(locale, {
    notation: "compact",
    maximumFractionDigits: Math.abs(value) < 1 ? 3 : 1,
  }).format(value);
}

export function metricPercent(value: number | null | undefined, locale?: string): string {
  if (!finiteMetric(value)) return "—";
  if (value > 0 && value < 0.00001) return "<0.001%";
  if (value < 1 && value > 0.99999) return ">99.999%";
  return new Intl.NumberFormat(locale, { style: "percent", maximumFractionDigits: 3 }).format(
    value,
  );
}

export function chartPath(
  points: HistoryPoint[],
  read: (point: HistoryPoint) => number | null,
  x: (time: number) => number,
  y: (value: number) => number,
  maxGap: number,
): string {
  let previous: number | null = null;
  return points
    .map((point) => {
      const value = read(point);
      if (!finiteMetric(value)) {
        previous = null;
        return "";
      }
      const command = previous === null || point.at_unix_ms - previous > maxGap ? "M" : "L";
      previous = point.at_unix_ms;
      return `${command}${x(point.at_unix_ms).toFixed(2)},${y(value).toFixed(2)}`;
    })
    .join(" ");
}
