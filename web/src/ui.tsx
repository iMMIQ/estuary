import { useTranslation } from "react-i18next";
import type { TranslationKey } from "./i18n";

export function statusTone(value: string): "positive" | "warning" | "negative" | "neutral" {
  if (["healthy", "ready", "serving", "closed", "accepting"].includes(value)) return "positive";
  if (["starting", "checking", "degraded", "half_open", "draining", "at_capacity", "waiting_watermark"].includes(value)) return "warning";
  if (["generic", "approximate"].includes(value)) return "neutral";
  return "negative";
}

export function StatusBadge({ value, label }: { value: string; label?: string }) {
  const { t } = useTranslation();
  return <span className={`status-badge ${statusTone(value)}`}>{label ?? t(`status.${value}` as TranslationKey, { defaultValue: value.replaceAll("_", " ") })}</span>;
}

export function formatBytes(value: number): string {
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KiB`;
  return `${(value / (1024 * 1024)).toFixed(1)} MiB`;
}

export function formatPercent(value: number | null | undefined, unavailable = "Unavailable"): string {
  return value == null || !Number.isFinite(value) ? unavailable : `${Math.round(value * 100)}%`;
}

export function formatTimestamp(value: number | null, locale = "en", never = "Never"): string {
  if (value === null) return never;
  return new Intl.DateTimeFormat(locale, {
    dateStyle: "medium",
    timeStyle: "medium",
  }).format(new Date(value));
}
