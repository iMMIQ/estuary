import { Popover } from "@mantine/core";
import { CircleHelp } from "lucide-react";
import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";
import type { TranslationKey } from "./i18n";

function statusTone(value: string): "positive" | "warning" | "negative" | "neutral" {
  if (["healthy", "ready", "serving", "closed", "accepting"].includes(value)) return "positive";
  if (
    [
      "starting",
      "checking",
      "degraded",
      "half_open",
      "draining",
      "at_capacity",
      "waiting_watermark",
    ].includes(value)
  )
    return "warning";
  if (["generic", "approximate"].includes(value)) return "neutral";
  return "negative";
}

export function StatusBadge({ value, label }: { value: string; label?: string }) {
  const { t } = useTranslation();
  return (
    <span className={`status-badge ${statusTone(value)}`}>
      {label ??
        t(`status.${value}` as TranslationKey, { defaultValue: value.replaceAll("_", " ") })}
    </span>
  );
}

export function formatBytes(value: number): string {
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KiB`;
  return `${(value / (1024 * 1024)).toFixed(1)} MiB`;
}

export function formatCompactNumber(value: number, locale = "en"): string {
  return new Intl.NumberFormat(locale, { notation: "compact", maximumFractionDigits: 1 }).format(
    value,
  );
}

export function formatPercent(
  value: number | null | undefined,
  unavailable = "Unavailable",
): string {
  return value == null || !Number.isFinite(value) ? unavailable : `${Math.round(value * 100)}%`;
}

export function formatTimestamp(value: number | null, locale = "en", never = "Never"): string {
  if (value === null) return never;
  return new Intl.DateTimeFormat(locale, {
    dateStyle: "medium",
    timeStyle: "medium",
  }).format(new Date(value));
}

export function HelpTip({ subject, children }: { subject: string; children: ReactNode }) {
  const { t } = useTranslation();
  const label = t("common.help", { subject });
  return (
    <Popover
      width={320}
      position="bottom-start"
      withArrow
      shadow="md"
      withinPortal
      trapFocus
      returnFocus
    >
      <Popover.Target>
        <button type="button" className="help-trigger" aria-label={label}>
          <CircleHelp size={14} />
        </button>
      </Popover.Target>
      <Popover.Dropdown className="help-content" role="dialog" aria-label={label} tabIndex={0}>
        {children}
      </Popover.Dropdown>
    </Popover>
  );
}
