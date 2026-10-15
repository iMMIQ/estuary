import type { TFunction } from "i18next";
import { useTranslation } from "react-i18next";
import { metricNumber } from "./metric-series";
import type { NodeRecord } from "./types";

export function relativeSync(value: number | null, t: TFunction): string {
  if (!value) return t("sync.never");
  const seconds = Math.max(0, Math.floor((Date.now() - value) / 1000));
  if (seconds < 5) return t("sync.now");
  if (seconds < 60) return t("sync.seconds", { count: seconds });
  return t("sync.minutes", { count: Math.floor(seconds / 60) });
}

export function MetricBox({
  label,
  value,
  accent,
}: {
  label: string;
  value: string | number | null;
  accent?: "green" | "amber" | "red";
}) {
  return (
    <div className="metric-box">
      <span>{label}</span>
      <strong className={accent ? `metric-${accent}` : ""}>{value ?? "--"}</strong>
    </div>
  );
}

export function formatRate(value: number | null | undefined, locale: string): string {
  return value == null || !Number.isFinite(value) ? "--" : metricNumber(Math.max(0, value), locale);
}

export function summarizeVllm(nodes: NodeRecord[]) {
  const vllm = nodes.filter((node) => node.config.provider.type === "vllm");
  const fresh = vllm.filter((node) => node.admission.telemetry_fresh);
  const sum = (read: (node: NodeRecord) => number | null) => {
    const values = fresh
      .map(read)
      .filter((value): value is number => value != null && Number.isFinite(value));
    return values.length ? values.reduce((total, value) => total + value, 0) : null;
  };
  return {
    nodes: vllm.length,
    preemptions: sum((node) => node.runtime.preemptions_total),
    exactReady: vllm.filter((node) => node.exact_kv_authoritative).length,
    waitingBlocked: vllm.filter((node) => node.admission.waiting_watermark_blocked).length,
    kvPressure: fresh.filter((node) => (node.runtime.kv_cache_usage ?? 0) >= 0.9).length,
  };
}

export function UsageRow({
  label,
  value,
  total,
  display,
  totalDisplay,
  tone = "blue",
}: {
  label: string;
  value: number | null;
  total: number | null;
  display?: string;
  totalDisplay?: string;
  tone?: "blue" | "green" | "amber";
}) {
  const { i18n } = useTranslation();
  const locale = i18n.resolvedLanguage === "zh-CN" ? "zh-CN" : "en";
  const percent =
    total !== null && value !== null && total > 0 ? Math.min(100, (value / total) * 100) : 0;
  return (
    <div className="usage-row">
      <div>
        <span>{label}</span>
        <strong>
          {display ?? value?.toLocaleString(locale) ?? "--"}{" "}
          <small>/ {totalDisplay ?? total?.toLocaleString(locale) ?? "--"}</small>
        </strong>
      </div>
      <div className="usage-track">
        <i className={tone} style={{ width: `${percent}%` }} />
      </div>
      <em>{value === null || total === null ? "--" : `${Math.round(percent)}%`}</em>
    </div>
  );
}

export function ProgressValue({
  value,
  total,
  tone = "green",
}: {
  value: number;
  total: number;
  tone?: "green" | "amber" | "blue";
}) {
  const { i18n } = useTranslation();
  const locale = i18n.resolvedLanguage === "zh-CN" ? "zh-CN" : "en";
  const percent = total > 0 ? Math.min(100, (value / total) * 100) : 0;
  return (
    <div className="progress-value">
      <span>
        <strong>{value.toLocaleString(locale)}</strong> / {total.toLocaleString(locale)}
      </span>
      <div>
        <i className={tone} style={{ width: `${percent}%` }} />
      </div>
      <small>{Math.round(percent)}%</small>
    </div>
  );
}
