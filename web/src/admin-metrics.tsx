import type { TFunction } from "i18next";
import { useTranslation } from "react-i18next";

import { formatCompactNumber } from "./node-config";
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
  return value == null || !Number.isFinite(value)
    ? "--"
    : formatCompactNumber(Math.max(0, value), locale);
}

export function formatRatio(value: number | null | undefined): string {
  return value == null || !Number.isFinite(value) ? "--" : `${Math.round(value * 100)}%`;
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
  const prefixQueries = sum((node) => node.runtime.prefix_cache_queries_total);
  const prefixHits = sum((node) => node.runtime.prefix_cache_hits_total);
  const kvValues = fresh
    .map((node) => node.runtime.kv_cache_usage)
    .filter((value): value is number => value != null && Number.isFinite(value));
  return {
    nodes: vllm.length,
    fresh: fresh.length,
    running: sum((node) => node.runtime.upstream_running),
    waiting: sum((node) => node.runtime.upstream_waiting),
    promptRate: sum((node) => node.runtime.prompt_tokens_per_second),
    generationRate: sum((node) => node.runtime.generation_tokens_per_second),
    requestRate: sum((node) => node.runtime.requests_per_second),
    prefixHitRate:
      prefixQueries && prefixHits !== null ? Math.min(1, prefixHits / prefixQueries) : null,
    maxKvUsage: kvValues.length ? Math.max(...kvValues) : null,
    preemptions: sum((node) => node.runtime.preemptions_total),
    exactReady: vllm.filter((node) => node.exact_kv_authoritative).length,
    waitingBlocked: vllm.filter((node) => node.admission.waiting_watermark_blocked).length,
    kvPressure: fresh.filter((node) => (node.runtime.kv_cache_usage ?? 0) >= 0.9).length,
  };
}

export function VllmRuntimePanel({
  nodes,
  compact = false,
}: {
  nodes: NodeRecord[];
  compact?: boolean;
}) {
  const { t, i18n } = useTranslation();
  const locale = i18n.resolvedLanguage === "zh-CN" ? "zh-CN" : "en";
  const runtime = summarizeVllm(nodes);
  return (
    <details
      open={window.matchMedia("(min-width: 721px)").matches}
      className={`dashboard-panel vllm-runtime-panel ${compact ? "compact" : ""}`}
    >
      <summary className="panel-heading">
        <h2>{t("vllm.runtime")}</h2>
        <span>{t("vllm.reporting", { fresh: runtime.fresh, total: runtime.nodes })}</span>
        <small>{t("vllm.showRuntime")}</small>
      </summary>
      <div className="vllm-runtime-grid">
        <MetricBox
          label={t("vllm.promptThroughput")}
          value={`${formatRate(runtime.promptRate, locale)} tok/s`}
        />
        <MetricBox
          label={t("vllm.generationThroughput")}
          value={`${formatRate(runtime.generationRate, locale)} tok/s`}
        />
        <MetricBox
          label={t("vllm.completedRequests")}
          value={`${formatRate(runtime.requestRate, locale)} req/s`}
        />
        <MetricBox
          label={t("vllm.engineDemand")}
          value={`${runtime.running ?? "--"} / ${runtime.waiting ?? "--"}`}
          accent={(runtime.waiting ?? 0) > 0 ? "amber" : undefined}
        />
        <MetricBox
          label={t("vllm.peakKv")}
          value={formatRatio(runtime.maxKvUsage)}
          accent={(runtime.maxKvUsage ?? 0) >= 0.9 ? "amber" : undefined}
        />
        <MetricBox label={t("vllm.prefixHit")} value={formatRatio(runtime.prefixHitRate)} />
      </div>
    </details>
  );
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
