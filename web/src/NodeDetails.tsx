import { Button, Tabs } from "@mantine/core";
import {
  Activity,
  AlertTriangle,
  ArrowLeft,
  Check,
  Edit3,
  Gauge,
  Layers3,
  PauseCircle,
  Play,
  Server,
  Trash2,
} from "lucide-react";
import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { effectiveCapability, effectiveModelMappings, protocolPaths } from "./node-config";
import type { NodeRecord } from "./types";
import { formatPercent, formatTimestamp, StatusBadge } from "./ui";

function metricRate(
  value: number | null | undefined,
  unit: string,
  locale: string,
  unavailable: string,
): string {
  if (value == null || !Number.isFinite(value)) return unavailable;
  return `${new Intl.NumberFormat(locale, { notation: "compact", maximumFractionDigits: 1 }).format(value)} ${unit}`;
}

function DataRow({ label, value }: { label: string; value: ReactNode }) {
  return (
    <div className="data-row">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function RuntimeCard({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section className="runtime-card">
      <h3>{title}</h3>
      <div className="runtime-card-body">{children}</div>
    </section>
  );
}

export function NodeDetails({
  node,
  busy,
  onClose,
  onEdit,
  onToggleDrain,
  onDelete,
}: {
  node: NodeRecord;
  busy: boolean;
  onClose: () => void;
  onEdit: () => void;
  onToggleDrain: () => void;
  onDelete: () => void;
}) {
  const { t, i18n } = useTranslation();
  const locale = i18n.resolvedLanguage === "zh-CN" ? "zh-CN" : "en";
  const unavailable = t("common.unavailable");
  const runtime = node.runtime;
  const isVllm = node.config.provider.type === "vllm";
  const telemetry = node.admission.telemetry_fresh;
  const running = isVllm
    ? telemetry
      ? (runtime.upstream_running ?? unavailable)
      : unavailable
    : runtime.active;
  const waiting = isVllm && telemetry ? (runtime.upstream_waiting ?? unavailable) : unavailable;
  const modelMappings = effectiveModelMappings(node.config);
  const hasError = Boolean(
    runtime.provider_last_error || runtime.last_error || runtime.error_ewma > 0.05,
  );

  return (
    <div className="detail-page page-frame">
      <button className="breadcrumb-button" type="button" onClick={onClose}>
        <ArrowLeft size={15} /> {t("nav.upstreams")} <span>/</span> {node.config.id}
      </button>

      <header className="detail-header">
        <div className="detail-title">
          <h1>{node.config.id}</h1>
          <StatusBadge
            value={node.admission.state}
            label={
              node.admission.accepting_assignments ? t("details.acceptingRequests") : undefined
            }
          />
          <div className="detail-meta">
            <span>
              {t("details.baseUrl")}
              <strong>{node.config.base_url}</strong>
            </span>
            <span>
              {t("details.provider")}
              <strong>{isVllm ? "vLLM 0.25+" : t("upstreams.openaiCompatible")}</strong>
            </span>
            <span>
              {t("details.revision")}
              <strong>{node.revision}</strong>
            </span>
            <span>
              {t("details.updated")}
              <strong>{formatTimestamp(node.updated_at_unix_ms, locale, t("common.never"))}</strong>
            </span>
          </div>
        </div>
        <div className="detail-actions">
          <Button
            variant="default"
            size="xs"
            leftSection={<Edit3 size={14} />}
            disabled={busy}
            onClick={onEdit}
          >
            {t("common.edit")}
          </Button>
          <Button
            variant="default"
            size="xs"
            leftSection={
              runtime.lifecycle === "serving" ? <PauseCircle size={14} /> : <Play size={14} />
            }
            disabled={busy}
            onClick={onToggleDrain}
          >
            {runtime.lifecycle === "serving" ? t("upstreams.drain") : t("upstreams.resume")}
          </Button>
          <Button
            color="red"
            variant="light"
            size="xs"
            leftSection={<Trash2 size={14} />}
            disabled={busy}
            onClick={onDelete}
          >
            {t("common.delete")}
          </Button>
        </div>
      </header>

      {hasError && (
        <div className="error-banner" role="alert">
          <AlertTriangle size={16} />
          <strong>{t("details.requiresAttention")}</strong>
          <span>
            {runtime.provider_last_error ??
              runtime.last_error ??
              t("details.errorRate", { rate: (runtime.error_ewma * 100).toFixed(2) })}
          </span>
        </div>
      )}

      <Tabs defaultValue="overview" className="node-tabs">
        <Tabs.List>
          <Tabs.Tab value="overview">{t("nav.overview")}</Tabs.Tab>
          <Tabs.Tab value="metrics">{t("details.metrics")}</Tabs.Tab>
          <Tabs.Tab value="models">{t("details.models")}</Tabs.Tab>
        </Tabs.List>

        <Tabs.Panel value="overview" pt="md">
          <div className="detail-grid-layout">
            <RuntimeCard title={t("details.admission")}>
              <div className="admission-callout">
                {node.admission.accepting_assignments ? (
                  <Check size={16} />
                ) : (
                  <AlertTriangle size={16} />
                )}
                <strong>
                  {t(`admission.${node.admission.state}`, { defaultValue: node.admission.reason })}
                </strong>
              </div>
              <DataRow
                label={t("details.decision")}
                value={<StatusBadge value={node.admission.state} />}
              />
              <DataRow label={t("details.weight")} value={runtime.weight} />
              <DataRow
                label={t("scheduler.ttft")}
                value={
                  runtime.ttft_ewma_ms == null
                    ? unavailable
                    : `${Math.round(runtime.ttft_ewma_ms)} ms`
                }
              />
              <DataRow
                label={t("scheduler.prefill")}
                value={runtime.pending_prefill_tokens?.toLocaleString(locale) ?? unavailable}
              />
              <DataRow
                label={t("scheduler.decode")}
                value={runtime.pending_decode_tokens?.toLocaleString(locale) ?? unavailable}
              />
              <DataRow label={t("details.maxConcurrency")} value={runtime.max_concurrency} />
              <DataRow
                label={t("details.availableConcurrency")}
                value={`${runtime.available} (${runtime.max_concurrency ? Math.round((runtime.available / runtime.max_concurrency) * 100) : 0}%)`}
              />
            </RuntimeCard>

            <RuntimeCard title={`${t("details.provider")} (${isVllm ? "vLLM" : "OpenAI"})`}>
              <DataRow
                label={t("details.version")}
                value={runtime.provider_version ?? unavailable}
              />
              <DataRow label={t("upstreams.runningWaiting")} value={`${running} / ${waiting}`} />
              {isVllm && (
                <>
                  <DataRow
                    label={t("details.kvUsage")}
                    value={formatPercent(telemetry ? runtime.kv_cache_usage : null, unavailable)}
                  />
                  <DataRow
                    label={t("details.kvBlocks")}
                    value={node.exact_kv_blocks.toLocaleString(locale)}
                  />
                  <DataRow
                    label={t("details.kvDirectory")}
                    value={
                      node.exact_kv_authoritative
                        ? t("details.authoritative")
                        : t("details.approximate")
                    }
                  />
                  <DataRow
                    label={t("details.telemetry")}
                    value={node.admission.telemetry_fresh ? t("details.fresh") : t("details.stale")}
                  />
                </>
              )}
            </RuntimeCard>

            <RuntimeCard title={t("details.health")}>
              <DataRow label={t("details.status")} value={<StatusBadge value={runtime.health} />} />
              <DataRow label={t("details.healthCheck")} value={node.config.health_path} />
              <DataRow
                label={t("details.lastTransition")}
                value={formatTimestamp(runtime.last_change_unix_ms, locale, t("common.never"))}
              />
              <DataRow
                label={t("details.lastError")}
                value={runtime.last_error ?? t("common.none")}
              />
            </RuntimeCard>

            <RuntimeCard title={t("details.circuitBreaker")}>
              <DataRow label={t("details.state")} value={<StatusBadge value={runtime.circuit} />} />
              <DataRow label={t("details.failureCount")} value={runtime.circuit_failures} />
              <DataRow label={t("details.halfOpen")} value={runtime.circuit_half_open_in_flight} />
              <DataRow
                label={t("details.errorRateEwma")}
                value={`${(runtime.error_ewma * 100).toFixed(2)}%`}
              />
            </RuntimeCard>

            <RuntimeCard title={t("details.providerCompatibility")}>
              <DataRow
                label={t("details.status")}
                value={<StatusBadge value={runtime.provider_state} />}
              />
              <DataRow
                label={t("details.provider")}
                value={isVllm ? "vLLM 0.25+" : t("upstreams.openaiCompatible")}
              />
              <DataRow
                label={t("details.anthropicProtocol")}
                value={t(`protocol.${node.config.provider.anthropic_protocol}`)}
              />
              <DataRow
                label={t("details.bearerCredential")}
                value={
                  node.credentials.api_key_source === "database"
                    ? t("details.databaseCredential")
                    : node.credentials.api_key_source === "environment"
                      ? t("details.environmentCredential")
                      : t("common.notConfigured")
                }
              />
              <DataRow label={t("details.warnings")} value={runtime.provider_last_error ? 1 : 0} />
              <DataRow label={t("details.generation")} value={runtime.provider_generation} />
            </RuntimeCard>
          </div>
        </Tabs.Panel>

        <Tabs.Panel value="metrics" pt="md">
          <div className="metric-tile-grid">
            <div>
              <Gauge size={16} />
              <span>
                {t("scheduler.ttft")}
                <strong>
                  {runtime.ttft_ewma_ms == null
                    ? unavailable
                    : `${Math.round(runtime.ttft_ewma_ms)} ms`}
                </strong>
              </span>
            </div>
            <div>
              <Activity size={16} />
              <span>
                {t("scheduler.prefill")}
                <strong>
                  {runtime.pending_prefill_tokens?.toLocaleString(locale) ?? unavailable}
                </strong>
              </span>
            </div>
            <div>
              <Activity size={16} />
              <span>
                {t("scheduler.decode")}
                <strong>
                  {runtime.pending_decode_tokens?.toLocaleString(locale) ?? unavailable}
                </strong>
              </span>
            </div>
            <div>
              <Activity size={16} />
              <span>
                {t("upstreams.localLoad")}
                <strong>
                  {runtime.active} / {runtime.max_concurrency}
                </strong>
              </span>
            </div>
            <div>
              <Server size={16} />
              <span>
                {t("details.upstreamDemand")}
                <strong>
                  {running} / {waiting}
                </strong>
              </span>
            </div>
            <div>
              <Gauge size={16} />
              <span>
                {t("vllm.promptThroughput")}
                <strong>
                  {metricRate(
                    telemetry ? runtime.prompt_tokens_per_second : null,
                    "tok/s",
                    locale,
                    unavailable,
                  )}
                </strong>
              </span>
            </div>
            <div>
              <Gauge size={16} />
              <span>
                {t("vllm.generationThroughput")}
                <strong>
                  {metricRate(
                    telemetry ? runtime.generation_tokens_per_second : null,
                    "tok/s",
                    locale,
                    unavailable,
                  )}
                </strong>
              </span>
            </div>
            <div>
              <Activity size={16} />
              <span>
                {t("vllm.completedRequests")}
                <strong>
                  {metricRate(
                    telemetry ? runtime.requests_per_second : null,
                    "req/s",
                    locale,
                    unavailable,
                  )}
                </strong>
              </span>
            </div>
            <div>
              <Layers3 size={16} />
              <span>
                {t("details.gpuPrefix")}
                <strong>
                  {formatPercent(telemetry ? runtime.kv_cache_usage : null, unavailable)} /{" "}
                  {formatPercent(telemetry ? runtime.prefix_cache_hit_rate : null, unavailable)}
                </strong>
              </span>
            </div>
            <div>
              <AlertTriangle size={16} />
              <span>
                {t("details.preemptions")}
                <strong>
                  {!telemetry || runtime.preemptions_total == null
                    ? unavailable
                    : runtime.preemptions_total.toLocaleString(locale)}
                </strong>
              </span>
            </div>
            <div>
              <Gauge size={16} />
              <span>
                {t("details.latencyError")}
                <strong>
                  {Math.round(runtime.latency_ewma_ms)} ms / {(runtime.error_ewma * 100).toFixed(2)}
                  %
                </strong>
              </span>
            </div>
          </div>
        </Tabs.Panel>

        <Tabs.Panel value="models" pt="md">
          <section className="models-panel">
            <div className="section-title">
              <h2>{t("details.modelCount", { count: modelMappings.length })}</h2>
              <span>{t("details.modelMappings")}</span>
            </div>
            <table className="models-table">
              <thead>
                <tr>
                  <th>{t("details.publicModel")}</th>
                  <th>{t("details.upstreamModel")}</th>
                  <th>{t("details.multimodal")}</th>
                  <th>{t("editor.modelFamily")}</th>
                </tr>
              </thead>
              <tbody>
                {modelMappings.map(([publicModel, upstreamModel]) => (
                  <tr key={publicModel}>
                    <td>{publicModel}</td>
                    <td>{upstreamModel}</td>
                    <td>
                      {effectiveCapability(node.config, publicModel).multimodal
                        ? t("common.enabled")
                        : t("common.disabled")}
                    </td>
                    <td>
                      {t(
                        `family.${effectiveCapability(node.config, publicModel).family ?? "generic"}`,
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </section>
          <section className="protocol-panel">
            <h3>{t("protocol.effective")}</h3>
            <p>{t("protocol.recipeOverride")}</p>
            {modelMappings.map(([model]) => (
              <div className="protocol-review" key={model}>
                <strong>{model}</strong>
                <small>
                  {Object.hasOwn(node.config.model_capabilities ?? {}, model)
                    ? t("editor.modelFamily")
                    : t("protocol.inherited")}
                  : {t(`family.${effectiveCapability(node.config, model).family ?? "generic"}`)}
                </small>
                <span>Codex: {protocolPaths(node.config, model).responses}</span>
                <span>Claude: {protocolPaths(node.config, model).messages}</span>
              </div>
            ))}
          </section>
        </Tabs.Panel>
      </Tabs>
    </div>
  );
}
