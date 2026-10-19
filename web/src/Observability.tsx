import { Alert, Badge, Group, Select, Table } from "@mantine/core";
import { useEffect, useId, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  chartPath,
  finiteMetric,
  metricNumber,
  metricPercent,
  nearestSample,
} from "./metric-series";
import type { HistoryPoint, Observability, ScopeMetrics } from "./observability-types";
import { formatBytes, HelpTip } from "./ui";

type Line = { label: string; read: (point: HistoryPoint) => number | null; dashed?: boolean };

function Trend({
  title,
  unit,
  data,
  lines,
  selectedAt,
  onInspect,
}: {
  title: string;
  unit: string;
  data: Observability;
  lines: Line[];
  selectedAt: number | null;
  onInspect: (value: number) => void;
}) {
  const { t, i18n } = useTranslation();
  const box = useRef<HTMLDivElement>(null);
  const id = useId();
  const [width, setWidth] = useState(480);
  useEffect(() => {
    if (!box.current) return;
    const observer = new ResizeObserver(([entry]) => {
      if (entry) setWidth(entry.contentRect.width);
    });
    observer.observe(box.current);
    return () => observer.disconnect();
  }, []);
  const history = data.history;
  const left = 52,
    right = Math.max(left + 1, width - 14),
    top = 14,
    bottom = 132;
  const start = data.generated_at_unix_ms - data.retention_seconds * 1000;
  const end = data.generated_at_unix_ms;
  const values = history.flatMap((point) =>
    lines.map((line) => line.read(point)).filter(finiteMetric),
  );
  const peak = Math.max(0, ...values);
  const maximum = unit === "%" ? 100 : peak > 0 ? peak * 1.12 : 1;
  const x = (time: number) => left + ((time - start) / Math.max(1, end - start)) * (right - left);
  const y = (value: number) => bottom - (value / maximum) * (bottom - top);
  const selectedPoint = selectedAt === null ? null : history[nearestSample(history, selectedAt)];
  const time = (value: number) =>
    new Date(value).toLocaleTimeString(i18n.resolvedLanguage, {
      hour: "2-digit",
      minute: "2-digit",
    });
  const inspect = (clientX: number, rect: DOMRect) => {
    if (!history.length) return;
    const target =
      start +
      Math.max(0, Math.min(1, (clientX - rect.left - left) / (right - left))) * (end - start);
    const nearest = history[nearestSample(history, target)];
    if (nearest) onInspect(nearest.at_unix_ms);
  };
  return (
    <figure className="metric-trend">
      <div className="metric-trend-heading">
        <strong>{title}</strong>
        <span>{unit}</span>
      </div>
      <div ref={box}>
        <svg
          width="100%"
          height="160"
          viewBox={`0 0 ${width} 160`}
          role="img"
          aria-labelledby={id}
          onPointerMove={(event) =>
            inspect(event.clientX, event.currentTarget.getBoundingClientRect())
          }
        >
          <title id={id}>
            {title} · {t("metrics.history")}
          </title>
          {[0, 0.5, 1].map((fraction) => (
            <g key={fraction}>
              <line
                className="metric-grid-line"
                x1={left}
                x2={right}
                y1={y(maximum * fraction)}
                y2={y(maximum * fraction)}
              />
              <text
                className="metric-axis"
                x={left - 7}
                y={y(maximum * fraction) + 4}
                textAnchor="end"
              >
                {metricNumber(maximum * fraction, i18n.resolvedLanguage)}
              </text>
            </g>
          ))}
          {[0, 0.5, 1].map((fraction) => (
            <text
              key={fraction}
              className="metric-axis"
              x={left + (right - left) * fraction}
              y="153"
              textAnchor={fraction === 0 ? "start" : fraction === 1 ? "end" : "middle"}
            >
              {time(start + (end - start) * fraction)}
            </text>
          ))}
          {lines.map((line, index) => (
            <path
              key={line.label}
              className={`metric-line metric-series-${index}`}
              strokeDasharray={line.dashed ? "4 4" : undefined}
              d={chartPath(history, line.read, x, y, data.step_seconds * 2000)}
            />
          ))}
          {selectedPoint && (
            <line
              className="metric-guide"
              x1={x(selectedPoint.at_unix_ms)}
              x2={x(selectedPoint.at_unix_ms)}
              y1={top}
              y2={bottom}
            />
          )}
          {history.length < 2 && (
            <text className="metric-axis" x={(left + right) / 2} y="76" textAnchor="middle">
              {t("metrics.collecting")}
            </text>
          )}
        </svg>
      </div>
      <figcaption>
        {lines.map((line, index) => (
          <span key={line.label}>
            <i className={`metric-swatch metric-series-${index}`} />
            {line.label}
            {selectedPoint
              ? ` ${metricNumber(line.read(selectedPoint), i18n.resolvedLanguage)} ${unit}`
              : ""}
          </span>
        ))}
        {selectedPoint && (
          <time dateTime={new Date(selectedPoint.at_unix_ms).toISOString()}>
            {new Date(selectedPoint.at_unix_ms).toLocaleTimeString(i18n.resolvedLanguage)}
          </time>
        )}
      </figcaption>
    </figure>
  );
}

function Value({
  label,
  value,
  detail,
  title,
}: {
  label: string;
  value: string;
  detail?: string;
  title?: string;
}) {
  const unit = value.match(/^(.*) (tok\/s|req\/s|ms|s)$/);
  return (
    <div className="metric-summary">
      <div className="metric-label">
        <span>{label}</span>
        {(title || detail) && (
          <HelpTip subject={label}>
            {title && <p>{title}</p>}
            {detail && <p>{detail}</p>}
          </HelpTip>
        )}
      </div>
      <strong>
        {unit ? (
          <>
            {unit[1]} <span className="metric-unit">{unit[2]}</span>
          </>
        ) : (
          value
        )}
      </strong>
    </div>
  );
}

export function ObservabilityPanel({
  data,
  stale,
  window,
  onWindow,
  model,
  onModel,
  nodeId,
  nodeInstance,
}: {
  data: Observability | null;
  stale: boolean;
  window: number;
  onWindow: (value: number) => void;
  model: string;
  onModel: (value: string) => void;
  nodeId?: string;
  nodeInstance?: number;
}) {
  const { t, i18n } = useTranslation();
  const locale = i18n.resolvedLanguage;
  const inspectionKey = JSON.stringify([
    data?.instance_id,
    data?.selected?.kind,
    data?.selected?.id,
    data?.selected?.instance,
  ]);
  const [inspected, setInspected] = useState<{ key: string; time: number } | null>(null);
  const inspection = inspected?.key === inspectionKey ? inspected.time : null;
  const setInspection = (time: number) => setInspected({ key: inspectionKey, time });
  const matches =
    data?.selected &&
    (nodeId
      ? data.selected.kind === "node" &&
        data.selected.id === nodeId &&
        (nodeInstance === undefined || data.selected.instance === nodeInstance)
      : model
        ? data.selected.kind === "model" && data.selected.id === model
        : data.selected.kind === "fleet");
  const scope = matches ? data.selected : null;
  const number = (value: number | null | undefined) => metricNumber(value, locale);
  const ms = (value: number | null | undefined) =>
    value == null ? "—" : `${number(value * 1000)} ms`;
  const percent = (value: number | null | undefined) => metricPercent(value, locale);
  const samples = (key: keyof ScopeMetrics["latency"]) =>
    t("metrics.samples", { count: scope?.latency[key].samples ?? 0 });
  const rate = (key: "input" | "output" | "incoming" | "completed", unit: string) =>
    `${number(scope?.rates[key].smoothed)} ${unit}`;
  const mean = (key: "input" | "output" | "incoming" | "completed") =>
    `${t("metrics.minuteMean")} ${number(scope?.rates[key].mean_60s)}`;
  return (
    <section className="dashboard-panel observability-panel" aria-label={t("metrics.title")}>
      <div className="observability-heading">
        <div>
          <h2>
            {t("metrics.title")}
            {nodeId ? ` · ${nodeId}` : model ? ` · ${model}` : ""}
          </h2>
        </div>
        <Group gap="xs">
          {!nodeId && (
            <Select
              aria-label={t("metrics.model")}
              data={[
                { value: "", label: t("metrics.allModels") },
                ...(data?.models ?? []).map((item) => ({ value: item.id, label: item.id })),
              ]}
              value={model}
              onChange={(value) => onModel(value ?? "")}
              allowDeselect={false}
            />
          )}
          <Select
            aria-label={t("metrics.window")}
            data={[
              { value: "60", label: t("metrics.window1") },
              { value: "300", label: t("metrics.window5") },
              { value: "900", label: t("metrics.window15") },
            ]}
            value={String(window)}
            onChange={(value) => onWindow(Number(value ?? 300))}
            allowDeselect={false}
          />
        </Group>
      </div>
      {stale && (
        <Alert color="yellow" role="status">
          {t("metrics.stale")}
        </Alert>
      )}
      {(data?.limited_events ?? 0) > 0 && (
        <Alert color="yellow">{t("metrics.limited", { count: data?.scope_limit_per_kind })}</Alert>
      )}
      {scope?.retired && <Alert color="yellow">{t("metrics.retired")}</Alert>}
      <div className="observability-summary">
        <Value
          label={t("metrics.output")}
          value={rate("output", "tok/s")}
          detail={mean("output")}
          title={t("metrics.usageHint")}
        />
        <Value
          label={t("metrics.completed")}
          value={rate("completed", "req/s")}
          detail={mean("completed")}
        />
        <Value
          label={t("metrics.ttft95")}
          value={ms(scope?.latency.ttft.p95)}
          detail={samples("ttft")}
          title={t("metrics.ttftHint")}
        />
        <Value
          label={t("metrics.failure")}
          value={percent(scope?.failure_ratio)}
          detail={t("metrics.failedCount", { count: scope?.window.failed ?? 0 })}
        />
        <Value
          label={t("metrics.active")}
          value={number(scope?.pressure.active)}
          detail={
            scope?.pressure.capacity
              ? `${t("metrics.capacity")} ${number(scope.pressure.capacity)}`
              : undefined
          }
        />
        <Value
          label={t("metrics.queue95")}
          value={ms(scope?.latency.queue.p95)}
          detail={samples("queue")}
        />
      </div>
      {scope && scope.latency.ttft.samples < 20 && (
        <div className="metrics-sample-warning" role="status" title={t("metrics.lowSamples")}>
          <Badge color="yellow" variant="light">
            {t("metrics.sampleWarning")}
          </Badge>
          <HelpTip subject={t("metrics.sampleWarning")}>{t("metrics.lowSamples")}</HelpTip>
        </div>
      )}
      {data && matches && (
        <>
          <div className="metrics-section-label">{t("metrics.trendTitle")}</div>
          <div className="metric-trends">
            <Trend
              title={t("metrics.output")}
              unit="tok/s"
              data={data}
              selectedAt={inspection}
              onInspect={setInspection}
              lines={[
                {
                  label: t("metrics.peakLabel"),
                  read: (point) => point.peaks.output,
                  dashed: true,
                },
                { label: t("metrics.smoothLabel"), read: (point) => point.rates.output.smoothed },
              ]}
            />
            <Trend
              title={t("metrics.requests")}
              unit="req/s"
              data={data}
              selectedAt={inspection}
              onInspect={setInspection}
              lines={[
                { label: t("metrics.incoming"), read: (point) => point.rates.incoming.smoothed },
                { label: t("metrics.completed"), read: (point) => point.rates.completed.smoothed },
              ]}
            />
            <Trend
              title={t("metrics.latencyTrend")}
              unit="ms"
              data={data}
              selectedAt={inspection}
              onInspect={setInspection}
              lines={[
                {
                  label: t("metrics.ttft95"),
                  read: (point) => (point.ttft_p95 === null ? null : point.ttft_p95 * 1000),
                },
                {
                  label: t("metrics.queue95"),
                  read: (point) => (point.queue_p95 === null ? null : point.queue_p95 * 1000),
                },
              ]}
            />
            <Trend
              title={t("metrics.pressure")}
              unit={t("metrics.requestsUnit")}
              data={data}
              selectedAt={inspection}
              onInspect={setInspection}
              lines={[
                { label: t("metrics.active"), read: (point) => point.peaks.active },
                {
                  label: t("metrics.queued"),
                  read: (point) =>
                    scope?.kind === "model"
                      ? null
                      : scope?.kind === "node"
                        ? point.peaks.waiting
                        : point.peaks.queued,
                },
              ]}
            />
          </div>
        </>
      )}
      {data && matches && data.history.length > 1 && (
        <label className="metric-time-inspector">
          <span>{t("metrics.inspect")}</span>
          <input
            className="metric-inspector"
            type="range"
            min="0"
            max={data.history.length - 1}
            value={
              inspection === null
                ? data.history.length - 1
                : nearestSample(data.history, inspection)
            }
            onChange={(event) => {
              const point = data.history[Number(event.currentTarget.value)];
              if (point) setInspection(point.at_unix_ms);
            }}
          />
        </label>
      )}
      <details className="metrics-details">
        <summary>{t("metrics.experience")}</summary>
        <div className="observability-extra">
          {(["ttft", "upstream_ttft", "duration", "ingress", "queue", "tpot"] as const).map(
            (key) => (
              <Value
                key={key}
                label={t(`metrics.latency.${key}`)}
                value={ms(scope?.latency[key].p95)}
                detail={`P50 ${ms(scope?.latency[key].p50)} · ${samples(key)}`}
              />
            ),
          )}
          <Value
            label={t("metrics.singleSpeed")}
            value={`${number(scope?.latency.generation_rate.p50)} tok/s`}
            detail={t("metrics.speedSources", {
              reported: scope?.window.tpot_reported ?? 0,
              estimated: scope?.window.tpot_estimated ?? 0,
            })}
            title={t("metrics.speedHint")}
          />
          <Value label={t("metrics.input")} value={rate("input", "tok/s")} detail={mean("input")} />
          <Value
            label={t("metrics.incoming")}
            value={rate("incoming", "req/s")}
            detail={mean("incoming")}
          />
          <Value
            label={t("metrics.cancelled")}
            value={percent(scope?.cancellation_ratio)}
            detail={number(scope?.window.cancelled)}
          />
          <Value
            label={t("metrics.unknown")}
            value={percent(scope?.unknown_ratio)}
            detail={number(scope?.window.unknown)}
          />
          <Value
            label={t("metrics.success")}
            value={percent(scope?.success_ratio)}
            detail={number(scope?.window.success)}
          />
          <Value
            label={t("metrics.retries")}
            value={number(scope?.window.retries)}
            detail={percent(
              scope &&
                scope.window.success +
                  scope.window.failed +
                  scope.window.cancelled +
                  scope.window.unknown >
                  0
                ? scope.window.retried_requests /
                    (scope.window.success +
                      scope.window.failed +
                      scope.window.cancelled +
                      scope.window.unknown)
                : null,
            )}
          />
          <Value
            label={scope?.kind === "model" ? t("metrics.activePeak") : t("metrics.windowPeaks")}
            value={
              scope?.kind === "model"
                ? number(scope.peaks.active)
                : `${number(scope?.peaks.active)} / ${number(scope?.kind === "node" ? scope.peaks.waiting : scope?.peaks.queued)}`
            }
            detail={t("metrics.peakHint")}
          />
          <Value
            label={t("metrics.cache")}
            value={percent(scope?.cache_hit_ratio)}
            detail={t("metrics.samples", { count: scope?.window.cache_samples ?? 0 })}
          />
          <Value
            label={t("metrics.routing")}
            value={`${number(scope?.window.route_exact)} / ${number(scope?.window.route_approximate)}`}
            detail={t("metrics.routingHint")}
          />
          {scope?.kind !== "model" && (
            <Value
              label={t("metrics.upstreamActive")}
              value={`${number(scope?.pressure.upstream_active)} / ${number(scope?.pressure.capacity)}`}
            />
          )}
          {scope?.kind === "fleet" && (
            <>
              <Value
                label={t("metrics.queued")}
                value={number(scope?.pressure.queued)}
                detail={`${t("metrics.admissionWaiters")} ${number(scope?.pressure.admission_waiters)}`}
              />
              <Value
                label={t("metrics.buffer")}
                value={scope ? formatBytes(scope.pressure.buffer_bytes) : "—"}
                detail={`${t("metrics.bufferWaiters")} ${number(scope?.pressure.buffer_waiters)}`}
              />
            </>
          )}
          <Value
            label={t("metrics.telemetryAge")}
            value={
              scope?.pressure.telemetry_age_ms == null
                ? "—"
                : `${number(scope.pressure.telemetry_age_ms / 1000)} s`
            }
          />
        </div>
      </details>
      {scope && scope.engine.expected_nodes > 0 && (
        <details className="metrics-details">
          <summary>{t("metrics.engineDetails")}</summary>
          <div className="engine-summary">
            <div className="section-title">
              <h3>{t("metrics.engine")}</h3>
              <HelpTip subject={t("metrics.engine")}>{t("metrics.engineHint")}</HelpTip>
            </div>
            <div className="observability-extra">
              <Value
                label={t("metrics.input")}
                value={`${number(scope.engine.input.smoothed)} tok/s`}
                detail={`${t("metrics.reporting")} ${scope.engine.input_nodes}/${scope.engine.expected_nodes}`}
              />
              <Value
                label={t("metrics.output")}
                value={`${number(scope.engine.output.smoothed)} tok/s`}
                detail={`${t("metrics.reporting")} ${scope.engine.output_nodes}/${scope.engine.expected_nodes}`}
              />
              <Value
                label={t("metrics.completed")}
                value={`${number(scope.engine.requests.smoothed)} req/s`}
                detail={`${t("metrics.reporting")} ${scope.engine.request_nodes}/${scope.engine.expected_nodes}`}
              />
            </div>
          </div>
          {data && matches && (
            <div className="metric-trends">
              <Trend
                title={t("metrics.engineThroughput")}
                unit="tok/s"
                data={data}
                selectedAt={inspection}
                onInspect={setInspection}
                lines={[
                  { label: t("metrics.input"), read: (point) => point.engine.input.smoothed },
                  { label: t("metrics.output"), read: (point) => point.engine.output.smoothed },
                ]}
              />
              <Trend
                title={t("metrics.kv")}
                unit="%"
                data={data}
                selectedAt={inspection}
                onInspect={setInspection}
                lines={[
                  {
                    label: t("metrics.kv"),
                    read: (point) =>
                      point.peaks.kv_usage === null ? null : point.peaks.kv_usage * 100,
                  },
                ]}
              />
            </div>
          )}
          <div className="observability-extra">
            <Value label={t("metrics.cacheMinute")} value={percent(scope.engine.cache_hit_ratio)} />
            <Value
              label={t("metrics.preemptions")}
              value={number(scope.engine.preemptions_per_minute)}
            />
            <Value label={t("metrics.kv")} value={percent(scope.pressure.kv_usage)} />
            <Value
              label={t("metrics.engineDemand")}
              value={`${number(scope.pressure.running)} / ${number(scope.pressure.waiting)}`}
            />
            <Value label={t("metrics.resets")} value={number(scope.engine.resets)} />
          </div>
        </details>
      )}
      {!nodeId && (data?.models.length ?? 0) > 0 && (
        <details className="metrics-details" open>
          <summary>{t("metrics.models")}</summary>
          <Table.ScrollContainer minWidth={600}>
            <Table highlightOnHover>
              <Table.Thead>
                <Table.Tr>
                  <Table.Th>{t("metrics.model")}</Table.Th>
                  <Table.Th>{t("metrics.incoming")}</Table.Th>
                  <Table.Th>{t("metrics.output")}</Table.Th>
                  <Table.Th>{t("metrics.ttft95")}</Table.Th>
                  <Table.Th>{t("metrics.failure")}</Table.Th>
                  <Table.Th>{t("metrics.usageCoverage")}</Table.Th>
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {data?.models.map((item) => (
                  <Table.Tr key={item.id}>
                    <Table.Td>
                      <button
                        type="button"
                        className="metrics-model-link"
                        onClick={() => onModel(item.id)}
                      >
                        {item.id}
                      </button>
                    </Table.Td>
                    <Table.Td>{number(item.rates.incoming.smoothed)}</Table.Td>
                    <Table.Td>{number(item.rates.output.smoothed)}</Table.Td>
                    <Table.Td>{ms(item.latency.ttft.p95)}</Table.Td>
                    <Table.Td>{percent(item.failure_ratio)}</Table.Td>
                    <Table.Td>{percent(item.usage_coverage)}</Table.Td>
                  </Table.Tr>
                ))}
              </Table.Tbody>
            </Table>
          </Table.ScrollContainer>
        </details>
      )}
      <details className="metrics-details metrics-definitions">
        <summary>{t("metrics.definitions")}</summary>
        <p className="metrics-note">{t("metrics.scopeHint")}</p>
        <div className="metrics-quality" role="status">
          <Badge variant="light">{t("metrics.smoothed")}</Badge>
          <span>{t("metrics.covered", { seconds: number(scope?.observed_seconds) })}</span>
          <span>
            {t("metrics.usageCoverage")} {percent(scope?.usage_coverage)}
          </span>
          <span>
            {t("metrics.telemetryCoverage")} {scope?.pressure.fresh_nodes ?? 0}/
            {scope?.pressure.provider_nodes ?? 0}
          </span>
          {(scope?.latency.ttft.samples ?? 0) < 20 && <span>{t("metrics.lowSamples")}</span>}
        </div>
        <p className="metrics-note">{t("metrics.history")}</p>
        <p className="metrics-note">{t("metrics.rawPeak")}</p>
        <p className="metrics-note">{t("metrics.semantics")}</p>
      </details>
    </section>
  );
}
