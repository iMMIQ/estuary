import { Button, TextInput } from "@mantine/core";
import { AlertTriangle, Check, Plus, RefreshCw, ShieldCheck, Trash2 } from "lucide-react";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import {
  MetricBox,
  relativeSync,
  summarizeVllm,
  UsageRow,
  VllmRuntimePanel,
} from "./admin-metrics";
import { formatCompactNumber } from "./node-config";
import type { GatewayStatus, NodeRecord } from "./types";
import { formatBytes, StatusBadge } from "./ui";
export function Overview({
  status,
  nodes,
  lastSync,
  refreshing,
  onRefresh,
  onAdd,
  onShowNodes,
  onSelectNode,
  onSetIpLimit,
  onDeleteIpLimit,
  loading,
  nodesLoaded,
}: {
  loading: boolean;
  nodesLoaded: boolean;
  status: GatewayStatus | null;
  nodes: NodeRecord[];
  lastSync: number | null;
  refreshing: boolean;
  onRefresh: () => void;
  onAdd: () => void;
  onShowNodes: () => void;
  onSelectNode: (node: NodeRecord) => void;
  onSetIpLimit: (ip: string, limit: number) => Promise<boolean>;
  onDeleteIpLimit: (ip: string) => Promise<void>;
}) {
  const { t, i18n } = useTranslation();
  const locale = i18n.resolvedLanguage === "zh-CN" ? "zh-CN" : "en";
  const draining = nodes.filter((node) => node.runtime.lifecycle === "draining").length;
  const notReady = nodes.filter(
    (node) => !node.admission.routable && node.runtime.lifecycle !== "draining",
  ).length;
  const connectionLost = nodes.filter(
    (node) => node.runtime.health === "unhealthy" && Boolean(node.runtime.provider_last_error),
  ).length;
  const attention = nodes.filter(
    (node) => !node.admission.accepting_assignments && node.admission.state !== "at_capacity",
  );
  const totalConcurrency = status?.fleet.total_concurrency ?? null;
  const active = status?.fleet.active_requests ?? null;
  const vllm = summarizeVllm(nodes);
  const [ip, setIp] = useState("");
  const [limit, setLimit] = useState("1");
  const [ipBusy, setIpBusy] = useState(false);

  if (loading)
    return (
      <div className="page-frame">
        <header className="page-title-row">
          <h1>{t("nav.overview")}</h1>
          <Button variant="default" disabled={refreshing} onClick={onRefresh}>
            {t("common.refresh")}
          </Button>
        </header>
        <div className="empty-state" role="status">
          {t("data.loading")}
        </div>
      </div>
    );
  return (
    <div className="page-frame overview-page">
      <header className="page-title-row">
        <div>
          <h1>{t("nav.overview")}</h1>
          <p>{t("overview.subtitle")}</p>
        </div>
      </header>

      <section className="status-strip">
        <span>{t("overview.gateway")}</span>
        {status ? (
          <StatusBadge value={status.ready ? "ready" : "not_ready"} />
        ) : (
          <span>{t("common.unavailable")}</span>
        )}
        <i />
        <span>Estuary v{status?.version ?? "--"}</span>
        <i />
        <span>{relativeSync(lastSync, t)}</span>
        <Button
          variant="default"
          size="compact-sm"
          leftSection={<RefreshCw className={refreshing ? "spin" : ""} size={13} />}
          disabled={refreshing}
          onClick={onRefresh}
        >
          {t("common.refresh")}
        </Button>
      </section>

      <section className="dashboard-panel fleet-summary-panel">
        <h2>{t("overview.fleetSummary")}</h2>
        <div className="fleet-summary-grid">
          <MetricBox label={t("overview.totalNodes")} value={status?.fleet.total_nodes ?? null} />
          <MetricBox
            label={t("overview.readyAccepting")}
            value={status?.fleet.accepting_nodes ?? null}
            accent="green"
          />
          <MetricBox
            label={t("overview.draining")}
            value={nodesLoaded ? draining : null}
            accent="amber"
          />
          <MetricBox
            label={t("overview.notReady")}
            value={nodesLoaded ? notReady : null}
            accent="red"
          />
          <MetricBox
            label={t("overview.connectionLost")}
            value={nodesLoaded ? connectionLost : null}
          />
        </div>
      </section>

      <div className="attention-actions-grid">
        <section className="dashboard-panel attention-panel-dark">
          <h2>{t("overview.attention")}</h2>
          {!nodesLoaded ? (
            <div className="empty-state">{t("common.unavailable")}</div>
          ) : attention.length === 0 ? (
            <div className="all-clear">
              <Check size={16} />
              <span>
                <strong>{t("overview.nominal")}</strong>
                {t("overview.noAttention")}
              </span>
            </div>
          ) : (
            <div className="attention-rows">
              {attention.slice(0, 5).map((node) => (
                <button type="button" key={node.config.id} onClick={() => onSelectNode(node)}>
                  <AlertTriangle size={15} />
                  <strong>{node.config.id}</strong>
                  <StatusBadge value={node.admission.state} />
                  <span>
                    {t(`admission.${node.admission.state}`, {
                      defaultValue: node.admission.reason,
                    })}
                  </span>
                  <small>{t("overview.active", { count: node.runtime.active })}</small>
                  <em>{t("common.view")}</em>
                </button>
              ))}
            </div>
          )}
        </section>
        <section className="dashboard-panel quick-actions">
          <h2>{t("overview.routingSignals")}</h2>
          <div className="routing-signal">
            <span>{t("overview.exactDirectories")}</span>
            <strong>
              {vllm.exactReady} / {vllm.nodes}
            </strong>
          </div>
          <div className="routing-signal">
            <span>{t("overview.waitingWatermark")}</span>
            <strong className={vllm.waitingBlocked ? "metric-amber" : ""}>
              {vllm.waitingBlocked}
            </strong>
          </div>
          <div className="routing-signal">
            <span>{t("overview.kvPressure")}</span>
            <strong className={vllm.kvPressure ? "metric-amber" : ""}>{vllm.kvPressure}</strong>
          </div>
          <div className="routing-signal">
            <span>{t("overview.preemptions")}</span>
            <strong>
              {vllm.preemptions === null ? "--" : formatCompactNumber(vllm.preemptions, locale)}
            </strong>
          </div>
          <Button fullWidth leftSection={<Plus size={14} />} onClick={onAdd}>
            {t("overview.addUpstream")}
          </Button>
          <Button fullWidth variant="default" onClick={onShowNodes}>
            {t("overview.viewAll")}
          </Button>
        </section>
      </div>

      <section className="dashboard-panel scheduler-panel">
        <h2>{t("scheduler.overview")}</h2>
        <div className="scheduler-grid">
          <MetricBox
            label={t("scheduler.maxTtft")}
            value={
              nodes.some((node) => node.runtime.ttft_ewma_ms != null)
                ? `${Math.round(Math.max(...nodes.flatMap((node) => (node.runtime.ttft_ewma_ms == null ? [] : [node.runtime.ttft_ewma_ms]))))} ms`
                : "--"
            }
          />
          <MetricBox
            label={t("scheduler.prefill")}
            value={
              nodesLoaded && nodes.every((node) => node.runtime.pending_prefill_tokens != null)
                ? nodes
                    .reduce((sum, node) => sum + node.runtime.pending_prefill_tokens, 0)
                    .toLocaleString(locale)
                : "--"
            }
          />
          <MetricBox
            label={t("scheduler.decode")}
            value={
              nodesLoaded && nodes.every((node) => node.runtime.pending_decode_tokens != null)
                ? nodes
                    .reduce((sum, node) => sum + node.runtime.pending_decode_tokens, 0)
                    .toLocaleString(locale)
                : "--"
            }
          />
        </div>
      </section>
      <VllmRuntimePanel nodes={nodes} />

      <div className="dashboard-two-column">
        <section className="dashboard-panel compact-panel">
          <h2>{t("overview.capacity")}</h2>
          <div className="usage-list">
            <UsageRow
              label={t("overview.localConcurrency")}
              value={active}
              total={totalConcurrency}
              tone="green"
            />
            <UsageRow
              label={t("overview.availableCapacity")}
              value={status?.fleet.available_concurrency ?? null}
              total={totalConcurrency}
            />
            <UsageRow
              label={t("overview.publicConnections")}
              value={status?.connections?.public ?? null}
              total={status?.connections?.max_public ?? null}
            />
            <div className="panel-stat-row">
              <span>{t("overview.routableNodes")}</span>
              <strong>
                {status?.fleet.routable_nodes ?? "--"}{" "}
                <small>/ {status?.fleet.total_nodes ?? "--"}</small>
              </strong>
            </div>
          </div>
        </section>
        <section className="dashboard-panel compact-panel">
          <h2>{t("overview.queueMemory")}</h2>
          <div className="usage-list">
            <UsageRow
              label={t("overview.queuedRequests")}
              value={status?.queue.requests ?? null}
              total={status?.queue.max_requests ?? null}
              tone="amber"
            />
            <div className="panel-stat-row">
              <span>{t("overview.waitingAdmission")}</span>
              <strong className={(status?.queue.admission_waiters ?? 0) > 0 ? "metric-amber" : ""}>
                {status?.queue.admission_waiters ?? "--"}
              </strong>
            </div>
            <UsageRow
              label={t("overview.queuedBodies")}
              value={status?.queue.bytes ?? null}
              total={status?.queue.max_bytes ?? null}
              display={status ? formatBytes(status?.queue.bytes ?? 0) : "--"}
              totalDisplay={status ? formatBytes(status?.queue.max_bytes ?? 0) : "--"}
              tone="amber"
            />
            <UsageRow
              label={t("overview.bufferedResponses")}
              value={status?.response_buffer?.used_bytes ?? null}
              total={status?.response_buffer?.max_bytes ?? null}
              display={status ? formatBytes(status?.response_buffer?.used_bytes ?? 0) : "--"}
              totalDisplay={status ? formatBytes(status?.response_buffer?.max_bytes ?? 0) : "--"}
            />
            <div className="panel-stat-row">
              <span>{t("overview.waitingMemory")}</span>
              <strong
                className={
                  (status?.response_buffer?.waiting_responses ?? 0) > 0 ? "metric-amber" : ""
                }
              >
                {status?.response_buffer?.waiting_responses ?? "--"}
              </strong>
            </div>
          </div>
        </section>
      </div>

      <section className="dashboard-panel ip-connections-panel">
        <div className="panel-heading">
          <h2>{t("overview.ipConnections")}</h2>
          <span>{t("overview.currentConnections")}</span>
        </div>
        <div className="ip-connections-grid">
          <div className="ip-ranking">
            {(status?.connections.top_ips ?? []).length === 0 ? (
              <p>{t("overview.noConnections")}</p>
            ) : (
              status?.connections.top_ips.map((item, index) => (
                <div key={item.ip}>
                  <span>#{index + 1}</span>
                  <code>{item.ip}</code>
                  <strong>{item.active}</strong>
                </div>
              ))
            )}
          </div>
          <form
            className="ip-limit-form"
            onSubmit={(event) => {
              event.preventDefault();
              if (ipBusy) return;
              setIpBusy(true);
              void onSetIpLimit(ip.trim(), Number(limit))
                .then((saved) => {
                  if (saved) setIp("");
                })
                .finally(() => setIpBusy(false));
            }}
          >
            <TextInput
              disabled={ipBusy}
              aria-label={t("overview.ipAddress")}
              placeholder={t("overview.ipAddress")}
              value={ip}
              onChange={(event) => setIp(event.currentTarget.value)}
              required
            />
            <TextInput
              disabled={ipBusy}
              aria-label={t("overview.connectionLimit")}
              type="number"
              min={1}
              value={limit}
              onChange={(event) => setLimit(event.currentTarget.value)}
              required
            />
            <Button
              type="submit"
              loading={ipBusy}
              size="compact-sm"
              leftSection={<ShieldCheck size={14} />}
            >
              {t("overview.applyLimit")}
            </Button>
          </form>
          {(status?.connections.ip_limits ?? []).length > 0 && (
            <div className="ip-limits">
              {status?.connections.ip_limits.map((item) => (
                <div key={item.ip}>
                  <code>{item.ip}</code>
                  <span>{t("overview.limitValue", { count: item.limit })}</span>
                  <button
                    type="button"
                    className="bare-icon"
                    title={t("overview.removeLimit")}
                    aria-label={t("overview.removeLimitFor", { ip: item.ip })}
                    onClick={() => void onDeleteIpLimit(item.ip)}
                  >
                    <Trash2 size={14} />
                  </button>
                </div>
              ))}
            </div>
          )}
        </div>
      </section>
    </div>
  );
}
