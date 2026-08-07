import {
  Button,
  Menu,
  Modal,
  Notification,
  Pagination,
  TextInput,
} from "@mantine/core";
import {
  AlertTriangle,
  Check,
  Edit3,
  ExternalLink,
  LayoutDashboard,
  Languages,
  LoaderCircle,
  MoreHorizontal,
  PauseCircle,
  Play,
  Plus,
  RefreshCw,
  Search,
  Server,
  Trash2,
  Waves,
  X,
} from "lucide-react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { TFunction } from "i18next";
import { useTranslation } from "react-i18next";
import * as api from "./api";
import { localeStorageKey, type Locale } from "./i18n";
import { NodeDetails } from "./NodeDetails";
import { NodeEditor, type EditorState } from "./NodeEditor";
import { createDraft, draftToConfig, formatCompactNumber, recordToDraft, shouldClearApiKey } from "./node-config";
import type { GatewayStatus, NodeRecord } from "./types";
import { formatBytes, StatusBadge } from "./ui";

type View = "overview" | "upstreams";
type NodeFilter = "all" | "accepting" | "attention" | "draining" | "not_ready";

interface ToastState {
  tone: "success" | "error";
  message: string;
}

function relativeSync(value: number | null, t: TFunction): string {
  if (!value) return t("sync.never");
  const seconds = Math.max(0, Math.floor((Date.now() - value) / 1000));
  if (seconds < 5) return t("sync.now");
  if (seconds < 60) return t("sync.seconds", { count: seconds });
  return t("sync.minutes", { count: Math.floor(seconds / 60) });
}

function MetricBox({ label, value, accent }: { label: string; value: string | number; accent?: "green" | "amber" | "red" }) {
  return <div className="metric-box"><span>{label}</span><strong className={accent ? `metric-${accent}` : ""}>{value}</strong></div>;
}

function formatRate(value: number | null | undefined, locale: string): string {
  return value == null || !Number.isFinite(value) ? "--" : formatCompactNumber(Math.max(0, value), locale);
}

function formatRatio(value: number | null | undefined): string {
  return value == null || !Number.isFinite(value) ? "--" : `${Math.round(value * 100)}%`;
}

function summarizeVllm(nodes: NodeRecord[]) {
  const vllm = nodes.filter((node) => node.config.provider.type === "vllm");
  const fresh = vllm.filter((node) => node.admission.telemetry_fresh);
  const sum = (read: (node: NodeRecord) => number | null) => {
    const values = fresh.map(read).filter((value): value is number => value !== null);
    return values.length ? values.reduce((total, value) => total + value, 0) : null;
  };
  const prefixQueries = sum((node) => node.runtime.prefix_cache_queries_total);
  const prefixHits = sum((node) => node.runtime.prefix_cache_hits_total);
  const kvValues = fresh.map((node) => node.runtime.kv_cache_usage).filter((value): value is number => value !== null);
  return {
    nodes: vllm.length,
    fresh: fresh.length,
    running: fresh.reduce((total, node) => total + (node.runtime.upstream_running ?? 0), 0),
    waiting: fresh.reduce((total, node) => total + (node.runtime.upstream_waiting ?? 0), 0),
    promptRate: sum((node) => node.runtime.prompt_tokens_per_second),
    generationRate: sum((node) => node.runtime.generation_tokens_per_second),
    requestRate: sum((node) => node.runtime.requests_per_second),
    prefixHitRate: prefixQueries && prefixHits !== null ? Math.min(1, prefixHits / prefixQueries) : null,
    maxKvUsage: kvValues.length ? Math.max(...kvValues) : null,
    preemptions: sum((node) => node.runtime.preemptions_total),
    exactReady: vllm.filter((node) => node.exact_kv_authoritative).length,
    waitingBlocked: vllm.filter((node) => node.admission.waiting_watermark_blocked).length,
    kvPressure: fresh.filter((node) => (node.runtime.kv_cache_usage ?? 0) >= 0.9).length,
  };
}

function VllmRuntimePanel({ nodes, compact = false }: { nodes: NodeRecord[]; compact?: boolean }) {
  const { t, i18n } = useTranslation();
  const locale = i18n.resolvedLanguage === "zh-CN" ? "zh-CN" : "en";
  const runtime = summarizeVllm(nodes);
  return <section className={`dashboard-panel vllm-runtime-panel ${compact ? "compact" : ""}`}>
    <div className="panel-heading"><h2>{t("vllm.runtime")}</h2><span>{t("vllm.reporting", { fresh: runtime.fresh, total: runtime.nodes })}</span></div>
    <div className="vllm-runtime-grid">
      <MetricBox label={t("vllm.promptThroughput")} value={`${formatRate(runtime.promptRate, locale)} tok/s`} />
      <MetricBox label={t("vllm.generationThroughput")} value={`${formatRate(runtime.generationRate, locale)} tok/s`} />
      <MetricBox label={t("vllm.completedRequests")} value={`${formatRate(runtime.requestRate, locale)} req/s`} />
      <MetricBox label={t("vllm.engineDemand")} value={`${runtime.running} / ${runtime.waiting}`} accent={runtime.waiting > 0 ? "amber" : undefined} />
      <MetricBox label={t("vllm.peakKv")} value={formatRatio(runtime.maxKvUsage)} accent={(runtime.maxKvUsage ?? 0) >= 0.9 ? "amber" : undefined} />
      <MetricBox label={t("vllm.prefixHit")} value={formatRatio(runtime.prefixHitRate)} />
    </div>
  </section>;
}

function UsageRow({ label, value, total, display, totalDisplay, tone = "blue" }: {
  label: string;
  value: number;
  total: number;
  display?: string;
  totalDisplay?: string;
  tone?: "blue" | "green" | "amber";
}) {
  const { i18n } = useTranslation();
  const locale = i18n.resolvedLanguage === "zh-CN" ? "zh-CN" : "en";
  const percent = total > 0 ? Math.min(100, value / total * 100) : 0;
  return <div className="usage-row">
    <div><span>{label}</span><strong>{display ?? value.toLocaleString(locale)} <small>/ {totalDisplay ?? total.toLocaleString(locale)}</small></strong></div>
    <div className="usage-track"><i className={tone} style={{ width: `${percent}%` }} /></div>
    <em>{Math.round(percent)}%</em>
  </div>;
}

function ProgressValue({ value, total, tone = "green" }: { value: number; total: number; tone?: "green" | "amber" | "blue" }) {
  const { i18n } = useTranslation();
  const locale = i18n.resolvedLanguage === "zh-CN" ? "zh-CN" : "en";
  const percent = total > 0 ? Math.min(100, value / total * 100) : 0;
  return <div className="progress-value"><span><strong>{value.toLocaleString(locale)}</strong> / {total.toLocaleString(locale)}</span><div><i className={tone} style={{ width: `${percent}%` }} /></div><small>{Math.round(percent)}%</small></div>;
}

function LanguageSwitch() {
  const { t, i18n } = useTranslation();
  const locale: Locale = i18n.resolvedLanguage === "zh-CN" ? "zh-CN" : "en";
  const select = (next: Locale) => {
    window.localStorage.setItem(localeStorageKey, next);
    void i18n.changeLanguage(next);
  };
  return <Menu position="bottom-end" shadow="md" width={160} withinPortal>
    <Menu.Target>
      <button className="language-trigger" aria-label={t("language.label")} title={t("language.label")}><Languages size={15} /><span>{locale === "zh-CN" ? "中文" : "EN"}</span></button>
    </Menu.Target>
    <Menu.Dropdown>
      <Menu.Item rightSection={locale === "en" ? <Check size={13} /> : null} onClick={() => select("en")}>{t("language.english")}</Menu.Item>
      <Menu.Item rightSection={locale === "zh-CN" ? <Check size={13} /> : null} onClick={() => select("zh-CN")}>{t("language.chinese")}</Menu.Item>
    </Menu.Dropdown>
  </Menu>;
}

function Overview({
  status,
  nodes,
  lastSync,
  refreshing,
  onRefresh,
  onAdd,
  onShowNodes,
  onSelectNode,
}: {
  status: GatewayStatus | null;
  nodes: NodeRecord[];
  lastSync: number | null;
  refreshing: boolean;
  onRefresh: () => void;
  onAdd: () => void;
  onShowNodes: () => void;
  onSelectNode: (node: NodeRecord) => void;
}) {
  const { t, i18n } = useTranslation();
  const locale = i18n.resolvedLanguage === "zh-CN" ? "zh-CN" : "en";
  const draining = nodes.filter((node) => node.runtime.lifecycle === "draining").length;
  const notReady = nodes.filter((node) => !node.admission.routable && node.runtime.lifecycle !== "draining").length;
  const connectionLost = nodes.filter((node) => node.runtime.health === "unhealthy" && Boolean(node.runtime.provider_last_error)).length;
  const attention = nodes.filter((node) => !node.admission.accepting_assignments && node.admission.state !== "at_capacity");
  const totalConcurrency = status?.fleet.total_concurrency ?? 0;
  const active = status?.fleet.active_requests ?? 0;
  const vllm = summarizeVllm(nodes);

  return <div className="page-frame overview-page">
    <header className="page-title-row"><div><h1>{t("nav.overview")}</h1><p>{t("overview.subtitle")}</p></div></header>

    <section className="status-strip">
      <span>{t("overview.gateway")}</span>
      <StatusBadge value={status?.ready ? "ready" : "not_ready"} />
      <i />
      <span>Estuary v{status?.version ?? "--"}</span>
      <i />
      <span>{relativeSync(lastSync, t)}</span>
      <Button variant="default" size="compact-sm" leftSection={<RefreshCw className={refreshing ? "spin" : ""} size={13} />} disabled={refreshing} onClick={onRefresh}>{t("common.refresh")}</Button>
    </section>

    <section className="dashboard-panel fleet-summary-panel">
      <h2>{t("overview.fleetSummary")}</h2>
      <div className="fleet-summary-grid">
        <MetricBox label={t("overview.totalNodes")} value={status?.fleet.total_nodes ?? 0} />
        <MetricBox label={t("overview.readyAccepting")} value={status?.fleet.accepting_nodes ?? 0} accent="green" />
        <MetricBox label={t("overview.draining")} value={draining} accent="amber" />
        <MetricBox label={t("overview.notReady")} value={notReady} accent="red" />
        <MetricBox label={t("overview.connectionLost")} value={connectionLost} />
      </div>
    </section>

    <VllmRuntimePanel nodes={nodes} />

    <div className="dashboard-two-column">
      <section className="dashboard-panel compact-panel">
        <h2>{t("overview.capacity")}</h2>
        <div className="usage-list">
          <UsageRow label={t("overview.localConcurrency")} value={active} total={totalConcurrency} tone="green" />
          <UsageRow label={t("overview.availableCapacity")} value={status?.fleet.available_concurrency ?? 0} total={totalConcurrency} />
          <UsageRow label={t("overview.publicConnections")} value={status?.connections?.public ?? 0} total={status?.connections?.max_public ?? 0} />
          <div className="panel-stat-row"><span>{t("overview.routableNodes")}</span><strong>{status?.fleet.routable_nodes ?? 0} <small>/ {status?.fleet.total_nodes ?? 0}</small></strong></div>
        </div>
      </section>
      <section className="dashboard-panel compact-panel">
        <h2>{t("overview.queueMemory")}</h2>
        <div className="usage-list">
          <UsageRow label={t("overview.queuedRequests")} value={status?.queue.requests ?? 0} total={status?.queue.max_requests ?? 0} tone="amber" />
          <div className="panel-stat-row"><span>{t("overview.waitingAdmission")}</span><strong className={(status?.queue.admission_waiters ?? 0) > 0 ? "metric-amber" : ""}>{status?.queue.admission_waiters ?? 0}</strong></div>
          <UsageRow label={t("overview.queuedBodies")} value={status?.queue.bytes ?? 0} total={status?.queue.max_bytes ?? 0} display={formatBytes(status?.queue.bytes ?? 0)} totalDisplay={formatBytes(status?.queue.max_bytes ?? 0)} tone="amber" />
          <UsageRow label={t("overview.bufferedResponses")} value={status?.response_buffer?.used_bytes ?? 0} total={status?.response_buffer?.max_bytes ?? 0} display={formatBytes(status?.response_buffer?.used_bytes ?? 0)} totalDisplay={formatBytes(status?.response_buffer?.max_bytes ?? 0)} />
          <div className="panel-stat-row"><span>{t("overview.waitingMemory")}</span><strong className={(status?.response_buffer?.waiting_responses ?? 0) > 0 ? "metric-amber" : ""}>{status?.response_buffer?.waiting_responses ?? 0}</strong></div>
        </div>
      </section>
    </div>

    <div className="attention-actions-grid">
      <section className="dashboard-panel attention-panel-dark">
        <h2>{t("overview.attention")}</h2>
        {attention.length === 0 ? <div className="all-clear"><Check size={16} /><span><strong>{t("overview.nominal")}</strong>{t("overview.noAttention")}</span></div> : <div className="attention-rows">
          {attention.slice(0, 5).map((node) => <button key={node.config.id} onClick={() => onSelectNode(node)}>
            <AlertTriangle size={15} /><strong>{node.config.id}</strong><StatusBadge value={node.admission.state} /><span>{t(`admission.${node.admission.state}`, { defaultValue: node.admission.reason })}</span><small>{t("overview.active", { count: node.runtime.active })}</small><em>{t("common.view")}</em>
          </button>)}
        </div>}
      </section>
      <section className="dashboard-panel quick-actions">
        <h2>{t("overview.routingSignals")}</h2>
        <div className="routing-signal"><span>{t("overview.exactDirectories")}</span><strong>{vllm.exactReady} / {vllm.nodes}</strong></div>
        <div className="routing-signal"><span>{t("overview.waitingWatermark")}</span><strong className={vllm.waitingBlocked ? "metric-amber" : ""}>{vllm.waitingBlocked}</strong></div>
        <div className="routing-signal"><span>{t("overview.kvPressure")}</span><strong className={vllm.kvPressure ? "metric-amber" : ""}>{vllm.kvPressure}</strong></div>
        <div className="routing-signal"><span>{t("overview.preemptions")}</span><strong>{vllm.preemptions === null ? "--" : formatCompactNumber(vllm.preemptions, locale)}</strong></div>
        <Button fullWidth leftSection={<Plus size={14} />} onClick={onAdd}>{t("overview.addUpstream")}</Button>
        <Button fullWidth variant="default" onClick={onShowNodes}>{t("overview.viewAll")}</Button>
      </section>
    </div>
  </div>;
}

function Upstreams({
  nodes,
  loading,
  query,
  filter,
  lastSync,
  refreshing,
  onQuery,
  onFilter,
  onRefresh,
  onSelect,
  onEdit,
  onToggleDrain,
  onDelete,
  onAdd,
}: {
  nodes: NodeRecord[];
  loading: boolean;
  query: string;
  filter: NodeFilter;
  lastSync: number | null;
  refreshing: boolean;
  onQuery: (value: string) => void;
  onFilter: (filter: NodeFilter) => void;
  onRefresh: () => void;
  onSelect: (node: NodeRecord) => void;
  onEdit: (node: NodeRecord) => void;
  onToggleDrain: (node: NodeRecord) => void;
  onDelete: (node: NodeRecord) => void;
  onAdd: () => void;
}) {
  const { t, i18n } = useTranslation();
  const locale = i18n.resolvedLanguage === "zh-CN" ? "zh-CN" : "en";
  const [page, setPage] = useState(1);
  const pageSize = 8;
  const filtered = useMemo(() => {
    const needle = query.trim().toLowerCase();
    return nodes.filter((node) => {
      const textMatch = !needle || [node.config.id, node.config.base_url, ...Object.keys(node.config.models), ...Object.values(node.config.models)].some((value) => value.toLowerCase().includes(needle));
      const filterMatch = filter === "all"
        || (filter === "accepting" && node.admission.accepting_assignments)
        || (filter === "attention" && !node.admission.accepting_assignments)
        || (filter === "draining" && node.runtime.lifecycle === "draining")
        || (filter === "not_ready" && !node.admission.routable && node.runtime.lifecycle !== "draining");
      return textMatch && filterMatch;
    }).sort((left, right) => left.config.id.localeCompare(right.config.id));
  }, [filter, nodes, query]);
  const pages = Math.max(1, Math.ceil(filtered.length / pageSize));
  const safePage = Math.min(page, pages);
  const visible = filtered.slice((safePage - 1) * pageSize, safePage * pageSize);
  const counts = {
    all: nodes.length,
    accepting: nodes.filter((node) => node.admission.accepting_assignments).length,
    attention: nodes.filter((node) => !node.admission.accepting_assignments).length,
    draining: nodes.filter((node) => node.runtime.lifecycle === "draining").length,
    not_ready: nodes.filter((node) => !node.admission.routable && node.runtime.lifecycle !== "draining").length,
  };

  const changeFilter = (value: NodeFilter) => { setPage(1); onFilter(value); };

  return <div className="page-frame upstreams-page">
    <header className="page-title-row upstream-title">
      <div><h1>{t("nav.upstreams")}</h1><span>{t("upstreams.summary", { count: nodes.length, sync: relativeSync(lastSync, t) })}</span></div>
      <div className="heading-actions"><Button variant="default" leftSection={<RefreshCw className={refreshing ? "spin" : ""} size={14} />} disabled={refreshing} onClick={onRefresh}>{t("common.refresh")}</Button><Button leftSection={<Plus size={14} />} onClick={onAdd}>{t("upstreams.addNode")}</Button></div>
    </header>

    <VllmRuntimePanel nodes={nodes} compact />

    <TextInput className="node-search" leftSection={<Search size={14} />} placeholder={t("upstreams.searchPlaceholder")} aria-label={t("upstreams.searchLabel")} value={query} onChange={(event) => { setPage(1); onQuery(event.target.value); }} />
    <div className="filter-row" aria-label={t("upstreams.filterLabel")}>
      {(["all", "accepting", "attention", "draining", "not_ready"] as NodeFilter[]).map((value) => <button key={value} className={filter === value ? `active ${value}` : ""} onClick={() => changeFilter(value)}>
        {t(`filter.${value}`)} ({counts[value]})
      </button>)}
    </div>

    <section className="upstream-table-shell">
      {loading ? <div className="empty-state"><LoaderCircle className="spin" size={20} />{t("upstreams.loading")}</div> : visible.length === 0 ? <div className="empty-state"><Server size={22} /><strong>{query || filter !== "all" ? t("upstreams.noMatch") : t("upstreams.empty")}</strong>{!query && filter === "all" && <Button leftSection={<Plus size={14} />} onClick={onAdd}>{t("upstreams.addNode")}</Button>}</div> : <div className="table-scroll">
        <table className="upstream-table">
          <thead><tr><th>{t("upstreams.nodeUrl")}</th><th>{t("upstreams.statusAdmission")}</th><th>{t("upstreams.providerTelemetry")}</th><th>{t("upstreams.engine")}</th><th>{t("upstreams.tokenRate")}</th><th>{t("upstreams.activeLimit")}</th><th>{t("upstreams.kvCache")}</th><th>{t("upstreams.latency")}</th><th /></tr></thead>
          <tbody>{visible.map((node) => {
            const running = node.runtime.upstream_running ?? node.runtime.active;
            const waiting = node.runtime.upstream_waiting ?? 0;
            return <tr key={node.config.id} tabIndex={0} onClick={() => onSelect(node)} onKeyDown={(event) => { if (event.key === "Enter") onSelect(node); }}>
              <td data-label={t("upstreams.node")}><strong>{node.config.id}</strong><span>{node.config.base_url}</span></td>
              <td data-label={t("upstreams.admission")}><StatusBadge value={node.admission.state} /><span>{t(`admission.${node.admission.state}`, { defaultValue: node.admission.reason })}</span></td>
              <td data-label={t("upstreams.provider")}><strong>{node.config.provider.type === "vllm" ? `vLLM ${node.runtime.provider_version ?? t("upstreams.checking")}` : t("upstreams.openaiCompatible")}</strong><span>{node.config.provider.type === "vllm" ? node.admission.telemetry_fresh ? t("upstreams.telemetryFresh") : t("upstreams.telemetryStale") : t("upstreams.genericProvider")}</span></td>
              <td data-label={t("upstreams.engine")}><strong>{running} / {waiting}</strong><span>{t("upstreams.runningWaiting")}</span></td>
              <td data-label={t("upstreams.tokenRateShort")}><strong>{formatRate(node.runtime.prompt_tokens_per_second, locale)} / {formatRate(node.runtime.generation_tokens_per_second, locale)}</strong><span>{t("upstreams.promptGeneration")}</span></td>
              <td data-label={t("upstreams.localLoad")}><ProgressValue value={node.runtime.active} total={node.runtime.max_concurrency} /></td>
              <td data-label={t("upstreams.kvCacheShort")}><strong>{node.config.provider.type === "vllm" ? t("upstreams.used", { value: formatRatio(node.runtime.kv_cache_usage) }) : "--"}</strong><span>{node.config.provider.type === "vllm" ? t("upstreams.kvDetail", { hit: formatRatio(node.runtime.prefix_cache_hit_rate), blocks: formatCompactNumber(node.exact_kv_blocks, locale), mode: node.exact_kv_authoritative ? t("upstreams.synced") : t("upstreams.fallback") }) : t("upstreams.noTelemetry")}</span></td>
              <td data-label={t("upstreams.latency")}><strong>{Math.round(node.runtime.latency_ewma_ms)} ms</strong><span>{t("upstreams.headerEwma")}</span></td>
              <td className="row-menu-cell" onClick={(event) => event.stopPropagation()}>
                <Menu position="bottom-end" shadow="md" width={170} withinPortal>
                  <Menu.Target><button className="bare-icon" aria-label={t("upstreams.actionsFor", { id: node.config.id })}><MoreHorizontal size={16} /></button></Menu.Target>
                  <Menu.Dropdown>
                    <Menu.Item leftSection={<ExternalLink size={13} />} onClick={() => onSelect(node)}>{t("upstreams.viewDetails")}</Menu.Item>
                    <Menu.Item leftSection={<Edit3 size={13} />} onClick={() => onEdit(node)}>{t("common.edit")}</Menu.Item>
                    <Menu.Item leftSection={node.runtime.lifecycle === "serving" ? <PauseCircle size={13} /> : <Play size={13} />} onClick={() => onToggleDrain(node)}>{node.runtime.lifecycle === "serving" ? t("upstreams.drain") : t("upstreams.resume")}</Menu.Item>
                    <Menu.Divider />
                    <Menu.Item color="red" leftSection={<Trash2 size={13} />} onClick={() => onDelete(node)}>{t("common.delete")}</Menu.Item>
                  </Menu.Dropdown>
                </Menu>
              </td>
            </tr>;
          })}</tbody>
        </table>
      </div>}
    </section>
    <footer className="table-footer"><span>{t("upstreams.showing", { from: visible.length ? (safePage - 1) * pageSize + 1 : 0, to: Math.min(safePage * pageSize, filtered.length), total: filtered.length })}</span>{pages > 1 && <Pagination total={pages} value={safePage} onChange={setPage} size="xs" />}</footer>
  </div>;
}

export default function App() {
  const { t, i18n } = useTranslation();
  const [view, setView] = useState<View>("overview");
  const [nodes, setNodes] = useState<NodeRecord[]>([]);
  const [status, setStatus] = useState<GatewayStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [connectionError, setConnectionError] = useState<string | null>(null);
  const [lastSync, setLastSync] = useState<number | null>(null);
  const [editor, setEditor] = useState<EditorState | null>(null);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [confirmDelete, setConfirmDelete] = useState<NodeRecord | null>(null);
  const [toast, setToast] = useState<ToastState | null>(null);
  const [query, setQuery] = useState("");
  const [filter, setFilter] = useState<NodeFilter>("all");
  const refreshSequence = useRef(0);

  useEffect(() => {
    const locale = i18n.resolvedLanguage === "zh-CN" ? "zh-CN" : "en";
    document.documentElement.lang = locale;
    document.title = t("app.title");
  }, [i18n.resolvedLanguage, t]);

  const refresh = useCallback(async (quiet = false) => {
    const sequence = ++refreshSequence.current;
    if (!quiet) setRefreshing(true);
    const [nodeResult, statusResult] = await Promise.allSettled([api.listNodes(), api.getStatus()]);
    if (sequence !== refreshSequence.current) return;
    if (nodeResult.status === "fulfilled") setNodes(nodeResult.value);
    if (statusResult.status === "fulfilled") setStatus(statusResult.value);
    const failure = nodeResult.status === "rejected" ? nodeResult.reason : statusResult.status === "rejected" ? statusResult.reason : null;
    setConnectionError(failure instanceof Error ? failure.message : failure ? t("controlPlane.unavailable") : null);
    if (!failure) setLastSync(Date.now());
    setLoading(false);
    setRefreshing(false);
  }, [t]);

  useEffect(() => {
    void refresh(true);
    const interval = window.setInterval(() => void refresh(true), 5000);
    return () => window.clearInterval(interval);
  }, [refresh]);

  useEffect(() => {
    if (!toast) return;
    const timeout = window.setTimeout(() => setToast(null), 6000);
    return () => window.clearTimeout(timeout);
  }, [toast]);

  useEffect(() => {
    window.scrollTo(0, 0);
  }, [view, selectedNodeId, editor?.mode]);

  const selectedNode = nodes.find((node) => node.config.id === selectedNodeId) ?? null;
  const openAdd = () => { setSelectedNodeId(null); setEditor({ mode: "create", draft: createDraft(), revision: null }); };
  const openEdit = (node: NodeRecord) => { setSelectedNodeId(null); setEditor({ mode: "edit", draft: recordToDraft(node), revision: node.revision }); };
  const changeView = (next: View) => { setEditor(null); setSelectedNodeId(null); setView(next); };

  const save = async (state: EditorState) => {
    setBusy(true);
    try {
      const config = draftToConfig(state.draft);
      if (state.mode === "create") await api.createNode(config);
      else await api.updateNode(config, state.revision, shouldClearApiKey(state.draft));
      setEditor(null);
      setView("upstreams");
      setToast({ tone: "success", message: state.mode === "create" ? t("toast.nodeAdded") : t("toast.nodeUpdated") });
      await refresh(true);
    } catch (error) {
      setToast({ tone: "error", message: error instanceof Error ? error.message : t("toast.operationFailed") });
      if (error instanceof api.ApiError && error.code === "revision_conflict") await refresh(true);
    } finally {
      setBusy(false);
    }
  };

  const toggleDrain = async (node: NodeRecord) => {
    setBusy(true);
    try {
      await api.setDraining(node.config.id, node.runtime.lifecycle === "serving");
      setToast({ tone: "success", message: node.runtime.lifecycle === "serving" ? t("toast.nodeDraining") : t("toast.nodeResumed") });
      await refresh(true);
    } catch (error) {
      setToast({ tone: "error", message: error instanceof Error ? error.message : t("toast.lifecycleFailed") });
    } finally {
      setBusy(false);
    }
  };

  const remove = async () => {
    if (!confirmDelete) return;
    setBusy(true);
    try {
      await api.deleteNode(confirmDelete.config.id, confirmDelete.revision);
      setToast({ tone: "success", message: t("toast.nodeDeleted") });
      setConfirmDelete(null);
      setSelectedNodeId(null);
      setView("upstreams");
      await refresh(true);
    } catch (error) {
      setToast({ tone: "error", message: error instanceof Error ? error.message : t("toast.deleteFailed") });
    } finally {
      setBusy(false);
    }
  };

  return <div className="app-shell">
    <aside className="desktop-sidebar">
      <div className="brand"><Waves size={25} /><strong>Estuary</strong></div>
      <nav aria-label={t("nav.label")}>
        <button className={view === "overview" && !selectedNode && !editor ? "active" : ""} onClick={() => changeView("overview")}><LayoutDashboard size={16} />{t("nav.overview")}</button>
        <button className={view === "upstreams" || selectedNode || editor ? "active" : ""} onClick={() => changeView("upstreams")}><Server size={16} />{t("nav.upstreams")}</button>
      </nav>
      <div className="sidebar-footer"><LanguageSwitch /><div className="system-status"><span>{t("controlPlane.label")}</span><div><i className={connectionError ? "down" : ""} /><strong>{connectionError ? t("controlPlane.disconnected") : t("controlPlane.connected")}</strong><small>estuary-admin</small></div></div></div>
    </aside>

    <div className="mobile-toolbar"><div className="brand"><Waves size={21} /><strong>Estuary</strong></div><LanguageSwitch /></div>

    <main className="main-content">
      {connectionError && <div className="connection-banner" role="alert"><AlertTriangle size={16} /><span><strong>{t("controlPlane.unavailable")}</strong>{t("controlPlane.stale", { error: connectionError })}</span><Button variant="default" size="compact-sm" onClick={() => void refresh()}>{t("common.retry")}</Button></div>}
      {editor ? <NodeEditor state={editor} busy={busy} onClose={() => setEditor(null)} onSave={save} />
        : selectedNode ? <NodeDetails node={selectedNode} busy={busy} onClose={() => setSelectedNodeId(null)} onEdit={() => openEdit(selectedNode)} onToggleDrain={() => void toggleDrain(selectedNode)} onDelete={() => setConfirmDelete(selectedNode)} />
          : view === "overview" ? <Overview status={status} nodes={nodes} lastSync={lastSync} refreshing={refreshing} onRefresh={() => void refresh()} onAdd={openAdd} onShowNodes={() => changeView("upstreams")} onSelectNode={(node) => setSelectedNodeId(node.config.id)} />
            : <Upstreams nodes={nodes} loading={loading} query={query} filter={filter} lastSync={lastSync} refreshing={refreshing} onQuery={setQuery} onFilter={setFilter} onRefresh={() => void refresh()} onSelect={(node) => setSelectedNodeId(node.config.id)} onEdit={openEdit} onToggleDrain={(node) => void toggleDrain(node)} onDelete={setConfirmDelete} onAdd={openAdd} />}
    </main>

    {!editor && !selectedNode && <nav className="mobile-bottom-nav" aria-label={t("nav.label")}>
      <button className={view === "overview" ? "active" : ""} onClick={() => changeView("overview")}><LayoutDashboard size={16} />{t("nav.overview")}</button>
      <button className={view === "upstreams" ? "active" : ""} onClick={() => changeView("upstreams")}><Server size={16} />{t("nav.upstreams")}</button>
      <button onClick={openAdd}><Plus size={16} />{t("nav.add")}</button>
    </nav>}

    <Modal opened={Boolean(confirmDelete)} onClose={() => !busy && setConfirmDelete(null)} title={t("delete.title", { id: confirmDelete?.config.id ?? "node" })} centered>
      <div className="delete-dialog"><div className="delete-icon"><Trash2 size={18} /></div><p>{t("delete.description")}</p><div><Button variant="default" disabled={busy} onClick={() => setConfirmDelete(null)}>{t("common.cancel")}</Button><Button color="red" disabled={busy} leftSection={busy ? <LoaderCircle className="spin" size={14} /> : <Trash2 size={14} />} onClick={() => void remove()}>{t("delete.node")}</Button></div></div>
    </Modal>

    {toast && <Notification className="app-notification" color={toast.tone === "success" ? "green" : "red"} icon={toast.tone === "success" ? <Check size={15} /> : <X size={15} />} withCloseButton onClose={() => setToast(null)}>{toast.message}</Notification>}
  </div>;
}
