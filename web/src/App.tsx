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
  BarChart3,
  Bell,
  Check,
  ChevronDown,
  Edit3,
  ExternalLink,
  LayoutDashboard,
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
import * as api from "./api";
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

function relativeSync(value: number | null): string {
  if (!value) return "Never synced";
  const seconds = Math.max(0, Math.floor((Date.now() - value) / 1000));
  if (seconds < 5) return "Synced just now";
  if (seconds < 60) return `Synced ${seconds}s ago`;
  return `Synced ${Math.floor(seconds / 60)}m ago`;
}

function MetricBox({ label, value, accent }: { label: string; value: string | number; accent?: "green" | "amber" | "red" }) {
  return <div className="metric-box"><span>{label}</span><strong className={accent ? `metric-${accent}` : ""}>{value}</strong></div>;
}

function ProgressValue({ value, total, tone = "green" }: { value: number; total: number; tone?: "green" | "amber" | "blue" }) {
  const percent = total > 0 ? Math.min(100, value / total * 100) : 0;
  return <div className="progress-value"><span><strong>{value.toLocaleString()}</strong> / {total.toLocaleString()}</span><div><i className={tone} style={{ width: `${percent}%` }} /></div><small>{Math.round(percent)}%</small></div>;
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
  const draining = nodes.filter((node) => node.runtime.lifecycle === "draining").length;
  const notReady = nodes.filter((node) => !node.admission.routable && node.runtime.lifecycle !== "draining").length;
  const connectionLost = nodes.filter((node) => node.runtime.health === "unhealthy" && Boolean(node.runtime.provider_last_error)).length;
  const attention = nodes.filter((node) => !node.admission.accepting_assignments && node.admission.state !== "at_capacity");
  const totalConcurrency = status?.fleet.total_concurrency ?? 0;
  const active = status?.fleet.active_requests ?? 0;

  return <div className="page-frame overview-page">
    <header className="page-title-row">
      <h1>Overview</h1>
      <div className="account-actions"><button className="bare-icon" aria-label="Notifications"><Bell size={16} /></button><span className="avatar">AD</span><span>Admin</span><ChevronDown size={13} /></div>
    </header>

    <section className="status-strip">
      <span>Gateway Status</span>
      <StatusBadge value={status?.ready ? "ready" : "not_ready"} />
      <i />
      <span>Estuary v{status?.version ?? "--"}</span>
      <i />
      <span>{relativeSync(lastSync)}</span>
      <Button variant="default" size="compact-sm" leftSection={<RefreshCw className={refreshing ? "spin" : ""} size={13} />} disabled={refreshing} onClick={onRefresh}>Refresh</Button>
    </section>

    <section className="dashboard-panel fleet-summary-panel">
      <h2>Fleet Summary</h2>
      <div className="fleet-summary-grid">
        <MetricBox label="Total Nodes" value={status?.fleet.total_nodes ?? 0} />
        <MetricBox label="Ready / Accepting" value={status?.fleet.accepting_nodes ?? 0} accent="green" />
        <MetricBox label="Draining" value={draining} accent="amber" />
        <MetricBox label="Not Ready" value={notReady} accent="red" />
        <MetricBox label="Connection Lost" value={connectionLost} />
      </div>
    </section>

    <div className="dashboard-two-column">
      <section className="dashboard-panel compact-panel">
        <h2>Capacity</h2>
        <div className="capacity-grid">
          <MetricBox label="Total Concurrency" value={totalConcurrency.toLocaleString()} />
          <div className="metric-box"><span>In Use</span><strong>{active.toLocaleString()}</strong><div className="thin-progress"><i style={{ width: `${totalConcurrency ? active / totalConcurrency * 100 : 0}%` }} /></div></div>
          <MetricBox label="Available Now" value={(status?.fleet.available_concurrency ?? 0).toLocaleString()} />
          <MetricBox label="Routable Nodes" value={`${status?.fleet.routable_nodes ?? 0} / ${status?.fleet.total_nodes ?? 0}`} />
        </div>
      </section>
      <section className="dashboard-panel compact-panel">
        <h2>Global Queue</h2>
        <div className="capacity-grid">
          <MetricBox label="Current Requests" value={status?.queue.requests ?? 0} />
          <MetricBox label="Request Limit" value={(status?.queue.max_requests ?? 0).toLocaleString()} />
          <MetricBox label="Request Body Bytes" value={formatBytes(status?.queue.bytes ?? 0)} />
          <MetricBox label="Bytes Limit" value={formatBytes(status?.queue.max_bytes ?? 0)} />
        </div>
      </section>
    </div>

    <div className="attention-actions-grid">
      <section className="dashboard-panel attention-panel-dark">
        <h2>Attention Required</h2>
        {attention.length === 0 ? <div className="all-clear"><Check size={16} /><span><strong>All systems nominal</strong>No nodes currently require operator attention.</span></div> : <div className="attention-rows">
          {attention.slice(0, 5).map((node) => <button key={node.config.id} onClick={() => onSelectNode(node)}>
            <AlertTriangle size={15} /><strong>{node.config.id}</strong><StatusBadge value={node.admission.state} /><span>{node.admission.reason}</span><small>{node.runtime.active} active</small><em>View</em>
          </button>)}
        </div>}
      </section>
      <section className="dashboard-panel quick-actions">
        <h2>Quick Actions</h2>
        <Button fullWidth leftSection={<Plus size={14} />} onClick={onAdd}>Add Upstream Node</Button>
        <Button fullWidth variant="default" onClick={onShowNodes}>View All Nodes</Button>
        <Button component="a" href="/metrics" target="_blank" fullWidth variant="default" leftSection={<BarChart3 size={14} />}>Raw Metrics</Button>
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
      <div><h1>Upstreams</h1><span>{nodes.length} nodes <i /> {relativeSync(lastSync)}</span></div>
      <div className="heading-actions"><Button variant="default" leftSection={<RefreshCw className={refreshing ? "spin" : ""} size={14} />} disabled={refreshing} onClick={onRefresh}>Refresh</Button><Button leftSection={<Plus size={14} />} onClick={onAdd}>Add Node</Button></div>
    </header>

    <TextInput className="node-search" leftSection={<Search size={14} />} placeholder="Search by Node ID, URL, public model, or upstream model..." aria-label="Search nodes" value={query} onChange={(event) => { setPage(1); onQuery(event.target.value); }} />
    <div className="filter-row" aria-label="Filter nodes">
      {(["all", "accepting", "attention", "draining", "not_ready"] as NodeFilter[]).map((value) => <button key={value} className={filter === value ? `active ${value}` : ""} onClick={() => changeFilter(value)}>
        {value === "all" ? "All" : value === "not_ready" ? "Not Ready" : value.charAt(0).toUpperCase() + value.slice(1)} ({counts[value]})
      </button>)}
    </div>

    <section className="upstream-table-shell">
      {loading ? <div className="empty-state"><LoaderCircle className="spin" size={20} />Loading nodes</div> : visible.length === 0 ? <div className="empty-state"><Server size={22} /><strong>{query || filter !== "all" ? "No matching nodes" : "No upstream nodes"}</strong>{!query && filter === "all" && <Button leftSection={<Plus size={14} />} onClick={onAdd}>Add Node</Button>}</div> : <div className="table-scroll">
        <table className="upstream-table">
          <thead><tr><th>Node ID / Base URL</th><th>Status / Admission</th><th>Provider</th><th>Requests (Upstream)</th><th>Active / Max Concurrency</th><th>Latency (EWMA)</th><th>KV Blocks</th><th>Models</th><th /></tr></thead>
          <tbody>{visible.map((node) => {
            const running = node.runtime.upstream_running ?? node.runtime.active;
            const waiting = node.runtime.upstream_waiting ?? 0;
            return <tr key={node.config.id} tabIndex={0} onClick={() => onSelect(node)} onKeyDown={(event) => { if (event.key === "Enter") onSelect(node); }}>
              <td data-label="Node"><strong>{node.config.id}</strong><span>{node.config.base_url}</span></td>
              <td data-label="Admission"><StatusBadge value={node.admission.state} /><span>{node.admission.reason}</span></td>
              <td data-label="Provider"><strong>{node.config.provider.type === "vllm" ? "vLLM 0.25+" : "OpenAI"}</strong><span>{node.runtime.provider_version ?? node.runtime.provider_state}</span></td>
              <td data-label="Upstream demand"><strong>{running} / {waiting}</strong><span>Running / Waiting</span></td>
              <td data-label="Local load"><ProgressValue value={node.runtime.active} total={node.runtime.max_concurrency} /></td>
              <td data-label="Latency"><strong>{Math.round(node.runtime.latency_ewma_ms)} ms</strong></td>
              <td data-label="KV blocks"><strong>{formatCompactNumber(node.exact_kv_blocks)}</strong><span>{node.exact_kv_authoritative ? "Authoritative" : "Approximate"}</span></td>
              <td data-label="Models"><strong>{Object.keys(node.config.models).length}</strong></td>
              <td className="row-menu-cell" onClick={(event) => event.stopPropagation()}>
                <Menu position="bottom-end" shadow="md" width={170} withinPortal>
                  <Menu.Target><button className="bare-icon" aria-label={`Actions for ${node.config.id}`}><MoreHorizontal size={16} /></button></Menu.Target>
                  <Menu.Dropdown>
                    <Menu.Item leftSection={<ExternalLink size={13} />} onClick={() => onSelect(node)}>View Details</Menu.Item>
                    <Menu.Item leftSection={<Edit3 size={13} />} onClick={() => onEdit(node)}>Edit</Menu.Item>
                    <Menu.Item leftSection={node.runtime.lifecycle === "serving" ? <PauseCircle size={13} /> : <Play size={13} />} onClick={() => onToggleDrain(node)}>{node.runtime.lifecycle === "serving" ? "Drain" : "Resume"}</Menu.Item>
                    <Menu.Divider />
                    <Menu.Item color="red" leftSection={<Trash2 size={13} />} onClick={() => onDelete(node)}>Delete</Menu.Item>
                  </Menu.Dropdown>
                </Menu>
              </td>
            </tr>;
          })}</tbody>
        </table>
      </div>}
    </section>
    <footer className="table-footer"><span>Showing {visible.length ? (safePage - 1) * pageSize + 1 : 0} to {Math.min(safePage * pageSize, filtered.length)} of {filtered.length} nodes</span>{pages > 1 && <Pagination total={pages} value={safePage} onChange={setPage} size="xs" />}</footer>
  </div>;
}

export default function App() {
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

  const refresh = useCallback(async (quiet = false) => {
    const sequence = ++refreshSequence.current;
    if (!quiet) setRefreshing(true);
    const [nodeResult, statusResult] = await Promise.allSettled([api.listNodes(), api.getStatus()]);
    if (sequence !== refreshSequence.current) return;
    if (nodeResult.status === "fulfilled") setNodes(nodeResult.value);
    if (statusResult.status === "fulfilled") setStatus(statusResult.value);
    const failure = nodeResult.status === "rejected" ? nodeResult.reason : statusResult.status === "rejected" ? statusResult.reason : null;
    setConnectionError(failure instanceof Error ? failure.message : failure ? "Control plane unavailable" : null);
    if (!failure) setLastSync(Date.now());
    setLoading(false);
    setRefreshing(false);
  }, []);

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
      setToast({ tone: "success", message: state.mode === "create" ? "Node added" : "Node updated" });
      await refresh(true);
    } catch (error) {
      setToast({ tone: "error", message: error instanceof Error ? error.message : "Node operation failed" });
      if (error instanceof api.ApiError && error.code === "revision_conflict") await refresh(true);
    } finally {
      setBusy(false);
    }
  };

  const toggleDrain = async (node: NodeRecord) => {
    setBusy(true);
    try {
      await api.setDraining(node.config.id, node.runtime.lifecycle === "serving");
      setToast({ tone: "success", message: node.runtime.lifecycle === "serving" ? "Node is draining" : "Node resumed" });
      await refresh(true);
    } catch (error) {
      setToast({ tone: "error", message: error instanceof Error ? error.message : "Lifecycle update failed" });
    } finally {
      setBusy(false);
    }
  };

  const remove = async () => {
    if (!confirmDelete) return;
    setBusy(true);
    try {
      await api.deleteNode(confirmDelete.config.id, confirmDelete.revision);
      setToast({ tone: "success", message: "Node deleted" });
      setConfirmDelete(null);
      setSelectedNodeId(null);
      setView("upstreams");
      await refresh(true);
    } catch (error) {
      setToast({ tone: "error", message: error instanceof Error ? error.message : "Delete failed" });
    } finally {
      setBusy(false);
    }
  };

  return <div className="app-shell">
    <aside className="desktop-sidebar">
      <div className="brand"><Waves size={25} /><strong>Estuary</strong></div>
      <nav aria-label="Control plane navigation">
        <button className={view === "overview" && !selectedNode && !editor ? "active" : ""} onClick={() => changeView("overview")}><LayoutDashboard size={16} />Overview</button>
        <button className={view === "upstreams" || selectedNode || editor ? "active" : ""} onClick={() => changeView("upstreams")}><Server size={16} />Upstreams</button>
        <a href="/metrics" target="_blank" rel="noreferrer"><BarChart3 size={16} />Raw Metrics</a>
      </nav>
      <div className="system-status"><span>System</span><div><i className={connectionError ? "down" : ""} /><strong>{connectionError ? "Disconnected" : "Connected"}</strong><small>estuary-admin</small></div></div>
      <button className="collapse-label"><ChevronDown size={13} /> Collapse</button>
    </aside>

    <main className="main-content">
      {connectionError && <div className="connection-banner" role="alert"><AlertTriangle size={16} /><span><strong>Control plane unavailable</strong>{connectionError}. Loaded values may be stale.</span><Button variant="default" size="compact-sm" onClick={() => void refresh()}>Retry</Button></div>}
      {editor ? <NodeEditor state={editor} busy={busy} onClose={() => setEditor(null)} onSave={save} />
        : selectedNode ? <NodeDetails node={selectedNode} busy={busy} onClose={() => setSelectedNodeId(null)} onEdit={() => openEdit(selectedNode)} onToggleDrain={() => void toggleDrain(selectedNode)} onDelete={() => setConfirmDelete(selectedNode)} />
          : view === "overview" ? <Overview status={status} nodes={nodes} lastSync={lastSync} refreshing={refreshing} onRefresh={() => void refresh()} onAdd={openAdd} onShowNodes={() => changeView("upstreams")} onSelectNode={(node) => setSelectedNodeId(node.config.id)} />
            : <Upstreams nodes={nodes} loading={loading} query={query} filter={filter} lastSync={lastSync} refreshing={refreshing} onQuery={setQuery} onFilter={setFilter} onRefresh={() => void refresh()} onSelect={(node) => setSelectedNodeId(node.config.id)} onEdit={openEdit} onToggleDrain={(node) => void toggleDrain(node)} onDelete={setConfirmDelete} onAdd={openAdd} />}
    </main>

    {!editor && !selectedNode && <nav className="mobile-bottom-nav" aria-label="Mobile navigation">
      <button className={view === "overview" ? "active" : ""} onClick={() => changeView("overview")}><LayoutDashboard size={16} />Overview</button>
      <button className={view === "upstreams" ? "active" : ""} onClick={() => changeView("upstreams")}><Server size={16} />Upstreams</button>
      <a href="/metrics" target="_blank" rel="noreferrer"><BarChart3 size={16} />Raw Metrics</a>
      <button onClick={openAdd}><Plus size={16} />Add</button>
    </nav>}

    <Modal opened={Boolean(confirmDelete)} onClose={() => !busy && setConfirmDelete(null)} title={`Delete ${confirmDelete?.config.id ?? "node"}?`} centered>
      <div className="delete-dialog"><div className="delete-icon"><Trash2 size={18} /></div><p>The node will drain first. Active requests must finish before its persisted configuration is removed.</p><div><Button variant="default" disabled={busy} onClick={() => setConfirmDelete(null)}>Cancel</Button><Button color="red" disabled={busy} leftSection={busy ? <LoaderCircle className="spin" size={14} /> : <Trash2 size={14} />} onClick={() => void remove()}>Delete Node</Button></div></div>
    </Modal>

    {toast && <Notification className="app-notification" color={toast.tone === "success" ? "green" : "red"} icon={toast.tone === "success" ? <Check size={15} /> : <X size={15} />} withCloseButton onClose={() => setToast(null)}>{toast.message}</Notification>}
  </div>;
}
