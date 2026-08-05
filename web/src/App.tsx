import {
  Activity,
  AlertTriangle,
  BarChart3,
  Check,
  ChevronDown,
  CircleGauge,
  Database,
  Edit3,
  LayoutDashboard,
  LoaderCircle,
  Menu,
  MoreHorizontal,
  Network,
  Plus,
  RefreshCw,
  Search,
  Server,
  Trash2,
  X,
} from "lucide-react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import * as api from "./api";
import { NodeDetails } from "./NodeDetails";
import { NodeEditor, type EditorState } from "./NodeEditor";
import { createDraft, draftToConfig, formatCompactNumber, recordToDraft } from "./node-config";
import type { GatewayStatus, NodeRecord } from "./types";
import { formatBytes, StatusBadge } from "./ui";

type View = "overview" | "upstreams";
type NodeFilter = "all" | "accepting" | "attention" | "draining";

interface ToastState {
  tone: "success" | "error";
  message: string;
}

interface MenuState {
  nodeId: string;
  top: number;
  left: number;
}

function FleetSummary({ status }: { status: GatewayStatus | null }) {
  const values = [
    { icon: Server, value: status?.fleet.total_nodes ?? 0, label: "Total nodes" },
    { icon: Network, value: status?.fleet.accepting_nodes ?? 0, label: "Accepting" },
    { icon: CircleGauge, value: status?.fleet.active_requests ?? 0, label: "Active requests" },
    { icon: ChevronDown, value: status?.queue.requests ?? 0, label: "Queued" },
  ];
  return (
    <section className="summary-band" aria-label="Fleet summary">
      {values.map(({ icon: Icon, value, label }) => <div key={label}><Icon size={18} /><span><strong>{value}</strong>{label}</span></div>)}
    </section>
  );
}

function Overview({ status, nodes, onSelectNode }: { status: GatewayStatus | null; nodes: NodeRecord[]; onSelectNode: (node: NodeRecord) => void }) {
  const attention = nodes.filter((node) => !node.admission.accepting_assignments && node.admission.state !== "at_capacity");
  const utilization = status && status.fleet.total_concurrency > 0
    ? Math.min(100, (status.fleet.active_requests / status.fleet.total_concurrency) * 100)
    : 0;
  const queueUtilization = status && status.queue.max_requests > 0
    ? Math.min(100, (status.queue.requests / status.queue.max_requests) * 100)
    : 0;

  return <>
    <FleetSummary status={status} />
    <div className="overview-grid">
      <section className="panel overview-capacity">
        <div className="panel-heading"><div><span className="eyebrow">Current capacity</span><h2>Fleet load</h2></div><StatusBadge value={status?.status ?? "unavailable"} /></div>
        <div className="capacity-value"><strong>{status?.fleet.active_requests ?? 0}</strong><span>of {status?.fleet.total_concurrency ?? 0} local permits in use</span></div>
        <div className="large-meter" aria-label={`${Math.round(utilization)}% local capacity used`}><span style={{ width: `${utilization}%` }} /></div>
        <div className="capacity-meta">
          <span><strong>{status?.fleet.available_concurrency ?? 0}</strong> immediately available</span>
          <span><strong>{status?.fleet.routable_nodes ?? 0}</strong> routable nodes</span>
        </div>
      </section>

      <section className="panel queue-panel">
        <div className="panel-heading"><div><span className="eyebrow">Backpressure</span><h2>Request queue</h2></div><span className="panel-value">{status?.queue.requests ?? 0}</span></div>
        <div className="large-meter queue" aria-label={`${Math.round(queueUtilization)}% queue request capacity used`}><span style={{ width: `${queueUtilization}%` }} /></div>
        <div className="detail-grid compact-grid">
          <div className="detail-item"><span>Request limit</span><strong>{status?.queue.max_requests ?? 0}</strong></div>
          <div className="detail-item"><span>Buffered bodies</span><strong>{formatBytes(status?.queue.bytes ?? 0)}</strong></div>
          <div className="detail-item"><span>Byte limit</span><strong>{formatBytes(status?.queue.max_bytes ?? 0)}</strong></div>
          <div className="detail-item"><span>Prefix routing</span><strong>{status?.routing.prefix_enabled ? "Enabled" : "Disabled"}</strong></div>
        </div>
      </section>

      <section className="panel fleet-list-panel">
        <div className="panel-heading"><div><span className="eyebrow">Runtime registry</span><h2>Node capacity</h2></div><span className="panel-value small">{nodes.length}</span></div>
        {nodes.length === 0 ? <div className="compact-empty"><Server size={20} /><span>No nodes configured</span></div> : <div className="fleet-list">
          {nodes.slice(0, 8).map((node) => {
            const load = node.runtime.max_concurrency > 0 ? Math.min(100, node.runtime.active / node.runtime.max_concurrency * 100) : 0;
            return <button key={node.config.id} onClick={() => onSelectNode(node)}>
              <span className={`node-dot ${node.admission.accepting_assignments ? "positive" : "warning"}`} />
              <span className="fleet-node-name"><strong>{node.config.id}</strong><small>{node.admission.state.replaceAll("_", " ")}</small></span>
              <span className="mini-meter"><i style={{ width: `${load}%` }} /></span>
              <strong>{node.runtime.active}/{node.runtime.max_concurrency}</strong>
            </button>;
          })}
        </div>}
      </section>

      <section className="panel attention-panel">
        <div className="panel-heading"><div><span className="eyebrow">Admission gates</span><h2>Needs attention</h2></div><span className={`panel-value small ${attention.length ? "warning" : "positive"}`}>{attention.length}</span></div>
        {attention.length === 0 ? <div className="healthy-empty"><Check size={20} /><div><strong>No blocking conditions</strong><span>All routable nodes can accept assignments.</span></div></div> : <div className="attention-list">
          {attention.slice(0, 6).map((node) => <button key={node.config.id} onClick={() => onSelectNode(node)}>
            <AlertTriangle size={17} /><span><strong>{node.config.id}</strong><small>{node.admission.reason}</small></span><StatusBadge value={node.admission.state} />
          </button>)}
        </div>}
      </section>
    </div>
  </>;
}

function UpstreamTable({
  nodes,
  loading,
  query,
  filter,
  onQuery,
  onFilter,
  onSelect,
  onOpenMenu,
  onAdd,
}: {
  nodes: NodeRecord[];
  loading: boolean;
  query: string;
  filter: NodeFilter;
  onQuery: (value: string) => void;
  onFilter: (value: NodeFilter) => void;
  onSelect: (node: NodeRecord) => void;
  onOpenMenu: (node: NodeRecord, button: HTMLButtonElement) => void;
  onAdd: () => void;
}) {
  return <section className="table-section">
    <div className="table-toolbar">
      <div className="search-field"><Search size={16} /><input aria-label="Search nodes" placeholder="Search nodes, URLs or models" value={query} onChange={(event) => onQuery(event.target.value)} /></div>
      <label className="filter-field"><span className="sr-only">Filter nodes</span><select value={filter} onChange={(event) => onFilter(event.target.value as NodeFilter)}>
        <option value="all">All states</option>
        <option value="accepting">Accepting</option>
        <option value="attention">Needs attention</option>
        <option value="draining">Draining</option>
      </select></label>
      <span className="result-count">{nodes.length} result{nodes.length === 1 ? "" : "s"}</span>
    </div>
    {loading ? <div className="empty-state"><LoaderCircle className="spin" size={22} />Loading nodes</div> : nodes.length === 0 ? <div className="empty-state"><Server size={24} /><strong>{query || filter !== "all" ? "No matching nodes" : "No upstream nodes"}</strong>{!query && filter === "all" && <button className="primary-button" onClick={onAdd}><Plus size={16} />Add node</button>}</div> : <div className="table-scroll">
      <table>
        <thead><tr><th>Node</th><th>Admission</th><th>Provider</th><th>Demand</th><th>KV cache</th><th>Models</th><th aria-label="Actions" /></tr></thead>
        <tbody>{nodes.map((node) => {
          const demand = (node.runtime.upstream_running ?? node.runtime.active) + (node.runtime.upstream_waiting ?? 0);
          const demandLimit = Math.max(node.runtime.max_concurrency, demand, 1);
          return <tr key={node.config.id} tabIndex={0} onClick={() => onSelect(node)} onKeyDown={(event) => { if (event.key === "Enter" || event.key === " ") { event.preventDefault(); onSelect(node); } }}>
            <td data-label="Node"><div className="node-cell"><strong>{node.config.id}</strong><span>{node.config.base_url}</span></div></td>
            <td data-label="Admission"><div className="admission-cell"><StatusBadge value={node.admission.state} /><span>{node.admission.reason}</span></div></td>
            <td data-label="Provider"><div className="provider-cell"><strong>{node.config.provider.type === "vllm" ? "vLLM" : "OpenAI"}</strong><span>{node.runtime.provider_version ?? node.runtime.provider_state}</span>{node.runtime.provider_last_error && <AlertTriangle size={14} aria-label="Provider warning" />}</div></td>
            <td data-label="Demand"><div className="load-cell"><strong>{node.runtime.upstream_running ?? node.runtime.active} running / {node.runtime.upstream_waiting ?? 0} waiting</strong><div className="meter"><span style={{ width: `${Math.min(100, demand / demandLimit * 100)}%` }} /></div><span>{node.runtime.active} / {node.runtime.max_concurrency} local, {Math.round(node.runtime.latency_ewma_ms)} ms</span></div></td>
            <td data-label="KV cache"><div className="kv-cell"><strong>{node.runtime.kv_cache_usage === null ? "Unavailable" : `${Math.round(node.runtime.kv_cache_usage * 100)}%`}</strong><span>{formatCompactNumber(node.exact_kv_blocks)} blocks, {node.exact_kv_authoritative ? "exact" : "approximate"}</span></div></td>
            <td data-label="Models"><div className="model-list">{Object.keys(node.config.models).slice(0, 2).map((model) => <span key={model}>{model}</span>)}{Object.keys(node.config.models).length > 2 && <em>+{Object.keys(node.config.models).length - 2}</em>}</div></td>
            <td className="actions-cell" data-label="Actions"><button className="icon-button quiet" title="Node actions" aria-label={`Actions for ${node.config.id}`} aria-haspopup="menu" onClick={(event) => { event.stopPropagation(); onOpenMenu(node, event.currentTarget); }}><MoreHorizontal size={18} /></button></td>
          </tr>;
        })}</tbody>
      </table>
    </div>}
  </section>;
}

export default function App() {
  const [view, setView] = useState<View>("overview");
  const [mobileNav, setMobileNav] = useState(false);
  const [nodes, setNodes] = useState<NodeRecord[]>([]);
  const [status, setStatus] = useState<GatewayStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [connectionError, setConnectionError] = useState<string | null>(null);
  const [lastSync, setLastSync] = useState<number | null>(null);
  const [editor, setEditor] = useState<EditorState | null>(null);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [menu, setMenu] = useState<MenuState | null>(null);
  const [confirmDelete, setConfirmDelete] = useState<NodeRecord | null>(null);
  const [toast, setToast] = useState<ToastState | null>(null);
  const [query, setQuery] = useState("");
  const [filter, setFilter] = useState<NodeFilter>("all");
  const refreshSequence = useRef(0);
  const menuRef = useRef<HTMLDivElement>(null);
  const menuTriggerRef = useRef<HTMLButtonElement | null>(null);

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
    if (!menu) return;
    const close = () => setMenu(null);
    const focusFrame = window.requestAnimationFrame(() => menuRef.current?.querySelector<HTMLElement>("button")?.focus());
    const keydown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        event.preventDefault();
        close();
      }
    };
    window.addEventListener("resize", close);
    window.addEventListener("scroll", close);
    document.addEventListener("keydown", keydown);
    return () => {
      window.cancelAnimationFrame(focusFrame);
      window.removeEventListener("resize", close);
      window.removeEventListener("scroll", close);
      document.removeEventListener("keydown", keydown);
      menuTriggerRef.current?.focus({ preventScroll: true });
    };
  }, [menu]);

  useEffect(() => {
    if (!confirmDelete) return;
    const previous = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    window.requestAnimationFrame(() => document.querySelector<HTMLElement>("[data-delete-autofocus]")?.focus());
    const keydown = (event: KeyboardEvent) => { if (event.key === "Escape" && !busy) setConfirmDelete(null); };
    document.addEventListener("keydown", keydown);
    return () => { document.removeEventListener("keydown", keydown); previous?.focus(); };
  }, [busy, confirmDelete]);

  const selectedNode = nodes.find((node) => node.config.id === selectedNodeId) ?? null;
  const filteredNodes = useMemo(() => {
    const needle = query.trim().toLowerCase();
    return nodes.filter((node) => {
      const matchesText = !needle || [node.config.id, node.config.base_url, ...Object.keys(node.config.models), ...Object.values(node.config.models)].some((value) => value.toLowerCase().includes(needle));
      const matchesFilter = filter === "all"
        || (filter === "accepting" && node.admission.accepting_assignments)
        || (filter === "attention" && !node.admission.accepting_assignments)
        || (filter === "draining" && node.runtime.lifecycle === "draining");
      return matchesText && matchesFilter;
    }).sort((left, right) => left.config.id.localeCompare(right.config.id));
  }, [filter, nodes, query]);

  const save = async (state: EditorState) => {
    setBusy(true);
    try {
      const config = draftToConfig(state.draft);
      if (state.mode === "create") await api.createNode(config);
      else await api.updateNode(config, state.revision);
      setEditor(null);
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
    setMenu(null);
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
      await refresh(true);
    } catch (error) {
      setToast({ tone: "error", message: error instanceof Error ? error.message : "Delete failed" });
    } finally {
      setBusy(false);
    }
  };

  const changeView = (next: View) => {
    setView(next);
    setMobileNav(false);
  };

  const openMenu = (node: NodeRecord, button: HTMLButtonElement) => {
    const rect = button.getBoundingClientRect();
    const width = 168;
    menuTriggerRef.current = button;
    setMenu((current) => current?.nodeId === node.config.id ? null : {
      nodeId: node.config.id,
      top: rect.bottom + 6,
      left: Math.max(8, Math.min(window.innerWidth - width - 8, rect.right - width)),
    });
  };

  const menuNode = menu ? nodes.find((node) => node.config.id === menu.nodeId) ?? null : null;
  const gatewayState = connectionError ? "unavailable" : status?.status ?? "checking";

  return (
    <div className="app-shell" onMouseDown={() => menu && setMenu(null)}>
      <header className="topbar">
        <button className="mobile-menu-button" aria-label="Open navigation" aria-expanded={mobileNav} onClick={() => setMobileNav((value) => !value)}><Menu size={19} /></button>
        <div className="brand-mark">E</div>
        <div className="brand"><strong>Estuary</strong><span>Control plane</span></div>
        <div className={`topbar-status ${gatewayState}`}><span className="live-dot" /><strong>{connectionError ? "Connection lost" : status?.ready ? "Gateway ready" : "Gateway not ready"}</strong><span>{status ? `v${status.version}` : ""}</span></div>
      </header>
      <aside className={`sidebar ${mobileNav ? "mobile-open" : ""}`}>
        <nav aria-label="Control plane navigation">
          <button className={view === "overview" ? "active" : ""} onClick={() => changeView("overview")}><LayoutDashboard size={17} />Overview</button>
          <button className={view === "upstreams" ? "active" : ""} onClick={() => changeView("upstreams")}><Server size={17} />Upstreams</button>
          <a href="/metrics" target="_blank" rel="noreferrer"><BarChart3 size={17} />Metrics<span className="external-label">Raw</span></a>
        </nav>
        <div className="sidebar-foot"><Database size={15} /><span>SQLite</span></div>
      </aside>
      {mobileNav && <button className="mobile-nav-scrim" aria-label="Close navigation" onClick={() => setMobileNav(false)} />}

      <main className="content">
        <div className="page-heading">
          <div><span className="eyebrow">{view === "overview" ? "Operations" : "Runtime registry"}</span><h1>{view === "overview" ? "Fleet overview" : "Upstream nodes"}</h1>{lastSync && <span className="last-sync">Updated {new Intl.DateTimeFormat("en", { hour: "2-digit", minute: "2-digit", second: "2-digit" }).format(lastSync)}</span>}</div>
          <div className="heading-actions">
            <button className="icon-button" title="Refresh" aria-label="Refresh" disabled={refreshing} onClick={() => void refresh()}><RefreshCw className={refreshing ? "spin" : ""} size={17} /></button>
            <button className="primary-button" onClick={() => setEditor({ mode: "create", draft: createDraft(), revision: null })}><Plus size={16} />Add node</button>
          </div>
        </div>

        {connectionError && <div className="connection-banner" role="alert"><AlertTriangle size={18} /><div><strong>Control plane data is unavailable</strong><span>{connectionError}. Previously loaded values may be stale.</span></div><button className="secondary-button" onClick={() => void refresh()}>Retry</button></div>}

        {view === "overview"
          ? <Overview status={status} nodes={nodes} onSelectNode={(node) => setSelectedNodeId(node.config.id)} />
          : <><FleetSummary status={status} /><UpstreamTable nodes={filteredNodes} loading={loading} query={query} filter={filter} onQuery={setQuery} onFilter={setFilter} onSelect={(node) => setSelectedNodeId(node.config.id)} onOpenMenu={openMenu} onAdd={() => setEditor({ mode: "create", draft: createDraft(), revision: null })} /></>}
      </main>

      {menu && menuNode && createPortal(<div ref={menuRef} className="action-menu fixed" role="menu" aria-label={`Actions for ${menuNode.config.id}`} style={{ top: menu.top, left: menu.left }} onMouseDown={(event) => event.stopPropagation()}>
        <button role="menuitem" onClick={() => { setMenu(null); setEditor({ mode: "edit", draft: recordToDraft(menuNode), revision: menuNode.revision }); }}><Edit3 size={15} />Edit</button>
        <button role="menuitem" onClick={() => void toggleDrain(menuNode)}>{menuNode.runtime.lifecycle === "serving" ? <ChevronDown size={15} /> : <Activity size={15} />}{menuNode.runtime.lifecycle === "serving" ? "Drain" : "Resume"}</button>
        <button role="menuitem" className="danger" onClick={() => { setMenu(null); setConfirmDelete(menuNode); }}><Trash2 size={15} />Delete</button>
      </div>, document.body)}

      {selectedNode && <NodeDetails node={selectedNode} busy={busy} onClose={() => setSelectedNodeId(null)} onEdit={() => { setSelectedNodeId(null); setEditor({ mode: "edit", draft: recordToDraft(selectedNode), revision: selectedNode.revision }); }} onToggleDrain={() => void toggleDrain(selectedNode)} onDelete={() => { setSelectedNodeId(null); setConfirmDelete(selectedNode); }} />}
      {editor && <NodeEditor state={editor} busy={busy} onClose={() => !busy && setEditor(null)} onSave={save} />}

      {confirmDelete && <div className="modal-layer" role="dialog" aria-modal="true" aria-label={`Delete ${confirmDelete.config.id}`}>
        <div className="confirm-modal">
          <div className="danger-icon"><Trash2 size={20} /></div>
          <h2>Delete {confirmDelete.config.id}?</h2>
          <p>The node will drain first. Active requests must finish before its persisted configuration is removed.</p>
          <div className="modal-actions"><button data-delete-autofocus className="secondary-button" disabled={busy} onClick={() => setConfirmDelete(null)}>Cancel</button><button className="danger-button" disabled={busy} onClick={() => void remove()}>{busy ? <LoaderCircle className="spin" size={16} /> : <Trash2 size={16} />}Delete node</button></div>
        </div>
      </div>}

      {toast && <div className={`toast ${toast.tone}`} role={toast.tone === "error" ? "alert" : "status"}><span>{toast.tone === "success" ? <Check size={16} /> : <AlertTriangle size={16} />}</span><span>{toast.message}</span><button className="icon-button quiet" aria-label="Dismiss" onClick={() => setToast(null)}><X size={15} /></button></div>}
    </div>
  );
}
