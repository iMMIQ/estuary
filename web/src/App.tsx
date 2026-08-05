import {
  Activity,
  AlertTriangle,
  Check,
  ChevronDown,
  CircleGauge,
  Database,
  Edit3,
  LoaderCircle,
  MoreHorizontal,
  Network,
  Plus,
  RefreshCw,
  Server,
  Trash2,
  X,
} from "lucide-react";
import { useCallback, useEffect, useMemo, useState } from "react";
import * as api from "./api";
import { createDraft, draftToConfig, formatCompactNumber, recordToDraft } from "./node-config";
import type { KvEventsConfig, NodeDraft, NodeRecord, Pair, ProviderKind } from "./types";

type EditorState =
  | { mode: "create"; draft: NodeDraft; revision: null }
  | { mode: "edit"; draft: NodeDraft; revision: number };

interface ToastState {
  tone: "success" | "error";
  message: string;
}

function statusTone(value: string): string {
  if (["healthy", "ready", "serving", "closed"].includes(value)) return "positive";
  if (["starting", "checking", "degraded", "half_open", "draining"].includes(value)) return "warning";
  return "negative";
}

function StatusBadge({ value }: { value: string }) {
  return <span className={`status-badge ${statusTone(value)}`}>{value.replaceAll("_", " ")}</span>;
}

function PairEditor({
  rows,
  onChange,
  keyPlaceholder,
  valuePlaceholder,
}: {
  rows: Pair[];
  onChange: (rows: Pair[]) => void;
  keyPlaceholder: string;
  valuePlaceholder: string;
}) {
  const update = (index: number, field: keyof Pair, value: string) => {
    onChange(rows.map((row, rowIndex) => (rowIndex === index ? { ...row, [field]: value } : row)));
  };
  return (
    <div className="pair-list">
      {rows.map((row, index) => (
        <div className="pair-row" key={index}>
          <input
            aria-label={keyPlaceholder}
            placeholder={keyPlaceholder}
            value={row.key}
            onChange={(event) => update(index, "key", event.target.value)}
          />
          <input
            aria-label={valuePlaceholder}
            placeholder={valuePlaceholder}
            value={row.value}
            onChange={(event) => update(index, "value", event.target.value)}
          />
          <button
            className="icon-button quiet"
            type="button"
            title="Remove row"
            aria-label="Remove row"
            onClick={() => onChange(rows.length === 1 ? [{ key: "", value: "" }] : rows.filter((_, i) => i !== index))}
          >
            <X size={16} />
          </button>
        </div>
      ))}
      <button className="text-button" type="button" onClick={() => onChange([...rows, { key: "", value: "" }])}>
        <Plus size={15} /> Add row
      </button>
    </div>
  );
}

function NumberField({
  label,
  value,
  min,
  step,
  onChange,
}: {
  label: string;
  value: number;
  min: number;
  step?: number;
  onChange: (value: number) => void;
}) {
  return (
    <label className="field">
      <span>{label}</span>
      <input
        type="number"
        value={value}
        min={min}
        step={step ?? 1}
        onChange={(event) => onChange(Number(event.target.value))}
      />
    </label>
  );
}

function NodeEditor({
  state,
  busy,
  onClose,
  onSave,
}: {
  state: EditorState;
  busy: boolean;
  onClose: () => void;
  onSave: (state: EditorState) => void;
}) {
  const [draft, setDraft] = useState(state.draft);
  const setProvider = (kind: ProviderKind) => {
    setDraft((current) => ({
      ...current,
      provider: { ...current.provider, type: kind, kv_events: kind === "openai" ? null : current.provider.kv_events },
    }));
  };
  const toggleKv = () => {
    setDraft((current) => ({
      ...current,
      provider: {
        ...current.provider,
        kv_events: current.provider.kv_events
          ? null
          : {
              endpoint: "tcp://127.0.0.1:5557",
              replay_endpoint: "tcp://127.0.0.1:5558",
              topic: "kv-events",
              reconnect_ms: 1000,
              max_blocks: 1_000_000,
              max_event_bytes: 16_777_216,
            },
      },
    }));
  };
  const updateKv = <K extends keyof KvEventsConfig>(key: K, value: KvEventsConfig[K]) => {
    setDraft((current) => ({
      ...current,
      provider: {
        ...current.provider,
        kv_events: current.provider.kv_events ? { ...current.provider.kv_events, [key]: value } : null,
      },
    }));
  };

  return (
    <div className="drawer-layer" role="dialog" aria-modal="true" aria-label={state.mode === "create" ? "Add node" : "Edit node"}>
      <button className="drawer-scrim" type="button" aria-label="Close editor" onClick={onClose} />
      <aside className="drawer">
        <header className="drawer-header">
          <div>
            <span className="eyebrow">Node configuration</span>
            <h2>{state.mode === "create" ? "Add upstream" : draft.id}</h2>
          </div>
          <button className="icon-button" type="button" title="Close" aria-label="Close" onClick={onClose}>
            <X size={18} />
          </button>
        </header>
        <form
          className="drawer-body"
          onSubmit={(event) => {
            event.preventDefault();
            onSave({ ...state, draft } as EditorState);
          }}
        >
          <section className="form-section">
            <h3>Identity</h3>
            <div className="field-grid">
              <label className="field">
                <span>Node ID</span>
                <input value={draft.id} disabled={state.mode === "edit"} required onChange={(event) => setDraft({ ...draft, id: event.target.value })} />
              </label>
              <label className="field wide">
                <span>Base URL</span>
                <input value={draft.base_url} required onChange={(event) => setDraft({ ...draft, base_url: event.target.value })} />
              </label>
              <label className="field wide">
                <span>Health path</span>
                <input value={draft.health_path} required onChange={(event) => setDraft({ ...draft, health_path: event.target.value })} />
              </label>
            </div>
          </section>

          <section className="form-section">
            <h3>Provider</h3>
            <div className="segmented" role="group" aria-label="Provider type">
              <button type="button" className={draft.provider.type === "vllm" ? "selected" : ""} onClick={() => setProvider("vllm")}>vLLM 0.25+</button>
              <button type="button" className={draft.provider.type === "openai" ? "selected" : ""} onClick={() => setProvider("openai")}>OpenAI compatible</button>
            </div>
            {draft.provider.type === "vllm" && (
              <div className="field-grid top-gap">
                <label className="field"><span>Version path</span><input value={draft.provider.version_path} onChange={(event) => setDraft({ ...draft, provider: { ...draft.provider, version_path: event.target.value } })} /></label>
                <label className="field"><span>Metrics path</span><input value={draft.provider.metrics_path} onChange={(event) => setDraft({ ...draft, provider: { ...draft.provider, metrics_path: event.target.value } })} /></label>
                <label className="field"><span>Tokenize path</span><input value={draft.provider.tokenize_path} onChange={(event) => setDraft({ ...draft, provider: { ...draft.provider, tokenize_path: event.target.value } })} /></label>
                <NumberField label="Monitor interval (ms)" value={draft.provider.monitor_interval_ms} min={1} onChange={(value) => setDraft({ ...draft, provider: { ...draft.provider, monitor_interval_ms: value } })} />
                <NumberField label="Request timeout (ms)" value={draft.provider.request_timeout_ms} min={1} onChange={(value) => setDraft({ ...draft, provider: { ...draft.provider, request_timeout_ms: value } })} />
                <NumberField label="Telemetry stale (ms)" value={draft.provider.telemetry_stale_ms} min={1} onChange={(value) => setDraft({ ...draft, provider: { ...draft.provider, telemetry_stale_ms: value } })} />
                <NumberField label="Waiting watermark" value={draft.provider.waiting_threshold} min={1} onChange={(value) => setDraft({ ...draft, provider: { ...draft.provider, waiting_threshold: value } })} />
              </div>
            )}
          </section>

          <section className="form-section">
            <h3>Routing</h3>
            <div className="field-grid">
              <NumberField label="Max concurrency" value={draft.max_concurrency} min={1} onChange={(value) => setDraft({ ...draft, max_concurrency: value })} />
              <NumberField label="Weight" value={draft.weight} min={0.01} step={0.05} onChange={(value) => setDraft({ ...draft, weight: value })} />
            </div>
            <div className="subsection-label">Model mappings</div>
            <PairEditor rows={draft.models} keyPlaceholder="Public model" valuePlaceholder="Upstream model" onChange={(models) => setDraft({ ...draft, models })} />
          </section>

          <section className="form-section">
            <h3>Credentials</h3>
            <label className="field"><span>Bearer key environment variable</span><input value={draft.api_key_env ?? ""} onChange={(event) => setDraft({ ...draft, api_key_env: event.target.value || null })} /></label>
            <div className="subsection-label">Environment-backed headers</div>
            <PairEditor rows={draft.headers_from_env} keyPlaceholder="Header name" valuePlaceholder="Environment variable" onChange={(headers_from_env) => setDraft({ ...draft, headers_from_env })} />
          </section>

          {draft.provider.type === "vllm" && (
            <section className="form-section">
              <div className="section-heading-row">
                <h3>KV events</h3>
                <label className="switch"><input type="checkbox" checked={draft.provider.kv_events !== null} onChange={toggleKv} /><span /></label>
              </div>
              {draft.provider.kv_events && (
                <div className="field-grid">
                  <label className="field wide"><span>Publisher endpoint</span><input value={draft.provider.kv_events.endpoint} onChange={(event) => updateKv("endpoint", event.target.value)} /></label>
                  <label className="field wide"><span>Replay endpoint</span><input value={draft.provider.kv_events.replay_endpoint ?? ""} onChange={(event) => updateKv("replay_endpoint", event.target.value || null)} /></label>
                  <label className="field"><span>Topic</span><input value={draft.provider.kv_events.topic} onChange={(event) => updateKv("topic", event.target.value)} /></label>
                  <NumberField label="Reconnect (ms)" value={draft.provider.kv_events.reconnect_ms} min={1} onChange={(value) => updateKv("reconnect_ms", value)} />
                  <NumberField label="Max blocks" value={draft.provider.kv_events.max_blocks} min={1} onChange={(value) => updateKv("max_blocks", value)} />
                  <NumberField label="Max event bytes" value={draft.provider.kv_events.max_event_bytes} min={1} onChange={(value) => updateKv("max_event_bytes", value)} />
                </div>
              )}
            </section>
          )}

          <section className="form-section compact">
            <div className="section-heading-row">
              <div><h3>Start draining</h3><span className="field-meta">No new assignments</span></div>
              <label className="switch"><input type="checkbox" checked={draft.draining} onChange={(event) => setDraft({ ...draft, draining: event.target.checked })} /><span /></label>
            </div>
          </section>

          <footer className="drawer-footer">
            <button type="button" className="secondary-button" onClick={onClose}>Cancel</button>
            <button type="submit" className="primary-button" disabled={busy}>
              {busy ? <LoaderCircle className="spin" size={16} /> : <Check size={16} />}
              {state.mode === "create" ? "Add node" : "Save changes"}
            </button>
          </footer>
        </form>
      </aside>
    </div>
  );
}

export default function App() {
  const [nodes, setNodes] = useState<NodeRecord[]>([]);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [editor, setEditor] = useState<EditorState | null>(null);
  const [busy, setBusy] = useState(false);
  const [menu, setMenu] = useState<string | null>(null);
  const [confirmDelete, setConfirmDelete] = useState<NodeRecord | null>(null);
  const [toast, setToast] = useState<ToastState | null>(null);

  const refresh = useCallback(async (quiet = false) => {
    if (!quiet) setRefreshing(true);
    try {
      setNodes(await api.listNodes());
    } catch (error) {
      setToast({ tone: "error", message: error instanceof Error ? error.message : "Could not load nodes" });
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, []);

  useEffect(() => {
    void refresh(true);
    const interval = window.setInterval(() => void refresh(true), 5000);
    return () => window.clearInterval(interval);
  }, [refresh]);

  useEffect(() => {
    if (!toast) return;
    const timeout = window.setTimeout(() => setToast(null), 4500);
    return () => window.clearTimeout(timeout);
  }, [toast]);

  const summary = useMemo(() => ({
    total: nodes.length,
    accepting: nodes.filter((node) => node.runtime.lifecycle === "serving" && ["healthy", "degraded"].includes(node.runtime.health) && node.runtime.circuit !== "open" && ["generic", "ready"].includes(node.runtime.provider_state)).length,
    active: nodes.reduce((total, node) => total + node.runtime.active, 0),
    kvBlocks: nodes.reduce((total, node) => total + node.exact_kv_blocks, 0),
  }), [nodes]);

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
    } finally {
      setBusy(false);
    }
  };

  const toggleDrain = async (node: NodeRecord) => {
    setMenu(null);
    try {
      await api.setDraining(node.config.id, node.runtime.lifecycle === "serving");
      setToast({ tone: "success", message: node.runtime.lifecycle === "serving" ? "Node draining" : "Node resumed" });
      await refresh(true);
    } catch (error) {
      setToast({ tone: "error", message: error instanceof Error ? error.message : "Lifecycle update failed" });
    }
  };

  const remove = async () => {
    if (!confirmDelete) return;
    setBusy(true);
    try {
      await api.deleteNode(confirmDelete.config.id, confirmDelete.revision);
      setToast({ tone: "success", message: "Node deleted" });
      setConfirmDelete(null);
      await refresh(true);
    } catch (error) {
      setToast({ tone: "error", message: error instanceof Error ? error.message : "Delete failed" });
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="app-shell" onClick={() => menu && setMenu(null)}>
      <header className="topbar">
        <div className="brand-mark">E</div>
        <div className="brand"><strong>Estuary</strong><span>Control plane</span></div>
        <div className="topbar-status"><span className="live-dot" /> Gateway online</div>
      </header>
      <aside className="sidebar">
        <nav>
          <a className="active" href="/admin/"><Server size={17} />Upstreams</a>
          <a href="/metrics"><Activity size={17} />Metrics</a>
        </nav>
        <div className="sidebar-foot"><Database size={15} /><span>SQLite</span></div>
      </aside>
      <main className="content">
        <div className="page-heading">
          <div><span className="eyebrow">Runtime registry</span><h1>Upstream nodes</h1></div>
          <div className="heading-actions">
            <button className="icon-button" title="Refresh" aria-label="Refresh" disabled={refreshing} onClick={(event) => { event.stopPropagation(); void refresh(); }}><RefreshCw className={refreshing ? "spin" : ""} size={17} /></button>
            <button className="primary-button" onClick={() => setEditor({ mode: "create", draft: createDraft(), revision: null })}><Plus size={16} />Add node</button>
          </div>
        </div>

        <section className="summary-band" aria-label="Fleet summary">
          <div><Server size={18} /><span><strong>{summary.total}</strong>Total nodes</span></div>
          <div><Network size={18} /><span><strong>{summary.accepting}</strong>Accepting</span></div>
          <div><CircleGauge size={18} /><span><strong>{summary.active}</strong>Active requests</span></div>
          <div><Database size={18} /><span><strong>{formatCompactNumber(summary.kvBlocks)}</strong>KV blocks</span></div>
        </section>

        <section className="table-section">
          <div className="table-title"><h2>Nodes</h2><span>{nodes.length}</span></div>
          {loading ? (
            <div className="empty-state"><LoaderCircle className="spin" size={22} />Loading nodes</div>
          ) : nodes.length === 0 ? (
            <div className="empty-state"><Server size={24} /><strong>No upstream nodes</strong><button className="primary-button" onClick={() => setEditor({ mode: "create", draft: createDraft(), revision: null })}><Plus size={16} />Add node</button></div>
          ) : (
            <div className="table-scroll">
              <table>
                <thead><tr><th>Node</th><th>State</th><th>Provider</th><th>Load</th><th>KV cache</th><th>Models</th><th aria-label="Actions" /></tr></thead>
                <tbody>
                  {nodes.map((node) => (
                    <tr key={node.config.id}>
                      <td data-label="Node"><div className="node-cell"><strong>{node.config.id}</strong><span>{node.config.base_url}</span></div></td>
                      <td data-label="State"><div className="status-stack"><StatusBadge value={node.runtime.health} /><StatusBadge value={node.runtime.lifecycle} />{node.runtime.circuit !== "closed" && <StatusBadge value={node.runtime.circuit} />}</div></td>
                      <td data-label="Provider"><div className="provider-cell"><strong>{node.config.provider.type === "vllm" ? "vLLM" : "OpenAI"}</strong><span>{node.runtime.provider_version ?? node.runtime.provider_state}</span>{node.runtime.provider_last_error && <AlertTriangle size={14} />}</div></td>
                      <td data-label="Load"><div className="load-cell"><strong>{node.runtime.active} / {node.runtime.max_concurrency}</strong><div className="meter"><span style={{ width: `${Math.min(100, (node.runtime.active / node.runtime.max_concurrency) * 100)}%` }} /></div><span>{Math.round(node.runtime.latency_ewma_ms)} ms</span></div></td>
                      <td data-label="KV cache"><div className="kv-cell"><strong>{formatCompactNumber(node.exact_kv_blocks)}</strong><span>{node.exact_kv_authoritative ? "authoritative" : "approximate"}</span></div></td>
                      <td data-label="Models"><div className="model-list">{Object.keys(node.config.models).slice(0, 2).map((model) => <span key={model}>{model}</span>)}{Object.keys(node.config.models).length > 2 && <em>+{Object.keys(node.config.models).length - 2}</em>}</div></td>
                      <td className="actions-cell" data-label="Actions">
                        <button className="icon-button quiet" title="Node actions" aria-label={`Actions for ${node.config.id}`} onClick={(event) => { event.stopPropagation(); setMenu(menu === node.config.id ? null : node.config.id); }}><MoreHorizontal size={18} /></button>
                        {menu === node.config.id && <div className="action-menu" onClick={(event) => event.stopPropagation()}>
                          <button onClick={() => { setMenu(null); setEditor({ mode: "edit", draft: recordToDraft(node), revision: node.revision }); }}><Edit3 size={15} />Edit</button>
                          <button onClick={() => void toggleDrain(node)}>{node.runtime.lifecycle === "serving" ? <ChevronDown size={15} /> : <Activity size={15} />}{node.runtime.lifecycle === "serving" ? "Drain" : "Resume"}</button>
                          <button className="danger" onClick={() => { setMenu(null); setConfirmDelete(node); }}><Trash2 size={15} />Delete</button>
                        </div>}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>
      </main>

      {editor && <NodeEditor state={editor} busy={busy} onClose={() => !busy && setEditor(null)} onSave={(value) => void save(value)} />}
      {confirmDelete && <div className="modal-layer" role="dialog" aria-modal="true" aria-label="Delete node"><div className="confirm-modal"><div className="danger-icon"><Trash2 size={20} /></div><h2>Delete {confirmDelete.config.id}?</h2><p>Active requests must finish before the node is removed.</p><div className="modal-actions"><button className="secondary-button" disabled={busy} onClick={() => setConfirmDelete(null)}>Cancel</button><button className="danger-button" disabled={busy} onClick={() => void remove()}>{busy ? <LoaderCircle className="spin" size={16} /> : <Trash2 size={16} />}Delete node</button></div></div></div>}
      {toast && <div className={`toast ${toast.tone}`}><span>{toast.tone === "success" ? <Check size={16} /> : <AlertTriangle size={16} />}</span>{toast.message}<button className="icon-button quiet" aria-label="Dismiss" onClick={() => setToast(null)}><X size={15} /></button></div>}
    </div>
  );
}
