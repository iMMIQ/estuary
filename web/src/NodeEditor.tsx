import {
  Check,
  ChevronDown,
  FlaskConical,
  LoaderCircle,
  Plus,
  X,
} from "lucide-react";
import { type ReactNode, useCallback, useMemo, useState } from "react";
import * as api from "./api";
import { Drawer } from "./dialog";
import { draftToConfig, validateDraft } from "./node-config";
import type { DraftErrors } from "./node-config";
import type { KvEventsConfig, NodeDraft, Pair, PreflightResponse, ProviderKind } from "./types";

export type EditorState =
  | { mode: "create"; draft: NodeDraft; revision: null }
  | { mode: "edit"; draft: NodeDraft; revision: number };

function Field({
  label,
  name,
  error,
  wide = false,
  children,
}: {
  label: string;
  name: string;
  error?: string;
  wide?: boolean;
  children: ReactNode;
}) {
  return (
    <label className={`field ${wide ? "wide" : ""}`} data-field={name}>
      <span>{label}</span>
      {children}
      {error && <small id={`${name}-error`} className="field-error" role="alert">{error}</small>}
    </label>
  );
}

function NumberField({
  label,
  name,
  value,
  min,
  step,
  error,
  onChange,
}: {
  label: string;
  name: string;
  value: number;
  min: number;
  step?: number;
  error?: string;
  onChange: (value: number) => void;
}) {
  return (
    <Field label={label} name={name} error={error}>
      <input
        type="number"
        value={value}
        min={min}
        step={step ?? 1}
        aria-invalid={Boolean(error)}
        aria-describedby={error ? `${name}-error` : undefined}
        onChange={(event) => onChange(Number(event.target.value))}
      />
    </Field>
  );
}

function PairEditor({
  rows,
  error,
  onChange,
  keyPlaceholder,
  valuePlaceholder,
}: {
  rows: Pair[];
  error?: string;
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
            aria-label={`${keyPlaceholder} ${index + 1}`}
            placeholder={keyPlaceholder}
            value={row.key}
            aria-invalid={Boolean(error)}
            onChange={(event) => update(index, "key", event.target.value)}
          />
          <input
            aria-label={`${valuePlaceholder} ${index + 1}`}
            placeholder={valuePlaceholder}
            value={row.value}
            aria-invalid={Boolean(error)}
            onChange={(event) => update(index, "value", event.target.value)}
          />
          <button
            className="icon-button quiet"
            type="button"
            title="Remove row"
            aria-label={`Remove row ${index + 1}`}
            onClick={() => onChange(rows.length === 1 ? [{ key: "", value: "" }] : rows.filter((_, rowIndex) => rowIndex !== index))}
          >
            <X size={16} />
          </button>
        </div>
      ))}
      {error && <small className="field-error" role="alert">{error}</small>}
      <button className="text-button" type="button" onClick={() => onChange([...rows, { key: "", value: "" }])}>
        <Plus size={15} /> Add row
      </button>
    </div>
  );
}

function firstError(errors: DraftErrors): string | null {
  return Object.keys(errors)[0] ?? null;
}

export function NodeEditor({
  state,
  busy,
  onClose,
  onSave,
}: {
  state: EditorState;
  busy: boolean;
  onClose: () => void;
  onSave: (state: EditorState) => Promise<void>;
}) {
  const [draft, setDraft] = useState(state.draft);
  const [errors, setErrors] = useState<DraftErrors>({});
  const [advanced, setAdvanced] = useState(state.mode === "edit");
  const [checking, setChecking] = useState(false);
  const [preflight, setPreflight] = useState<PreflightResponse | null>(null);
  const [preflightError, setPreflightError] = useState<string | null>(null);
  const initialDraft = useMemo(() => JSON.stringify(state.draft), [state.draft]);
  const dirty = JSON.stringify(draft) !== initialDraft;

  const update = useCallback((change: (current: NodeDraft) => NodeDraft) => {
    setDraft((current) => change(current));
    setPreflight(null);
    setPreflightError(null);
  }, []);

  const requestClose = useCallback(() => {
    if (!dirty || window.confirm("Discard unsaved changes?")) onClose();
  }, [dirty, onClose]);

  const focusError = (validation: DraftErrors) => {
    const field = firstError(validation);
    if (!field) return;
    window.requestAnimationFrame(() => {
      document.querySelector<HTMLElement>(`[data-field="${field}"] input`)?.focus();
    });
  };

  const validate = () => {
    const validation = validateDraft(draft);
    setErrors(validation);
    if (Object.keys(validation).length > 0) {
      focusError(validation);
      return false;
    }
    return true;
  };

  const testConnection = async () => {
    if (!validate()) return;
    setChecking(true);
    setPreflight(null);
    setPreflightError(null);
    try {
      setPreflight(await api.preflightNode(draftToConfig(draft)));
    } catch (error) {
      setPreflightError(error instanceof Error ? error.message : "Connection test failed");
    } finally {
      setChecking(false);
    }
  };

  const setProvider = (kind: ProviderKind) => update((current) => ({
    ...current,
    provider: {
      ...current.provider,
      type: kind,
      kv_events: kind === "openai" ? null : current.provider.kv_events,
    },
  }));

  const toggleKv = () => update((current) => ({
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

  const updateKv = <K extends keyof KvEventsConfig>(key: K, value: KvEventsConfig[K]) => update((current) => ({
    ...current,
    provider: {
      ...current.provider,
      kv_events: current.provider.kv_events ? { ...current.provider.kv_events, [key]: value } : null,
    },
  }));

  return (
    <Drawer
      title={state.mode === "create" ? "Add upstream" : draft.id}
      eyebrow="Node configuration"
      ariaLabel={state.mode === "create" ? "Add node" : `Edit ${draft.id}`}
      busy={busy || checking}
      onClose={requestClose}
    >
      <form
        className="editor-form"
        onSubmit={(event) => {
          event.preventDefault();
          if (validate()) void onSave({ ...state, draft } as EditorState);
        }}
      >
        <div className="drawer-body">
        <section className="form-section">
          <div className="form-section-heading">
            <div><h3>Connection</h3><p>Identity and OpenAI-compatible endpoint.</p></div>
            <span className="required-note">Required</span>
          </div>
          <div className="field-grid">
            <Field label="Node ID" name="id" error={errors.id}>
              <input
                data-autofocus
                value={draft.id}
                disabled={state.mode === "edit"}
                required
                aria-invalid={Boolean(errors.id)}
                aria-describedby={errors.id ? "id-error" : undefined}
                onChange={(event) => update((current) => ({ ...current, id: event.target.value }))}
              />
            </Field>
            <Field label="Base URL" name="base_url" error={errors.base_url} wide>
              <input
                value={draft.base_url}
                required
                spellCheck={false}
                aria-invalid={Boolean(errors.base_url)}
                aria-describedby={errors.base_url ? "base_url-error" : undefined}
                onChange={(event) => update((current) => ({ ...current, base_url: event.target.value }))}
              />
            </Field>
          </div>
          <div className="subsection-label">Provider</div>
          <div className="segmented" role="group" aria-label="Provider type">
            <button type="button" aria-pressed={draft.provider.type === "vllm"} className={draft.provider.type === "vllm" ? "selected" : ""} onClick={() => setProvider("vllm")}>vLLM 0.25+</button>
            <button type="button" aria-pressed={draft.provider.type === "openai"} className={draft.provider.type === "openai" ? "selected" : ""} onClick={() => setProvider("openai")}>OpenAI compatible</button>
          </div>
        </section>

        <section className="form-section">
          <div className="form-section-heading"><div><h3>Routing</h3><p>Capacity and public model exposure.</p></div></div>
          <div className="field-grid">
            <NumberField label="Max concurrency" name="max_concurrency" value={draft.max_concurrency} min={1} error={errors.max_concurrency} onChange={(value) => update((current) => ({ ...current, max_concurrency: value }))} />
            <NumberField label="Weight" name="weight" value={draft.weight} min={0.01} step={0.05} error={errors.weight} onChange={(value) => update((current) => ({ ...current, weight: value }))} />
          </div>
          <div className="subsection-label" data-field="models">Model mappings</div>
          <PairEditor rows={draft.models} error={errors.models} keyPlaceholder="Public model" valuePlaceholder="Upstream model" onChange={(models) => update((current) => ({ ...current, models }))} />
        </section>

        <section className="advanced-section">
          <button className="advanced-toggle" type="button" aria-expanded={advanced} onClick={() => setAdvanced((value) => !value)}>
            <span><strong>Advanced settings</strong><small>Health, telemetry, credentials and KV events</small></span>
            <ChevronDown className={advanced ? "rotated" : ""} size={18} />
          </button>
          {advanced && (
            <div className="advanced-content">
              <section className="form-section nested">
                <h3>Health and telemetry</h3>
                <div className="field-grid">
                  <Field label="Health path" name="health_path" error={errors.health_path} wide>
                    <input value={draft.health_path} aria-invalid={Boolean(errors.health_path)} onChange={(event) => update((current) => ({ ...current, health_path: event.target.value }))} />
                  </Field>
                  {draft.provider.type === "vllm" && <>
                    <Field label="Version path" name="version_path" error={errors.version_path}><input value={draft.provider.version_path} onChange={(event) => update((current) => ({ ...current, provider: { ...current.provider, version_path: event.target.value } }))} /></Field>
                    <Field label="Metrics path" name="metrics_path" error={errors.metrics_path}><input value={draft.provider.metrics_path} onChange={(event) => update((current) => ({ ...current, provider: { ...current.provider, metrics_path: event.target.value } }))} /></Field>
                    <Field label="Tokenize path" name="tokenize_path" error={errors.tokenize_path}><input value={draft.provider.tokenize_path} onChange={(event) => update((current) => ({ ...current, provider: { ...current.provider, tokenize_path: event.target.value } }))} /></Field>
                    <NumberField label="Monitor interval (ms)" name="monitor_interval_ms" value={draft.provider.monitor_interval_ms} min={1} error={errors.monitor_interval_ms} onChange={(value) => update((current) => ({ ...current, provider: { ...current.provider, monitor_interval_ms: value } }))} />
                    <NumberField label="Request timeout (ms)" name="request_timeout_ms" value={draft.provider.request_timeout_ms} min={1} error={errors.request_timeout_ms} onChange={(value) => update((current) => ({ ...current, provider: { ...current.provider, request_timeout_ms: value } }))} />
                    <NumberField label="Telemetry stale (ms)" name="telemetry_stale_ms" value={draft.provider.telemetry_stale_ms} min={1} error={errors.telemetry_stale_ms} onChange={(value) => update((current) => ({ ...current, provider: { ...current.provider, telemetry_stale_ms: value } }))} />
                    <NumberField label="Waiting watermark" name="waiting_threshold" value={draft.provider.waiting_threshold} min={1} error={errors.waiting_threshold} onChange={(value) => update((current) => ({ ...current, provider: { ...current.provider, waiting_threshold: value } }))} />
                    <NumberField label="Tokenize cache entries" name="tokenize_cache_entries" value={draft.provider.tokenize_cache_entries} min={1} onChange={(value) => update((current) => ({ ...current, provider: { ...current.provider, tokenize_cache_entries: value } }))} />
                  </>}
                </div>
              </section>

              <section className="form-section nested">
                <h3>Credentials</h3>
                <Field label="Bearer key environment variable" name="api_key_env" wide>
                  <input value={draft.api_key_env ?? ""} autoComplete="off" onChange={(event) => update((current) => ({ ...current, api_key_env: event.target.value || null }))} />
                </Field>
                <div className="subsection-label" data-field="headers_from_env">Environment-backed headers</div>
                <PairEditor rows={draft.headers_from_env} error={errors.headers_from_env} keyPlaceholder="Header name" valuePlaceholder="Environment variable" onChange={(headers_from_env) => update((current) => ({ ...current, headers_from_env }))} />
              </section>

              {draft.provider.type === "vllm" && (
                <section className="form-section nested">
                  <div className="section-heading-row">
                    <div><h3>KV events</h3><span className="field-meta">Exact prefix-cache synchronization</span></div>
                    <label className="switch"><input aria-label="Enable KV events" type="checkbox" checked={draft.provider.kv_events !== null} onChange={toggleKv} /><span /></label>
                  </div>
                  {draft.provider.kv_events && <div className="field-grid">
                    <Field label="Publisher endpoint" name="kv_endpoint" wide><input value={draft.provider.kv_events.endpoint} onChange={(event) => updateKv("endpoint", event.target.value)} /></Field>
                    <Field label="Replay endpoint" name="kv_replay" wide><input value={draft.provider.kv_events.replay_endpoint ?? ""} onChange={(event) => updateKv("replay_endpoint", event.target.value || null)} /></Field>
                    <Field label="Topic" name="kv_topic"><input value={draft.provider.kv_events.topic} onChange={(event) => updateKv("topic", event.target.value)} /></Field>
                    <NumberField label="Reconnect (ms)" name="kv_reconnect" value={draft.provider.kv_events.reconnect_ms} min={1} onChange={(value) => updateKv("reconnect_ms", value)} />
                    <NumberField label="Max blocks" name="kv_blocks" value={draft.provider.kv_events.max_blocks} min={1} onChange={(value) => updateKv("max_blocks", value)} />
                    <NumberField label="Max event bytes" name="kv_event_bytes" value={draft.provider.kv_events.max_event_bytes} min={1} onChange={(value) => updateKv("max_event_bytes", value)} />
                  </div>}
                </section>
              )}

              <section className="form-section nested compact">
                <div className="section-heading-row">
                  <div><h3>Start draining</h3><span className="field-meta">Persist the node without assigning new requests.</span></div>
                  <label className="switch"><input aria-label="Start node draining" type="checkbox" checked={draft.draining} onChange={(event) => update((current) => ({ ...current, draining: event.target.checked }))} /><span /></label>
                </div>
              </section>
            </div>
          )}
        </section>

        {(preflight || preflightError) && (
          <div className={`preflight-result ${preflight ? "success" : "error"}`} role={preflight ? "status" : "alert"}>
            {preflight ? <Check size={17} /> : <X size={17} />}
            <div><strong>{preflight ? "Connection verified" : "Connection failed"}</strong><span>{preflight ? `${preflight.runtime.provider === "vllm" ? "vLLM" : "OpenAI-compatible"} ${preflight.runtime.provider_version ?? "provider"} passed configuration, provider and health checks.` : preflightError}</span></div>
          </div>
        )}
        </div>

        <footer className="drawer-footer">
          <button type="button" className="secondary-button test-button" disabled={busy || checking} onClick={() => void testConnection()}>
            {checking ? <LoaderCircle className="spin" size={16} /> : <FlaskConical size={16} />} Test connection
          </button>
          <span className="footer-spacer" />
          <button type="button" className="secondary-button" disabled={busy || checking} onClick={requestClose}>Cancel</button>
          <button type="submit" className="primary-button" disabled={busy || checking}>
            {busy ? <LoaderCircle className="spin" size={16} /> : <Check size={16} />}
            {state.mode === "create" ? "Add node" : "Save changes"}
          </button>
        </footer>
      </form>
    </Drawer>
  );
}
