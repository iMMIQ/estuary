import {
  Alert,
  Button,
  NumberInput,
  PasswordInput,
  Select,
  Stepper,
  Switch,
  TextInput,
} from "@mantine/core";
import {
  ArrowLeft,
  Check,
  FlaskConical,
  Info,
  LoaderCircle,
  Plus,
  Trash2,
  X,
} from "lucide-react";
import { useCallback, useMemo, useState } from "react";
import * as api from "./api";
import { draftToConfig, shouldClearApiKey, validateDraft } from "./node-config";
import type { DraftErrors } from "./node-config";
import type { KvEventsConfig, NodeDraft, Pair, PreflightResponse, ProviderKind } from "./types";

export type EditorState =
  | { mode: "create"; draft: NodeDraft; revision: null }
  | { mode: "edit"; draft: NodeDraft; revision: number };

function numeric(value: string | number): number {
  return typeof value === "number" ? value : Number(value);
}

function PairEditor({
  rows,
  error,
  onChange,
  keyLabel,
  valueLabel,
}: {
  rows: Pair[];
  error?: string;
  onChange: (rows: Pair[]) => void;
  keyLabel: string;
  valueLabel: string;
}) {
  const update = (index: number, key: keyof Pair, value: string) => {
    onChange(rows.map((row, rowIndex) => rowIndex === index ? { ...row, [key]: value } : row));
  };

  return <div className="mapping-editor">
    <div className="mapping-head"><span>{keyLabel}</span><span>{valueLabel}</span><span /></div>
    {rows.map((row, index) => <div className="mapping-row" key={index}>
      <TextInput aria-label={`${keyLabel} ${index + 1}`} value={row.key} error={Boolean(error)} onChange={(event) => update(index, "key", event.target.value)} />
      <TextInput aria-label={`${valueLabel} ${index + 1}`} value={row.value} error={Boolean(error)} onChange={(event) => update(index, "value", event.target.value)} />
      <Button variant="subtle" color="gray" px={6} aria-label={`Remove mapping ${index + 1}`} onClick={() => onChange(rows.length === 1 ? [{ key: "", value: "" }] : rows.filter((_, rowIndex) => rowIndex !== index))}><Trash2 size={14} /></Button>
    </div>)}
    {error && <span className="form-error" role="alert">{error}</span>}
    <Button variant="subtle" size="compact-sm" leftSection={<Plus size={14} />} onClick={() => onChange([...rows, { key: "", value: "" }])}>Add mapping</Button>
  </div>;
}

function ReviewRow({ label, value }: { label: string; value: string | number }) {
  return <div className="review-row"><span>{label}</span><strong>{value}</strong></div>;
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
  const [step, setStep] = useState(0);
  const [errors, setErrors] = useState<DraftErrors>({});
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

  const requestClose = () => {
    if (!dirty || window.confirm("Discard unsaved changes?")) onClose();
  };

  const validate = () => {
    const validation = validateDraft(draft);
    setErrors(validation);
    if (Object.keys(validation).length === 0) return true;
    const basicFields = ["id", "base_url", "max_concurrency", "weight", "models"];
    if (Object.keys(validation).some((field) => basicFields.includes(field))) setStep(0);
    else setStep(1);
    window.requestAnimationFrame(() => document.querySelector<HTMLElement>("[data-invalid='true'] input, input[data-invalid='true']")?.focus());
    return false;
  };

  const next = () => {
    if (step === 0 && !validate()) return;
    if (step === 1 && !validate()) return;
    setStep((current) => Math.min(2, current + 1));
  };

  const testConnection = async () => {
    if (!validate()) return;
    setChecking(true);
    setPreflight(null);
    setPreflightError(null);
    try {
      setPreflight(await api.preflightNode(draftToConfig(draft), shouldClearApiKey(draft)));
    } catch (error) {
      setPreflightError(error instanceof Error ? error.message : "Connection test failed");
    } finally {
      setChecking(false);
    }
  };

  const setProvider = (kind: ProviderKind | null) => {
    if (!kind) return;
    update((current) => ({
      ...current,
      provider: { ...current.provider, type: kind, kv_events: kind === "openai" ? null : current.provider.kv_events },
    }));
  };

  const toggleKv = () => update((current) => ({
    ...current,
    provider: {
      ...current.provider,
      kv_events: current.provider.kv_events ? null : {
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
    provider: { ...current.provider, kv_events: current.provider.kv_events ? { ...current.provider.kv_events, [key]: value } : null },
  }));

  return <div className="editor-page page-frame">
    <button className="breadcrumb-button" type="button" onClick={requestClose}><ArrowLeft size={15} /> Back to upstreams</button>
    <header className="editor-header"><h1>{state.mode === "create" ? "Add Upstream Node" : `Edit ${draft.id}`}</h1></header>

    <Stepper active={step} onStepClick={(nextStep) => nextStep < step && setStep(nextStep)} className="node-stepper" size="sm">
      <Stepper.Step label="Basic" description="Connection and routing" />
      <Stepper.Step label="Advanced" description="Health and telemetry" />
      <Stepper.Step label="Review" description="Verify configuration" />
    </Stepper>

    <form className="wizard-form" onSubmit={(event) => {
      event.preventDefault();
      if (step < 2) next();
      else if (validate()) void onSave({ ...state, draft } as EditorState);
    }}>
      <div className="wizard-content">
        {step === 0 && <>
          <section className="wizard-section">
            <div className="section-title"><h2>Basic Information</h2><span>Connection identity and provider settings.</span></div>
            <div className="wizard-grid">
              <TextInput label="Node ID" description="Unique identifier for this upstream node." required disabled={state.mode === "edit"} value={draft.id} error={errors.id} onChange={(event) => update((current) => ({ ...current, id: event.target.value }))} />
              <TextInput label="Base URL" description="Include http(s) and port when needed." required value={draft.base_url} error={errors.base_url} onChange={(event) => update((current) => ({ ...current, base_url: event.target.value }))} />
              <Select label="Provider" required data={[{ value: "vllm", label: "vLLM 0.25+" }, { value: "openai", label: "OpenAI compatible" }]} value={draft.provider.type} onChange={(value) => setProvider(value as ProviderKind | null)} />
              <NumberInput label="Max concurrency" required min={1} value={draft.max_concurrency} error={errors.max_concurrency} onChange={(value) => update((current) => ({ ...current, max_concurrency: numeric(value) }))} />
              <NumberInput label="Scheduling weight" description="Higher weight receives proportionally more traffic." required min={0.01} step={0.05} value={draft.weight} error={errors.weight} onChange={(value) => update((current) => ({ ...current, weight: numeric(value) }))} />
            </div>
          </section>
          <section className="wizard-section">
            <div className="section-title"><h2>Model Mappings</h2><span>{draft.models.filter((row) => row.key && row.value).length} configured</span></div>
            <PairEditor rows={draft.models} error={errors.models} keyLabel="Public model" valueLabel="Upstream model" onChange={(models) => update((current) => ({ ...current, models }))} />
          </section>
        </>}

        {step === 1 && <>
          <section className="wizard-section">
            <div className="section-title"><h2>Health and Telemetry</h2><span>Runtime probing and admission signals.</span></div>
            <div className="wizard-grid two-columns">
              <TextInput label="Health path" value={draft.health_path} error={errors.health_path} onChange={(event) => update((current) => ({ ...current, health_path: event.target.value }))} />
              {draft.provider.type === "vllm" && <>
                <TextInput label="Version path" value={draft.provider.version_path} error={errors.version_path} onChange={(event) => update((current) => ({ ...current, provider: { ...current.provider, version_path: event.target.value } }))} />
                <TextInput label="Metrics path" value={draft.provider.metrics_path} error={errors.metrics_path} onChange={(event) => update((current) => ({ ...current, provider: { ...current.provider, metrics_path: event.target.value } }))} />
                <TextInput label="Tokenize path" value={draft.provider.tokenize_path} error={errors.tokenize_path} onChange={(event) => update((current) => ({ ...current, provider: { ...current.provider, tokenize_path: event.target.value } }))} />
                <NumberInput label="Monitor interval (ms)" min={1} value={draft.provider.monitor_interval_ms} error={errors.monitor_interval_ms} onChange={(value) => update((current) => ({ ...current, provider: { ...current.provider, monitor_interval_ms: numeric(value) } }))} />
                <NumberInput label="Request timeout (ms)" min={1} value={draft.provider.request_timeout_ms} error={errors.request_timeout_ms} onChange={(value) => update((current) => ({ ...current, provider: { ...current.provider, request_timeout_ms: numeric(value) } }))} />
                <NumberInput label="Telemetry stale (ms)" min={1} value={draft.provider.telemetry_stale_ms} error={errors.telemetry_stale_ms} onChange={(value) => update((current) => ({ ...current, provider: { ...current.provider, telemetry_stale_ms: numeric(value) } }))} />
                <NumberInput label="Waiting watermark" min={1} value={draft.provider.waiting_threshold} error={errors.waiting_threshold} onChange={(value) => update((current) => ({ ...current, provider: { ...current.provider, waiting_threshold: numeric(value) } }))} />
                <NumberInput label="Tokenize cache entries" min={1} value={draft.provider.tokenize_cache_entries} onChange={(value) => update((current) => ({ ...current, provider: { ...current.provider, tokenize_cache_entries: numeric(value) } }))} />
              </>}
            </div>
          </section>

          <section className="wizard-section">
            <div className="section-title"><h2>Credentials</h2><span>{draft.api_key.trim() || draft.preserve_api_key ? "Database key configured" : draft.api_key_env ? "Environment key configured" : "No Bearer key"}</span></div>
            <div className="credential-editor">
              <PasswordInput
                label="Bearer API key"
                autoComplete="new-password"
                placeholder={draft.preserve_api_key ? "Stored key unchanged" : "Optional"}
                value={draft.api_key}
                onChange={(event) => {
                  const value = event.currentTarget.value;
                  update((current) => ({
                    ...current,
                    api_key: value,
                    preserve_api_key: value ? false : current.preserve_api_key,
                  }));
                }}
              />
              {draft.preserve_api_key && <Button variant="default" color="red" leftSection={<Trash2 size={14} />} onClick={() => update((current) => ({ ...current, api_key: "", preserve_api_key: false }))}>Remove key</Button>}
              {draft.api_key_env && <div className="legacy-credential"><span>Environment fallback</span><strong>{draft.api_key_env}</strong><Button variant="subtle" color="gray" px={6} aria-label="Remove environment fallback" onClick={() => update((current) => ({ ...current, api_key_env: null }))}><Trash2 size={14} /></Button></div>}
            </div>
          </section>

          {draft.provider.type === "vllm" && <section className="wizard-section">
            <div className="switch-heading"><div><h2>KV Events</h2><span>Exact prefix-cache synchronization.</span></div><Switch aria-label="Enable KV events" checked={draft.provider.kv_events !== null} onChange={toggleKv} /></div>
            {draft.provider.kv_events && <div className="wizard-grid two-columns">
              <TextInput label="Publisher endpoint" value={draft.provider.kv_events.endpoint} onChange={(event) => updateKv("endpoint", event.target.value)} />
              <TextInput label="Replay endpoint" value={draft.provider.kv_events.replay_endpoint ?? ""} onChange={(event) => updateKv("replay_endpoint", event.target.value || null)} />
              <TextInput label="Topic" value={draft.provider.kv_events.topic} onChange={(event) => updateKv("topic", event.target.value)} />
              <NumberInput label="Reconnect (ms)" min={1} value={draft.provider.kv_events.reconnect_ms} onChange={(value) => updateKv("reconnect_ms", numeric(value))} />
              <NumberInput label="Max blocks" min={1} value={draft.provider.kv_events.max_blocks} onChange={(value) => updateKv("max_blocks", numeric(value))} />
              <NumberInput label="Max event bytes" min={1} value={draft.provider.kv_events.max_event_bytes} onChange={(value) => updateKv("max_event_bytes", numeric(value))} />
            </div>}
          </section>}

          <section className="wizard-section compact-section">
            <div className="switch-heading"><div><h2>Start Draining</h2><span>Persist without assigning new requests.</span></div><Switch aria-label="Start node draining" checked={draft.draining} onChange={(event) => update((current) => ({ ...current, draining: event.currentTarget.checked }))} /></div>
          </section>
        </>}

        {step === 2 && <section className="wizard-section review-section">
          <div className="section-title"><h2>Review Configuration</h2><span>Confirm the node before applying it to the runtime registry.</span></div>
          <div className="review-grid">
            <ReviewRow label="Node ID" value={draft.id} />
            <ReviewRow label="Base URL" value={draft.base_url} />
            <ReviewRow label="Provider" value={draft.provider.type === "vllm" ? "vLLM 0.25+" : "OpenAI compatible"} />
            <ReviewRow label="Max concurrency" value={draft.max_concurrency} />
            <ReviewRow label="Scheduling weight" value={draft.weight} />
            <ReviewRow label="Model mappings" value={draft.models.filter((row) => row.key && row.value).length} />
            <ReviewRow label="Health path" value={draft.health_path} />
            <ReviewRow label="Bearer credential" value={draft.api_key.trim() || draft.preserve_api_key || draft.api_key_env ? "Configured" : "Not configured"} />
            <ReviewRow label="Lifecycle" value={draft.draining ? "Draining" : "Serving"} />
          </div>
          <Alert icon={<Info size={16} />} color="indigo" title="Connection verification recommended">Run Test Connection before applying this configuration to verify compatibility and health.</Alert>
        </section>}

        {(preflight || preflightError) && <Alert className="preflight-alert" icon={preflight ? <Check size={16} /> : <X size={16} />} color={preflight ? "green" : "red"} title={preflight ? "Connection verified" : "Connection failed"}>
          {preflight ? `${preflight.runtime.provider === "vllm" ? "vLLM" : "OpenAI-compatible"} provider passed configuration, compatibility and health checks.` : preflightError}
        </Alert>}
      </div>

      <footer className="wizard-footer">
        <Button variant="default" leftSection={checking ? <LoaderCircle className="spin" size={15} /> : <FlaskConical size={15} />} disabled={busy || checking} onClick={() => void testConnection()}>Test Connection</Button>
        <span className="footer-spacer" />
        <Button variant="default" disabled={busy || checking} onClick={step === 0 ? requestClose : () => setStep((current) => current - 1)}>{step === 0 ? "Cancel" : "Back"}</Button>
        <Button type="submit" disabled={busy || checking} leftSection={busy ? <LoaderCircle className="spin" size={15} /> : step === 2 ? <Check size={15} /> : undefined}>{step === 2 ? state.mode === "create" ? "Add Node" : "Save Changes" : "Next"}</Button>
      </footer>
    </form>
  </div>;
}
