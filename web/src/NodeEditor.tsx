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
import { useTranslation } from "react-i18next";
import * as api from "./api";
import type { TranslationKey } from "./i18n";
import { draftToConfig, shouldClearApiKey, validateDraft } from "./node-config";
import type { DraftErrors } from "./node-config";
import type { AnthropicProtocol, KvEventsConfig, NodeDraft, Pair, PreflightResponse, ProviderKind } from "./types";

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
  const { t } = useTranslation();
  const update = (index: number, key: keyof Pair, value: string | boolean) => {
    onChange(rows.map((row, rowIndex) => rowIndex === index ? { ...row, [key]: value } : row));
  };

  return <div className="mapping-editor">
    <div className="mapping-head"><span>{keyLabel}</span><span>{valueLabel}</span><span>{t("editor.multimodal")}</span><span /></div>
    {rows.map((row, index) => <div className="mapping-row" key={index}>
      <TextInput aria-label={`${keyLabel} ${index + 1}`} value={row.key} error={Boolean(error)} onChange={(event) => update(index, "key", event.target.value)} />
      <TextInput aria-label={`${valueLabel} ${index + 1}`} value={row.value} error={Boolean(error)} onChange={(event) => update(index, "value", event.target.value)} />
      <Switch size="sm" aria-label={`${t("editor.multimodal")} ${index + 1}`} checked={row.multimodal !== false} onChange={(event) => update(index, "multimodal", event.currentTarget.checked)} />
      <Button variant="subtle" color="gray" px={6} aria-label={t("editor.removeMapping", { count: index + 1 })} onClick={() => onChange(rows.length === 1 ? [{ key: "", value: "" }] : rows.filter((_, rowIndex) => rowIndex !== index))}><Trash2 size={14} /></Button>
    </div>)}
    {error && <span className="form-error" role="alert">{t(error as TranslationKey)}</span>}
    <Button variant="subtle" size="compact-sm" leftSection={<Plus size={14} />} onClick={() => onChange([...rows, { key: "", value: "", multimodal: true }])}>{t("editor.addMapping")}</Button>
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
  const { t } = useTranslation();
  const errorText = (key?: string) => key ? t(key as TranslationKey) : undefined;
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
    if (!dirty || window.confirm(t("editor.discard"))) onClose();
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
      setPreflightError(error instanceof Error ? error.message : t("editor.connectionTestFailed"));
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
        max_directory_bytes: 536_870_912,
        max_event_bytes: 16_777_216,
      },
    },
  }));

  const updateKv = <K extends keyof KvEventsConfig>(key: K, value: KvEventsConfig[K]) => update((current) => ({
    ...current,
    provider: { ...current.provider, kv_events: current.provider.kv_events ? { ...current.provider.kv_events, [key]: value } : null },
  }));

  const anthropicProtocols = (["auto", "native", "responses", "chat"] as AnthropicProtocol[])
    .map((value) => ({ value, label: t(`protocol.${value}` as TranslationKey) }));

  return <div className="editor-page page-frame">
    <button className="breadcrumb-button" type="button" onClick={requestClose}><ArrowLeft size={15} /> {t("editor.back")}</button>
    <header className="editor-header"><h1>{state.mode === "create" ? t("editor.addTitle") : t("editor.editTitle", { id: draft.id })}</h1></header>

    <Stepper active={step} onStepClick={(nextStep) => nextStep < step && setStep(nextStep)} className="node-stepper" size="sm">
      <Stepper.Step label={t("editor.stepBasic")} description={t("editor.stepBasicDescription")} />
      <Stepper.Step label={t("editor.stepAdvanced")} description={t("editor.stepAdvancedDescription")} />
      <Stepper.Step label={t("editor.stepReview")} description={t("editor.stepReviewDescription")} />
    </Stepper>

    <form className="wizard-form" noValidate onSubmit={(event) => {
      event.preventDefault();
      if (step < 2) next();
      else if (validate()) void onSave({ ...state, draft } as EditorState);
    }}>
      <div className="wizard-content">
        {step === 0 && <>
          <section className="wizard-section">
            <div className="section-title"><h2>{t("editor.basicInformation")}</h2><span>{t("editor.basicDescription")}</span></div>
            <div className="wizard-grid">
              <TextInput label={t("editor.nodeId")} description={t("editor.nodeIdDescription")} required disabled={state.mode === "edit"} value={draft.id} error={errorText(errors.id)} onChange={(event) => update((current) => ({ ...current, id: event.target.value }))} />
              <TextInput label={t("editor.baseUrl")} description={t("editor.baseUrlDescription")} required value={draft.base_url} error={errorText(errors.base_url)} onChange={(event) => update((current) => ({ ...current, base_url: event.target.value }))} />
              <Select label={t("editor.provider")} required data={[{ value: "vllm", label: "vLLM 0.25+" }, { value: "openai", label: t("upstreams.openaiCompatible") }]} value={draft.provider.type} onChange={(value) => setProvider(value as ProviderKind | null)} />
              <Select
                label={t("editor.anthropicProtocol")}
                description={t("editor.anthropicDescription")}
                required
                data={anthropicProtocols}
                value={draft.provider.anthropic_protocol}
                onChange={(value) => value && update((current) => ({
                  ...current,
                  provider: { ...current.provider, anthropic_protocol: value as AnthropicProtocol },
                }))}
              />
              <NumberInput label={t("editor.maxConcurrency")} required min={1} value={draft.max_concurrency} error={errorText(errors.max_concurrency)} onChange={(value) => update((current) => ({ ...current, max_concurrency: numeric(value) }))} />
              <NumberInput label={t("editor.schedulingWeight")} description={t("editor.weightDescription")} required min={0.01} step={0.05} value={draft.weight} error={errorText(errors.weight)} onChange={(value) => update((current) => ({ ...current, weight: numeric(value) }))} />
            </div>
          </section>
          <section className="wizard-section">
            <div className="section-title"><h2>{t("editor.modelMappings")}</h2><span>{t("editor.configuredCount", { count: draft.models.filter((row) => row.key && row.value).length })}</span></div>
            <PairEditor rows={draft.models} error={errors.models} keyLabel={t("editor.publicModel")} valueLabel={t("editor.upstreamModel")} onChange={(models) => update((current) => ({ ...current, models }))} />
          </section>
        </>}

        {step === 1 && <>
          <section className="wizard-section">
            <div className="section-title"><h2>{t("editor.healthTelemetry")}</h2><span>{t("editor.healthTelemetryDescription")}</span></div>
            <div className="wizard-grid two-columns">
              <TextInput label={t("editor.healthPath")} value={draft.health_path} error={errorText(errors.health_path)} onChange={(event) => update((current) => ({ ...current, health_path: event.target.value }))} />
              {draft.provider.type === "vllm" && <>
                <TextInput label={t("editor.versionPath")} value={draft.provider.version_path} error={errorText(errors.version_path)} onChange={(event) => update((current) => ({ ...current, provider: { ...current.provider, version_path: event.target.value } }))} />
                <TextInput label={t("editor.metricsPath")} value={draft.provider.metrics_path} error={errorText(errors.metrics_path)} onChange={(event) => update((current) => ({ ...current, provider: { ...current.provider, metrics_path: event.target.value } }))} />
                <TextInput label={t("editor.tokenizePath")} value={draft.provider.tokenize_path} error={errorText(errors.tokenize_path)} onChange={(event) => update((current) => ({ ...current, provider: { ...current.provider, tokenize_path: event.target.value } }))} />
                <NumberInput label={t("editor.monitorInterval")} min={100} value={draft.provider.monitor_interval_ms} error={errorText(errors.monitor_interval_ms)} onChange={(value) => update((current) => ({ ...current, provider: { ...current.provider, monitor_interval_ms: numeric(value) } }))} />
                <NumberInput label={t("editor.requestTimeout")} min={1} value={draft.provider.request_timeout_ms} error={errorText(errors.request_timeout_ms)} onChange={(value) => update((current) => ({ ...current, provider: { ...current.provider, request_timeout_ms: numeric(value) } }))} />
                <NumberInput label={t("editor.telemetryStale")} min={1} value={draft.provider.telemetry_stale_ms} error={errorText(errors.telemetry_stale_ms)} onChange={(value) => update((current) => ({ ...current, provider: { ...current.provider, telemetry_stale_ms: numeric(value) } }))} />
                <NumberInput label={t("editor.waitingWatermark")} min={1} value={draft.provider.waiting_threshold} error={errorText(errors.waiting_threshold)} onChange={(value) => update((current) => ({ ...current, provider: { ...current.provider, waiting_threshold: numeric(value) } }))} />
                <NumberInput label={t("editor.tokenizeEntries")} min={1} value={draft.provider.tokenize_cache_entries} onChange={(value) => update((current) => ({ ...current, provider: { ...current.provider, tokenize_cache_entries: numeric(value) } }))} />
              </>}
            </div>
          </section>

          <section className="wizard-section">
            <div className="section-title"><h2>{t("editor.credentials")}</h2><span>{draft.api_key.trim() || draft.preserve_api_key ? t("editor.databaseKey") : draft.api_key_env ? t("editor.environmentKey") : t("editor.noBearerKey")}</span></div>
            <div className="credential-editor">
              <PasswordInput
                label={t("editor.bearerKey")}
                autoComplete="new-password"
                placeholder={draft.preserve_api_key ? t("editor.storedKey") : t("common.optional")}
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
              {draft.preserve_api_key && <Button variant="default" color="red" leftSection={<Trash2 size={14} />} onClick={() => update((current) => ({ ...current, api_key: "", preserve_api_key: false }))}>{t("editor.removeKey")}</Button>}
              {draft.api_key_env && <div className="legacy-credential"><span>{t("editor.environmentFallback")}</span><strong>{draft.api_key_env}</strong><Button variant="subtle" color="gray" px={6} aria-label={t("editor.removeEnvironment")} onClick={() => update((current) => ({ ...current, api_key_env: null }))}><Trash2 size={14} /></Button></div>}
            </div>
          </section>

          {draft.provider.type === "vllm" && <section className="wizard-section">
            <div className="switch-heading"><div><h2>{t("editor.kvEvents")}</h2><span>{t("editor.kvDescription")}</span></div><Switch aria-label={t("editor.enableKv")} checked={draft.provider.kv_events !== null} onChange={toggleKv} /></div>
            {draft.provider.kv_events && <div className="wizard-grid two-columns">
              <TextInput label={t("editor.publisherEndpoint")} value={draft.provider.kv_events.endpoint} onChange={(event) => updateKv("endpoint", event.target.value)} />
              <TextInput label={t("editor.replayEndpoint")} value={draft.provider.kv_events.replay_endpoint ?? ""} onChange={(event) => updateKv("replay_endpoint", event.target.value || null)} />
              <TextInput label={t("editor.topic")} value={draft.provider.kv_events.topic} onChange={(event) => updateKv("topic", event.target.value)} />
              <NumberInput label={t("editor.reconnect")} min={1} value={draft.provider.kv_events.reconnect_ms} error={errorText(errors.kv_reconnect_ms)} onChange={(value) => updateKv("reconnect_ms", numeric(value))} />
              <NumberInput label={t("editor.maxBlocks")} min={1} value={draft.provider.kv_events.max_blocks} error={errorText(errors.kv_max_blocks)} onChange={(value) => updateKv("max_blocks", numeric(value))} />
              <NumberInput label={t("editor.directoryBytes")} min={1} value={draft.provider.kv_events.max_directory_bytes} error={errorText(errors.kv_max_directory_bytes)} onChange={(value) => updateKv("max_directory_bytes", numeric(value))} />
              <NumberInput label={t("editor.maxEventBytes")} min={1} value={draft.provider.kv_events.max_event_bytes} error={errorText(errors.kv_max_event_bytes)} onChange={(value) => updateKv("max_event_bytes", numeric(value))} />
            </div>}
          </section>}

          <section className="wizard-section compact-section">
            <div className="switch-heading"><div><h2>{t("editor.startDraining")}</h2><span>{t("editor.startDrainingDescription")}</span></div><Switch aria-label={t("editor.startDrainingLabel")} checked={draft.draining} onChange={(event) => update((current) => ({ ...current, draining: event.currentTarget.checked }))} /></div>
          </section>
        </>}

        {step === 2 && <section className="wizard-section review-section">
          <div className="section-title"><h2>{t("editor.reviewConfiguration")}</h2><span>{t("editor.reviewDescription")}</span></div>
          <div className="review-grid">
            <ReviewRow label={t("editor.nodeId")} value={draft.id} />
            <ReviewRow label={t("editor.baseUrl")} value={draft.base_url} />
            <ReviewRow label={t("editor.provider")} value={draft.provider.type === "vllm" ? "vLLM 0.25+" : t("upstreams.openaiCompatible")} />
            <ReviewRow label={t("details.anthropicProtocol")} value={t(`protocol.${draft.provider.anthropic_protocol}` as TranslationKey)} />
            <ReviewRow label={t("editor.maxConcurrency")} value={draft.max_concurrency} />
            <ReviewRow label={t("editor.schedulingWeight")} value={draft.weight} />
            <ReviewRow label={t("editor.modelMappings")} value={draft.models.filter((row) => row.key && row.value).length} />
            <ReviewRow label={t("editor.healthPath")} value={draft.health_path} />
            <ReviewRow label={t("editor.bearerCredential")} value={draft.api_key.trim() || draft.preserve_api_key || draft.api_key_env ? t("common.configured") : t("common.notConfigured")} />
            <ReviewRow label={t("editor.lifecycle")} value={draft.draining ? t("overview.draining") : t("editor.serving")} />
          </div>
          <Alert icon={<Info size={16} />} color="indigo" title={t("editor.connectionRecommended")}>{t("editor.connectionRecommendation")}</Alert>
        </section>}

        {(preflight || preflightError) && <Alert className="preflight-alert" icon={preflight ? <Check size={16} /> : <X size={16} />} color={preflight ? "green" : "red"} title={preflight ? t("editor.connectionVerified") : t("editor.connectionFailed")}>
          {preflight ? t("editor.connectionPassed", { provider: preflight.runtime.provider === "vllm" ? "vLLM" : "OpenAI-compatible" }) : preflightError}
        </Alert>}
      </div>

      <footer className="wizard-footer">
        <Button variant="default" leftSection={checking ? <LoaderCircle className="spin" size={15} /> : <FlaskConical size={15} />} disabled={busy || checking} onClick={() => void testConnection()}>{t("editor.testConnection")}</Button>
        <span className="footer-spacer" />
        <Button variant="default" disabled={busy || checking} onClick={step === 0 ? requestClose : () => setStep((current) => current - 1)}>{step === 0 ? t("common.cancel") : t("common.back")}</Button>
        <Button type="submit" disabled={busy || checking} leftSection={busy ? <LoaderCircle className="spin" size={15} /> : step === 2 ? <Check size={15} /> : undefined}>{step === 2 ? state.mode === "create" ? t("upstreams.addNode") : t("editor.saveChanges") : t("common.next")}</Button>
      </footer>
    </form>
  </div>;
}
