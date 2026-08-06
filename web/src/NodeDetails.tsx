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
import type { NodeRecord } from "./types";
import { formatPercent, formatTimestamp, StatusBadge } from "./ui";

const anthropicProtocolLabels = {
  auto: "Automatic",
  native: "Native Anthropic Messages",
  responses: "OpenAI Responses",
  chat: "OpenAI Chat Completions",
} as const;

function metricRate(value: number | null | undefined, unit: string): string {
  if (value == null || !Number.isFinite(value)) return "Unavailable";
  return `${new Intl.NumberFormat("en", { notation: "compact", maximumFractionDigits: 1 }).format(value)} ${unit}`;
}

function DataRow({ label, value }: { label: string; value: ReactNode }) {
  return <div className="data-row"><span>{label}</span><strong>{value}</strong></div>;
}

function RuntimeCard({ title, children }: { title: string; children: ReactNode }) {
  return <section className="runtime-card"><h3>{title}</h3><div className="runtime-card-body">{children}</div></section>;
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
  const runtime = node.runtime;
  const isVllm = node.config.provider.type === "vllm";
  const hasError = Boolean(runtime.provider_last_error || runtime.last_error || runtime.error_ewma > 0.05);

  return (
    <div className="detail-page page-frame">
      <button className="breadcrumb-button" type="button" onClick={onClose}>
        <ArrowLeft size={15} /> Upstreams <span>/</span> {node.config.id}
      </button>

      <header className="detail-header">
        <div className="detail-title">
          <h1>{node.config.id}</h1>
          <StatusBadge value={node.admission.state} label={node.admission.accepting_assignments ? "Accepting Requests" : undefined} />
          <div className="detail-meta">
            <span>Base URL<strong>{node.config.base_url}</strong></span>
            <span>Provider<strong>{isVllm ? "vLLM 0.25+" : "OpenAI compatible"}</strong></span>
            <span>Revision<strong>{node.revision}</strong></span>
            <span>Updated<strong>{formatTimestamp(node.updated_at_unix_ms)}</strong></span>
          </div>
        </div>
        <div className="detail-actions">
          <Button variant="default" size="xs" leftSection={<Edit3 size={14} />} disabled={busy} onClick={onEdit}>Edit</Button>
          <Button variant="default" size="xs" leftSection={runtime.lifecycle === "serving" ? <PauseCircle size={14} /> : <Play size={14} />} disabled={busy} onClick={onToggleDrain}>
            {runtime.lifecycle === "serving" ? "Drain" : "Resume"}
          </Button>
          <Button color="red" variant="light" size="xs" leftSection={<Trash2 size={14} />} disabled={busy} onClick={onDelete}>Delete</Button>
        </div>
      </header>

      {hasError && <div className="error-banner" role="alert">
        <AlertTriangle size={16} />
        <strong>Node requires attention</strong>
        <span>{runtime.provider_last_error ?? runtime.last_error ?? `Error rate EWMA is ${(runtime.error_ewma * 100).toFixed(2)}%`}</span>
      </div>}

      <Tabs defaultValue="overview" className="node-tabs">
        <Tabs.List>
          <Tabs.Tab value="overview">Overview</Tabs.Tab>
          <Tabs.Tab value="metrics">Metrics</Tabs.Tab>
          <Tabs.Tab value="models">Models</Tabs.Tab>
        </Tabs.List>

        <Tabs.Panel value="overview" pt="md">
          <div className="detail-grid-layout">
            <RuntimeCard title="Admission">
              <div className="admission-callout"><Check size={16} /><strong>{node.admission.reason}</strong></div>
              <DataRow label="Decision" value={<StatusBadge value={node.admission.state} />} />
              <DataRow label="Weight" value={runtime.weight} />
              <DataRow label="Max concurrency" value={runtime.max_concurrency} />
              <DataRow label="Available concurrency" value={`${runtime.available} (${runtime.max_concurrency ? Math.round(runtime.available / runtime.max_concurrency * 100) : 0}%)`} />
            </RuntimeCard>

            <RuntimeCard title={`Provider (${isVllm ? "vLLM" : "OpenAI"})`}>
              <DataRow label="Version" value={runtime.provider_version ?? "Unavailable"} />
              <DataRow label="Running / Waiting" value={`${runtime.upstream_running ?? runtime.active} / ${runtime.upstream_waiting ?? 0}`} />
              {isVllm && <>
                <DataRow label="KV cache usage" value={formatPercent(runtime.kv_cache_usage)} />
                <DataRow label="KV blocks" value={node.exact_kv_blocks.toLocaleString()} />
                <DataRow label="KV directory" value={node.exact_kv_authoritative ? "Authoritative" : "Approximate"} />
                <DataRow label="Telemetry" value={node.admission.telemetry_fresh ? "Fresh" : "Stale"} />
              </>}
            </RuntimeCard>

            <RuntimeCard title="Health">
              <DataRow label="Status" value={<StatusBadge value={runtime.health} />} />
              <DataRow label="Health check" value={node.config.health_path} />
              <DataRow label="Last transition" value={formatTimestamp(runtime.last_change_unix_ms)} />
              <DataRow label="Last error" value={runtime.last_error ?? "None"} />
            </RuntimeCard>

            <RuntimeCard title="Circuit Breaker">
              <DataRow label="State" value={<StatusBadge value={runtime.circuit} />} />
              <DataRow label="Failure count" value={runtime.circuit_failures} />
              <DataRow label="Half-open in flight" value={runtime.circuit_half_open_in_flight} />
              <DataRow label="Error rate (EWMA)" value={`${(runtime.error_ewma * 100).toFixed(2)}%`} />
            </RuntimeCard>

            <RuntimeCard title="Provider Compatibility">
              <DataRow label="Status" value={<StatusBadge value={runtime.provider_state} />} />
              <DataRow label="Provider" value={isVllm ? "vLLM 0.25+" : "OpenAI compatible"} />
              <DataRow label="Anthropic protocol" value={anthropicProtocolLabels[node.config.provider.anthropic_protocol]} />
              <DataRow label="Bearer credential" value={node.credentials.api_key_source === "database" ? "Configured in database" : node.credentials.api_key_source === "environment" ? "Configured by environment" : "Not configured"} />
              <DataRow label="Warnings" value={runtime.provider_last_error ? 1 : 0} />
              <DataRow label="Generation" value={runtime.provider_generation} />
            </RuntimeCard>
          </div>
        </Tabs.Panel>

        <Tabs.Panel value="metrics" pt="md">
          <div className="metric-tile-grid">
            <div><Activity size={16} /><span>Local load<strong>{runtime.active} / {runtime.max_concurrency}</strong></span></div>
            <div><Server size={16} /><span>Upstream demand<strong>{runtime.upstream_running ?? runtime.active} / {runtime.upstream_waiting ?? 0}</strong></span></div>
            <div><Gauge size={16} /><span>Prompt throughput<strong>{metricRate(runtime.prompt_tokens_per_second, "tok/s")}</strong></span></div>
            <div><Gauge size={16} /><span>Generation throughput<strong>{metricRate(runtime.generation_tokens_per_second, "tok/s")}</strong></span></div>
            <div><Activity size={16} /><span>Completed requests<strong>{metricRate(runtime.requests_per_second, "req/s")}</strong></span></div>
            <div><Layers3 size={16} /><span>GPU KV / Prefix hit<strong>{formatPercent(runtime.kv_cache_usage)} / {formatPercent(runtime.prefix_cache_hit_rate)}</strong></span></div>
            <div><AlertTriangle size={16} /><span>Preemptions<strong>{runtime.preemptions_total == null ? "Unavailable" : runtime.preemptions_total.toLocaleString()}</strong></span></div>
            <div><Gauge size={16} /><span>Latency / Error EWMA<strong>{Math.round(runtime.latency_ewma_ms)} ms / {(runtime.error_ewma * 100).toFixed(2)}%</strong></span></div>
          </div>
        </Tabs.Panel>

        <Tabs.Panel value="models" pt="md">
          <section className="models-panel">
            <div className="section-title"><h2>Models ({Object.keys(node.config.models).length})</h2><span>Public to upstream mappings</span></div>
            <table className="models-table"><thead><tr><th>Public model</th><th>Upstream model</th></tr></thead><tbody>
              {Object.entries(node.config.models).map(([publicModel, upstreamModel]) => <tr key={publicModel}><td>{publicModel}</td><td>{upstreamModel}</td></tr>)}
            </tbody></table>
          </section>
        </Tabs.Panel>

      </Tabs>
    </div>
  );
}
