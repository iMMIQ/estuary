import {
  Activity,
  AlertTriangle,
  ChevronDown,
  Edit3,
  Gauge,
  Network,
  Server,
  Trash2,
} from "lucide-react";
import type { ReactNode } from "react";
import { Drawer } from "./dialog";
import type { NodeRecord } from "./types";
import { formatPercent, formatTimestamp, StatusBadge } from "./ui";

function DetailItem({ label, value }: { label: string; value: ReactNode }) {
  return <div className="detail-item"><span>{label}</span><strong>{value}</strong></div>;
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
  const providerError = runtime.provider_last_error;
  const requestError = runtime.last_error;

  return (
    <Drawer title={node.config.id} eyebrow="Upstream details" ariaLabel={`Details for ${node.config.id}`} busy={busy} onClose={onClose}>
      <div className="drawer-body detail-drawer-body">
        <section className={`admission-banner ${node.admission.accepting_assignments ? "positive" : "warning"}`}>
          {node.admission.accepting_assignments ? <Network size={19} /> : <AlertTriangle size={19} />}
          <div><strong>{node.admission.state.replaceAll("_", " ")}</strong><span>{node.admission.reason}</span></div>
        </section>

        <section className="detail-metrics" aria-label="Node runtime summary">
          <div><Activity size={17} /><span>Local load<strong>{runtime.active} / {runtime.max_concurrency}</strong></span></div>
          <div><Gauge size={17} /><span>Latency EWMA<strong>{Math.round(runtime.latency_ewma_ms)} ms</strong></span></div>
          <div><Server size={17} /><span>Upstream running<strong>{runtime.upstream_running ?? "Unavailable"}</strong></span></div>
          <div><ChevronDown size={17} /><span>Upstream waiting<strong>{runtime.upstream_waiting ?? "Unavailable"}</strong></span></div>
        </section>

        {(providerError || requestError) && <section className="diagnostic-alert" role="alert">
          <AlertTriangle size={18} />
          <div><strong>Node needs attention</strong>{providerError && <span>{providerError}</span>}{requestError && <span>{requestError}</span>}</div>
        </section>}

        <section className="detail-section">
          <div className="detail-section-heading"><h3>Routing state</h3><StatusBadge value={node.admission.state} /></div>
          <div className="detail-grid">
            <DetailItem label="Health" value={<StatusBadge value={runtime.health} />} />
            <DetailItem label="Lifecycle" value={<StatusBadge value={runtime.lifecycle} />} />
            <DetailItem label="Circuit" value={<StatusBadge value={runtime.circuit} />} />
            <DetailItem label="Provider" value={<StatusBadge value={runtime.provider_state} />} />
            <DetailItem label="Available permits" value={runtime.available} />
            <DetailItem label="Weight" value={runtime.weight} />
            <DetailItem label="Error EWMA" value={`${(runtime.error_ewma * 100).toFixed(1)}%`} />
            <DetailItem label="Circuit failures" value={runtime.circuit_failures} />
          </div>
        </section>

        {node.config.provider.type === "vllm" && <section className="detail-section">
          <div className="detail-section-heading"><h3>vLLM telemetry</h3><StatusBadge value={node.admission.telemetry_fresh ? "ready" : "degraded"} label={node.admission.telemetry_fresh ? "Fresh" : "Stale"} /></div>
          <div className="detail-grid">
            <DetailItem label="Provider version" value={runtime.provider_version ?? "Unavailable"} />
            <DetailItem label="KV cache usage" value={formatPercent(runtime.kv_cache_usage)} />
            <DetailItem label="Exact KV directory" value={node.exact_kv_authoritative ? "Authoritative" : "Approximate"} />
            <DetailItem label="Exact KV blocks" value={node.exact_kv_blocks.toLocaleString()} />
            <DetailItem label="Waiting watermark" value={node.config.provider.waiting_threshold} />
            <DetailItem label="Telemetry updated" value={formatTimestamp(runtime.provider_telemetry_updated_unix_ms)} />
          </div>
        </section>}

        <section className="detail-section">
          <div className="detail-section-heading"><h3>Configuration</h3><span className="revision-label">Revision {node.revision}</span></div>
          <div className="detail-grid single">
            <DetailItem label="Base URL" value={node.config.base_url} />
            <DetailItem label="Health path" value={node.config.health_path} />
            <DetailItem label="Updated" value={formatTimestamp(node.updated_at_unix_ms)} />
          </div>
          <div className="mapping-list">
            <span className="subsection-label">Model mappings</span>
            {Object.entries(node.config.models).map(([publicModel, upstreamModel]) => (
              <div key={publicModel}><code>{publicModel}</code><span>to</span><code>{upstreamModel}</code></div>
            ))}
          </div>
        </section>
      </div>

      <footer className="drawer-footer">
        <button type="button" className="secondary-button" disabled={busy} onClick={onEdit}><Edit3 size={16} />Edit</button>
        <button type="button" className="secondary-button" disabled={busy} onClick={onToggleDrain}>
          {runtime.lifecycle === "serving" ? <ChevronDown size={16} /> : <Activity size={16} />}
          {runtime.lifecycle === "serving" ? "Drain" : "Resume"}
        </button>
        <span className="footer-spacer" />
        <button type="button" className="danger-text-button" disabled={busy} onClick={onDelete}><Trash2 size={16} />Delete</button>
      </footer>
    </Drawer>
  );
}
