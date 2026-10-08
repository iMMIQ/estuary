import { Alert, Badge, Button, Group, Select, Table } from "@mantine/core";
import { ArrowLeft, RefreshCw } from "lucide-react";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import * as api from "./api";
import type { LogDetail, LogPage, LogSession, LogStatus } from "./session-log-types";

function tokens(usage: unknown, key: string): string | number {
  if (!usage || typeof usage !== "object") return "—";
  const value = (usage as Record<string, unknown>)[key];
  return typeof value === "number" ? value : "—";
}

function duration(value: number | undefined) {
  return value === undefined ? "—" : `${(value / 1000).toFixed(1)} ms`;
}

export function SessionLogs() {
  const { t } = useTranslation();
  const [status, setStatus] = useState<LogStatus | null>(null);
  const [page, setPage] = useState<LogPage>({ requests: [], next_cursor: null });
  const [sessions, setSessions] = useState<LogSession[]>([]);
  const [session, setSession] = useState<string | null>(null);
  const [cursor, setCursor] = useState<string | null>(null);
  const [days, setDays] = useState("7");
  const [revision, setRevision] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [detail, setDetail] = useState<LogDetail | null>(null);
  const [expanded, setExpanded] = useState<string | null>(null);

  useEffect(() => {
    void revision;
    const controller = new AbortController();
    setLoading(true);
    setError(null);
    void (async () => {
      try {
        const health = await api.getLogStatus(controller.signal);
        if (controller.signal.aborted) return;
        setStatus(health);
        if (!health.enabled) {
          setPage({ requests: [], next_cursor: null });
          return;
        }
        const since = Date.now() - Number(days) * 86400000;
        const [requests, groups] = await Promise.all([
          api.listLogRequests(session, cursor, since, controller.signal),
          api.listLogSessions(since, controller.signal),
        ]);
        if (!controller.signal.aborted) {
          setPage(requests);
          setSessions(groups);
        }
      } catch (cause) {
        if (!controller.signal.aborted)
          setError(cause instanceof Error ? cause.message : String(cause));
      } finally {
        if (!controller.signal.aborted) setLoading(false);
      }
    })();
    return () => controller.abort();
  }, [session, cursor, days, revision]);

  useEffect(() => {
    setDetail(null);
    setExpanded(null);
    if (!selected) return;
    const controller = new AbortController();
    void api
      .getLogRequest(selected, controller.signal)
      .then((value) => {
        if (!controller.signal.aborted) setDetail(value);
      })
      .catch((cause: unknown) => {
        if (!controller.signal.aborted)
          setError(cause instanceof Error ? cause.message : String(cause));
      });
    return () => controller.abort();
  }, [selected]);

  if (selected) {
    return (
      <section className="session-logs">
        <Button
          variant="subtle"
          leftSection={<ArrowLeft size={16} />}
          onClick={() => {
            setSelected(null);
            setError(null);
          }}
        >
          {t("common.back")}
        </Button>
        <h1>{t("logs.detail")}</h1>
        {error && <Alert color="red">{error}</Alert>}
        {!detail && !error && <p role="status">{t("common.loading")}</p>}
        {detail && (
          <>
            <Group gap="sm">
              <Badge color={detail.request.outcome === "success" ? "green" : "orange"}>
                {detail.request.outcome}
              </Badge>
              <span>{detail.request.model}</span>
              <span>{detail.request.protocol}</span>
            </Group>
            <p>{detail.request.id}</p>
            <p>
              {t("logs.capture")}: {detail.request.capture_state} · {t("logs.delivery")}:{" "}
              {detail.request.delivery}
            </p>
            {detail.request.error_class && (
              <Alert color="orange">
                {detail.request.error_phase}: {detail.request.error_class}
              </Alert>
            )}
            <h2>{t("logs.timings")}</h2>
            <div className="log-timings">
              {Object.entries(detail.request.timings_us).map(([name, value]) => (
                <div key={name}>
                  <span>{t(`logs.timing.${name}`, { defaultValue: name })}</span>
                  <strong>{duration(value)}</strong>
                </div>
              ))}
            </div>
            <h2>{t("logs.attempts")}</h2>
            {detail.request.attempts.map((attempt) => (
              <article className="log-attempt" key={attempt.number}>
                <strong>
                  #{attempt.number} · {attempt.node} · {attempt.outcome}
                </strong>
                <p>
                  {attempt.endpoint} · {attempt.model} · {attempt.adapter} · HTTP{" "}
                  {attempt.http_status ?? "—"}
                </p>
                <p>
                  {t("logs.timings")}: {duration(attempt.timings_us.total)} · {attempt.error_class}{" "}
                  {attempt.retry_reason}
                </p>
                <details>
                  <summary>{t("logs.routing")}</summary>
                  <pre>{JSON.stringify(attempt.route, null, 2)}</pre>
                </details>
              </article>
            ))}
            <h2>{t("logs.content")}</h2>
            <p>{t("logs.contentNote")}</p>
            {detail.payloads.length === 0 && <p>{t("logs.noContent")}</p>}
            {detail.payloads.map((payload) => {
              const key = `${payload.stage}:${payload.attempt}`;
              return (
                <article className="log-payload" key={key}>
                  <Group justify="space-between">
                    <span>
                      {t(`logs.stage.${payload.stage}`, { defaultValue: payload.stage })}
                      {payload.attempt > 0 ? ` #${payload.attempt}` : ""} · {payload.state} ·{" "}
                      {payload.bytes_seen.toLocaleString()} B
                    </span>
                    <Button
                      variant="default"
                      size="compact-sm"
                      aria-expanded={expanded === key}
                      onClick={() => setExpanded(expanded === key ? null : key)}
                    >
                      {t(expanded === key ? "logs.hide" : "logs.expand")}
                    </Button>
                  </Group>
                  {expanded === key && <pre>{JSON.stringify(payload.content, null, 2)}</pre>}
                </article>
              );
            })}
            {detail.request.events.length > 0 && (
              <>
                <h2>{t("logs.events")}</h2>
                <ul>
                  {detail.request.events.map((event) => (
                    <li key={`${event.kind}:${event.elapsed_us}`}>
                      {event.kind} · {duration(event.elapsed_us)}
                    </li>
                  ))}
                </ul>
              </>
            )}
          </>
        )}
      </section>
    );
  }

  return (
    <section className="session-logs">
      <Group justify="space-between">
        <div>
          <h1>{t("logs.title")}</h1>
          <p>{t("logs.subtitle")}</p>
        </div>
        <Button
          variant="default"
          loading={loading}
          leftSection={<RefreshCw size={15} />}
          onClick={() => {
            setCursor(null);
            setRevision((value) => value + 1);
          }}
        >
          {t("common.refresh")}
        </Button>
      </Group>
      {error && <Alert color="red">{error}</Alert>}
      {status && !status.enabled && <Alert>{t("logs.disabled")}</Alert>}
      {status?.enabled && (
        <>
          <p role="status">
            {t(status.available ? "logs.available" : "logs.unavailable")} · {t("logs.committed")}:{" "}
            {status.committed} · {t("logs.dropped")}: {status.dropped} · {t("logs.truncated")}:{" "}
            {status.truncated}
          </p>
          <Group align="end" mb="md">
            <Select
              label={t("logs.session")}
              value={session ?? ""}
              data={[
                { value: "", label: t("logs.allSessions") },
                ...sessions.map((group) => ({
                  value: group.id,
                  label: `${group.id} (${group.requests})`,
                })),
              ]}
              onChange={(value) => {
                setSession(value || null);
                setCursor(null);
              }}
              searchable
            />
            <Select
              label={t("logs.range")}
              value={days}
              data={[
                { value: "1", label: t("logs.days", { count: 1 }) },
                { value: "7", label: t("logs.days", { count: 7 }) },
                { value: "30", label: t("logs.days", { count: 30 }) },
              ]}
              onChange={(value) => {
                setDays(value ?? "7");
                setCursor(null);
              }}
            />
          </Group>
          <div className="log-table-wrap">
            <Table className="log-table" striped highlightOnHover>
              <Table.Thead>
                <Table.Tr>
                  {(
                    ["time", "model", "session", "outcome", "latency", "tokens", "detail"] as const
                  ).map((name) => (
                    <Table.Th key={name}>{t(`logs.${name}`)}</Table.Th>
                  ))}
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {page.requests.map((item) => (
                  <Table.Tr key={item.id}>
                    <Table.Td>{new Date(item.started_at_ms).toLocaleString()}</Table.Td>
                    <Table.Td>
                      {item.model ?? "—"}
                      <small>{item.protocol}</small>
                    </Table.Td>
                    <Table.Td>{item.session_id ?? "—"}</Table.Td>
                    <Table.Td>{item.outcome}</Table.Td>
                    <Table.Td>{duration(item.timings_us.total)}</Table.Td>
                    <Table.Td>
                      {tokens(item.usage, "input_tokens")} / {tokens(item.usage, "output_tokens")}
                    </Table.Td>
                    <Table.Td>
                      <Button
                        variant="subtle"
                        size="compact-sm"
                        aria-label={`${t("logs.detail")} ${item.id}`}
                        onClick={() => {
                          setSelected(item.id);
                          setError(null);
                        }}
                      >
                        {t("common.view")}
                      </Button>
                    </Table.Td>
                  </Table.Tr>
                ))}
              </Table.Tbody>
            </Table>
          </div>
          {page.requests.length === 0 && !loading && <p>{t("logs.empty")}</p>}
          <Group mt="md">
            <Button variant="default" disabled={!cursor} onClick={() => setCursor(null)}>
              {t("logs.firstPage")}
            </Button>
            <Button
              variant="default"
              disabled={!page.next_cursor || loading}
              onClick={() => setCursor(page.next_cursor)}
            >
              {t("common.next")}
            </Button>
          </Group>
        </>
      )}
    </section>
  );
}
