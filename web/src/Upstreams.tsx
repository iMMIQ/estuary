import { Button, Menu, Pagination, TextInput } from "@mantine/core";
import {
  Edit3,
  ExternalLink,
  LoaderCircle,
  MoreHorizontal,
  PauseCircle,
  Play,
  Plus,
  RefreshCw,
  Search,
  Server,
  Trash2,
} from "lucide-react";
import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  formatRate,
  formatRatio,
  ProgressValue,
  relativeSync,
  VllmRuntimePanel,
} from "./admin-metrics";
import { formatCompactNumber } from "./node-config";
import type { NodeRecord } from "./types";
import { StatusBadge } from "./ui";
export type NodeFilter = "all" | "accepting" | "attention" | "draining" | "not_ready";

export function Upstreams({
  nodes,
  loading,
  busy,
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
  busy: boolean;
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
    return nodes
      .filter((node) => {
        const textMatch =
          !needle ||
          [
            node.config.id,
            node.config.base_url,
            ...Object.keys(node.config.models),
            ...Object.values(node.config.models),
          ].some((value) => value.toLowerCase().includes(needle));
        const filterMatch =
          filter === "all" ||
          (filter === "accepting" && node.admission.accepting_assignments) ||
          (filter === "attention" &&
            !node.admission.accepting_assignments &&
            node.admission.state !== "at_capacity") ||
          (filter === "draining" && node.runtime.lifecycle === "draining") ||
          (filter === "not_ready" &&
            !node.admission.routable &&
            node.runtime.lifecycle !== "draining");
        return textMatch && filterMatch;
      })
      .sort((left, right) => left.config.id.localeCompare(right.config.id));
  }, [filter, nodes, query]);
  const pages = Math.max(1, Math.ceil(filtered.length / pageSize));
  const safePage = Math.min(page, pages);
  const visible = filtered.slice((safePage - 1) * pageSize, safePage * pageSize);
  const counts = {
    all: nodes.length,
    accepting: nodes.filter((node) => node.admission.accepting_assignments).length,
    attention: nodes.filter(
      (node) => !node.admission.accepting_assignments && node.admission.state !== "at_capacity",
    ).length,
    draining: nodes.filter((node) => node.runtime.lifecycle === "draining").length,
    not_ready: nodes.filter(
      (node) => !node.admission.routable && node.runtime.lifecycle !== "draining",
    ).length,
  };

  const changeFilter = (value: NodeFilter) => {
    setPage(1);
    onFilter(value);
  };

  return (
    <div className="page-frame upstreams-page">
      <header className="page-title-row upstream-title">
        <div>
          <h1>{t("nav.upstreams")}</h1>
          <span>
            {t("upstreams.summary", { count: nodes.length, sync: relativeSync(lastSync, t) })}
          </span>
        </div>
        <div className="heading-actions">
          <Button
            variant="default"
            leftSection={<RefreshCw className={refreshing ? "spin" : ""} size={14} />}
            disabled={refreshing}
            onClick={onRefresh}
          >
            {t("common.refresh")}
          </Button>
          <Button leftSection={<Plus size={14} />} onClick={onAdd}>
            {t("upstreams.addNode")}
          </Button>
        </div>
      </header>

      <VllmRuntimePanel nodes={nodes} compact />

      <TextInput
        className="node-search"
        leftSection={<Search size={14} />}
        placeholder={t("upstreams.searchPlaceholder")}
        aria-label={t("upstreams.searchLabel")}
        value={query}
        onChange={(event) => {
          setPage(1);
          onQuery(event.target.value);
        }}
      />
      <fieldset className="filter-row" aria-label={t("upstreams.filterLabel")}>
        {(["all", "accepting", "attention", "draining", "not_ready"] as NodeFilter[]).map(
          (value) => (
            <button
              type="button"
              key={value}
              className={filter === value ? `active ${value}` : ""}
              onClick={() => changeFilter(value)}
            >
              {t(`filter.${value}`)} ({counts[value]})
            </button>
          ),
        )}
      </fieldset>

      <section className="upstream-table-shell">
        {loading ? (
          <div className="empty-state">
            <LoaderCircle className="spin" size={20} />
            {t("upstreams.loading")}
          </div>
        ) : visible.length === 0 ? (
          <div className="empty-state">
            <Server size={22} />
            <strong>
              {query || filter !== "all" ? t("upstreams.noMatch") : t("upstreams.empty")}
            </strong>
            {!query && filter === "all" && (
              <Button leftSection={<Plus size={14} />} onClick={onAdd}>
                {t("upstreams.addNode")}
              </Button>
            )}
          </div>
        ) : (
          <div className="table-scroll">
            <table className="upstream-table">
              <thead>
                <tr>
                  <th>{t("upstreams.nodeUrl")}</th>
                  <th>{t("upstreams.statusAdmission")}</th>
                  <th>{t("upstreams.providerTelemetry")}</th>
                  <th>{t("upstreams.engine")}</th>
                  <th>{t("upstreams.tokenRate")}</th>
                  <th>{t("upstreams.activeLimit")}</th>
                  <th>{t("upstreams.kvCache")}</th>
                  <th>{t("upstreams.latency")}</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {visible.map((node) => {
                  const telemetry = node.admission.telemetry_fresh;
                  const running =
                    node.config.provider.type === "vllm"
                      ? telemetry
                        ? (node.runtime.upstream_running ?? "--")
                        : "--"
                      : node.runtime.active;
                  const waiting =
                    node.config.provider.type === "vllm"
                      ? telemetry
                        ? (node.runtime.upstream_waiting ?? "--")
                        : "--"
                      : "--";
                  return (
                    <tr
                      key={node.config.id}
                      tabIndex={0}
                      onClick={() => onSelect(node)}
                      onKeyDown={(event) => {
                        if (event.key === "Enter") onSelect(node);
                      }}
                    >
                      <td data-label={t("upstreams.node")}>
                        <strong>{node.config.id}</strong>
                        <span>{node.config.base_url}</span>
                      </td>
                      <td data-label={t("upstreams.admission")}>
                        <StatusBadge value={node.admission.state} />
                        <span>
                          {t(`admission.${node.admission.state}`, {
                            defaultValue: node.admission.reason,
                          })}
                        </span>
                      </td>
                      <td data-label={t("upstreams.provider")}>
                        <strong>
                          {node.config.provider.type === "vllm"
                            ? `vLLM ${node.runtime.provider_version ?? t("upstreams.checking")}`
                            : t("upstreams.openaiCompatible")}
                        </strong>
                        <span>
                          {node.config.provider.type === "vllm"
                            ? node.admission.telemetry_fresh
                              ? t("upstreams.telemetryFresh")
                              : t("upstreams.telemetryStale")
                            : t("upstreams.genericProvider")}
                        </span>
                      </td>
                      <td data-label={t("upstreams.engine")}>
                        <strong>
                          {running} / {waiting}
                        </strong>
                        <span>{t("upstreams.runningWaiting")}</span>
                      </td>
                      <td data-label={t("upstreams.tokenRateShort")}>
                        <strong>
                          {formatRate(
                            telemetry ? node.runtime.prompt_tokens_per_second : null,
                            locale,
                          )}{" "}
                          /{" "}
                          {formatRate(
                            telemetry ? node.runtime.generation_tokens_per_second : null,
                            locale,
                          )}
                        </strong>
                        <span>{t("upstreams.promptGeneration")}</span>
                      </td>
                      <td data-label={t("upstreams.localLoad")}>
                        <ProgressValue
                          value={node.runtime.active}
                          total={node.runtime.max_concurrency}
                        />
                      </td>
                      <td data-label={t("upstreams.kvCacheShort")}>
                        <strong>
                          {node.config.provider.type === "vllm"
                            ? t("upstreams.used", {
                                value: formatRatio(telemetry ? node.runtime.kv_cache_usage : null),
                              })
                            : "--"}
                        </strong>
                        <span>
                          {node.config.provider.type === "vllm"
                            ? t("upstreams.kvDetail", {
                                hit: formatRatio(
                                  telemetry ? node.runtime.prefix_cache_hit_rate : null,
                                ),
                                blocks: formatCompactNumber(node.exact_kv_blocks, locale),
                                mode: node.exact_kv_authoritative
                                  ? t("upstreams.synced")
                                  : t("upstreams.fallback"),
                              })
                            : t("upstreams.noTelemetry")}
                        </span>
                      </td>
                      <td data-label={t("upstreams.latency")}>
                        <strong>
                          {node.runtime.ttft_ewma_ms == null
                            ? "--"
                            : `${Math.round(node.runtime.ttft_ewma_ms)} ms`}
                        </strong>
                        <span>
                          {t("scheduler.ttft")} · {t("upstreams.headerEwma")}:{" "}
                          {Math.round(node.runtime.latency_ewma_ms)} ms
                        </span>
                      </td>
                      <td
                        className="row-menu-cell"
                        onClick={(event) => event.stopPropagation()}
                        onKeyDown={(event) => event.stopPropagation()}
                      >
                        <Menu position="bottom-end" shadow="md" width={170} withinPortal>
                          <Menu.Target>
                            <button
                              type="button"
                              className="bare-icon"
                              aria-label={t("upstreams.actionsFor", { id: node.config.id })}
                            >
                              <MoreHorizontal size={16} />
                            </button>
                          </Menu.Target>
                          <Menu.Dropdown>
                            <Menu.Item
                              leftSection={<ExternalLink size={13} />}
                              onClick={() => onSelect(node)}
                            >
                              {t("upstreams.viewDetails")}
                            </Menu.Item>
                            <Menu.Item
                              disabled={busy}
                              leftSection={<Edit3 size={13} />}
                              onClick={() => onEdit(node)}
                            >
                              {t("common.edit")}
                            </Menu.Item>
                            <Menu.Item
                              disabled={busy}
                              leftSection={
                                node.runtime.lifecycle === "serving" ? (
                                  <PauseCircle size={13} />
                                ) : (
                                  <Play size={13} />
                                )
                              }
                              onClick={() => onToggleDrain(node)}
                            >
                              {node.runtime.lifecycle === "serving"
                                ? t("upstreams.drain")
                                : t("upstreams.resume")}
                            </Menu.Item>
                            <Menu.Divider />
                            <Menu.Item
                              disabled={busy}
                              color="red"
                              leftSection={<Trash2 size={13} />}
                              onClick={() => onDelete(node)}
                            >
                              {t("common.delete")}
                            </Menu.Item>
                          </Menu.Dropdown>
                        </Menu>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </section>
      <footer className="table-footer">
        <span>
          {t("upstreams.showing", {
            from: visible.length ? (safePage - 1) * pageSize + 1 : 0,
            to: Math.min(safePage * pageSize, filtered.length),
            total: filtered.length,
          })}
        </span>
        {pages > 1 && <Pagination total={pages} value={safePage} onChange={setPage} size="xs" />}
      </footer>
    </div>
  );
}
