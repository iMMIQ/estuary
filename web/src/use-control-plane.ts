import { useCallback, useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import * as api from "./api";
import type { Observability } from "./observability-types";
import type { GatewayStatus, NodeRecord } from "./types";

export function useControlPlane(metricsQuery = "scope=fleet&window=300") {
  const { t } = useTranslation();
  const [nodes, setNodes] = useState<NodeRecord[]>([]);
  const [status, setStatus] = useState<GatewayStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [connectionError, setConnectionError] = useState<string | null>(null);
  const [lastSync, setLastSync] = useState<number | null>(null);
  const pending = useRef<AbortController | null>(null);
  const failures = useRef(0);
  const nextPoll = useRef(0);
  const [nodesStale, setNodesStale] = useState(false);
  const [statusStale, setStatusStale] = useState(false);
  const [nodesLoaded, setNodesLoaded] = useState(false);
  const [observability, setObservability] = useState<Observability | null>(null);
  const [metricsStale, setMetricsStale] = useState(false);

  const refresh = useCallback(
    async (quiet = false) => {
      // Explicit refreshes must read changes made after an older request started.
      pending.current?.abort();
      const controller = new AbortController();
      pending.current = controller;
      if (!quiet) setRefreshing(true);
      const [nodeResult, statusResult, metricsResult] = await Promise.allSettled([
        api.listNodes(controller.signal),
        api.getStatus(controller.signal),
        api.getObservability(metricsQuery, controller.signal),
      ]);
      if (controller.signal.aborted) return;
      pending.current = null;
      if (nodeResult.status === "fulfilled") {
        setNodes(nodeResult.value);
        setNodesLoaded(true);
      }
      if (statusResult.status === "fulfilled") setStatus(statusResult.value);
      if (metricsResult.status === "fulfilled") setObservability(metricsResult.value);
      setMetricsStale(metricsResult.status === "rejected");
      setNodesStale(nodeResult.status === "rejected");
      setStatusStale(statusResult.status === "rejected");
      const failure =
        nodeResult.status === "rejected"
          ? nodeResult
          : statusResult.status === "rejected"
            ? statusResult
            : null;
      setConnectionError(
        failure
          ? failure.reason instanceof Error
            ? failure.reason.message
            : t("controlPlane.unavailable")
          : null,
      );
      if (!failure) setLastSync(Date.now());
      failures.current = failure ? failures.current + 1 : 0;
      nextPoll.current = failure
        ? Date.now() + Math.min(60000, 5000 * 2 ** Math.min(failures.current, 4))
        : 0;
      setLoading(false);
      setRefreshing(false);
    },
    [t, metricsQuery],
  );

  useEffect(() => {
    void refresh(true);
    const interval = window.setInterval(() => {
      // A slow response remains useful; polling must not supersede it.
      if (
        !pending.current &&
        document.visibilityState === "visible" &&
        Date.now() >= nextPoll.current
      )
        void refresh(true);
    }, 5000);
    const resume = () => {
      if (document.visibilityState === "visible" && !pending.current) void refresh(true);
    };
    document.addEventListener("visibilitychange", resume);
    return () => {
      document.removeEventListener("visibilitychange", resume);
      window.clearInterval(interval);
      pending.current?.abort();
      pending.current = null;
    };
  }, [refresh]);

  return {
    nodes,
    status,
    loading,
    refreshing,
    connectionError,
    lastSync,
    refresh,
    nodesStale,
    statusStale,
    nodesLoaded,
    observability,
    metricsStale,
  };
}
