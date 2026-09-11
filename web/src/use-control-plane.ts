import { useCallback, useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import * as api from "./api";
import type { GatewayStatus, NodeRecord } from "./types";

export function useControlPlane() {
  const { t } = useTranslation();
  const [nodes, setNodes] = useState<NodeRecord[]>([]);
  const [status, setStatus] = useState<GatewayStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [connectionError, setConnectionError] = useState<string | null>(null);
  const [lastSync, setLastSync] = useState<number | null>(null);
  const pending = useRef<AbortController | null>(null);

  const refresh = useCallback(async (quiet = false) => {
    // Explicit refreshes must read changes made after an older request started.
    pending.current?.abort();
    const controller = new AbortController();
    pending.current = controller;
    if (!quiet) setRefreshing(true);
    const [nodeResult, statusResult] = await Promise.allSettled([
      api.listNodes(controller.signal), api.getStatus(controller.signal),
    ]);
    if (controller.signal.aborted) return;
    pending.current = null;
    if (nodeResult.status === "fulfilled") setNodes(nodeResult.value);
    if (statusResult.status === "fulfilled") setStatus(statusResult.value);
    const failure = nodeResult.status === "rejected" ? nodeResult : statusResult.status === "rejected" ? statusResult : null;
    setConnectionError(failure ? failure.reason instanceof Error ? failure.reason.message : t("controlPlane.unavailable") : null);
    if (!failure) setLastSync(Date.now());
    setLoading(false);
    setRefreshing(false);
  }, [t]);

  useEffect(() => {
    void refresh(true);
    const interval = window.setInterval(() => {
      // A slow response remains useful; polling must not supersede it.
      if (!pending.current) void refresh(true);
    }, 5000);
    return () => {
      window.clearInterval(interval);
      pending.current?.abort();
      pending.current = null;
    };
  }, [refresh]);

  return { nodes, status, loading, refreshing, connectionError, lastSync, refresh };
}
