import type { GatewayStatus, NodeConfig, NodeRecord, PreflightResponse } from "./types";

interface NodeListResponse {
  nodes: NodeRecord[];
}

interface ErrorEnvelope {
  error?: { message?: string; code?: string };
}

export class ApiError extends Error {
  constructor(
    message: string,
    public readonly status: number,
    public readonly code: string | null,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, {
    ...init,
    headers: init?.body
      ? { "content-type": "application/json", ...init.headers }
      : init?.headers,
  });
  if (!response.ok) {
    const body = (await response.json().catch(() => ({}))) as ErrorEnvelope;
    throw new ApiError(
      body.error?.message || `Request failed with HTTP ${response.status}`,
      response.status,
      body.error?.code ?? null,
    );
  }
  return (await response.json()) as T;
}

export async function listNodes(): Promise<NodeRecord[]> {
  return (await request<NodeListResponse>("/admin/api/nodes")).nodes;
}

export function getStatus(): Promise<GatewayStatus> {
  return request("/admin/api/status");
}

export function preflightNode(config: NodeConfig, clearApiKey = false): Promise<PreflightResponse> {
  const query = clearApiKey ? "?clear_api_key=true" : "";
  return request(`/admin/api/nodes/preflight${query}`, {
    method: "POST",
    body: JSON.stringify(config),
  });
}

export function createNode(config: NodeConfig): Promise<NodeRecord> {
  return request("/admin/api/nodes", {
    method: "POST",
    body: JSON.stringify(config),
  });
}

export function updateNode(config: NodeConfig, revision: number, clearApiKey: boolean): Promise<NodeRecord> {
  return request(`/admin/api/nodes/${encodeURIComponent(config.id)}?timeout_ms=30000`, {
    method: "PUT",
    body: JSON.stringify({ config, revision, clear_api_key: clearApiKey }),
  });
}

export function deleteNode(id: string, revision: number): Promise<{ deleted: boolean }> {
  return request(
    `/admin/api/nodes/${encodeURIComponent(id)}?revision=${revision}&timeout_ms=30000`,
    { method: "DELETE" },
  );
}

export function setDraining(id: string, draining: boolean): Promise<unknown> {
  return request(`/admin/api/nodes/${encodeURIComponent(id)}/drain`, {
    method: draining ? "PUT" : "DELETE",
  });
}
