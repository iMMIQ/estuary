import type { NodeConfig, NodeRecord } from "./types";

interface NodeListResponse {
  nodes: NodeRecord[];
}

interface ErrorEnvelope {
  error?: { message?: string };
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
    throw new Error(body.error?.message || `Request failed with HTTP ${response.status}`);
  }
  return (await response.json()) as T;
}

export async function listNodes(): Promise<NodeRecord[]> {
  return (await request<NodeListResponse>("/admin/api/nodes")).nodes;
}

export function createNode(config: NodeConfig): Promise<NodeRecord> {
  return request("/admin/api/nodes", {
    method: "POST",
    body: JSON.stringify(config),
  });
}

export function updateNode(config: NodeConfig, revision: number): Promise<NodeRecord> {
  return request(`/admin/api/nodes/${encodeURIComponent(config.id)}?timeout_ms=30000`, {
    method: "PUT",
    body: JSON.stringify({ config, revision }),
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
