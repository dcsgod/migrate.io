import axios from 'axios';

const BASE = import.meta.env.VITE_API_URL || 'http://localhost:8000';
const WS_BASE = import.meta.env.VITE_WS_URL || 'ws://localhost:8000';

export const api = axios.create({
  baseURL: `${BASE}/api/v1`,
  headers: { 'Content-Type': 'application/json' },
});

// ── Auth token injection ─────────────────────────────────────
api.interceptors.request.use((config) => {
  const token = localStorage.getItem('migrate_io_token');
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

// ── Connections ──────────────────────────────────────────────
export const connectionsApi = {
  types: () => api.get('/connections/types'),
  create: (body: { connector_type: string; name: string; config: Record<string, unknown> }) =>
    api.post('/connections/', body),
  status: (id: string) => api.get(`/connections/${id}/status`),
  objects: (id: string) => api.get(`/connections/${id}/objects`),
  schema: (id: string, objectId: string) => api.get(`/connections/${id}/schema/${objectId}`),
  delete: (id: string) => api.delete(`/connections/${id}`),
};

// ── Graph ─────────────────────────────────────────────────────
export const graphApi = {
  build: (body: { source_connection_id: string; dest_connection_id: string; run_inference?: boolean }) =>
    api.post('/graph/build', body),
  get: (id: string) => api.get(`/graph/${id}`),
  nodes: (id: string, connectorId?: string) =>
    api.get(`/graph/${id}/nodes`, { params: { connector_id: connectorId } }),
  edges: (id: string, minConfidence?: number) =>
    api.get(`/graph/${id}/edges`, { params: { min_confidence: minConfidence } }),
  drift: (id: string) => api.get(`/graph/${id}/drift`),
};

// ── Commands ─────────────────────────────────────────────────
export const commandsApi = {
  submit: (body: { graph_id: string; nl_command: string }) => api.post('/commands/', body),
  get: (id: string) => api.get(`/commands/${id}`),
  explain: (id: string) => api.get(`/commands/${id}/explain`),
  confirmMapping: (intentId: string, body: { entity_name: string; confirmed_node_id: string }) =>
    api.post(`/commands/${intentId}/confirm-mapping`, body),
};

// ── DAG ───────────────────────────────────────────────────────
export const dagApi = {
  build: (body: { graph_id: string; intent_id: string }) => api.post('/dag/build', body),
  get: (id: string) => api.get(`/dag/${id}`),
  validate: (id: string) => api.get(`/dag/${id}/validate`),
  compiled: (id: string, target: 'spark' | 'sql' = 'spark') =>
    api.get(`/dag/${id}/compiled`, { params: { target } }),
  patch: (id: string, body: { op: string; node_id?: string; params: Record<string, unknown> }) =>
    api.patch(`/dag/${id}`, body),
};

// ── Preview / Runs ────────────────────────────────────────────
export const previewApi = {
  run: (body: { dag_id: string; dry_run?: boolean; preview_rows?: number }) =>
    api.post('/preview/run', body),
  runStatus: (runId: string) => api.get(`/preview/runs/${runId}`),
  preview: (runId: string, limit?: number) =>
    api.get(`/preview/runs/${runId}/preview`, { params: { limit } }),
};

// ── Commit ────────────────────────────────────────────────────
export const commitApi = {
  approve: (body: { run_id: string; destination_path: string; mode?: string; merge_keys?: string[] }) =>
    api.post('/commit/approve', body),
  reject: (body: { run_id: string; reason?: string }) =>
    api.post('/commit/reject', body),
};

// ── Plans ─────────────────────────────────────────────────────
export const plansApi = {
  save: (body: { run_id: string; dag_id: string; label?: string; tags?: string[] }) =>
    api.post('/plans/', body),
  list: (limit?: number) => api.get('/plans/', { params: { limit } }),
  get: (id: string) => api.get(`/plans/${id}`),
  rerun: (id: string) => api.post(`/plans/${id}/rerun`),
};

// ── WebSocket factory ─────────────────────────────────────────
export const createLogSocket = (runId: string) =>
  new WebSocket(`${WS_BASE}/api/v1/preview/runs/${runId}/log`);
