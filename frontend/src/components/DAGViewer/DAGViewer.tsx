import { useState } from 'react';
import { dagApi } from '../../api/client';
import type { MigrationState } from '../../pages/MigrationPage';

interface Props {
  state: MigrationState;
  setState: React.Dispatch<React.SetStateAction<MigrationState>>;
  onDone: () => void;
}

export function DAGViewer({ state, setState, onDone }: Props) {
  const [dag, setDag] = useState<any>(null);
  const [validation, setValidation] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const buildDag = async () => {
    if (!state.graphId || !state.intentId) return;
    setLoading(true);
    setError('');
    try {
      const res = await dagApi.build({ graph_id: state.graphId, intent_id: state.intentId });
      setDag(res.data.dag);
      setValidation(res.data.validation);
      setState(s => ({ ...s, dagId: res.data.dag_id }));
    } catch (e: any) {
      setError(e.response?.data?.detail || 'DAG build failed');
    } finally {
      setLoading(false);
    }
  };

  const NODE_COLORS: Record<string, string> = {
    read: 'var(--step-connect)',
    filter: 'var(--accent-warning)',
    join: 'var(--step-graph)',
    transform: 'var(--accent-primary)',
    quality_gate: 'var(--accent-info)',
    select: 'var(--accent-cyan)',
    write: 'var(--step-compile)',
  };

  return (
    <div style={{ maxWidth: 900 }}>
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ fontSize: 22, marginBottom: 6 }}>📊 Logical DAG</h1>
        <p style={{ color: 'var(--text-muted)', fontSize: 14 }}>
          Engine-agnostic directed acyclic graph. Each node is a pipeline operation.
          Edit join keys, filters, or transforms before compiling.
        </p>
      </div>

      <div className="flex gap-3" style={{ marginBottom: 20 }}>
        <button className="btn btn-primary" onClick={buildDag} disabled={loading || !state.intentId}>
          {loading ? '⏳ Building…' : '🔨 Build DAG'}
        </button>
        {dag && <button className="btn btn-success" onClick={onDone}>Compile to Spark →</button>}
      </div>

      {error && <div style={{ color: 'var(--accent-error)', fontSize: 13, marginBottom: 12 }}>⚠ {error}</div>}

      {/* ── Validation summary ────────────────────────────── */}
      {validation && (
        <div className="glass-card" style={{ marginBottom: 20, border: `1px solid ${validation.valid ? 'rgba(16,185,129,0.3)' : 'rgba(239,68,68,0.3)'}`, background: validation.valid ? 'rgba(16,185,129,0.04)' : 'rgba(239,68,68,0.04)' }}>
          <div style={{ fontSize: 13, fontWeight: 600, color: validation.valid ? 'var(--accent-success)' : 'var(--accent-error)', marginBottom: 8 }}>
            {validation.valid ? '✅ Validation passed' : '❌ Validation errors'} — {validation.warning_count} warnings
          </div>
          {validation.issues?.map((issue: any, i: number) => (
            <div key={i} style={{ fontSize: 12, color: issue.severity === 'error' ? 'var(--accent-error)' : 'var(--accent-warning)', marginBottom: 4 }}>
              [{issue.severity.toUpperCase()}] {issue.message}
            </div>
          ))}
        </div>
      )}

      {/* ── Node list ─────────────────────────────────────── */}
      {dag && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
          {dag.nodes.map((node: any, i: number) => (
            <div key={node.id} className="glass-card" style={{
              padding: '12px 16px',
              border: `1px solid ${NODE_COLORS[node.type] || 'var(--border-default)'}30`,
              background: `${NODE_COLORS[node.type] || 'var(--accent-primary)'}08`,
              display: 'flex', alignItems: 'center', gap: 16,
            }}>
              <div style={{
                width: 32, height: 32, borderRadius: 8,
                background: `${NODE_COLORS[node.type] || 'var(--accent-primary)'}20`,
                border: `1px solid ${NODE_COLORS[node.type] || 'var(--accent-primary)'}40`,
                display: 'flex', alignItems: 'center', justifyContent: 'center',
                fontSize: 12, fontWeight: 700, color: NODE_COLORS[node.type] || 'var(--accent-primary)',
                flexShrink: 0,
              }}>
                {i + 1}
              </div>
              <div style={{ flex: 1 }}>
                <div style={{ fontSize: 13, fontWeight: 600 }}>{node.label || node.type}</div>
                <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>
                  {node.type.toUpperCase()} · {node.id.slice(0, 12)}…
                  {node.rows_out !== null && node.rows_out !== undefined && ` · ${node.rows_out.toLocaleString()} rows`}
                </div>
              </div>
              <span className="badge badge-info" style={{ fontSize: 10 }}>{node.type}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
