import { useState } from 'react';
import { previewApi } from '../../api/client';
import type { MigrationState } from '../../pages/MigrationPage';

interface Props { state: MigrationState; onDone: () => void; }

export function PreviewGrid({ state, onDone }: Props) {
  const [preview, setPreview] = useState<any>(null);
  const [loading, setLoading] = useState(false);

  const loadPreview = async () => {
    if (!state.runId) return;
    setLoading(true);
    try {
      const res = await previewApi.preview(state.runId, 100);
      setPreview(res.data);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{ maxWidth: 900 }}>
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ fontSize: 22, marginBottom: 6 }}>👁️ Preview DataFrame</h1>
        <p style={{ color: 'var(--text-muted)', fontSize: 14 }}>
          Sampled result from staging — real materialized data, not a query plan.
          Schema diff shows what will change vs the current destination.
        </p>
      </div>

      <div className="flex gap-3" style={{ marginBottom: 20 }}>
        <button className="btn btn-primary" onClick={loadPreview} disabled={loading || !state.runId}>
          {loading ? '⏳ Loading…' : '👁 Load Preview'}
        </button>
        {preview && <button className="btn btn-success" onClick={onDone}>Approve / Reject →</button>}
      </div>

      {preview && (
        <>
          <div className="grid-3" style={{ marginBottom: 20 }}>
            {[
              { label: 'Rows previewed', value: preview.rows?.length || 0, color: 'var(--accent-primary)' },
              { label: 'Columns', value: preview.schema?.length || 0, color: 'var(--step-compile)' },
              { label: 'Schema changes', value: (preview.schema_diff?.added?.length || 0) + (preview.schema_diff?.removed?.length || 0) + (preview.schema_diff?.type_changed?.length || 0), color: 'var(--accent-warning)' },
            ].map(s => (
              <div key={s.label} className="glass-card" style={{ textAlign: 'center', padding: 16 }}>
                <div style={{ fontSize: 24, fontWeight: 800, color: s.color }}>{s.value}</div>
                <div style={{ fontSize: 12, color: 'var(--text-muted)' }}>{s.label}</div>
              </div>
            ))}
          </div>

          {/* ── Schema diff ───────────────────────────────── */}
          {(preview.schema_diff?.added?.length > 0 || preview.schema_diff?.removed?.length > 0 || preview.schema_diff?.type_changed?.length > 0) && (
            <div className="glass-card" style={{ marginBottom: 16 }}>
              <div style={{ fontSize: 12, fontWeight: 600, color: 'var(--accent-warning)', marginBottom: 8 }}>⚠ Schema Diff</div>
              {preview.schema_diff?.added?.map((c: string) => <div key={c} style={{ fontSize: 12, color: '#6ee7b7' }}>+ {c} (new column)</div>)}
              {preview.schema_diff?.removed?.map((c: string) => <div key={c} style={{ fontSize: 12, color: '#fca5a5' }}>− {c} (removed)</div>)}
              {preview.schema_diff?.type_changed?.map((c: string) => <div key={c} style={{ fontSize: 12, color: '#fcd34d' }}>~ {c} (type changed)</div>)}
            </div>
          )}

          {/* ── Data grid ─────────────────────────────────── */}
          {preview.rows?.length > 0 ? (
            <div className="glass-card" style={{ padding: 0, overflow: 'auto' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12, fontFamily: 'var(--font-mono)' }}>
                <thead>
                  <tr>
                    {preview.schema?.map((col: any) => (
                      <th key={col.name} style={{ padding: '8px 12px', textAlign: 'left', background: 'rgba(0,0,0,0.3)', color: 'var(--text-secondary)', borderBottom: '1px solid var(--border-subtle)', whiteSpace: 'nowrap' }}>
                        {col.name}
                        <div style={{ fontSize: 10, color: 'var(--text-muted)', fontWeight: 400 }}>{col.dtype}</div>
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {preview.rows.map((row: any, i: number) => (
                    <tr key={i} style={{ borderBottom: '1px solid var(--border-subtle)' }}
                      onMouseEnter={e => (e.currentTarget.style.background = 'var(--bg-hover)')}
                      onMouseLeave={e => (e.currentTarget.style.background = 'transparent')}>
                      {preview.schema?.map((col: any) => (
                        <td key={col.name} style={{ padding: '6px 12px', color: 'var(--text-secondary)', maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                          {String(row[col.name] ?? '—')}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="glass-card">
              <div className="empty-state">
                <div className="empty-state-icon">📊</div>
                <div className="empty-state-title">Preview available after real Spark execution</div>
                <div className="empty-state-desc">Connect a real Databricks workspace to see actual data rows here.</div>
              </div>
            </div>
          )}
        </>
      )}
    </div>
  );
}
