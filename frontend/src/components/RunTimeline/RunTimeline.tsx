import { useState, useEffect } from 'react';
import { previewApi } from '../../api/client';
import type { MigrationState } from '../../pages/MigrationPage';

interface Props { state: MigrationState; setState: React.Dispatch<React.SetStateAction<MigrationState>>; onDone: () => void; }

export function RunTimeline({ state, setState, onDone }: Props) {
  const [runStatus, setRunStatus] = useState<any>(null);
  const [steps, setSteps] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!state.runId) return;
    loadStatus();
  }, [state.runId]);

  const loadStatus = async () => {
    if (!state.runId) return;
    setLoading(true);
    try {
      const res = await previewApi.runStatus(state.runId);
      setRunStatus(res.data);
      setSteps(res.data.steps || []);
    } finally {
      setLoading(false);
    }
  };

  const statusColor: Record<string, string> = {
    pending: 'var(--text-muted)', running: 'var(--accent-primary)',
    done: 'var(--accent-success)', staged: 'var(--accent-cyan)',
    committed: 'var(--accent-success)', rejected: 'var(--accent-error)',
    error: 'var(--accent-error)',
  };

  return (
    <div style={{ maxWidth: 800 }}>
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ fontSize: 22, marginBottom: 6 }}>▶️ Staged Run</h1>
        <p style={{ color: 'var(--text-muted)', fontSize: 14 }}>
          Execution timeline with row counts in/out per step. Data is written to staging — not production — until you approve.
        </p>
      </div>

      {runStatus && (
        <div className="glass-card" style={{ marginBottom: 20 }}>
          <div className="flex items-center justify-between">
            <div>
              <div style={{ fontSize: 14, fontWeight: 600, marginBottom: 4 }}>Run {state.runId?.slice(0, 8)}…</div>
              <div style={{ fontSize: 12, color: 'var(--text-muted)' }}>DAG: {runStatus.dag_id?.slice(0, 8)}…</div>
            </div>
            <span className="badge" style={{
              background: `${statusColor[runStatus.status] || 'var(--accent-info)'}20`,
              color: statusColor[runStatus.status] || 'var(--accent-info)',
              border: `1px solid ${statusColor[runStatus.status] || 'var(--accent-info)'}40`,
              fontSize: 13, padding: '4px 12px',
            }}>
              {runStatus.status?.toUpperCase()}
            </span>
          </div>

          {runStatus.status === 'staged' && (
            <div style={{ marginTop: 12, padding: 12, background: 'rgba(6,182,212,0.08)', borderRadius: 8, border: '1px solid rgba(6,182,212,0.2)' }}>
              <div style={{ fontSize: 13, color: 'var(--accent-cyan)', fontWeight: 600 }}>
                ✓ Staged and ready for preview
              </div>
              <div style={{ fontSize: 12, color: 'var(--text-muted)', marginTop: 4 }}>
                Data is in staging. Review the preview then approve or reject.
              </div>
            </div>
          )}
        </div>
      )}

      {/* ── Step timeline ─────────────────────────────────── */}
      {steps.length > 0 && (
        <div className="glass-card" style={{ marginBottom: 20 }}>
          <div style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-muted)', marginBottom: 12, textTransform: 'uppercase', letterSpacing: '0.06em' }}>
            Execution Steps
          </div>
          <ul className="timeline">
            {steps.map((step: any) => (
              <li key={step.step_id} className="timeline-item">
                <div className="timeline-dot" style={{ background: `${statusColor[step.status] || 'var(--text-muted)'}20`, color: statusColor[step.status] || 'var(--text-muted)' }}>
                  {step.status === 'done' ? '✓' : step.status === 'error' ? '✗' : '○'}
                </div>
                <div className="timeline-body">
                  <div className="timeline-title">{step.name}</div>
                  <div className="timeline-meta">
                    {step.duration_ms && `${step.duration_ms}ms`}
                    {step.rows_in != null && ` · ${step.rows_in.toLocaleString()} in`}
                    {step.rows_out != null && ` → ${step.rows_out.toLocaleString()} out`}
                    {step.error && <span style={{ color: 'var(--accent-error)' }}> — {step.error}</span>}
                  </div>
                </div>
              </li>
            ))}
          </ul>
        </div>
      )}

      {/* ── Compiled code preview ─────────────────────────── */}
      {runStatus?.compiled_code && (
        <div className="glass-card" style={{ marginBottom: 20 }}>
          <div style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-accent)', marginBottom: 8 }}>Generated Script</div>
          <pre className="code-block" style={{ maxHeight: 200 }}>{runStatus.compiled_code.slice(0, 1500)}{runStatus.compiled_code.length > 1500 ? '\n… (truncated)' : ''}</pre>
        </div>
      )}

      <button className="btn btn-success" onClick={onDone} disabled={!runStatus || runStatus.status === 'pending'}>
        View Preview →
      </button>
    </div>
  );
}
