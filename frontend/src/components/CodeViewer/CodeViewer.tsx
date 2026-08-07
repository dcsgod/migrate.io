import { useState } from 'react';
import { dagApi, previewApi } from '../../api/client';
import type { MigrationState } from '../../pages/MigrationPage';

interface Props {
  state: MigrationState;
  setState: React.Dispatch<React.SetStateAction<MigrationState>>;
  onDone: () => void;
}

export function CodeViewer({ state, setState, onDone }: Props) {
  const [code, setCode] = useState('');
  const [loading, setLoading] = useState(false);
  const [running, setRunning] = useState(false);
  const [copied, setCopied] = useState(false);
  const [target, setTarget] = useState<'spark' | 'sql'>('spark');
  const [error, setError] = useState('');

  const loadCode = async () => {
    if (!state.dagId) return;
    setLoading(true);
    setError('');
    try {
      const res = await dagApi.compiled(state.dagId, target);
      setCode(res.data.code);
    } catch (e: any) {
      setError(e.response?.data?.detail || 'Failed to get compiled code');
    } finally {
      setLoading(false);
    }
  };

  const startRun = async () => {
    if (!state.dagId) return;
    setRunning(true);
    try {
      const res = await previewApi.run({ dag_id: state.dagId });
      setState(s => ({ ...s, runId: res.data.run_id, compiledCode: code }));
      onDone();
    } catch (e: any) {
      setError(e.response?.data?.detail || 'Run failed');
    } finally {
      setRunning(false);
    }
  };

  const copy = () => {
    navigator.clipboard.writeText(code);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div style={{ maxWidth: 900 }}>
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ fontSize: 22, marginBottom: 6 }}>⚙️ Compiled Spark Code</h1>
        <p style={{ color: 'var(--text-muted)', fontSize: 14 }}>
          Auto-generated PySpark script. Review, copy, or submit directly to Databricks Workflows.
          This is the exact code that will execute — nothing hidden.
        </p>
      </div>

      <div className="flex items-center gap-3" style={{ marginBottom: 16 }}>
        <div className="flex items-center gap-1" style={{ background: 'var(--bg-elevated)', borderRadius: 8, padding: 3 }}>
          {(['spark', 'sql'] as const).map(t => (
            <button key={t} onClick={() => setTarget(t)}
              style={{
                padding: '4px 12px', borderRadius: 6, fontSize: 12, fontWeight: 500,
                background: target === t ? 'var(--accent-primary)' : 'transparent',
                color: target === t ? 'white' : 'var(--text-secondary)',
                border: 'none', cursor: 'pointer', transition: 'all 150ms',
              }}>
              {t === 'spark' ? 'PySpark' : 'SQL'}
            </button>
          ))}
        </div>
        <button className="btn btn-primary" onClick={loadCode} disabled={loading || !state.dagId}>
          {loading ? '⏳ Compiling…' : '⚡ Compile'}
        </button>
        {code && <>
          <button className="btn btn-ghost" onClick={copy} style={{ fontSize: 12 }}>
            {copied ? '✓ Copied' : '📋 Copy'}
          </button>
          <button className="btn btn-success" onClick={startRun} disabled={running}>
            {running ? '⏳ Starting…' : '▶ Start Staged Run →'}
          </button>
        </>}
      </div>

      {error && <div style={{ color: 'var(--accent-error)', fontSize: 13, marginBottom: 12 }}>⚠ {error}</div>}

      {code ? (
        <div className="glass-card" style={{ padding: 0, overflow: 'hidden' }}>
          <div style={{
            padding: '8px 16px',
            background: 'rgba(0,0,0,0.3)',
            borderBottom: '1px solid var(--border-subtle)',
            display: 'flex', alignItems: 'center', gap: 8, fontSize: 12, color: 'var(--text-muted)',
          }}>
            <span style={{ width: 10, height: 10, borderRadius: '50%', background: '#ef4444', display: 'inline-block' }} />
            <span style={{ width: 10, height: 10, borderRadius: '50%', background: '#f59e0b', display: 'inline-block' }} />
            <span style={{ width: 10, height: 10, borderRadius: '50%', background: '#10b981', display: 'inline-block' }} />
            <span style={{ marginLeft: 8 }}>migrate_io_{state.dagId?.slice(0, 8)}.py</span>
          </div>
          <pre className="code-block" style={{ borderRadius: 0, border: 'none', maxHeight: 500, margin: 0 }}>
            {code}
          </pre>
        </div>
      ) : (
        <div className="glass-card">
          <div className="empty-state">
            <div className="empty-state-icon">⚙️</div>
            <div className="empty-state-title">No compiled code yet</div>
            <div className="empty-state-desc">Click "Compile" to generate the PySpark script from your DAG.</div>
          </div>
        </div>
      )}
    </div>
  );
}
