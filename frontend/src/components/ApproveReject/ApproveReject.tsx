import { useState } from 'react';
import { commitApi, plansApi } from '../../api/client';
import type { MigrationState } from '../../pages/MigrationPage';

interface Props { state: MigrationState; setState: React.Dispatch<React.SetStateAction<MigrationState>>; onDone: () => void; }

export function ApproveReject({ state, setState, onDone }: Props) {
  const [destPath, setDestPath] = useState('');
  const [mode, setMode] = useState<'overwrite' | 'append' | 'merge'>('overwrite');
  const [mergeKeys, setMergeKeys] = useState('');
  const [reason, setReason] = useState('');
  const [confirming, setConfirming] = useState<'approve' | 'reject' | null>(null);
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<any>(null);
  const [error, setError] = useState('');

  const approve = async () => {
    if (!state.runId) return;
    setLoading(true);
    setError('');
    try {
      await commitApi.approve({
        run_id: state.runId,
        destination_path: destPath || '/tmp/migrate_io/production',
        mode,
        merge_keys: mode === 'merge' ? mergeKeys.split(',').map(k => k.trim()).filter(Boolean) : undefined,
      });
      // Save plan version
      if (state.dagId) {
        await plansApi.save({ run_id: state.runId, dag_id: state.dagId, label: `Approved ${new Date().toISOString()}` });
      }
      setResult({ approved: true });
      setConfirming(null);
      onDone();
    } catch (e: any) {
      setError(e.response?.data?.detail || 'Commit failed');
    } finally {
      setLoading(false);
    }
  };

  const reject = async () => {
    if (!state.runId) return;
    setLoading(true);
    try {
      await commitApi.reject({ run_id: state.runId, reason });
      setResult({ rejected: true, reason });
      setConfirming(null);
    } catch (e: any) {
      setError(e.response?.data?.detail || 'Reject failed');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{ maxWidth: 700 }}>
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ fontSize: 22, marginBottom: 6 }}>✅ Approve or Reject</h1>
        <p style={{ color: 'var(--text-muted)', fontSize: 14 }}>
          This is the explicit gate. <strong>Approve</strong> atomically commits staging → production via Delta MERGE.
          <strong> Reject</strong> discards staging — production is untouched.
        </p>
      </div>

      {result ? (
        <div className="glass-card" style={{ border: `1px solid ${result.approved ? 'rgba(16,185,129,0.4)' : 'rgba(239,68,68,0.3)'}`, background: result.approved ? 'rgba(16,185,129,0.05)' : 'rgba(239,68,68,0.05)' }}>
          <div style={{ fontSize: 16, fontWeight: 700, color: result.approved ? 'var(--accent-success)' : 'var(--accent-error)', marginBottom: 8 }}>
            {result.approved ? '✅ Committed to production' : '❌ Staging discarded'}
          </div>
          <div style={{ fontSize: 13, color: 'var(--text-secondary)' }}>
            {result.approved ? `Destination: ${destPath || '/tmp/migrate_io/production'}` : `Reason: ${result.reason || 'No reason provided'}`}
          </div>
        </div>
      ) : (
        <>
          {/* ── Approve config ──────────────────────────── */}
          <div className="glass-card" style={{ marginBottom: 20 }}>
            <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 16 }}>Commit Configuration</div>
            <div className="form-group">
              <label className="input-label">Destination Path / Table</label>
              <input className="input" placeholder="/prod/fact_orders or catalog.schema.table"
                value={destPath} onChange={e => setDestPath(e.target.value)} />
            </div>
            <div className="form-group">
              <label className="input-label">Write Mode</label>
              <select className="input" value={mode} onChange={e => setMode(e.target.value as any)}>
                <option value="overwrite">Overwrite (atomic swap)</option>
                <option value="append">Append</option>
                <option value="merge">Merge (UPSERT via Delta MERGE INTO)</option>
              </select>
            </div>
            {mode === 'merge' && (
              <div className="form-group">
                <label className="input-label">Merge Keys (comma-separated)</label>
                <input className="input" placeholder="order_id, customer_id"
                  value={mergeKeys} onChange={e => setMergeKeys(e.target.value)} />
              </div>
            )}
          </div>

          {error && <div style={{ color: 'var(--accent-error)', fontSize: 13, marginBottom: 12 }}>⚠ {error}</div>}

          {/* ── Action buttons ──────────────────────────── */}
          {confirming === 'approve' ? (
            <div className="glass-card" style={{ border: '1px solid rgba(16,185,129,0.3)', background: 'rgba(16,185,129,0.05)', marginBottom: 16 }}>
              <div style={{ fontSize: 14, fontWeight: 600, marginBottom: 12 }}>⚠ Confirm Production Commit</div>
              <p style={{ fontSize: 13, color: 'var(--text-secondary)', marginBottom: 16 }}>
                This will write data to <code>{destPath || '/tmp/migrate_io/production'}</code> using mode <strong>{mode}</strong>.
                This action cannot be undone without Delta time travel rollback.
              </p>
              <div className="flex gap-2">
                <button className="btn btn-success" onClick={approve} disabled={loading} id="confirm-approve-btn">
                  {loading ? '⏳ Committing…' : '✅ Confirm & Commit'}
                </button>
                <button className="btn btn-ghost" onClick={() => setConfirming(null)}>Cancel</button>
              </div>
            </div>
          ) : confirming === 'reject' ? (
            <div className="glass-card" style={{ border: '1px solid rgba(239,68,68,0.3)', background: 'rgba(239,68,68,0.05)', marginBottom: 16 }}>
              <div style={{ fontSize: 14, fontWeight: 600, marginBottom: 12 }}>Discard Staging</div>
              <div className="form-group">
                <label className="input-label">Reason (optional)</label>
                <textarea className="input" rows={2} value={reason} onChange={e => setReason(e.target.value)} />
              </div>
              <div className="flex gap-2">
                <button className="btn btn-danger" onClick={reject} disabled={loading} id="confirm-reject-btn">
                  {loading ? '⏳…' : '❌ Discard Staging'}
                </button>
                <button className="btn btn-ghost" onClick={() => setConfirming(null)}>Cancel</button>
              </div>
            </div>
          ) : (
            <div className="flex gap-4">
              <button id="approve-btn" className="btn btn-success" style={{ padding: '12px 32px', fontSize: 15, flex: 1 }}
                onClick={() => setConfirming('approve')}>
                ✅ Approve & Commit
              </button>
              <button id="reject-btn" className="btn btn-danger" style={{ padding: '12px 32px', fontSize: 15, flex: 1 }}
                onClick={() => setConfirming('reject')}>
                ❌ Reject & Discard
              </button>
            </div>
          )}
        </>
      )}
    </div>
  );
}
