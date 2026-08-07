import { useState, useEffect } from 'react';
import { plansApi } from '../../api/client';
import type { MigrationState } from '../../pages/MigrationPage';

interface Props { state: MigrationState; }

export function CommitLog({ state }: Props) {
  const [plans, setPlans] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => { loadPlans(); }, []);

  const loadPlans = async () => {
    setLoading(true);
    try {
      const res = await plansApi.list(50);
      setPlans(res.data);
    } finally {
      setLoading(false);
    }
  };

  const rerun = async (planId: string) => {
    await plansApi.rerun(planId);
    alert('Re-run submitted! Check the Run Timeline step for progress.');
  };

  const statusColor: Record<string, string> = {
    committed: 'var(--accent-success)',
    staged: 'var(--accent-cyan)',
    rejected: 'var(--accent-error)',
    pending: 'var(--text-muted)',
  };

  return (
    <div style={{ maxWidth: 900 }}>
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ fontSize: 22, marginBottom: 6 }}>📋 Production Commit Log</h1>
        <p style={{ color: 'var(--text-muted)', fontSize: 14 }}>
          Immutable audit trail of every approved migration. Re-run any past version with one click.
          Delta time travel gives destination-side rollback.
        </p>
      </div>

      <div className="flex gap-3" style={{ marginBottom: 20 }}>
        <button className="btn btn-ghost" onClick={loadPlans} disabled={loading}>
          {loading ? '⏳' : '🔄 Refresh'}
        </button>
      </div>

      {plans.length === 0 ? (
        <div className="glass-card">
          <div className="empty-state">
            <div className="empty-state-icon">📋</div>
            <div className="empty-state-title">No commits yet</div>
            <div className="empty-state-desc">Approved migrations will appear here with their full lineage.</div>
          </div>
        </div>
      ) : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
          {plans.map((plan: any) => (
            <div key={plan.plan_id} className="glass-card" style={{ padding: '14px 20px' }}>
              <div className="flex items-center justify-between">
                <div>
                  <div className="flex items-center gap-2" style={{ marginBottom: 4 }}>
                    <span style={{ fontSize: 13, fontWeight: 600 }}>
                      {plan.label || `Migration ${plan.plan_id.slice(0, 8)}`}
                    </span>
                    {plan.tags?.map((t: string) => (
                      <span key={t} className="badge badge-info" style={{ fontSize: 10 }}>{t}</span>
                    ))}
                  </div>
                  <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>
                    {new Date(plan.saved_at).toLocaleString()} · Plan ID: {plan.plan_id.slice(0, 12)}…
                  </div>
                </div>
                <div className="flex items-center gap-2">
                  <span style={{
                    fontSize: 12, fontWeight: 600, padding: '3px 10px', borderRadius: 100,
                    color: statusColor[plan.status] || 'var(--text-muted)',
                    background: `${statusColor[plan.status] || 'var(--text-muted)'}15`,
                    border: `1px solid ${statusColor[plan.status] || 'var(--text-muted)'}30`,
                  }}>
                    {plan.status}
                  </span>
                  <button className="btn btn-ghost" style={{ fontSize: 11, padding: '4px 10px' }}
                    onClick={() => rerun(plan.plan_id)}>
                    ↻ Re-run
                  </button>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
