import { useState, useEffect } from 'react';
import { commandsApi, graphApi } from '../../api/client';
import type { MigrationState } from '../../pages/MigrationPage';

interface Props {
  state: MigrationState;
  onDone: () => void;
}

export function IntentViewer({ state, onDone }: Props) {
  const [grounded, setGrounded] = useState<any>(null);
  const [explanation, setExplanation] = useState<any>(null);
  const [nodes, setNodes] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [confirmingEntity, setConfirmingEntity] = useState<string | null>(null);
  const [selectedNodeMap, setSelectedNodeMap] = useState<Record<string, string>>({});
  const [error, setError] = useState('');

  useEffect(() => {
    if (state.intentId) {
      load();
    }
  }, [state.intentId]);

  const load = async () => {
    if (!state.intentId) return;
    setLoading(true);
    setError('');
    try {
      const [intentRes, explainRes] = await Promise.all([
        commandsApi.get(state.intentId),
        commandsApi.explain(state.intentId),
      ]);

      if (state.graphId) {
        try {
          const nodesRes = await graphApi.nodes(state.graphId);
          setNodes(nodesRes.data || []);
        } catch {
          // ignore nodes load error
        }
      }

      setGrounded(intentRes.data?.grounded || null);
      setExplanation(explainRes.data?.explanation || null);
    } catch (e: any) {
      setError(e.response?.data?.detail || 'Failed to load intent');
    } finally {
      setLoading(false);
    }
  };

  const handleConfirm = async (entityName: string) => {
    const selectedNodeId = selectedNodeMap[entityName];
    if (!selectedNodeId || !state.intentId) return;
    setConfirmingEntity(entityName);
    setError('');
    try {
      await commandsApi.confirmMapping(state.intentId, {
        entity_name: entityName,
        confirmed_node_id: selectedNodeId,
      });
      // Re-fetch clean grounded intent & explanation from backend
      await load();
    } catch (e: any) {
      setError(e.response?.data?.detail || 'Failed to confirm mapping');
    } finally {
      setConfirmingEntity(null);
    }
  };

  const ConfidencePill = ({ score }: { score: number }) => (
    <span style={{
      padding: '2px 10px', borderRadius: 100, fontSize: 11, fontWeight: 700,
      background: score >= 0.85 ? 'rgba(16,185,129,0.15)' : score >= 0.6 ? 'rgba(245,158,11,0.15)' : 'rgba(244,63,94,0.15)',
      color: score >= 0.85 ? '#34d399' : score >= 0.6 ? '#fbbf24' : '#f87171',
      border: `1px solid ${score >= 0.85 ? 'rgba(16,185,129,0.35)' : score >= 0.6 ? 'rgba(245,158,11,0.35)' : 'rgba(244,63,94,0.35)'}`,
    }}>
      {Math.round((score || 0) * 100)}% Match
    </span>
  );

  const hasUnconfirmed = grounded?.source_tables?.some((st: any) => st.needs_user_confirmation || !st.node_id) ||
    grounded?.target_table?.needs_user_confirmation ||
    !grounded?.target_table?.node_id;

  return (
    <div style={{ maxWidth: 960 }}>
      {/* Title section */}
      <div style={{ marginBottom: 32 }}>
        <div style={{ fontSize: 11, fontWeight: 700, letterSpacing: '0.08em', color: 'var(--accent-cyan)', marginBottom: 6 }}>
          STEP 4 OF 10 — ENTITY RESOLUTION & XAI
        </div>
        <h1 style={{ fontSize: 26, fontWeight: 800, marginBottom: 8 }}>🎯 Grounded Intent & Mapping Confirmation</h1>
        <p style={{ color: 'var(--text-secondary)', fontSize: 14, maxWidth: 720 }}>
          Natural language entities resolved against real GraphNode IDs. Select and confirm low-confidence or unresolved entities below.
        </p>
      </div>

      <div className="flex gap-3" style={{ marginBottom: 24 }}>
        <button className="btn btn-primary" onClick={load} disabled={loading || !state.intentId}>
          {loading ? '⏳ Resolving Graph Nodes…' : '🔄 Refresh Resolutions'}
        </button>
        {grounded && (
          <button
            className="btn btn-success"
            onClick={onDone}
            disabled={hasUnconfirmed}
            style={{ padding: '10px 24px' }}
          >
            {hasUnconfirmed ? '⚠ Confirm Low-Confidence Mappings to Build DAG' : 'Build Logical DAG →'}
          </button>
        )}
      </div>

      {error && (
        <div className="glass-card" style={{ border: '1px solid rgba(244,63,94,0.35)', background: 'rgba(244,63,94,0.06)', color: '#f87171', fontSize: 13, marginBottom: 20 }}>
          ⚠ {error}
        </div>
      )}

      {grounded && (
        <div className="grid-2" style={{ alignItems: 'start' }}>
          {/* ── Source & Target Tables ─────────────────────── */}
          <div className="flex flex-col gap-4">
            <div className="glass-card">
              <div style={{ fontSize: 12, fontWeight: 800, letterSpacing: '0.06em', color: 'var(--accent-cyan)', textTransform: 'uppercase', marginBottom: 16 }}>
                Mapped Table Entities
              </div>

              {/* Source Tables */}
              {grounded.source_tables?.map((rt: any) => {
                const needsConfirm = rt.needs_user_confirmation || !rt.node_id;
                return (
                  <div key={rt.entity_name} style={{
                    marginBottom: 16, padding: 16,
                    background: needsConfirm ? 'rgba(245,158,11,0.05)' : 'rgba(15,23,42,0.6)',
                    border: `1px solid ${needsConfirm ? 'rgba(245,158,11,0.35)' : 'var(--border-subtle)'}`,
                    borderRadius: 'var(--radius-md)',
                  }}>
                    <div className="flex items-center justify-between" style={{ marginBottom: 8 }}>
                      <div>
                        <div style={{ fontSize: 11, color: 'var(--text-muted)', fontWeight: 700 }}>SOURCE TABLE</div>
                        <div style={{ fontSize: 15, fontWeight: 700, color: 'var(--text-primary)' }}>{rt.entity_name}</div>
                      </div>
                      <ConfidencePill score={rt.confidence} />
                    </div>

                    <div style={{ fontSize: 12, color: 'var(--text-secondary)', fontFamily: 'var(--font-mono)', marginBottom: 10 }}>
                      Resolved: <span style={{ color: rt.node_qualified_name ? '#a5b4fc' : '#f87171' }}>
                        {rt.node_qualified_name || 'UNRESOLVED'}
                      </span>
                    </div>

                    {/* Confirmation selector */}
                    {needsConfirm && (
                      <div style={{ marginTop: 12, paddingTop: 12, borderTop: '1px solid rgba(245,158,11,0.2)' }}>
                        <label className="input-label" style={{ color: '#fbbf24' }}>
                          ⚠ Select Target Graph Node to Confirm Mapping
                        </label>
                        <div className="flex gap-2">
                          <select
                            className="input"
                            style={{ flex: 1, fontSize: 12 }}
                            value={selectedNodeMap[rt.entity_name] || ''}
                            onChange={e => setSelectedNodeMap({ ...selectedNodeMap, [rt.entity_name]: e.target.value })}
                          >
                            <option value="">-- Choose Schema Graph Node --</option>
                            {nodes.map(n => (
                              <option key={n.id} value={n.id}>
                                {n.name} ({n.qualified_name}) [{n.kind}]
                              </option>
                            ))}
                          </select>
                          <button
                            className="btn btn-primary"
                            style={{ fontSize: 12 }}
                            disabled={!selectedNodeMap[rt.entity_name] || confirmingEntity === rt.entity_name}
                            onClick={() => handleConfirm(rt.entity_name)}
                          >
                            {confirmingEntity === rt.entity_name ? '⏳…' : '✅ Confirm'}
                          </button>
                        </div>
                      </div>
                    )}
                  </div>
                );
              })}

              {/* Target Table */}
              {grounded.target_table && (
                <div style={{
                  padding: 16,
                  background: grounded.target_table.needs_user_confirmation || !grounded.target_table.node_id ? 'rgba(245,158,11,0.05)' : 'rgba(15,23,42,0.6)',
                  border: `1px solid ${grounded.target_table.needs_user_confirmation || !grounded.target_table.node_id ? 'rgba(245,158,11,0.35)' : 'var(--border-subtle)'}`,
                  borderRadius: 'var(--radius-md)',
                }}>
                  <div className="flex items-center justify-between" style={{ marginBottom: 8 }}>
                    <div>
                      <div style={{ fontSize: 11, color: 'var(--text-muted)', fontWeight: 700 }}>TARGET TABLE</div>
                      <div style={{ fontSize: 15, fontWeight: 700, color: 'var(--text-primary)' }}>
                        {grounded.target_table.entity_name}
                      </div>
                    </div>
                    <ConfidencePill score={grounded.target_table.confidence} />
                  </div>

                  <div style={{ fontSize: 12, color: 'var(--text-secondary)', fontFamily: 'var(--font-mono)' }}>
                    Resolved: <span style={{ color: grounded.target_table.node_qualified_name ? '#a5b4fc' : '#f87171' }}>
                      {grounded.target_table.node_qualified_name || 'UNRESOLVED'}
                    </span>
                  </div>

                  {(grounded.target_table.needs_user_confirmation || !grounded.target_table.node_id) && (
                    <div style={{ marginTop: 12, paddingTop: 12, borderTop: '1px solid rgba(245,158,11,0.2)' }}>
                      <label className="input-label" style={{ color: '#fbbf24' }}>
                        ⚠ Select Target Graph Node to Confirm Mapping
                      </label>
                      <div className="flex gap-2">
                        <select
                          className="input"
                          style={{ flex: 1, fontSize: 12 }}
                          value={selectedNodeMap[grounded.target_table.entity_name] || ''}
                          onChange={e => setSelectedNodeMap({ ...selectedNodeMap, [grounded.target_table.entity_name]: e.target.value })}
                        >
                          <option value="">-- Choose Schema Graph Node --</option>
                          {nodes.map(n => (
                            <option key={n.id} value={n.id}>
                              {n.name} ({n.qualified_name}) [{n.kind}]
                            </option>
                          ))}
                        </select>
                        <button
                          className="btn btn-primary"
                          style={{ fontSize: 12 }}
                          disabled={!selectedNodeMap[grounded.target_table.entity_name] || confirmingEntity === grounded.target_table.entity_name}
                          onClick={() => handleConfirm(grounded.target_table.entity_name)}
                        >
                          {confirmingEntity === grounded.target_table.entity_name ? '⏳…' : '✅ Confirm'}
                        </button>
                      </div>
                    </div>
                  )}
                </div>
              )}
            </div>
          </div>

          {/* ── XAI Explainability Card ─────────────────────── */}
          <div className="glass-card">
            <div style={{ fontSize: 12, fontWeight: 800, letterSpacing: '0.06em', color: 'var(--accent-primary)', textTransform: 'uppercase', marginBottom: 16 }}>
              Explainable AI (XAI) Rationales
            </div>

            {explanation?.warnings?.length > 0 && (
              <div style={{ marginBottom: 16, padding: 12, background: 'rgba(245,158,11,0.06)', border: '1px solid rgba(245,158,11,0.25)', borderRadius: 'var(--radius-md)' }}>
                <div style={{ fontSize: 12, fontWeight: 700, color: '#fbbf24', marginBottom: 6 }}>
                  Resolution Warnings
                </div>
                {explanation.warnings.map((w: string, i: number) => (
                  <div key={i} style={{ fontSize: 12, color: 'var(--text-secondary)', marginBottom: 4 }}>
                    ⚠ {w}
                  </div>
                ))}
              </div>
            )}

            <div style={{ fontSize: 13, color: 'var(--text-secondary)', marginBottom: 16 }}>
              Overall Decision Confidence Score:{' '}
              <ConfidencePill score={explanation?.overall_confidence || 1} />
            </div>

            {explanation?.join_explanations?.map((je: any, i: number) => (
              <div key={i} style={{ padding: 14, background: 'rgba(15,23,42,0.6)', border: '1px solid var(--border-subtle)', borderRadius: 'var(--radius-md)', marginBottom: 12 }}>
                <div style={{ fontSize: 13, fontWeight: 700, color: '#a5b4fc', marginBottom: 4 }}>
                  Join Condition: {je.left_table}.{je.left_key} ↔ {je.right_table}.{je.right_key}
                </div>
                <div style={{ fontSize: 12, color: 'var(--text-muted)', lineHeight: 1.5 }}>
                  {je.reasoning}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
