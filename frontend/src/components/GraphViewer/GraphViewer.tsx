import { useState } from 'react';
import ReactFlow, {
  Background, Controls, MiniMap, BackgroundVariant,
} from 'reactflow';
import type { Node, Edge } from 'reactflow';
import 'reactflow/dist/style.css';
import { graphApi } from '../../api/client';
import type { MigrationState } from '../../pages/MigrationPage';

interface Props {
  state: MigrationState;
  setState: React.Dispatch<React.SetStateAction<MigrationState>>;
  onDone: () => void;
}

function buildFlowElements(graphData: any): { nodes: Node[]; edges: Edge[] } {
  if (!graphData) return { nodes: [], edges: [] };

  const nodeList: Node[] = (graphData.nodes || []).map((n: any, i: number) => ({
    id: n.id,
    position: { x: (i % 4) * 240, y: Math.floor(i / 4) * 160 },
    data: {
      label: (
        <div style={{ padding: 4 }}>
          <div style={{ fontWeight: 600, fontSize: 13, marginBottom: 4 }}>{n.name}</div>
          <div style={{ fontSize: 10, color: '#9ca3af' }}>{n.kind} · {n.columns?.length || 0} cols</div>
          {n.row_count && <div style={{ fontSize: 10, color: '#6b7280' }}>{n.row_count.toLocaleString()} rows</div>}
        </div>
      ),
    },
    style: {
      background: n.connector_id?.includes('source') || n.connector_id?.includes('s3') || n.connector_id?.includes('mock_object')
        ? 'rgba(6,182,212,0.08)' : 'rgba(99,102,241,0.08)',
      border: `1px solid ${n.connector_id?.includes('source') ? 'rgba(6,182,212,0.3)' : 'rgba(99,102,241,0.3)'}`,
      borderRadius: 10,
      minWidth: 160,
      color: '#f9fafb',
    },
  }));

  const edgeList: Edge[] = (graphData.edges || []).map((e: any) => ({
    id: e.id,
    source: e.source_node_id,
    target: e.target_node_id,
    animated: e.kind !== 'explicit',
    style: {
      stroke: e.confidence >= 0.85 ? '#10b981' : e.confidence >= 0.6 ? '#f59e0b' : '#ef4444',
      strokeDasharray: e.kind === 'explicit' ? undefined : '5,5',
      strokeWidth: 1.5,
    },
    label: e.kind === 'explicit' ? '🔑' : `${Math.round(e.confidence * 100)}%`,
    labelStyle: { fill: '#9ca3af', fontSize: 10 },
  }));

  return { nodes: nodeList, edges: edgeList };
}

export function GraphViewer({ state, setState, onDone }: Props) {
  const [graphData, setGraphData] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [runInference, setRunInference] = useState(false);

  const buildGraph = async () => {
    if (!state.sourceConnectionId || !state.destConnectionId) return;
    setLoading(true);
    setError('');
    try {
      const res = await graphApi.build({
        source_connection_id: state.sourceConnectionId,
        dest_connection_id: state.destConnectionId,
        run_inference: runInference,
      });
      const fullGraph = await graphApi.get(res.data.graph_id);
      setGraphData({ ...fullGraph.data, graph_id: res.data.graph_id, ...res.data });
      setState(s => ({ ...s, graphId: res.data.graph_id }));
    } catch (e: any) {
      setError(e.response?.data?.detail || 'Graph build failed');
    } finally {
      setLoading(false);
    }
  };

  const { nodes, edges } = buildFlowElements(graphData);

  return (
    <div style={{ maxWidth: 1000 }}>
      <div style={{ marginBottom: 20 }}>
        <h1 style={{ fontSize: 22, marginBottom: 6 }}>🕸️ Schema Relationship Graph</h1>
        <p style={{ color: 'var(--text-muted)', fontSize: 14 }}>
          Visual map of tables, files, and inferred relationships across source and destination.
          <br />Solid edges = explicit FK · Dashed = inferred · Color = confidence score.
        </p>
      </div>

      {/* ── Stats ─────────────────────────────────────────── */}
      {graphData && (
        <div className="grid-3" style={{ marginBottom: 20 }}>
          {[
            { label: 'Nodes', value: graphData.node_count || nodes.length, color: 'var(--step-graph)' },
            { label: 'Edges', value: graphData.edge_count || edges.length, color: 'var(--accent-primary)' },
            { label: 'Low-confidence', value: graphData.low_confidence_edges || 0, color: 'var(--accent-warning)' },
          ].map(stat => (
            <div key={stat.label} className="glass-card" style={{ textAlign: 'center', padding: 16 }}>
              <div style={{ fontSize: 28, fontWeight: 800, color: stat.color }}>{stat.value}</div>
              <div style={{ fontSize: 12, color: 'var(--text-muted)' }}>{stat.label}</div>
            </div>
          ))}
        </div>
      )}

      {/* ── Controls ──────────────────────────────────────── */}
      <div className="flex items-center gap-3" style={{ marginBottom: 16 }}>
        <button className="btn btn-primary" onClick={buildGraph} disabled={loading}>
          {loading ? '⏳ Building…' : '🔨 Build Graph'}
        </button>
        <label className="flex items-center gap-2" style={{ fontSize: 13, color: 'var(--text-secondary)', cursor: 'pointer' }}>
          <input type="checkbox" checked={runInference}
            onChange={e => setRunInference(e.target.checked)}
            style={{ accentColor: 'var(--accent-primary)' }} />
          Run edge inference
        </label>
        {graphData && (
          <button className="btn btn-success" onClick={onDone}>
            Issue NL Command →
          </button>
        )}
      </div>

      {error && <div style={{ color: 'var(--accent-error)', fontSize: 13, marginBottom: 12 }}>⚠ {error}</div>}

      {/* ── Graph Canvas ──────────────────────────────────── */}
      <div className="glass-card" style={{ height: 500, padding: 0, overflow: 'hidden' }}>
        {graphData ? (
          <ReactFlow
            nodes={nodes}
            edges={edges}
            fitView
            attributionPosition="bottom-right"
          >
            <Background variant={BackgroundVariant.Dots} color="rgba(255,255,255,0.04)" gap={20} />
            <Controls style={{ background: 'var(--bg-elevated)', border: '1px solid var(--border-subtle)' }} />
            <MiniMap
              style={{ background: 'var(--bg-elevated)' }}
              nodeColor={n => n.style?.background as string || '#374151'}
            />
          </ReactFlow>
        ) : (
          <div className="empty-state" style={{ height: '100%' }}>
            <div className="empty-state-icon">🕸️</div>
            <div className="empty-state-title">No graph yet</div>
            <div className="empty-state-desc">Click "Build Graph" to crawl your connectors and discover relationships.</div>
          </div>
        )}
      </div>

      {graphData?.drift?.length > 0 && (
        <div className="glass-card" style={{ marginTop: 16, border: '1px solid rgba(245,158,11,0.3)', background: 'rgba(245,158,11,0.05)' }}>
          <div style={{ color: 'var(--accent-warning)', fontWeight: 600, marginBottom: 8 }}>⚠ Schema Drift Detected</div>
          {graphData.drift.map((d: any) => (
            <div key={d.node_id} style={{ fontSize: 12, color: 'var(--text-secondary)', marginBottom: 4 }}>
              <strong>{d.node_name}</strong>: {d.added_columns.length > 0 && `+${d.added_columns.join(', ')} `}
              {d.removed_columns.length > 0 && `-${d.removed_columns.join(', ')}`}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
