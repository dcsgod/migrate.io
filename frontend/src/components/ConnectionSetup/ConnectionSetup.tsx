import { useState } from 'react';
import { connectionsApi } from '../../api/client';
import type { MigrationState } from '../../pages/MigrationPage';

interface Props {
  state: MigrationState;
  setState: React.Dispatch<React.SetStateAction<MigrationState>>;
  onDone: () => void;
}

const CONNECTOR_TYPES = [
  { value: 'mock_object_storage', label: '🪣 Synthetic S3 Bucket (Mock)', group: 'Synthetic' },
  { value: 'mock_warehouse', label: '🏭 Synthetic Databricks (Mock)', group: 'Synthetic' },
  { value: 'mock_rdbms', label: '🗄️ Synthetic PostgreSQL (Mock)', group: 'Synthetic' },
  { value: 'mock_erp', label: '🏢 Synthetic SAP ECC (Mock)', group: 'Synthetic' },
  { value: 's3', label: '☁️ Amazon S3', group: 'Object Storage' },
  { value: 'adls', label: '🔷 Azure ADLS Gen2', group: 'Object Storage' },
  { value: 'gcs', label: '🌐 Google Cloud Storage', group: 'Object Storage' },
  { value: 'minio', label: '📦 MinIO Storage', group: 'Object Storage' },
  { value: 'databricks', label: '🧱 Databricks Unity Catalog', group: 'Warehouse' },
  { value: 'snowflake', label: '❄️ Snowflake Data Warehouse', group: 'Warehouse' },
  { value: 'postgres', label: '🐘 PostgreSQL Database', group: 'RDBMS' },
  { value: 'sap_ecc', label: '🏗️ SAP ECC (RFC / ABAP)', group: 'ERP' },
];

interface Connection {
  id: string;
  name: string;
  connector_type: string;
  is_connected: boolean;
  role: 'source' | 'destination';
}

export function ConnectionSetup({ state, setState, onDone }: Props) {
  const [connections, setConnections] = useState<Connection[]>([]);
  const [adding, setAdding] = useState<'source' | 'destination' | null>(null);
  const [form, setForm] = useState({ connector_type: 'mock_object_storage', name: '', config: '{}' });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const handleAdd = async () => {
    setLoading(true);
    setError('');
    try {
      let config = {};
      try { config = JSON.parse(form.config); } catch { config = {}; }
      const res = await connectionsApi.create({
        connector_type: form.connector_type,
        name: form.name || form.connector_type,
        config,
      });
      const conn: Connection = { ...res.data, role: adding! };
      setConnections(prev => [...prev.filter(c => c.role !== adding), conn]);
      if (adding === 'source') setState(s => ({ ...s, sourceConnectionId: conn.id }));
      else setState(s => ({ ...s, destConnectionId: conn.id }));
      setAdding(null);
      setForm({ connector_type: 'mock_object_storage', name: '', config: '{}' });
    } catch (e: any) {
      setError(e.response?.data?.detail || 'Connection failed');
    } finally {
      setLoading(false);
    }
  };

  const sourceConn = connections.find(c => c.role === 'source');
  const destConn = connections.find(c => c.role === 'destination');
  const canProceed = !!sourceConn && !!destConn;

  return (
    <div style={{ maxWidth: 960 }}>
      {/* Title section */}
      <div style={{ marginBottom: 32 }}>
        <div style={{ fontSize: 11, fontWeight: 700, letterSpacing: '0.08em', color: 'var(--accent-cyan)', marginBottom: 6 }}>
          STEP 1 OF 10 — CONNECTOR DISCOVERY
        </div>
        <h1 style={{ fontSize: 26, fontWeight: 800, marginBottom: 8 }}>🔌 Universal Connector Registration</h1>
        <p style={{ color: 'var(--text-secondary)', fontSize: 14, maxWidth: 680 }}>
          Establish high-throughput adapters for source and destination systems. Supports real cloud storage, data warehouses, databases, ERPs, and instant synthetic mocks.
        </p>
      </div>

      {/* Connection Grid */}
      <div className="grid-2" style={{ marginBottom: 32 }}>
        {(['source', 'destination'] as const).map(role => {
          const conn = connections.find(c => c.role === role);
          const isSource = role === 'source';
          return (
            <div key={role} className="glass-card glass-card-interactive" style={{ minHeight: 220, position: 'relative' }}>
              <div className="flex items-center justify-between" style={{ marginBottom: 16 }}>
                <span style={{
                  fontSize: 11, fontWeight: 800, letterSpacing: '0.08em',
                  color: isSource ? '#06b6d4' : '#10b981', textTransform: 'uppercase',
                }}>
                  {isSource ? '── SOURCE ADAPTER ──' : '── DESTINATION ADAPTER ──'}
                </span>
                {conn && <span className="badge badge-connected">● HEALTHY</span>}
              </div>

              {conn ? (
                <div>
                  <div style={{ fontSize: 18, fontWeight: 700, marginBottom: 4 }}>{conn.name}</div>
                  <div style={{ fontSize: 12, color: 'var(--text-muted)', fontFamily: 'var(--font-mono)' }}>
                    Type: <span style={{ color: '#a5b4fc' }}>{conn.connector_type}</span>
                  </div>
                  <div style={{ fontSize: 12, color: 'var(--text-muted)', fontFamily: 'var(--font-mono)', marginTop: 4 }}>
                    UUID: {conn.id}
                  </div>

                  <div className="flex gap-2" style={{ marginTop: 24 }}>
                    <button className="btn btn-ghost" style={{ fontSize: 12 }} onClick={() => setAdding(role)}>
                      🔄 Swap Adapter
                    </button>
                  </div>
                </div>
              ) : (
                <div style={{ textAlign: 'center', padding: '24px 0' }}>
                  <div style={{ fontSize: 36, marginBottom: 12 }}>{isSource ? '📂' : '🎯'}</div>
                  <div style={{ fontSize: 15, fontWeight: 700, marginBottom: 4 }}>No {role} configured</div>
                  <div style={{ fontSize: 12, color: 'var(--text-muted)', marginBottom: 20 }}>
                    Select from 12+ universal connector plugins
                  </div>
                  <button className="btn btn-primary" onClick={() => setAdding(role)}>
                    + Connect {role === 'source' ? 'Source System' : 'Destination System'}
                  </button>
                </div>
              )}
            </div>
          );
        })}
      </div>

      {/* Add Adapter Drawer Form */}
      {adding && (
        <div className="glass-card fade-in" style={{ marginBottom: 32, border: '1px solid var(--border-accent)' }}>
          <div style={{ fontSize: 16, fontWeight: 700, color: 'var(--text-primary)', marginBottom: 20 }}>
            Configure {adding.toUpperCase()} Connector Plugin
          </div>

          <div className="form-group">
            <label className="input-label">Select Connector Plugin</label>
            <select className="input" value={form.connector_type}
              onChange={e => setForm(f => ({ ...f, connector_type: e.target.value }))}>
              {CONNECTOR_TYPES.map(t => (
                <option key={t.value} value={t.value}>{t.label}</option>
              ))}
            </select>
          </div>

          <div className="form-group">
            <label className="input-label">Connection Alias</label>
            <input className="input" placeholder={`Production ${adding}`} value={form.name}
              onChange={e => setForm(f => ({ ...f, name: e.target.value }))} />
          </div>

          <div className="form-group">
            <label className="input-label">Connection Credentials & Options (JSON)</label>
            <textarea className="input" rows={4} value={form.config}
              onChange={e => setForm(f => ({ ...f, config: e.target.value }))}
              style={{ fontFamily: 'var(--font-mono)', fontSize: 12 }} />
            <div style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 6 }}>
              💡 Mocks work with empty JSON <code>{'{}'}</code>. Real connectors validate credentials on connection.
            </div>
          </div>

          {error && <div style={{ color: 'var(--accent-rose)', fontSize: 13, marginBottom: 16 }}>⚠ {error}</div>}

          <div className="flex gap-3">
            <button className="btn btn-primary" onClick={handleAdd} disabled={loading}>
              {loading ? '⏳ Validating Connection…' : '⚡ Connect & Register'}
            </button>
            <button className="btn btn-ghost" onClick={() => setAdding(null)}>Cancel</button>
          </div>
        </div>
      )}

      {/* Proceed Button */}
      <button className="btn btn-success" disabled={!canProceed} onClick={onDone}
        style={{ padding: '14px 32px', fontSize: 15 }}>
        Proceed to Schema Crawling →
      </button>
    </div>
  );
}
