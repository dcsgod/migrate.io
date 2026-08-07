import { useState } from 'react';
import { commandsApi } from '../../api/client';
import type { MigrationState } from '../../pages/MigrationPage';

interface Props {
  state: MigrationState;
  setState: React.Dispatch<React.SetStateAction<MigrationState>>;
  onDone: () => void;
}

const EXAMPLE_COMMANDS = [
  "Copy orders table from S3 to Databricks, excluding cancelled orders",
  "Join customers and orders on customer_id, mask email, write to gold layer",
  "Incremental load of BKPF table from SAP to Snowflake since last week",
  "Deduplicate orders on order_id keeping most recent, then write to fact_orders",
  "Copy all parquet files from S3 bucket to BigQuery, cast total_amount to DECIMAL(18,2)",
];

export function CommandInput({ state, setState, onDone }: Props) {
  const [command, setCommand] = useState('');
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<any>(null);
  const [error, setError] = useState('');

  const submit = async () => {
    if (!command.trim() || !state.graphId) return;
    setLoading(true);
    setError('');
    try {
      const res = await commandsApi.submit({ graph_id: state.graphId, nl_command: command });
      setResult(res.data);
      setState(s => ({ ...s, intentId: res.data.intent_id }));
    } catch (e: any) {
      setError(e.response?.data?.detail || 'Command failed');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{ maxWidth: 800 }}>
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ fontSize: 22, marginBottom: 6 }}>💬 Natural Language Command</h1>
        <p style={{ color: 'var(--text-muted)', fontSize: 14 }}>
          Describe your migration in plain English. The LLM parses it against your schema graph.
          Transforms, joins, filters, masking — all supported inline.
        </p>
      </div>

      {/* ── NL Input ──────────────────────────────────────── */}
      <div className="glass-card" style={{ marginBottom: 16 }}>
        <label className="input-label">Migration Command</label>
        <textarea
          id="nl-command-input"
          className="input"
          rows={4}
          placeholder="e.g. Copy orders from S3 to Databricks, join with customers on customer_id, mask email column, exclude cancelled status"
          value={command}
          onChange={e => setCommand(e.target.value)}
          onKeyDown={e => { if (e.key === 'Enter' && e.metaKey) submit(); }}
          style={{ fontSize: 14 }}
        />
        <div style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 6 }}>
          ⌘ + Enter to submit • {state.graphId ? `Graph: ${state.graphId.slice(0, 8)}…` : 'No graph — go back to step 2'}
        </div>

        <div style={{ marginTop: 16 }}>
          <div style={{ fontSize: 11, color: 'var(--text-muted)', marginBottom: 8, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
            Example commands
          </div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
            {EXAMPLE_COMMANDS.map(ex => (
              <button key={ex} className="btn btn-ghost" style={{ fontSize: 11, padding: '4px 10px' }}
                onClick={() => setCommand(ex)}>
                {ex.slice(0, 50)}…
              </button>
            ))}
          </div>
        </div>
      </div>

      <div className="flex gap-3" style={{ marginBottom: 24 }}>
        <button className="btn btn-primary" onClick={submit} disabled={loading || !command.trim() || !state.graphId}
          id="submit-command-btn">
          {loading ? '⏳ Parsing…' : '🚀 Parse Command'}
        </button>
      </div>

      {error && <div className="glass-card" style={{ border: '1px solid rgba(239,68,68,0.3)', background: 'rgba(239,68,68,0.05)', color: 'var(--accent-error)', fontSize: 13, marginBottom: 16 }}>
        ⚠ {error}
      </div>}

      {/* ── Raw IntentJSON preview ─────────────────────────── */}
      {result && (
        <div>
          <div className="glass-card" style={{ marginBottom: 16 }}>
            <div style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-accent)', marginBottom: 12, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <span>Raw LLM → IntentJSON</span>
              <span className="badge badge-connected">Parsed</span>
            </div>
            <div className="code-block" style={{ maxHeight: 220 }}>
              {JSON.stringify(result.intent, null, 2)}
            </div>
          </div>

          {result.needs_confirmation && (
            <div className="glass-card" style={{ border: '1px solid rgba(245,158,11,0.3)', background: 'rgba(245,158,11,0.05)', marginBottom: 16 }}>
              <div style={{ color: 'var(--accent-warning)', fontWeight: 600, marginBottom: 8 }}>
                ⚠ {result.unresolved?.length || 0} entity/entities need confirmation before proceeding
              </div>
              <p style={{ fontSize: 13, color: 'var(--text-secondary)' }}>
                Go to "Grounded Intent" step to review and confirm low-confidence mappings.
              </p>
            </div>
          )}

          <button className="btn btn-success" onClick={onDone} style={{ padding: '10px 24px' }}>
            Review Grounded Intent →
          </button>
        </div>
      )}
    </div>
  );
}
