import './index.css';
import { MigrationPage } from './pages/MigrationPage';

export default function App() {
  return (
    <div className="app-layout">
      {/* ── Enterprise App Header ──────────────────────────── */}
      <header className="app-header">
        <div className="flex items-center gap-4">
          <div className="flex items-center gap-3">
            <img
              src="/logo.png"
              alt="Migrate.io Logo"
              style={{
                width: 36,
                height: 36,
                borderRadius: 10,
                objectFit: 'cover',
                boxShadow: '0 0 16px rgba(99, 102, 241, 0.4)',
                border: '1px solid rgba(99, 102, 241, 0.3)',
              }}
            />
            <div>
              <span className="logo-glow">Migrate.io</span>
            </div>
          </div>
          <div style={{ height: 20, width: 1, background: 'var(--border-subtle)' }} />
          <span style={{
            fontSize: 11, fontWeight: 700, letterSpacing: '0.08em',
            padding: '3px 10px', borderRadius: 99,
            background: 'linear-gradient(135deg, rgba(99,102,241,0.2), rgba(6,182,212,0.2))',
            color: '#a5b4fc', border: '1px solid rgba(99,102,241,0.4)',
          }}>
            ENTERPRISE AI PLATFORM
          </span>
        </div>

        {/* ── Live System Telemetry Badges ─────────────────── */}
        <div className="flex items-center gap-4">
          <div className="flex items-center gap-2" style={{
            fontSize: 12, padding: '4px 12px', borderRadius: 99,
            background: 'rgba(15, 23, 42, 0.8)', border: '1px solid var(--border-subtle)',
          }}>
            <span className="pulse-dot" style={{ background: '#10b981' }} />
            <span style={{ color: 'var(--text-muted)' }}>Graph Engine:</span>
            <span style={{ color: '#34d399', fontWeight: 600 }}>Neo4j Active</span>
          </div>

          <div className="flex items-center gap-2" style={{
            fontSize: 12, padding: '4px 12px', borderRadius: 99,
            background: 'rgba(15, 23, 42, 0.8)', border: '1px solid var(--border-subtle)',
          }}>
            <span style={{ color: 'var(--text-muted)' }}>LLM Engine:</span>
            <span style={{ color: '#a5b4fc', fontWeight: 600 }}>Groq Llama 3 70B</span>
          </div>

          <div className="flex items-center gap-2" style={{
            fontSize: 12, padding: '4px 12px', borderRadius: 99,
            background: 'rgba(15, 23, 42, 0.8)', border: '1px solid var(--border-subtle)',
          }}>
            <span style={{ color: 'var(--text-muted)' }}>Target Compiler:</span>
            <span style={{ color: '#06b6d4', fontWeight: 600 }}>PySpark Delta</span>
          </div>

          {/* User Profile Avatar */}
          <div style={{
            width: 34, height: 34, borderRadius: 10,
            background: 'linear-gradient(135deg, #6366f1, #a855f7)',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            fontSize: 13, fontWeight: 700, color: 'white',
            boxShadow: '0 0 12px rgba(99,102,241,0.4)', cursor: 'pointer',
          }}>
            AI
          </div>
        </div>
      </header>

      {/* ── Main Page Content ─────────────────────────────── */}
      <MigrationPage />
    </div>
  );
}
