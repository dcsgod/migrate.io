import { useState } from 'react';
import { ConnectionSetup } from '../components/ConnectionSetup/ConnectionSetup';
import { GraphViewer } from '../components/GraphViewer/GraphViewer';
import { CommandInput } from '../components/CommandInput/CommandInput';
import { IntentViewer } from '../components/IntentViewer/IntentViewer';
import { DAGViewer } from '../components/DAGViewer/DAGViewer';
import { CodeViewer } from '../components/CodeViewer/CodeViewer';
import { RunTimeline } from '../components/RunTimeline/RunTimeline';
import { PreviewGrid } from '../components/PreviewGrid/PreviewGrid';
import { ApproveReject } from '../components/ApproveReject/ApproveReject';
import { CommitLog } from '../components/CommitLog/CommitLog';

const STEPS = [
  { id: 'connections', icon: '🔌', label: '1. Connections', color: '#06b6d4' },
  { id: 'graph', icon: '🕸️', label: '2. Schema Graph', color: '#a855f7' },
  { id: 'command', icon: '💬', label: '3. NL Command', color: '#6366f1' },
  { id: 'intent', icon: '🎯', label: '4. Grounded Intent', color: '#38bdf8' },
  { id: 'dag', icon: '📊', label: '5. Logical DAG', color: '#f59e0b' },
  { id: 'code', icon: '⚙️', label: '6. Spark Compiler', color: '#10b981' },
  { id: 'run', icon: '▶️', label: '7. Staged Execution', color: '#3b82f6' },
  { id: 'preview', icon: '👁️', label: '8. Data Preview', color: '#ec4899' },
  { id: 'approve', icon: '✅', label: '9. Governance Gate', color: '#22c55e' },
  { id: 'log', icon: '📋', label: '10. Commit Audit', color: '#10b981' },
];

export interface MigrationState {
  sourceConnectionId: string | null;
  destConnectionId: string | null;
  graphId: string | null;
  intentId: string | null;
  dagId: string | null;
  runId: string | null;
  compiledCode: string | null;
  commitLog: unknown[];
}

export function MigrationPage() {
  const [activeStep, setActiveStep] = useState('connections');
  const [stepStatuses, setStepStatuses] = useState<Record<string, string>>({});
  const [state, setState] = useState<MigrationState>({
    sourceConnectionId: null, destConnectionId: null,
    graphId: null, intentId: null, dagId: null,
    runId: null, compiledCode: null, commitLog: [],
  });

  const setStepDone = (step: string) =>
    setStepStatuses(s => ({ ...s, [step]: 'done' }));

  const activeIndex = STEPS.findIndex(s => s.id === activeStep);
  const progressPct = Math.round(((activeIndex + 1) / STEPS.length) * 100);

  const renderStep = () => {
    switch (activeStep) {
      case 'connections':
        return <ConnectionSetup state={state} setState={setState} onDone={() => { setStepDone('connections'); setActiveStep('graph'); }} />;
      case 'graph':
        return <GraphViewer state={state} setState={setState} onDone={() => { setStepDone('graph'); setActiveStep('command'); }} />;
      case 'command':
        return <CommandInput state={state} setState={setState} onDone={() => { setStepDone('command'); setActiveStep('intent'); }} />;
      case 'intent':
        return <IntentViewer state={state} onDone={() => { setStepDone('intent'); setActiveStep('dag'); }} />;
      case 'dag':
        return <DAGViewer state={state} setState={setState} onDone={() => { setStepDone('dag'); setActiveStep('code'); }} />;
      case 'code':
        return <CodeViewer state={state} setState={setState} onDone={() => { setStepDone('code'); setActiveStep('run'); }} />;
      case 'run':
        return <RunTimeline state={state} setState={setState} onDone={() => { setStepDone('run'); setActiveStep('preview'); }} />;
      case 'preview':
        return <PreviewGrid state={state} onDone={() => { setStepDone('preview'); setActiveStep('approve'); }} />;
      case 'approve':
        return <ApproveReject state={state} setState={setState} onDone={() => { setStepDone('approve'); setActiveStep('log'); }} />;
      case 'log':
        return <CommitLog state={state} />;
      default:
        return null;
    }
  };

  return (
    <div className="app-main">
      {/* ── Enterprise Pipeline Navigator ──────────────────── */}
      <nav className="sidebar">
        <div style={{ padding: '16px 20px', borderBottom: '1px solid var(--border-subtle)' }}>
          <div className="flex items-center justify-between" style={{ marginBottom: 8 }}>
            <span style={{ fontSize: 11, fontWeight: 700, letterSpacing: '0.08em', color: 'var(--text-muted)' }}>
              PIPELINE PROGRESS
            </span>
            <span style={{ fontSize: 12, fontWeight: 800, color: 'var(--accent-cyan)' }}>{progressPct}%</span>
          </div>
          <div style={{ height: 4, background: 'rgba(255,255,255,0.08)', borderRadius: 99, overflow: 'hidden' }}>
            <div style={{
              height: '100%', width: `${progressPct}%`,
              background: 'linear-gradient(90deg, #6366f1, #06b6d4)',
              borderRadius: 99, transition: 'width 300ms ease',
            }} />
          </div>
        </div>

        <div style={{ flex: 1, overflowY: 'auto', padding: '12px 10px' }}>
          {STEPS.map((step) => {
            const isActive = activeStep === step.id;
            const isDone = stepStatuses[step.id] === 'done';
            return (
              <button
                key={step.id}
                className={`pipeline-step ${isActive ? 'active' : ''}`}
                onClick={() => setActiveStep(step.id)}
                style={{ width: '100%', textAlign: 'left', background: 'transparent', color: 'inherit' }}
                id={`step-${step.id}`}
              >
                <div className="step-icon-badge" style={{
                  background: isActive ? `${step.color}30` : 'rgba(255,255,255,0.04)',
                  color: isActive ? step.color : 'var(--text-muted)',
                  border: `1px solid ${isActive ? step.color : 'transparent'}`,
                }}>
                  {isDone ? '✓' : step.icon}
                </div>
                <div style={{ flex: 1 }}>
                  <div style={{
                    fontSize: 13, fontWeight: isActive ? 700 : 500,
                    color: isActive ? 'var(--text-primary)' : isDone ? 'var(--text-secondary)' : 'var(--text-muted)',
                  }}>
                    {step.label}
                  </div>
                </div>
                {isDone && <span className="badge badge-connected" style={{ fontSize: 9, padding: '1px 6px' }}>READY</span>}
              </button>
            );
          })}
        </div>

        {/* ── Environment Status Box ──────────────────────── */}
        <div style={{ padding: 16, borderTop: '1px solid var(--border-subtle)', background: 'rgba(5,7,13,0.5)' }}>
          <div style={{ fontSize: 11, fontWeight: 700, letterSpacing: '0.06em', color: 'var(--text-muted)', marginBottom: 8 }}>
            ENV HEALTH
          </div>
          <div className="flex flex-col gap-2">
            <StatusRow label="Source Conn" ok={!!state.sourceConnectionId} />
            <StatusRow label="Dest Conn" ok={!!state.destConnectionId} />
            <StatusRow label="Graph Index" ok={!!state.graphId} />
            <StatusRow label="Compiled DAG" ok={!!state.dagId} />
          </div>
        </div>
      </nav>

      {/* ── Main View Area ─────────────────────────────────── */}
      <main className="content-area fade-in" key={activeStep} style={{ flex: 1, overflowY: 'auto', padding: 32 }}>
        {renderStep()}
      </main>
    </div>
  );
}

function StatusRow({ label, ok }: { label: string; ok: boolean }) {
  return (
    <div className="flex items-center justify-between" style={{ fontSize: 11 }}>
      <span style={{ color: 'var(--text-muted)' }}>{label}</span>
      <span style={{ color: ok ? '#34d399' : 'var(--text-muted)', fontWeight: 600 }}>
        {ok ? '● ONLINE' : '○ PENDING'}
      </span>
    </div>
  );
}
