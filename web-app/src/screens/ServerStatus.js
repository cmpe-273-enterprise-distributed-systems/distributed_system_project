import { useState, useEffect, useRef } from 'react';
import { useAuth } from '../context/AuthContext';
import { registerNode, sendHeartbeat } from '../api';

export default function ServerStatus() {
  const { user, logout } = useAuth();
  const NODE_ID = useRef(`node_${(user?.name || 'node').toLowerCase().replace(/\s+/g, '_')}_${Math.random().toString(36).substr(2, 4)}`).current;

  const [status,         setStatus]         = useState('offline');
  const [isLeader,       setIsLeader]       = useState(false);
  const [tasksCompleted, setTasksCompleted] = useState(0); // eslint-disable-line no-unused-vars
  const [currentSkill,   setCurrentSkill]   = useState('general');
  const [uptime,         setUptime]         = useState(0);
  const [log,            setLog]            = useState([]);
  const heartbeatRef = useRef(null);
  const uptimeRef    = useRef(null);

  const addLog = (msg, type = 'info') => {
    const time = new Date().toLocaleTimeString();
    setLog(prev => [{ time, msg, type }, ...prev].slice(0, 30));
  };

  useEffect(() => {
    register();
    heartbeatRef.current = setInterval(beat, 5000);
    uptimeRef.current    = setInterval(() => setUptime(s => s + 1), 1000);
    return () => { clearInterval(heartbeatRef.current); clearInterval(uptimeRef.current); };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const register = async () => {
    addLog('Registering with leader node…', 'info');
    try {
      await registerNode({ node_id: NODE_ID, ram_gb: 16, model: 'mistral-7b', skills: ['general', 'coding'] });
      setStatus('idle');
      addLog('Registered. Awaiting task assignment.', 'success');
    } catch {
      setStatus('offline');
      addLog('Could not reach leader. Will retry via heartbeat.', 'error');
    }
  };

  const beat = async () => {
    try {
      const res = await sendHeartbeat({ node_id: NODE_ID, status, tasks_completed: tasksCompleted });
      setStatus('idle');
      if (res.is_leader) { setIsLeader(true);            addLog('Elected as cluster leader!',     'success'); }
      if (res.new_skill) { setCurrentSkill(res.new_skill); addLog(`Skill updated → ${res.new_skill}`, 'info'); }
    } catch {
      setStatus('offline');
      addLog('Heartbeat failed — leader unreachable.', 'error');
    }
  };

  const displayStatus = isLeader ? 'leader' : status;
  const statusMeta = {
    idle:    { color: 'var(--teal)',   bg: 'var(--teal-light)',   border: 'var(--teal-border)',      anim: 'pulse-teal 2s ease infinite', label: 'Idle'    },
    busy:    { color: 'var(--yellow)', bg: 'var(--yellow-light)', border: 'rgba(217,119,6,0.25)',    anim: undefined,                     label: 'Busy'    },
    offline: { color: 'var(--red)',    bg: 'var(--red-light)',    border: 'rgba(229,66,77,0.25)',    anim: 'pulse-red 2s ease infinite',  label: 'Offline' },
    leader:  { color: 'var(--purple)', bg: 'var(--purple-light)', border: 'var(--purple-border)',    anim: 'pulse-ring 2s ease infinite', label: 'Leader'  },
  };
  const sm = statusMeta[displayStatus] || statusMeta.offline;

  const formatUptime = (s) => {
    const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), sec = s % 60;
    if (h > 0) return `${h}h ${m}m ${sec}s`;
    if (m > 0) return `${m}m ${sec}s`;
    return `${sec}s`;
  };

  const logStyle = {
    info:    { color: 'var(--text-sub)',  prefix: '◦' },
    success: { color: 'var(--teal)',      prefix: '✓' },
    error:   { color: 'var(--red)',       prefix: '✕' },
  };

  return (
    <div className="screen" style={{ background: 'var(--bg)' }}>

      {/* Header */}
      <div style={{ padding: '12px 24px', background: 'var(--surface)', borderBottom: '1px solid var(--border)', display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexShrink: 0, boxShadow: 'var(--shadow-sm)' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
          <div style={{ width: '32px', height: '32px', borderRadius: '9px', background: 'var(--teal)', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '15px', boxShadow: '0 3px 10px rgba(14,168,130,0.3)' }}>🖥</div>
          <div>
            <span style={{ fontWeight: '700', fontSize: '14px', color: 'var(--text)' }}>Server Node</span>
            <code style={{ display: 'inline-block', marginLeft: '8px', fontSize: '11px', color: 'var(--text-muted)', background: 'var(--surface-3)', border: '1px solid var(--border)', borderRadius: '4px', padding: '1px 7px' }}>{NODE_ID}</code>
          </div>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '7px', background: 'var(--surface-3)', border: '1px solid var(--border)', borderRadius: '100px', padding: '4px 12px 4px 6px' }}>
            <div style={{ width: '22px', height: '22px', borderRadius: '50%', background: 'var(--teal-light)', border: '1px solid var(--teal-border)', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '10px', fontWeight: '700', color: 'var(--teal)' }}>
              {user?.name?.charAt(0)}
            </div>
            <span style={{ fontSize: '12px', fontWeight: '600', color: 'var(--text-sub)' }}>{user?.name}</span>
          </div>
          <button onClick={logout} style={{
            background: 'var(--surface-3)', border: '1px solid var(--border)', color: 'var(--text-sub)',
            borderRadius: 'var(--radius-xs)', padding: '6px 12px', cursor: 'pointer',
            fontSize: '12px', fontFamily: 'inherit', fontWeight: '500', transition: 'all 0.15s',
          }}
            onMouseEnter={e => { e.currentTarget.style.background = 'var(--red-light)'; e.currentTarget.style.color = 'var(--red)'; }}
            onMouseLeave={e => { e.currentTarget.style.background = 'var(--surface-3)'; e.currentTarget.style.color = 'var(--text-sub)'; }}
          >
            Sign out
          </button>
        </div>
      </div>

      {/* Body */}
      <div style={{ flex: 1, overflowY: 'auto', padding: '24px', display: 'flex', flexDirection: 'column', gap: '16px' }}>

        {/* Status banner */}
        <div style={{ padding: '16px 20px', borderRadius: 'var(--radius)', background: sm.bg, border: `1.5px solid ${sm.border}`, display: 'flex', alignItems: 'center', gap: '14px', boxShadow: 'var(--shadow-sm)' }}>
          <div style={{ width: '11px', height: '11px', borderRadius: '50%', background: sm.color, flexShrink: 0, animation: sm.anim }} />
          <div style={{ flex: 1 }}>
            <span style={{ fontWeight: '700', fontSize: '15px', color: sm.color, textTransform: 'uppercase', letterSpacing: '0.06em' }}>{sm.label}</span>
            {isLeader && <span style={{ marginLeft: '10px', fontSize: '12px', color: 'var(--purple)', fontWeight: '500' }}>★ Coordinating the cluster</span>}
          </div>
          <div style={{ fontSize: '13px', color: 'var(--text-muted)', fontFamily: 'monospace' }}>Up {formatUptime(uptime)}</div>
        </div>

        {/* Stats */}
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: '12px' }}>
          <StatCard label="Tasks Completed" value={tasksCompleted} accent="var(--purple)" accentBg="var(--purple-light)" />
          <StatCard label="Current Skill"   value={currentSkill}  accent="var(--teal)"   accentBg="var(--teal-light)" />
          <StatCard label="Model"           value="mistral-7b"    accent="var(--yellow)" accentBg="var(--yellow-light)" />
        </div>

        {/* Activity log */}
        <div style={{ background: 'var(--surface)', border: '1px solid var(--border)', borderRadius: 'var(--radius)', flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden', boxShadow: 'var(--shadow-sm)' }}>
          <div style={{ padding: '13px 18px', borderBottom: '1px solid var(--border)', background: 'var(--surface-2)', display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexShrink: 0 }}>
            <span style={{ fontSize: '12px', fontWeight: '600', color: 'var(--text-sub)', letterSpacing: '0.06em', textTransform: 'uppercase' }}>Activity Log</span>
            <span style={{ fontSize: '11px', color: 'var(--text-muted)', background: 'var(--surface-3)', border: '1px solid var(--border)', padding: '1px 8px', borderRadius: '100px' }}>{log.length} entries</span>
          </div>
          <div style={{ overflowY: 'auto', flex: 1, padding: '8px 0' }}>
            {log.length === 0
              ? <p style={{ color: 'var(--text-muted)', fontSize: '13px', textAlign: 'center', padding: '32px' }}>No activity yet…</p>
              : log.map((entry, i) => {
                  const ls = logStyle[entry.type] || logStyle.info;
                  return (
                    <div key={i} style={{ display: 'flex', gap: '10px', alignItems: 'baseline', padding: '8px 18px', borderBottom: i < log.length - 1 ? '1px solid var(--border)' : 'none', background: i === 0 ? 'rgba(108,86,245,0.02)' : 'transparent' }}>
                      <span style={{ fontSize: '13px', color: ls.color, fontWeight: '700', flexShrink: 0 }}>{ls.prefix}</span>
                      <span style={{ fontSize: '11px', color: 'var(--text-muted)', fontFamily: 'monospace', flexShrink: 0, minWidth: '72px' }}>{entry.time}</span>
                      <span style={{ fontSize: '13px', color: ls.color, lineHeight: '1.5' }}>{entry.msg}</span>
                    </div>
                  );
                })}
          </div>
        </div>
      </div>
    </div>
  );
}

function StatCard({ label, value, accent, accentBg }) {
  return (
    <div style={{ background: 'var(--surface)', border: '1px solid var(--border)', borderRadius: 'var(--radius-sm)', padding: '18px 20px', boxShadow: 'var(--shadow-sm)' }}>
      <div style={{ display: 'inline-flex', alignItems: 'center', justifyContent: 'center', width: '28px', height: '28px', borderRadius: '7px', background: accentBg, marginBottom: '12px' }}>
        <div style={{ width: '8px', height: '8px', borderRadius: '50%', background: accent }} />
      </div>
      <p style={{ fontSize: '11px', fontWeight: '600', color: 'var(--text-muted)', letterSpacing: '0.07em', textTransform: 'uppercase', marginBottom: '6px' }}>{label}</p>
      <p style={{ fontSize: '22px', fontWeight: '700', color: 'var(--text)', letterSpacing: '-0.02em' }}>{value}</p>
    </div>
  );
}