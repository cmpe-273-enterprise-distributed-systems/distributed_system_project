import { useState, useEffect } from 'react';
import { useAuth } from '../context/AuthContext';
import { getClusterStats, getNodes, getRequests, getUsers, updateUserRole, removeUser } from '../api';

const TABS = ['Overview', 'Nodes', 'Users', 'Requests'];

const timeAgo = (ts) => {
  const d = Date.now() - ts;
  if (d < 10000)   return 'just now';
  if (d < 60000)   return `${Math.floor(d / 1000)}s ago`;
  if (d < 3600000) return `${Math.floor(d / 60000)}m ago`;
  return `${Math.floor(d / 3600000)}h ago`;
};

const truncate = (str, n = 55) => str.length > n ? str.slice(0, n) + '…' : str;

export default function AdminDashboard() {
  const { user, logout } = useAuth();
  const [tab,      setTab]      = useState('Overview');
  const [stats,    setStats]    = useState(null);
  const [nodes,    setNodes]    = useState([]);
  const [users,    setUsers]    = useState([]);
  const [requests, setRequests] = useState([]);
  const [loading,  setLoading]  = useState(true);
  const [nodeFilter, setNodeFilter] = useState('all');

  useEffect(() => {
    fetchAll();
    const interval = setInterval(fetchAll, 10000); // refresh every 10s
    return () => clearInterval(interval);
  }, []);

  const fetchAll = async () => {
    const [s, n, u, r] = await Promise.all([
      getClusterStats(), getNodes(), getUsers(), getRequests(),
    ]);
    setStats(s); setNodes(n); setUsers(u); setRequests(r);
    setLoading(false);
  };

  const handleRoleChange = async (userId, newRole) => {
    await updateUserRole(userId, newRole);
    setUsers(prev => prev.map(u => u.id === userId ? { ...u, role: newRole } : u));
  };

  const handleRemoveUser = async (userId) => {
    await removeUser(userId);
    setUsers(prev => prev.filter(u => u.id !== userId));
  };

  const filteredNodes = nodeFilter === 'all'
    ? nodes
    : nodes.filter(n => nodeFilter === 'online' ? n.status !== 'offline' : n.status === 'offline');

  return (
    <div style={{ display: 'flex', height: '100vh', background: 'var(--bg)', fontFamily: 'inherit' }}>

      {/* ── Sidebar ── */}
      <aside style={{
        width: '220px', flexShrink: 0,
        background: 'var(--surface)', borderRight: '1px solid var(--border)',
        display: 'flex', flexDirection: 'column', padding: '0',
      }}>
        {/* Logo */}
        <div style={{ padding: '20px 20px 16px', borderBottom: '1px solid var(--border)' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
            <div style={{
              width: '32px', height: '32px', borderRadius: '9px',
              background: 'var(--purple)', display: 'flex',
              alignItems: 'center', justifyContent: 'center', fontSize: '15px',
              boxShadow: '0 3px 10px rgba(108,86,245,0.3)', flexShrink: 0,
            }}>⚡</div>
            <div>
              <div style={{ fontWeight: '800', fontSize: '13px', color: 'var(--text)', letterSpacing: '-0.01em' }}>AI Gateway</div>
              <div style={{ fontSize: '10px', color: 'var(--text-muted)', fontWeight: '500', letterSpacing: '0.06em', textTransform: 'uppercase' }}>Admin Panel</div>
            </div>
          </div>
        </div>

        {/* Nav */}
        <nav style={{ flex: 1, padding: '12px 10px', display: 'flex', flexDirection: 'column', gap: '2px' }}>
          {TABS.map(t => (
            <button key={t} onClick={() => setTab(t)} style={{
              display: 'flex', alignItems: 'center', gap: '10px',
              padding: '9px 12px', borderRadius: 'var(--radius-xs)',
              border: 'none', cursor: 'pointer', fontFamily: 'inherit',
              background: tab === t ? 'var(--purple-light)' : 'transparent',
              color: tab === t ? 'var(--purple)' : 'var(--text-sub)',
              fontWeight: tab === t ? '600' : '500', fontSize: '13px',
              textAlign: 'left', transition: 'all 0.15s',
              borderLeft: tab === t ? '3px solid var(--purple)' : '3px solid transparent',
            }}
              onMouseEnter={e => { if (tab !== t) e.currentTarget.style.background = 'var(--surface-2)'; }}
              onMouseLeave={e => { if (tab !== t) e.currentTarget.style.background = 'transparent'; }}
            >
              <span style={{ fontSize: '15px' }}>{tabIcon(t)}</span>
              {t}
            </button>
          ))}
        </nav>

        {/* Live indicator */}
        <div style={{
          padding: '12px 16px', borderTop: '1px solid var(--border)',
          display: 'flex', alignItems: 'center', gap: '8px',
        }}>
          <span style={{
            width: '7px', height: '7px', borderRadius: '50%',
            background: 'var(--teal)', display: 'inline-block',
            animation: 'pulse-teal 2s ease infinite',
          }} />
          <span style={{ fontSize: '11px', color: 'var(--text-muted)' }}>Live — refreshes every 10s</span>
        </div>

        {/* User + logout */}
        <div style={{ padding: '12px 14px', borderTop: '1px solid var(--border)' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '10px', marginBottom: '10px' }}>
            <div style={{
              width: '32px', height: '32px', borderRadius: '50%', flexShrink: 0,
              background: 'var(--purple-light)', border: '1px solid var(--purple-border)',
              display: 'flex', alignItems: 'center', justifyContent: 'center',
              fontSize: '13px', color: 'var(--purple)', fontWeight: '700',
            }}>
              {user?.name?.charAt(0).toUpperCase()}
            </div>
            <div style={{ minWidth: 0 }}>
              <div style={{ fontSize: '12px', fontWeight: '600', color: 'var(--text)', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{user?.name}</div>
              <div style={{ fontSize: '10px', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em', fontWeight: '600' }}>Admin</div>
            </div>
          </div>
          <button onClick={logout} style={{
            width: '100%', padding: '7px', borderRadius: 'var(--radius-xs)',
            background: 'var(--surface-2)', border: '1px solid var(--border)',
            color: 'var(--text-sub)', fontSize: '12px', fontWeight: '600',
            fontFamily: 'inherit', cursor: 'pointer', transition: 'all 0.15s',
          }}
            onMouseEnter={e => { e.currentTarget.style.background = 'var(--red-light)'; e.currentTarget.style.color = 'var(--red)'; e.currentTarget.style.borderColor = 'rgba(229,66,77,0.25)'; }}
            onMouseLeave={e => { e.currentTarget.style.background = 'var(--surface-2)'; e.currentTarget.style.color = 'var(--text-sub)'; e.currentTarget.style.borderColor = 'var(--border)'; }}
          >
            Sign out
          </button>
        </div>
      </aside>

      {/* ── Main content ── */}
      <main style={{ flex: 1, overflowY: 'auto', display: 'flex', flexDirection: 'column' }}>
        {/* Page header */}
        <div style={{
          padding: '20px 28px', borderBottom: '1px solid var(--border)',
          background: 'var(--surface)', flexShrink: 0,
          display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        }}>
          <div>
            <h2 style={{ fontSize: '18px', fontWeight: '800', color: 'var(--text)', letterSpacing: '-0.02em' }}>{tab}</h2>
            <p style={{ fontSize: '12px', color: 'var(--text-muted)', marginTop: '2px' }}>{tabSubtitle(tab)}</p>
          </div>
          {!loading && stats && (
            <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
              <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
                {stats.nodesOnline}/{stats.nodesTotal} nodes online
              </span>
            </div>
          )}
        </div>

        {/* Tab content */}
        <div style={{ flex: 1, padding: '24px 28px' }}>
          {loading ? <LoadingSpinner /> : (
            <>
              {tab === 'Overview'  && <OverviewTab  stats={stats} nodes={nodes} requests={requests} />}
              {tab === 'Nodes'     && <NodesTab     nodes={filteredNodes} filter={nodeFilter} onFilter={setNodeFilter} />}
              {tab === 'Users'     && <UsersTab     users={users} onRoleChange={handleRoleChange} onRemove={handleRemoveUser} />}
              {tab === 'Requests'  && <RequestsTab  requests={requests} />}
            </>
          )}
        </div>
      </main>
    </div>
  );
}

/* ── Overview tab ─────────────────────────── */
function OverviewTab({ stats, nodes, requests }) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
      {/* Stat cards */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: '14px' }}>
        <StatCard label="Nodes Online"     value={`${stats.nodesOnline}/${stats.nodesTotal}`} icon="🖥"  accent="var(--purple)" accentBg="var(--purple-light)" />
        <StatCard label="Tasks Completed"  value={stats.tasksCompleted}  icon="✅"  accent="var(--teal)"   accentBg="var(--teal-light)" />
        <StatCard label="Avg Response"     value={stats.avgResponseTime} icon="⏱"  accent="var(--yellow)" accentBg="var(--yellow-light)" />
        <StatCard label="Active Clients"   value={stats.activeUsers}     icon="👤"  accent="var(--purple)" accentBg="var(--purple-light)" />
      </div>

      {/* Bottom row */}
      <div style={{ display: 'grid', gridTemplateColumns: '1.2fr 1fr', gap: '16px' }}>
        {/* Node status grid */}
        <div style={{ background: 'var(--surface)', borderRadius: 'var(--radius)', border: '1px solid var(--border)', overflow: 'hidden', boxShadow: 'var(--shadow-sm)' }}>
          <SectionHeader title="Node Status" count={nodes.length} />
          <div style={{ padding: '14px 18px', display: 'flex', flexDirection: 'column', gap: '8px' }}>
            {nodes.map(n => (
              <div key={n.id} style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
                <StatusDot status={n.status} />
                <code style={{ fontSize: '12px', color: 'var(--text-sub)', flex: 1 }}>{n.id}</code>
                <span style={{ fontSize: '11px', color: 'var(--text-muted)' }}>{n.model}</span>
                <StatusBadge status={n.status} />
              </div>
            ))}
          </div>
        </div>

        {/* Recent requests */}
        <div style={{ background: 'var(--surface)', borderRadius: 'var(--radius)', border: '1px solid var(--border)', overflow: 'hidden', boxShadow: 'var(--shadow-sm)' }}>
          <SectionHeader title="Recent Requests" count={requests.length} />
          <div style={{ padding: '14px 18px', display: 'flex', flexDirection: 'column', gap: '10px' }}>
            {requests.slice(0, 5).map(r => (
              <div key={r.id} style={{ borderBottom: '1px solid var(--border)', paddingBottom: '10px' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '3px' }}>
                  <span style={{ fontSize: '11px', fontWeight: '600', color: 'var(--purple)' }}>{r.userName}</span>
                  <span style={{ fontSize: '11px', color: 'var(--text-muted)' }}>{timeAgo(r.time)}</span>
                </div>
                <p style={{ fontSize: '12px', color: 'var(--text-sub)', lineHeight: '1.4' }}>{truncate(r.prompt, 52)}</p>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

/* ── Nodes tab ────────────────────────────── */
function NodesTab({ nodes, filter, onFilter }) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
      {/* Filter pills */}
      <div style={{ display: 'flex', gap: '8px' }}>
        {['all', 'online', 'offline'].map(f => (
          <button key={f} onClick={() => onFilter(f)} style={{
            padding: '5px 14px', borderRadius: '100px', fontFamily: 'inherit',
            fontSize: '12px', fontWeight: '600', cursor: 'pointer',
            border: '1px solid',
            background: filter === f ? 'var(--purple)' : 'var(--surface)',
            borderColor: filter === f ? 'var(--purple)' : 'var(--border)',
            color: filter === f ? '#fff' : 'var(--text-sub)',
            transition: 'all 0.15s',
          }}>
            {f.charAt(0).toUpperCase() + f.slice(1)}
          </button>
        ))}
      </div>

      <div style={{ background: 'var(--surface)', borderRadius: 'var(--radius)', border: '1px solid var(--border)', overflow: 'hidden', boxShadow: 'var(--shadow-sm)' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead>
            <tr style={{ borderBottom: '1px solid var(--border)', background: 'var(--surface-2)' }}>
              {['Node ID', 'VPN IP', 'Status', 'Model', 'Skills', 'Tasks', 'Last Seen'].map(h => (
                <th key={h} style={{ padding: '11px 16px', textAlign: 'left', fontSize: '11px', fontWeight: '700', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.06em', whiteSpace: 'nowrap' }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {nodes.map((n, i) => (
              <tr key={n.id} style={{ borderBottom: i < nodes.length - 1 ? '1px solid var(--border)' : 'none' }}
                onMouseEnter={e => e.currentTarget.style.background = 'var(--surface-2)'}
                onMouseLeave={e => e.currentTarget.style.background = 'transparent'}
              >
                <td style={td}><code style={{ fontSize: '12px', color: 'var(--text-sub)', background: 'var(--surface-3)', padding: '2px 6px', borderRadius: '4px', border: '1px solid var(--border)' }}>{n.id}</code></td>
                <td style={td}><code style={{ fontSize: '12px', color: 'var(--text-muted)' }}>{n.ip}</code></td>
                <td style={td}><StatusBadge status={n.status} /></td>
                <td style={td}><span style={{ fontSize: '12px', color: 'var(--text-sub)' }}>{n.model}</span></td>
                <td style={td}>
                  <div style={{ display: 'flex', gap: '4px', flexWrap: 'wrap' }}>
                    {n.skills.map(s => (
                      <span key={s} style={{ fontSize: '10px', fontWeight: '600', background: 'var(--purple-light)', color: 'var(--purple)', border: '1px solid var(--purple-border)', borderRadius: '100px', padding: '1px 7px' }}>{s}</span>
                    ))}
                  </div>
                </td>
                <td style={td}><span style={{ fontSize: '13px', fontWeight: '600', color: 'var(--text)' }}>{n.tasksCompleted}</span></td>
                <td style={td}><span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>{timeAgo(n.lastSeen)}</span></td>
              </tr>
            ))}
          </tbody>
        </table>
        {nodes.length === 0 && <p style={{ padding: '32px', textAlign: 'center', color: 'var(--text-muted)', fontSize: '13px' }}>No nodes match this filter.</p>}
      </div>
    </div>
  );
}

/* ── Users tab ────────────────────────────── */
function UsersTab({ users, onRoleChange, onRemove }) {
  return (
    <div style={{ background: 'var(--surface)', borderRadius: 'var(--radius)', border: '1px solid var(--border)', overflow: 'hidden', boxShadow: 'var(--shadow-sm)' }}>
      <table style={{ width: '100%', borderCollapse: 'collapse' }}>
        <thead>
          <tr style={{ borderBottom: '1px solid var(--border)', background: 'var(--surface-2)' }}>
            {['User', 'Email', 'Role', 'Joined', 'Actions'].map(h => (
              <th key={h} style={{ padding: '11px 16px', textAlign: 'left', fontSize: '11px', fontWeight: '700', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.06em' }}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {users.map((u, i) => (
            <tr key={u.id} style={{ borderBottom: i < users.length - 1 ? '1px solid var(--border)' : 'none', transition: 'background 0.1s' }}
              onMouseEnter={e => e.currentTarget.style.background = 'var(--surface-2)'}
              onMouseLeave={e => e.currentTarget.style.background = 'transparent'}
            >
              {/* Avatar + name */}
              <td style={td}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                  <div style={{
                    width: '30px', height: '30px', borderRadius: '50%', flexShrink: 0,
                    background: u.role === 'server' ? 'var(--teal-light)' : 'var(--purple-light)',
                    border: `1px solid ${u.role === 'server' ? 'var(--teal-border)' : 'var(--purple-border)'}`,
                    display: 'flex', alignItems: 'center', justifyContent: 'center',
                    fontSize: '12px', fontWeight: '700',
                    color: u.role === 'server' ? 'var(--teal)' : 'var(--purple)',
                  }}>{u.name.charAt(0)}</div>
                  <span style={{ fontSize: '13px', fontWeight: '600', color: 'var(--text)' }}>{u.name}</span>
                </div>
              </td>
              <td style={td}><span style={{ fontSize: '13px', color: 'var(--text-muted)' }}>{u.email}</span></td>
              {/* Role dropdown */}
              <td style={td}>
                <select
                  value={u.role}
                  onChange={e => onRoleChange(u.id, e.target.value)}
                  style={{
                    padding: '5px 10px', borderRadius: 'var(--radius-xs)',
                    border: '1px solid var(--border)', background: 'var(--surface-3)',
                    color: roleMeta(u.role).color, fontSize: '12px', fontWeight: '600',
                    fontFamily: 'inherit', cursor: 'pointer', outline: 'none',
                  }}
                >
                  <option value="client">Client</option>
                  <option value="server">Server</option>
                </select>
              </td>
              <td style={td}><span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>{u.joinedAt}</span></td>
              {/* Remove */}
              <td style={td}>
                <button
                  onClick={() => { if (window.confirm(`Remove ${u.name}?`)) onRemove(u.id); }}
                  style={{
                    padding: '5px 12px', borderRadius: 'var(--radius-xs)',
                    background: 'var(--surface-3)', border: '1px solid var(--border)',
                    color: 'var(--text-muted)', fontSize: '12px', fontWeight: '600',
                    fontFamily: 'inherit', cursor: 'pointer', transition: 'all 0.15s',
                  }}
                  onMouseEnter={e => { e.currentTarget.style.background = 'var(--red-light)'; e.currentTarget.style.color = 'var(--red)'; e.currentTarget.style.borderColor = 'rgba(229,66,77,0.25)'; }}
                  onMouseLeave={e => { e.currentTarget.style.background = 'var(--surface-3)'; e.currentTarget.style.color = 'var(--text-muted)'; e.currentTarget.style.borderColor = 'var(--border)'; }}
                >
                  Remove
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {users.length === 0 && <p style={{ padding: '32px', textAlign: 'center', color: 'var(--text-muted)', fontSize: '13px' }}>No users found.</p>}
    </div>
  );
}

/* ── Requests tab ─────────────────────────── */
function RequestsTab({ requests }) {
  return (
    <div style={{ background: 'var(--surface)', borderRadius: 'var(--radius)', border: '1px solid var(--border)', overflow: 'hidden', boxShadow: 'var(--shadow-sm)' }}>
      <table style={{ width: '100%', borderCollapse: 'collapse' }}>
        <thead>
          <tr style={{ borderBottom: '1px solid var(--border)', background: 'var(--surface-2)' }}>
            {['User', 'Prompt', 'Worker Node', 'Duration', 'Time'].map(h => (
              <th key={h} style={{ padding: '11px 16px', textAlign: 'left', fontSize: '11px', fontWeight: '700', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.06em' }}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {requests.map((r, i) => (
            <tr key={r.id} style={{ borderBottom: i < requests.length - 1 ? '1px solid var(--border)' : 'none' }}
              onMouseEnter={e => e.currentTarget.style.background = 'var(--surface-2)'}
              onMouseLeave={e => e.currentTarget.style.background = 'transparent'}
            >
              <td style={td}>
                <span style={{ fontSize: '12px', fontWeight: '600', background: 'var(--purple-light)', color: 'var(--purple)', border: '1px solid var(--purple-border)', borderRadius: '100px', padding: '2px 9px' }}>
                  {r.userName}
                </span>
              </td>
              <td style={{ ...td, maxWidth: '300px' }}>
                <span style={{ fontSize: '13px', color: 'var(--text-sub)' }}>{truncate(r.prompt)}</span>
              </td>
              <td style={td}><code style={{ fontSize: '11px', background: 'var(--surface-3)', border: '1px solid var(--border)', borderRadius: '4px', padding: '2px 6px', color: 'var(--text-muted)' }}>{r.worker}</code></td>
              <td style={td}>
                <span style={{ fontSize: '12px', fontWeight: '600', color: 'var(--teal)' }}>{r.duration}</span>
              </td>
              <td style={td}><span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>{timeAgo(r.time)}</span></td>
            </tr>
          ))}
        </tbody>
      </table>
      {requests.length === 0 && <p style={{ padding: '32px', textAlign: 'center', color: 'var(--text-muted)', fontSize: '13px' }}>No requests yet.</p>}
    </div>
  );
}

/* ── Shared sub-components ────────────────── */
function StatCard({ label, value, icon, accent, accentBg }) {
  return (
    <div style={{ background: 'var(--surface)', borderRadius: 'var(--radius-sm)', border: '1px solid var(--border)', padding: '20px', boxShadow: 'var(--shadow-sm)' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '12px' }}>
        <span style={{ fontSize: '11px', fontWeight: '700', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.07em' }}>{label}</span>
        <div style={{ width: '28px', height: '28px', borderRadius: '7px', background: accentBg, display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '14px' }}>{icon}</div>
      </div>
      <div style={{ fontSize: '28px', fontWeight: '800', color: 'var(--text)', letterSpacing: '-0.03em' }}>{value}</div>
    </div>
  );
}

function SectionHeader({ title, count }) {
  return (
    <div style={{ padding: '14px 18px', borderBottom: '1px solid var(--border)', background: 'var(--surface-2)', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
      <span style={{ fontSize: '12px', fontWeight: '700', color: 'var(--text-sub)', textTransform: 'uppercase', letterSpacing: '0.06em' }}>{title}</span>
      <span style={{ fontSize: '11px', color: 'var(--text-muted)', background: 'var(--surface-3)', border: '1px solid var(--border)', padding: '1px 8px', borderRadius: '100px' }}>{count}</span>
    </div>
  );
}

const nodeMeta = {
  idle:    { color: 'var(--teal)',   bg: 'var(--teal-light)',   border: 'var(--teal-border)',         label: 'Idle'    },
  busy:    { color: 'var(--yellow)', bg: 'var(--yellow-light)', border: 'rgba(217,119,6,0.25)',        label: 'Busy'    },
  offline: { color: 'var(--red)',    bg: 'var(--red-light)',    border: 'rgba(229,66,77,0.25)',        label: 'Offline' },
  leader:  { color: 'var(--purple)', bg: 'var(--purple-light)', border: 'var(--purple-border)',        label: 'Leader'  },
};

function StatusDot({ status }) {
  const m = nodeMeta[status] || nodeMeta.offline;
  return <span style={{ width: '8px', height: '8px', borderRadius: '50%', background: m.color, display: 'inline-block', flexShrink: 0 }} />;
}

function StatusBadge({ status }) {
  const m = nodeMeta[status] || nodeMeta.offline;
  return (
    <span style={{ fontSize: '11px', fontWeight: '600', background: m.bg, color: m.color, border: `1px solid ${m.border}`, borderRadius: '100px', padding: '2px 9px', textTransform: 'uppercase', letterSpacing: '0.04em' }}>
      {m.label}
    </span>
  );
}

function LoadingSpinner() {
  return (
    <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '200px' }}>
      <div style={{ width: '28px', height: '28px', borderRadius: '50%', border: '3px solid var(--purple-light)', borderTopColor: 'var(--purple)', animation: 'spin 0.7s linear infinite' }} />
    </div>
  );
}

const roleMeta = (role) => ({
  client: { color: 'var(--purple)' },
  server: { color: 'var(--teal)' },
})[role] || { color: 'var(--text-muted)' };

const td    = { padding: '13px 16px', verticalAlign: 'middle' };
const tabIcon = (t) => ({ Overview: '◈', Nodes: '🖥', Users: '👥', Requests: '📋' })[t];
const tabSubtitle = (t) => ({
  Overview: 'Live cluster health and activity summary',
  Nodes:    'All registered worker nodes and their status',
  Users:    'Manage user accounts and role assignments',
  Requests: 'Full history of AI prompt requests',
})[t];
