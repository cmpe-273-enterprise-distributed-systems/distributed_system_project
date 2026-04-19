import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import { signup } from '../api';

export default function SignupScreen() {
  const { login: setUser } = useAuth();
  const navigate = useNavigate();
  const [name,     setName]     = useState('');
  const [email,    setEmail]    = useState('');
  const [password, setPassword] = useState('');
  const [role,     setRole]     = useState(null); // 'client' | 'server'
  const [error,    setError]    = useState('');
  const [loading,  setLoading]  = useState(false);

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!role) { setError('Please choose a role to continue.'); return; }
    setError('');
    setLoading(true);
    try {
      const user = await signup(name, email, password, role);
      setUser(user);
    } catch (err) {
      setError(err.message);
    }
    setLoading(false);
  };

  return (
    <div style={{
      height: '100vh', background: 'var(--bg)',
      display: 'flex', alignItems: 'center', justifyContent: 'center',
      padding: '24px', position: 'relative', overflow: 'hidden',
    }}>
      {/* Grid bg */}
      <div style={{
        position: 'absolute', inset: 0, pointerEvents: 'none',
        backgroundImage: `
          linear-gradient(to right, rgba(108,86,245,0.04) 1px, transparent 1px),
          linear-gradient(to bottom, rgba(108,86,245,0.04) 1px, transparent 1px)`,
        backgroundSize: '40px 40px',
      }} />

      {/* Card */}
      <div className="animate-in" style={{
        width: '100%', maxWidth: '460px',
        background: 'var(--surface)', borderRadius: '20px',
        border: '1px solid var(--border)', boxShadow: 'var(--shadow-lg)',
        padding: '40px 36px',
      }}>
        {/* Logo */}
        <div style={{ textAlign: 'center', marginBottom: '28px' }}>
          <div style={{
            width: '52px', height: '52px', borderRadius: '14px',
            background: 'var(--purple)', display: 'inline-flex',
            alignItems: 'center', justifyContent: 'center',
            fontSize: '22px', boxShadow: '0 6px 20px rgba(108,86,245,0.35)',
            marginBottom: '16px',
          }}>⚡</div>
          <h1 style={{ fontSize: '22px', fontWeight: '800', color: 'var(--text)', letterSpacing: '-0.02em', marginBottom: '4px' }}>
            Create account
          </h1>
          <p style={{ fontSize: '14px', color: 'var(--text-muted)' }}>Join the Distributed AI Gateway</p>
        </div>

        <form onSubmit={handleSubmit} style={{ display: 'flex', flexDirection: 'column', gap: '14px' }}>
          <Field label="Full name">
            <input
              type="text" value={name} onChange={e => setName(e.target.value)}
              placeholder="Your name" required style={inputStyle}
              onFocus={e => { e.target.style.borderColor = 'var(--purple)'; e.target.style.boxShadow = '0 0 0 3px rgba(108,86,245,0.12)'; }}
              onBlur={e =>  { e.target.style.borderColor = 'var(--border)';  e.target.style.boxShadow = 'none'; }}
            />
          </Field>
          <Field label="Email address">
            <input
              type="email" value={email} onChange={e => setEmail(e.target.value)}
              placeholder="you@example.com" required style={inputStyle}
              onFocus={e => { e.target.style.borderColor = 'var(--purple)'; e.target.style.boxShadow = '0 0 0 3px rgba(108,86,245,0.12)'; }}
              onBlur={e =>  { e.target.style.borderColor = 'var(--border)';  e.target.style.boxShadow = 'none'; }}
            />
          </Field>
          <Field label="Password">
            <input
              type="password" value={password} onChange={e => setPassword(e.target.value)}
              placeholder="Min. 8 characters" required minLength={6} style={inputStyle}
              onFocus={e => { e.target.style.borderColor = 'var(--purple)'; e.target.style.boxShadow = '0 0 0 3px rgba(108,86,245,0.12)'; }}
              onBlur={e =>  { e.target.style.borderColor = 'var(--border)';  e.target.style.boxShadow = 'none'; }}
            />
          </Field>

          {/* Role picker */}
          <Field label="Choose your role">
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '10px' }}>
              <RoleCard
                selected={role === 'client'}
                onClick={() => setRole('client')}
                icon="💬"
                title="Client"
                subtitle="Send prompts to the cluster"
                accent="var(--purple)"
                accentLight="var(--purple-light)"
                accentBorder="var(--purple-border)"
              />
              <RoleCard
                selected={role === 'server'}
                onClick={() => setRole('server')}
                icon="🖥"
                title="Server"
                subtitle="Contribute compute power"
                accent="var(--teal)"
                accentLight="var(--teal-light)"
                accentBorder="var(--teal-border)"
              />
            </div>
            <p style={{ fontSize: '11px', color: 'var(--text-muted)', marginTop: '6px' }}>
              Admin can change your role later
            </p>
          </Field>

          {error && (
            <div style={{
              background: 'var(--red-light)', border: '1px solid rgba(229,66,77,0.25)',
              borderRadius: 'var(--radius-xs)', padding: '10px 14px',
              fontSize: '13px', color: 'var(--red)',
            }}>
              {error}
            </div>
          )}

          <button type="submit" disabled={loading} style={{
            marginTop: '4px', padding: '13px', borderRadius: 'var(--radius-sm)',
            background: loading ? 'var(--surface-3)' : 'var(--purple)',
            border: 'none', color: loading ? 'var(--text-muted)' : '#fff',
            fontSize: '15px', fontWeight: '700', fontFamily: 'inherit',
            cursor: loading ? 'not-allowed' : 'pointer',
            boxShadow: loading ? 'none' : '0 4px 16px rgba(108,86,245,0.35)',
            transition: 'all 0.2s', display: 'flex', alignItems: 'center',
            justifyContent: 'center', gap: '8px',
          }}>
            {loading && <span style={{
              width: '14px', height: '14px', borderRadius: '50%',
              border: '2px solid rgba(255,255,255,0.3)', borderTopColor: '#fff',
              display: 'inline-block', animation: 'spin 0.7s linear infinite',
            }} />}
            {loading ? 'Creating account…' : 'Create account'}
          </button>
        </form>

        <div style={{ marginTop: '24px', textAlign: 'center' }}>
          <p style={{ fontSize: '13px', color: 'var(--text-muted)' }}>
            Already have an account?{' '}
            <button onClick={() => navigate('/login')} style={{
              background: 'none', border: 'none', color: 'var(--purple)',
              fontWeight: '600', cursor: 'pointer', fontSize: '13px', fontFamily: 'inherit',
            }}>
              Sign in
            </button>
          </p>
        </div>
      </div>
    </div>
  );
}

function RoleCard({ selected, onClick, icon, title, subtitle, accent, accentLight, accentBorder }) {
  return (
    <button
      type="button"
      onClick={onClick}
      style={{
        padding: '14px 12px', borderRadius: 'var(--radius-sm)', cursor: 'pointer',
        border: `1.5px solid ${selected ? accentBorder : 'var(--border)'}`,
        background: selected ? accentLight : 'var(--surface-2)',
        textAlign: 'left', fontFamily: 'inherit', transition: 'all 0.18s',
        outline: selected ? `3px solid ${accent}22` : 'none',
        outlineOffset: '2px',
      }}
      onMouseEnter={e => { if (!selected) e.currentTarget.style.border = `1.5px solid ${accentBorder}`; }}
      onMouseLeave={e => { if (!selected) e.currentTarget.style.border = `1.5px solid var(--border)`; }}
    >
      <div style={{ fontSize: '20px', marginBottom: '6px' }}>{icon}</div>
      <div style={{ fontSize: '13px', fontWeight: '700', color: selected ? accent : 'var(--text)', marginBottom: '2px' }}>
        {title}
      </div>
      <div style={{ fontSize: '11px', color: 'var(--text-muted)', lineHeight: '1.4' }}>{subtitle}</div>
    </button>
  );
}

function Field({ label, children }) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
      <label style={{ fontSize: '13px', fontWeight: '600', color: 'var(--text-sub)' }}>{label}</label>
      {children}
    </div>
  );
}

const inputStyle = {
  padding: '11px 14px', borderRadius: 'var(--radius-sm)',
  border: '1.5px solid var(--border)', background: 'var(--surface-2)',
  color: 'var(--text)', fontSize: '14px', outline: 'none',
  fontFamily: 'inherit', transition: 'border-color 0.15s, box-shadow 0.15s',
};
