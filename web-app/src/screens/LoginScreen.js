import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import { login } from '../api';

export default function LoginScreen() {
  const { login: setUser } = useAuth();
  const navigate = useNavigate();
  const [email,    setEmail]    = useState('');
  const [password, setPassword] = useState('');
  const [error,    setError]    = useState('');
  const [loading,  setLoading]  = useState(false);

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');
    setLoading(true);
    try {
      const user = await login(email, password);
      setUser(user);
      // routing handled by App.js based on role
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
      {/* Glow */}
      <div style={{
        position: 'absolute', width: '500px', height: '300px',
        top: '-60px', left: '50%', transform: 'translateX(-50%)',
        background: 'radial-gradient(ellipse, rgba(108,86,245,0.09) 0%, transparent 70%)',
        pointerEvents: 'none',
      }} />

      {/* Card */}
      <div className="animate-in" style={{
        width: '100%', maxWidth: '420px',
        background: 'var(--surface)', borderRadius: '20px',
        border: '1px solid var(--border)', boxShadow: 'var(--shadow-lg)',
        padding: '40px 36px', position: 'relative',
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
            Welcome back
          </h1>
          <p style={{ fontSize: '14px', color: 'var(--text-muted)' }}>Sign in to the AI Gateway</p>
        </div>

        {/* Form */}
        <form onSubmit={handleSubmit} style={{ display: 'flex', flexDirection: 'column', gap: '14px' }}>
          <Field label="Email address">
            <input
              type="email" value={email} onChange={e => setEmail(e.target.value)}
              placeholder="you@example.com" required
              style={inputStyle}
              onFocus={e => { e.target.style.borderColor = 'var(--purple)'; e.target.style.boxShadow = '0 0 0 3px rgba(108,86,245,0.12)'; }}
              onBlur={e =>  { e.target.style.borderColor = 'var(--border)';  e.target.style.boxShadow = 'none'; }}
            />
          </Field>

          <Field label="Password">
            <input
              type="password" value={password} onChange={e => setPassword(e.target.value)}
              placeholder="••••••••" required
              style={inputStyle}
              onFocus={e => { e.target.style.borderColor = 'var(--purple)'; e.target.style.boxShadow = '0 0 0 3px rgba(108,86,245,0.12)'; }}
              onBlur={e =>  { e.target.style.borderColor = 'var(--border)';  e.target.style.boxShadow = 'none'; }}
            />
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
            {loading && <span style={spinnerStyle} />}
            {loading ? 'Signing in…' : 'Sign in'}
          </button>
        </form>

        {/* Divider hint */}
        <div style={{ marginTop: '24px', textAlign: 'center' }}>
          <p style={{ fontSize: '13px', color: 'var(--text-muted)' }}>
            Don't have an account?{' '}
            <button onClick={() => navigate('/signup')} style={{
              background: 'none', border: 'none', color: 'var(--purple)',
              fontWeight: '600', cursor: 'pointer', fontSize: '13px', fontFamily: 'inherit',
            }}>
              Sign up
            </button>
          </p>
        </div>

        {/* Demo hint */}
        <div style={{
          marginTop: '20px', padding: '12px 14px', borderRadius: 'var(--radius-xs)',
          background: 'var(--surface-2)', border: '1px solid var(--border)',
          fontSize: '12px', color: 'var(--text-muted)', lineHeight: '1.6',
        }}>
          <strong style={{ color: 'var(--text-sub)' }}>Demo accounts</strong><br />
          Admin: <code style={codeStyle}>admin@cluster.local</code> / <code style={codeStyle}>admin</code><br />
          User: <code style={codeStyle}>shan@example.com</code> / <code style={codeStyle}>password</code>
        </div>
      </div>
    </div>
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
const spinnerStyle = {
  width: '14px', height: '14px', borderRadius: '50%',
  border: '2px solid rgba(255,255,255,0.3)', borderTopColor: '#fff',
  display: 'inline-block', animation: 'spin 0.7s linear infinite',
};
const codeStyle = {
  background: 'var(--surface-3)', border: '1px solid var(--border)',
  borderRadius: '4px', padding: '1px 5px', fontFamily: 'monospace', fontSize: '11px',
};
