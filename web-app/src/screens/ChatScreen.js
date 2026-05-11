import { useState, useRef, useEffect } from 'react';
import { useAuth } from '../context/AuthContext';
import { healthCheck, sendPromptStream } from '../api';

export default function ChatScreen() {
  const { user, logout } = useAuth();
  const [messages, setMessages] = useState([
    { role: 'assistant', content: `Hello ${user?.name}! I'm your distributed AI assistant. Ask me anything — your query will be routed to the best available node in the cluster.` }
  ]);
  const [input,   setInput]   = useState('');
  const [loading, setLoading] = useState(false);
  const [status,  setStatus]  = useState('connecting');
  const bottomRef = useRef(null);

  useEffect(() => {
    healthCheck()
      .then(() => setStatus('online'))
      .catch(() => setStatus('offline'));
  }, []);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, loading]);

  const send = async () => {
    const text = input.trim();
    if (!text || loading) return;
    setInput('');
    setMessages(prev => [...prev, { role: 'user', content: text }]);
    setLoading(true);
    try {
      // Insert an empty assistant message that we update as the stream arrives.
      const assistantIndex = messages.length + 1; // user message already appended above
      setMessages(prev => [...prev, { role: 'assistant', content: '' }]);

      let latest = '';
      const res = await sendPromptStream(text, user?.id, user?.name, ({ type, data }) => {
        if (type === 'result') {
          latest = data;
          setMessages(prev => prev.map((m, i) => (i === assistantIndex ? { ...m, content: latest } : m)));
        }
      });
      if (!latest && res?.response) {
        setMessages(prev => prev.map((m, i) => (i === assistantIndex ? { ...m, content: res.response } : m)));
      }
      setStatus('online');
    } catch {
      setStatus('offline');
      setMessages(prev => [...prev, { role: 'assistant', content: '⚠️ Could not reach the cluster. Make sure the VPN is connected and the leader node is running.' }]);
    }
    setLoading(false);
  };

  const statusMap = {
    online:     { color: 'var(--teal)',   bg: 'var(--teal-light)',   label: 'Online',      anim: 'pulse-teal 2s ease infinite' },
    offline:    { color: 'var(--red)',    bg: 'var(--red-light)',    label: 'Offline',     anim: 'pulse-red 2s ease infinite' },
    connecting: { color: 'var(--purple)', bg: 'var(--purple-light)', label: 'Connecting…', anim: 'pulse-ring 1.5s ease infinite' },
  };
  const sm = statusMap[status];

  return (
    <div className="screen" style={{ background: 'var(--bg)' }}>

      {/* Header */}
      <div style={{
        padding: '12px 20px', background: 'var(--surface)',
        borderBottom: '1px solid var(--border)',
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        flexShrink: 0, boxShadow: 'var(--shadow-sm)',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
          <div style={{
            width: '32px', height: '32px', borderRadius: '9px', background: 'var(--purple)',
            display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '15px',
            boxShadow: '0 3px 10px rgba(108,86,245,0.3)',
          }}>⚡</div>
          <div>
            <span style={{ fontWeight: '700', fontSize: '14px', color: 'var(--text)' }}>AI Gateway</span>
            <span style={{ display: 'inline-block', marginLeft: '8px', fontSize: '11px', color: 'var(--text-muted)', background: 'var(--surface-3)', border: '1px solid var(--border)', borderRadius: '4px', padding: '1px 7px' }}>
              Client
            </span>
          </div>
          {/* Status */}
          <div style={{ display: 'flex', alignItems: 'center', gap: '6px', background: sm.bg, border: `1px solid ${sm.color}44`, borderRadius: '100px', padding: '4px 11px' }}>
            <span style={{ width: '7px', height: '7px', borderRadius: '50%', background: sm.color, display: 'inline-block', animation: sm.anim }} />
            <span style={{ fontSize: '11px', color: sm.color, fontWeight: '600' }}>{sm.label}</span>
          </div>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
          {/* User chip */}
          <div style={{ display: 'flex', alignItems: 'center', gap: '7px', background: 'var(--surface-3)', border: '1px solid var(--border)', borderRadius: '100px', padding: '4px 12px 4px 6px' }}>
            <div style={{ width: '22px', height: '22px', borderRadius: '50%', background: 'var(--purple-light)', border: '1px solid var(--purple-border)', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '10px', fontWeight: '700', color: 'var(--purple)' }}>
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

      {/* Messages */}
      <div style={{ flex: 1, overflowY: 'auto', padding: '24px 20px', display: 'flex', flexDirection: 'column', gap: '16px' }}>
        {messages.map((msg, i) => (
          <div key={i} style={{ display: 'flex', justifyContent: msg.role === 'user' ? 'flex-end' : 'flex-start', alignItems: 'flex-end', gap: '8px' }}>
            {msg.role === 'assistant' && (
              <div style={{ width: '28px', height: '28px', borderRadius: '8px', flexShrink: 0, background: 'var(--purple)', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '12px', boxShadow: '0 2px 8px rgba(108,86,245,0.25)' }}>⚡</div>
            )}
            <div style={{
              maxWidth: '68%', padding: '12px 16px',
              borderRadius: msg.role === 'user' ? '18px 18px 4px 18px' : '18px 18px 18px 4px',
              background: msg.role === 'user' ? 'var(--purple)' : 'var(--surface)',
              color: msg.role === 'user' ? '#fff' : 'var(--text)',
              fontSize: '14px', lineHeight: '1.65',
              border: msg.role === 'assistant' ? '1px solid var(--border)' : 'none',
              boxShadow: msg.role === 'user' ? '0 4px 16px rgba(108,86,245,0.25)' : 'var(--shadow-sm)',
              whiteSpace: 'pre-wrap',
            }}>
              {msg.content}
            </div>
          </div>
        ))}

        {loading && (
          <div style={{ display: 'flex', alignItems: 'flex-end', gap: '8px' }}>
            <div style={{ width: '28px', height: '28px', borderRadius: '8px', background: 'var(--purple)', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '12px', boxShadow: '0 2px 8px rgba(108,86,245,0.25)' }}>⚡</div>
            <div style={{ padding: '14px 18px', borderRadius: '18px 18px 18px 4px', background: 'var(--surface)', border: '1px solid var(--border)', boxShadow: 'var(--shadow-sm)', display: 'flex', gap: '5px', alignItems: 'center' }}>
              {[0, 0.15, 0.3].map((delay, i) => (
                <span key={i} style={{ width: '7px', height: '7px', borderRadius: '50%', background: 'var(--purple-mid)', display: 'inline-block', animation: `bounce-dot 1.2s ${delay}s ease infinite` }} />
              ))}
            </div>
          </div>
        )}
        <div ref={bottomRef} />
      </div>

      {/* Input bar */}
      <div style={{ padding: '14px 20px', background: 'var(--surface)', borderTop: '1px solid var(--border)', display: 'flex', gap: '10px', alignItems: 'flex-end', flexShrink: 0 }}>
        <input
          value={input} onChange={e => setInput(e.target.value)}
          onKeyDown={e => e.key === 'Enter' && send()}
          placeholder="Ask the cluster anything…"
          style={{ flex: 1, padding: '12px 16px', borderRadius: 'var(--radius-sm)', border: '1.5px solid var(--border)', background: 'var(--surface-2)', color: 'var(--text)', fontSize: '14px', outline: 'none', fontFamily: 'inherit', transition: 'border-color 0.15s, box-shadow 0.15s' }}
          onFocus={e => { e.target.style.borderColor = 'var(--purple)'; e.target.style.boxShadow = '0 0 0 3px rgba(108,86,245,0.12)'; }}
          onBlur={e =>  { e.target.style.borderColor = 'var(--border)';  e.target.style.boxShadow = 'none'; }}
        />
        <button
          onClick={send} disabled={loading || !input.trim()}
          style={{
            padding: '12px 22px', borderRadius: 'var(--radius-sm)',
            background: loading || !input.trim() ? 'var(--surface-3)' : 'var(--purple)',
            border: `1.5px solid ${loading || !input.trim() ? 'var(--border)' : 'transparent'}`,
            color: loading || !input.trim() ? 'var(--text-muted)' : '#fff',
            fontSize: '14px', fontWeight: '600', fontFamily: 'inherit',
            cursor: loading || !input.trim() ? 'not-allowed' : 'pointer',
            transition: 'all 0.2s', flexShrink: 0,
            boxShadow: loading || !input.trim() ? 'none' : '0 3px 12px rgba(108,86,245,0.35)',
          }}
        >
          Send
        </button>
      </div>
    </div>
  );
}