export default function ModeSelection({ onSelect }) {
  return (
    <div style={{
      height: '100vh',
      background: 'var(--bg)',
      display: 'flex',
      flexDirection: 'column',
      alignItems: 'center',
      justifyContent: 'center',
      padding: '32px',
      position: 'relative',
      overflow: 'hidden',
    }}>

      {/* Subtle grid pattern background */}
      <div style={{
        position: 'absolute', inset: 0, pointerEvents: 'none',
        backgroundImage: `
          linear-gradient(to right, rgba(108,86,245,0.04) 1px, transparent 1px),
          linear-gradient(to bottom, rgba(108,86,245,0.04) 1px, transparent 1px)
        `,
        backgroundSize: '40px 40px',
      }} />

      {/* Top accent blob */}
      <div style={{
        position: 'absolute', width: '500px', height: '300px',
        top: '-80px', left: '50%', transform: 'translateX(-50%)',
        background: 'radial-gradient(ellipse, rgba(108,86,245,0.08) 0%, transparent 70%)',
        pointerEvents: 'none',
      }} />

      {/* Logo mark */}
      <div className="animate-in" style={{
        width: '56px', height: '56px', borderRadius: '16px',
        background: 'var(--purple)', display: 'flex', alignItems: 'center',
        justifyContent: 'center', marginBottom: '20px',
        boxShadow: '0 8px 24px rgba(108,86,245,0.35)',
        fontSize: '24px',
      }}>
        ⚡
      </div>

      {/* Heading */}
      <h1 className="animate-in delay-1" style={{
        fontSize: '36px', fontWeight: '800', letterSpacing: '-0.03em',
        color: 'var(--text)', marginBottom: '10px', textAlign: 'center',
      }}>
        Distributed AI Gateway
      </h1>
      <p className="animate-in delay-2" style={{
        color: 'var(--text-sub)', fontSize: '15px', maxWidth: '380px',
        textAlign: 'center', lineHeight: '1.65', marginBottom: '44px',
      }}>
        Join the network as a user or contribute your machine's compute to the cluster.
      </p>

      {/* Cards */}
      <div className="animate-in delay-3" style={{
        display: 'flex', gap: '16px', width: '100%', maxWidth: '580px',
      }}>
        <ModeCard
          title="Use as Client"
          subtitle="Send prompts and receive AI-generated responses from the distributed cluster."
          icon={<ChatIcon />}
          accent="var(--purple)"
          accentLight="var(--purple-light)"
          accentBorder="var(--purple-border)"
          onClick={() => onSelect('client')}
        />
        <ModeCard
          title="Contribute as Server"
          subtitle="Run AI tasks locally and donate your compute power to the network."
          icon={<ServerIcon />}
          accent="var(--teal)"
          accentLight="var(--teal-light)"
          accentBorder="var(--teal-border)"
          onClick={() => onSelect('server')}
        />
      </div>

      <p className="animate-in delay-3" style={{
        marginTop: '28px', fontSize: '12px', color: 'var(--text-muted)',
      }}>
        Your selection is saved locally and can be changed anytime.
      </p>
    </div>
  );
}

function ModeCard({ title, subtitle, icon, accent, accentLight, accentBorder, onClick }) {
  return (
    <button
      onClick={onClick}
      style={{
        flex: 1, textAlign: 'left', cursor: 'pointer',
        background: 'var(--surface)', border: `1.5px solid var(--border)`,
        borderRadius: 'var(--radius)', padding: '28px 24px',
        display: 'flex', flexDirection: 'column', gap: '14px',
        boxShadow: 'var(--shadow-sm)', transition: 'all 0.22s cubic-bezier(0.22,1,0.36,1)',
        position: 'relative', overflow: 'hidden', color: 'var(--text)',
        fontFamily: 'inherit',
      }}
      onMouseEnter={e => {
        const el = e.currentTarget;
        el.style.transform = 'translateY(-5px)';
        el.style.boxShadow = 'var(--shadow-lg)';
        el.style.borderColor = accentBorder;
      }}
      onMouseLeave={e => {
        const el = e.currentTarget;
        el.style.transform = 'translateY(0)';
        el.style.boxShadow = 'var(--shadow-sm)';
        el.style.borderColor = 'var(--border)';
      }}
    >
      {/* Top-right accent stripe */}
      <div style={{
        position: 'absolute', top: 0, right: 0,
        width: '80px', height: '80px',
        background: `radial-gradient(circle at top right, ${accentLight}, transparent 70%)`,
        pointerEvents: 'none',
      }} />

      {/* Icon */}
      <div style={{
        width: '44px', height: '44px', borderRadius: '12px',
        background: accentLight, border: `1px solid ${accentBorder}`,
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        color: accent, flexShrink: 0,
      }}>
        {icon}
      </div>

      <div style={{ flex: 1 }}>
        <div style={{ fontSize: '16px', fontWeight: '700', color: 'var(--text)', marginBottom: '6px' }}>
          {title}
        </div>
        <div style={{ fontSize: '13px', color: 'var(--text-sub)', lineHeight: '1.6' }}>
          {subtitle}
        </div>
      </div>

      {/* Arrow footer */}
      <div style={{ display: 'flex', justifyContent: 'flex-end' }}>
        <span style={{
          fontSize: '12px', fontWeight: '600', color: accent,
          display: 'flex', alignItems: 'center', gap: '4px',
        }}>
          Get started <span style={{ fontSize: '15px' }}>→</span>
        </span>
      </div>
    </button>
  );
}

function ChatIcon() {
  return (
    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/>
    </svg>
  );
}

function ServerIcon() {
  return (
    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round">
      <rect x="2" y="2" width="20" height="8" rx="2"/>
      <rect x="2" y="14" width="20" height="8" rx="2"/>
      <circle cx="6" cy="6" r="1" fill="currentColor" stroke="none"/>
      <circle cx="6" cy="18" r="1" fill="currentColor" stroke="none"/>
    </svg>
  );
}