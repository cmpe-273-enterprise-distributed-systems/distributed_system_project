import './App.css';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { AuthProvider, useAuth } from './context/AuthContext';
import LoginScreen    from './screens/LoginScreen';
import SignupScreen   from './screens/SignupScreen';
import AdminDashboard from './screens/AdminDashboard';
import ModeSelection  from './screens/ModeSelection';
import ChatScreen     from './screens/ChatScreen';
import ServerStatus   from './screens/ServerStatus';

function AppRoutes() {
  const { user, loading, updateRole } = useAuth();

  if (loading) return (
    <div style={{
      height: '100vh', display: 'flex', alignItems: 'center',
      justifyContent: 'center', background: 'var(--bg)',
    }}>
      <div style={{
        width: '32px', height: '32px', borderRadius: '50%',
        border: '3px solid var(--purple-light)', borderTopColor: 'var(--purple)',
        animation: 'spin 0.7s linear infinite',
      }} />
    </div>
  );

  // ── Not logged in ──
  if (!user) return (
    <Routes>
      <Route path="/signup" element={<SignupScreen />} />
      <Route path="*"       element={<LoginScreen />} />
    </Routes>
  );

  // ── Admin ──
  if (user.role === 'admin') return (
    <Routes>
      <Route path="/admin" element={<AdminDashboard />} />
      <Route path="*"      element={<Navigate to="/admin" replace />} />
    </Routes>
  );

  // ── Client ──
  if (user.role === 'client') return <ChatScreen />;

  // ── Server ──
  if (user.role === 'server') return <ServerStatus />;

  // ── No role yet (edge case) ──
  return <ModeSelection onSelect={updateRole} />;
}

export default function App() {
  return (
    <AuthProvider>
      <BrowserRouter>
        <AppRoutes />
      </BrowserRouter>
    </AuthProvider>
  );
}