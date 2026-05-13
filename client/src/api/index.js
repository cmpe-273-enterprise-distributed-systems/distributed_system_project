/*
  api/index.js
  All network calls in one place.

  Part 5 wiring:
  - Dynamic leader resolution via a Discovery Service on boot
  - Self-healing axios interceptor that retries after leader failover
  - Streaming chat via SSE (POST /ask/stream) with fetch stream reader
*/

import axios from 'axios';

const DISCOVERY_URL = (process.env.REACT_APP_DISCOVERY_URL || '').trim();
const FALLBACK_LEADER_URL = (process.env.REACT_APP_LEADER_FALLBACK_URL || 'http://localhost:8000').trim();
// Backwards-compatible: older env used an IP only (no scheme/port).
const LEGACY_LEADER_IP = (process.env.REACT_APP_LEADER_IP || '').trim();

const LEADER_CACHE_KEY = 'ai_gateway_leader_url';

let _leaderUrl = null;
let _leaderPromise = null;

function _normalizeBase(url) {
  if (!url) return null;
  const u = String(url).trim().replace(/\/+$/, '');
  if (!u) return null;
  return u;
}

async function _resolveLeaderFromDiscovery() {
  if (!DISCOVERY_URL) return null;
  const res = await fetch(DISCOVERY_URL, { method: 'GET' });
  if (!res.ok) return null;
  const txt = (await res.text()).trim();
  return _normalizeBase(txt);
}

function _fromLegacyIp(ip) {
  const v = String(ip || '').trim();
  if (!v) return null;
  if (v.startsWith('http://') || v.startsWith('https://')) return _normalizeBase(v);
  // Treat as host/IP, default port 8000
  return _normalizeBase(`http://${v}:8000`);
}

function _readCachedLeader() {
  try {
    return _normalizeBase(localStorage.getItem(LEADER_CACHE_KEY));
  } catch {
    return null;
  }
}

function _writeCachedLeader(url) {
  try {
    if (url) localStorage.setItem(LEADER_CACHE_KEY, url);
  } catch {
    // ignore
  }
}

async function _isHealthy(baseUrl) {
  const u = _normalizeBase(baseUrl);
  if (!u) return false;
  try {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), 1200);
    const res = await fetch(`${u}/health`, { method: 'GET', signal: ctrl.signal });
    clearTimeout(t);
    return res.ok;
  } catch {
    return false;
  }
}

export async function ensureLeader({ forceRefresh = false } = {}) {
  if (!forceRefresh && _leaderUrl) return _leaderUrl;

  if (!_leaderPromise || forceRefresh) {
    _leaderPromise = (async () => {
      const discovered = await _resolveLeaderFromDiscovery();
      const cached = _readCachedLeader();
      const legacy = _fromLegacyIp(LEGACY_LEADER_IP);
      const fallback = _normalizeBase(FALLBACK_LEADER_URL);

      // Try candidates in order, but verify they respond to /health.
      const candidates = [discovered, cached, legacy, fallback].map(_normalizeBase).filter(Boolean);
      for (const c of candidates) {
        if (await _isHealthy(c)) {
          _leaderUrl = c;
          _writeCachedLeader(c);
          return c;
        }
      }

      // If nothing responds (e.g. leader booting), still return fallback so UI can show errors.
      _leaderUrl = fallback || candidates[0] || null;
      if (_leaderUrl) _writeCachedLeader(_leaderUrl);
      return _leaderUrl;
    })().finally(() => {
      // keep _leaderPromise for dedupe; do not clear
    });
  }

  return await _leaderPromise;
}

const api = axios.create({
  baseURL: FALLBACK_LEADER_URL,
  timeout: 15000,
});

api.interceptors.request.use(async (config) => {
  const leader = await ensureLeader();
  config.baseURL = leader;
  return config;
});

api.interceptors.response.use(
  (res) => res,
  async (err) => {
    const cfg = err?.config;
    const hasResponse = !!err?.response;
    const status = err?.response?.status;

    // Retry only once for leader failover / network errors.
    if (cfg && !cfg.__retried && (!hasResponse || [502, 503, 504].includes(status))) {
      cfg.__retried = true;
      await ensureLeader({ forceRefresh: true });
      return api.request(cfg);
    }
    throw err;
  }
);

/* ── Auth ─────────────────────────────────── */

// Real: POST /auth/login  { email, password }
export async function login(email, password) {
  try {
    const res = await api.post(`/auth/login`, { email, password });
    return res.data;
  } catch (err) {
    if (err.response && err.response.data && err.response.data.detail) {
      throw new Error(err.response.data.detail);
    }
    throw new Error('Failed to connect to the server.');
  }
}

// Real: POST /auth/signup  { name, email, password, role }
export async function signup(name, email, password, role) {
  try {
    const res = await api.post(`/auth/signup`, { name, email, password, role });
    return res.data;
  } catch (err) {
    if (err.response && err.response.data && err.response.data.detail) {
      throw new Error(err.response.data.detail);
    }
    throw new Error('Failed to connect to the server.');
  }
}

/* ── Cluster stats ────────────────────────── */

// Real: GET /cluster/stats
export async function getClusterStats() {
  const res = await api.get(`/cluster/stats`);
  return res.data;
}

// Real: GET /cluster/nodes
export async function getNodes() {
  const res = await api.get(`/cluster/nodes`);
  return res.data;
}

// Real: GET /cluster/requests
export async function getRequests() {
  const res = await api.get(`/cluster/requests`);
  return res.data;
}

/* ── Admin ────────────────────────────────── */

// Real: GET /admin/users
export async function getUsers() {
  const res = await api.get(`/admin/users`);
  return res.data;
}

// Real: PATCH /admin/users/:id  { role }
export async function updateUserRole(userId, role) {
  const res = await api.patch(`/admin/users/${userId}`, { role });
  return res.data;
}

// Real: DELETE /admin/users/:id
export async function removeUser(userId) {
  const res = await api.delete(`/admin/users/${userId}`);
  return res.data;
}

/* ── Node ─────────────────────────────────── */

// Real: POST /register  { node_id, ram_gb, model, skills }
export async function registerNode(nodeData) {
  const res = await api.post(`/register`, nodeData);
  return res.data;
}

// Real: POST /heartbeat  { node_id, status, tasks_completed }
export async function sendHeartbeat(nodeData) {
  const res = await api.post(`/heartbeat`, nodeData);
  return res.data;
}

/* ── Chat ─────────────────────────────────── */

// Real: GET /health
export async function healthCheck() {
  try {
    const res = await api.get(`/health`, { timeout: 5000 });
    return res.data;
  } catch (err) {
    // ChatScreen uses .catch() to flip the UI to "offline"
    throw new Error('Leader health check failed.');
  }
}

// Real: POST /ask  { prompt }
export async function sendPrompt(prompt, userId, userName) {
  const res = await api.post(`/ask`, { prompt, user_id: userId || '', user_name: userName || 'anonymous' });
  return res.data;
}

// Streaming: POST /ask/stream (SSE)
// Usage: sendPromptStream(prompt, userId, userName, ({ type, data }) => { ... })
//
// Manual one-shot retry on 503/network failure to mirror the axios interceptor:
// fetch() doesn't go through axios, so the dormant-FastAPI gate (Scenario 2B
// Task 3) returns 503 here without anyone catching it. Re-resolve the leader
// via discovery and try once more.
export async function sendPromptStream(prompt, userId, userName, onEvent) {
  const body = JSON.stringify({ prompt, user_id: userId || '', user_name: userName || 'anonymous' });

  let res;
  let leader = await ensureLeader();
  try {
    res = await fetch(`${leader}/ask/stream`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body,
    });
  } catch (networkErr) {
    leader = await ensureLeader({ forceRefresh: true });
    res = await fetch(`${leader}/ask/stream`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body,
    });
  }

  if (res && [502, 503, 504].includes(res.status)) {
    leader = await ensureLeader({ forceRefresh: true });
    res = await fetch(`${leader}/ask/stream`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body,
    });
  }

  if (!res.ok || !res.body) {
    throw new Error('Failed to start streaming request.');
  }

  const reader = res.body.getReader();
  const decoder = new TextDecoder('utf-8');
  let buf = '';
  let finalResponse = '';

  const emit = (type, data) => {
    try { onEvent && onEvent({ type, data }); } catch { /* ignore */ }
  };

  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    buf += decoder.decode(value, { stream: true });

    // Parse SSE frames split by blank line
    while (true) {
      const idx = buf.indexOf('\n\n');
      if (idx === -1) break;
      const frame = buf.slice(0, idx);
      buf = buf.slice(idx + 2);

      const lines = frame.split('\n').map(l => l.replace(/\r$/, ''));
      let evt = 'message';
      let dataLines = [];
      for (const line of lines) {
        if (line.startsWith('event:')) evt = line.slice(6).trim();
        else if (line.startsWith('data:')) dataLines.push(line.slice(5).trim());
      }
      const data = dataLines.join('\n');
      if (!data) continue;

      if (evt === 'result') {
        finalResponse = data.replace(/\\n/g, '\n');
        emit('result', finalResponse);
      } else if (evt === 'error') {
        emit('error', data);
        throw new Error(data);
      } else {
        emit(evt, data);
      }
    }
  }

  if (!finalResponse) {
    throw new Error('Stream ended without a response.');
  }
  return { response: finalResponse };
}
