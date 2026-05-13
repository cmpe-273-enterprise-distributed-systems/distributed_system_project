/*
  api/index.js
  All network calls in one place.

  Part 5 wiring:
  - Dynamic leader resolution via a Discovery Service on boot
  - Self-healing axios interceptor that retries after leader failover
  - Streaming chat via SSE (POST /ask/stream) with fetch stream reader
*/

import axios from 'axios';

// Upstash Redis REST endpoint where the leader publishes its URL.
// The READ-ONLY token is intentionally exposed to the client bundle — its
// scope is reading from this single database, and the value behind the
// `leader` key (a routable URL) is broadcast across the cluster anyway.
// CORS must be enabled in the Upstash dashboard for the client's origin
// (Database → REST API → Allowed Origins).
const UPSTASH_REDIS_REST_URL = (process.env.REACT_APP_UPSTASH_REDIS_REST_URL || '').trim().replace(/\/+$/, '');
const UPSTASH_REDIS_REST_READ_ONLY_TOKEN = (process.env.REACT_APP_UPSTASH_REDIS_REST_READ_ONLY_TOKEN || '').trim();
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
  if (!UPSTASH_REDIS_REST_URL || !UPSTASH_REDIS_REST_READ_ONLY_TOKEN) return null;
  try {
    const res = await fetch(`${UPSTASH_REDIS_REST_URL}/get/leader`, {
      method: 'GET',
      headers: { Authorization: `Bearer ${UPSTASH_REDIS_REST_READ_ONLY_TOKEN}` },
    });
    if (!res.ok) return null;
    // Upstash envelopes the value in {"result": "<stored string>"}. The leader
    // publishes a JSON-encoded record (see server/leader/discovery.py) so the
    // stored string itself parses to {leader_url, node_id, cluster_id, updated_at}.
    const payload = await res.json();
    const raw = payload?.result;
    if (!raw) return null;
    let leaderUrl;
    try {
      leaderUrl = JSON.parse(raw)?.leader_url;
    } catch {
      // Legacy publishers may have stored a plain URL string.
      leaderUrl = String(raw);
    }
    return _normalizeBase((leaderUrl || '').trim());
  } catch {
    return null;
  }
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

// Real: GET /cluster/skills
// Returns the union of skills advertised by alive workers, sorted.
// Used by the chat-screen skill picker so users can override the leader's
// keyword-based auto-classifier when they want a specific skill applied.
export async function getClusterSkills() {
  try {
    const res = await api.get(`/cluster/skills`);
    return res.data?.skills || [];
  } catch {
    // Soft failure — picker just shows "Auto" with no other options.
    return [];
  }
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

// Real: POST /ask  { prompt, skill? }
export async function sendPrompt(prompt, userId, userName, skill) {
  const body = { prompt, user_id: userId || '', user_name: userName || 'anonymous' };
  if (skill) body.skill = skill;
  const res = await api.post(`/ask`, body);
  return res.data;
}

// Streaming: POST /ask/stream (SSE)
// Usage: sendPromptStream(prompt, userId, userName, skill, ({ type, data }) => { ... })
//
// `skill` is optional ("" / null = let the leader auto-classify from prompt
// keywords). When set, the leader treats it as a hard requirement and 503s
// if no worker advertises it.
//
// Manual one-shot retry on 503/network failure to mirror the axios interceptor:
// fetch() doesn't go through axios, so the dormant-FastAPI gate (Scenario 2B
// Task 3) returns 503 here without anyone catching it. Re-resolve the leader
// via discovery and try once more.
export async function sendPromptStream(prompt, userId, userName, skill, onEvent) {
  const payload = { prompt, user_id: userId || '', user_name: userName || 'anonymous' };
  if (skill) payload.skill = skill;
  const body = JSON.stringify(payload);

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
