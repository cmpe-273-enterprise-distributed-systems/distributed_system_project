/* ─────────────────────────────────────────────
   api/index.js
   All network calls in one place.
   Mock responses are used now — swap each
   function body for real axios calls once the
   backend is ready. The signatures stay the same.
───────────────────────────────────────────── */

import axios from 'axios';
const BASE = 'http://localhost:8002';

const delay = (ms = 650) => new Promise(r => setTimeout(r, ms));

/* ── Mock data store ──────────────────────── */

const ADMIN_USER = {
  id: 0, name: 'Admin', email: 'admin@cluster.local', role: 'admin', joinedAt: '2026-04-01',
};

let mockUsers = [
  { id: 1, name: 'Shan',   email: 'shan@example.com',   password: 'password', role: 'client', joinedAt: '2026-04-10' },
  { id: 2, name: 'Abhin',  email: 'abhin@example.com',  password: 'password', role: 'server', joinedAt: '2026-04-10' },
  { id: 3, name: 'Conlyn', email: 'conlyn@example.com', password: 'password', role: 'server', joinedAt: '2026-04-11' },
];
let _nextUserId = 4;

let mockNodes = [
  { id: 'node_4f2a1c', ip: '100.64.0.2', status: 'idle',    model: 'mistral-7b', tasksCompleted: 34, lastSeen: Date.now() - 1000,   skills: ['general', 'coding'] },
  { id: 'node_8b3e9d', ip: '100.64.0.3', status: 'busy',    model: 'mistral-7b', tasksCompleted: 52, lastSeen: Date.now() - 2000,   skills: ['general'] },
  { id: 'node_2c7f5a', ip: '100.64.0.4', status: 'offline', model: 'phi-2',      tasksCompleted: 18, lastSeen: Date.now() - 240000, skills: ['coding'] },
  { id: 'node_9a1b6e', ip: '100.64.0.5', status: 'leader',  model: 'mistral-7b', tasksCompleted: 41, lastSeen: Date.now() - 500,   skills: ['general', 'coding'] },
];

let mockRequests = [
  { id: 'req_001', userId: 1, userName: 'Shan',   prompt: 'Explain recursion in simple terms',              worker: 'node_8b3e9d', duration: '12.4s', status: 'completed', time: Date.now() - 120000 },
  { id: 'req_002', userId: 1, userName: 'Shan',   prompt: 'Write a Python merge sort implementation',       worker: 'node_4f2a1c', duration: '8.1s',  status: 'completed', time: Date.now() - 300000 },
  { id: 'req_003', userId: 2, userName: 'Abhin',  prompt: 'What is Tailscale and how does it work?',       worker: 'node_9a1b6e', duration: '5.3s',  status: 'completed', time: Date.now() - 600000 },
  { id: 'req_004', userId: 3, userName: 'Conlyn', prompt: 'Kafka consumer group example in Java',          worker: 'node_4f2a1c', duration: '15.7s', status: 'completed', time: Date.now() - 900000 },
  { id: 'req_005', userId: 1, userName: 'Shan',   prompt: 'What are the CAP theorem trade-offs?',          worker: 'node_9a1b6e', duration: '9.2s',  status: 'completed', time: Date.now() - 1800000 },
];
let _nextReqId = 6;

/* ── Auth ─────────────────────────────────── */

// Real: POST /auth/login  { email, password }
export async function login(email, password) {
  try {
    const res = await axios.post(`${BASE}/auth/login`, { email, password });
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
    const res = await axios.post(`${BASE}/auth/signup`, { name, email, password, role });
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
  await delay(400);
  const online = mockNodes.filter(n => n.status !== 'offline').length;
  const tasks  = mockNodes.reduce((s, n) => s + n.tasksCompleted, 0);
  return {
    nodesOnline:     online,
    nodesTotal:      mockNodes.length,
    tasksCompleted:  tasks,
    avgResponseTime: '10.4s',
    activeUsers:     mockUsers.filter(u => u.role === 'client').length,
  };
}

// Real: GET /cluster/nodes
export async function getNodes() {
  await delay(400);
  return [...mockNodes];
}

// Real: GET /cluster/requests
export async function getRequests() {
  await delay(400);
  return [...mockRequests].sort((a, b) => b.time - a.time);
}

/* ── Admin ────────────────────────────────── */

// Real: GET /admin/users
export async function getUsers() {
  await delay(400);
  return mockUsers.map(({ password: _, ...u }) => u);
}

// Real: PATCH /admin/users/:id  { role }
export async function updateUserRole(userId, role) {
  await delay(400);
  const user = mockUsers.find(u => u.id === userId);
  if (!user) throw new Error('User not found.');
  user.role = role;
  const { password: _, ...safe } = user;
  return safe;
}

// Real: DELETE /admin/users/:id
export async function removeUser(userId) {
  await delay(400);
  const idx = mockUsers.findIndex(u => u.id === userId);
  if (idx === -1) throw new Error('User not found.');
  mockUsers.splice(idx, 1);
  return { success: true };
}

/* ── Node ─────────────────────────────────── */

// Real: POST /register  { node_id, ram_gb, model, skills }
export async function registerNode(nodeData) {
  await delay(800);
  return { status: 'registered', assigned_queue: `worker_${nodeData.node_id}` };
}

// Real: POST /heartbeat  { node_id, status, tasks_completed }
export async function sendHeartbeat(nodeData) {
  await delay(300);
  return { status: 'ok', is_leader: false, new_skill: null };
}

/* ── Chat ─────────────────────────────────── */

// Real: GET /health
export async function healthCheck() {
  try {
    const res = await axios.get(`${BASE}/health`, { timeout: 5000 });
    return res.data;
  } catch (err) {
    // ChatScreen uses .catch() to flip the UI to "offline"
    throw new Error('Leader health check failed.');
  }
}

// Real: POST /ask  { prompt }
export async function sendPrompt(prompt, userId, userName) {
  await delay(1800);
  const workers = mockNodes.filter(n => n.status !== 'offline');
  const worker  = workers[Math.floor(Math.random() * workers.length)];
  const duration = `${(Math.random() * 15 + 3).toFixed(1)}s`;
  mockRequests.push({
    id: `req_${String(_nextReqId++).padStart(3, '0')}`,
    userId, userName, prompt,
    worker: worker?.id || 'unknown',
    duration, status: 'completed', time: Date.now(),
  });
  return {
    response: `[Mock] This is a simulated response to: "${prompt}"\n\nIn production this reply comes from an Ollama model running on a worker node in the cluster.`,
  };
}
