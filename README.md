# Distributed AI Gateway

A distributed system where laptops pool compute over a VPN to run local AI models. Users send prompts from any device — the cluster handles the rest.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     CLIENT DEVICES                      │
│                                                         │
│   Browser → localhost:3000 (React App)                  │
│       ↓ Login / Signup                                  │
│       ↓ Send prompt (POST /ask)                         │
└───────────────────────┬─────────────────────────────────┘
                        │  Tailscale VPN
                        ▼
┌───────────────────────────────────────────────────────────┐
│                    LEADER NODE                            │
│                                                           │
│   FastAPI server on port 8000                             │
│   ├── GET  /health          → status check               │
│   ├── POST /ask             → accept prompt, return AI   │
│   ├── POST /register        → worker joins cluster       │
│   ├── POST /heartbeat       → worker keep-alive          │
│   └── GET  /cluster/*       → admin dashboard data       │
│                                                           │
│   Kafka → distributes tasks to available workers          │
│   Leader election → if leader dies, next node takes over │
└──────────┬───────────────────────┬────────────────────────┘
           │                       │
           ▼                       ▼
┌──────────────────┐   ┌──────────────────┐
│   WORKER NODE 1  │   │   WORKER NODE 2  │   ...
│                  │   │                  │
│  Ollama (local)  │   │  Ollama (local)  │
│  mistral-7b      │   │  phi-2           │
│  Skills:         │   │  Skills:         │
│   - general      │   │   - coding       │
│   - coding       │   │                  │
└──────────────────┘   └──────────────────┘
```

**VPN:** All nodes connect via Tailscale. Clients only need the leader's VPN IP.

---

## What's done ✅ / What's left 🔲

### Frontend — `web-app/` ✅ DONE
| Feature | Status |
|---|---|
| Login / Signup screens | ✅ Done |
| Role-based routing (admin / client / server) | ✅ Done |
| Admin dashboard — Overview, Nodes, Users, Requests tabs | ✅ Done |
| Client chat screen (send prompts, show responses) | ✅ Done |
| Server node screen (register, heartbeat, activity log) | ✅ Done |
| API layer (`src/api/index.js`) wired up with mock data | ✅ Done |
| Auth context (user persisted to localStorage) | ✅ Done |
| Connect to real backend (swap mocks in `api/index.js`) | 🔲 Waiting on backend |

### Backend — Leader Node 🔲 NOT STARTED
| Feature | Status |
|---|---|
| `GET /health` | 🔲 |
| `POST /ask` — receive prompt, route via Kafka, return response | 🔲 |
| `POST /register` — add worker node to registry | 🔲 |
| `POST /heartbeat` — update node status, trigger re-election if needed | 🔲 |
| `GET /cluster/stats`, `/cluster/nodes`, `/cluster/requests` | 🔲 |
| `GET/PATCH/DELETE /admin/users` | 🔲 |
| Leader election logic | 🔲 |
| Kafka task queue setup | 🔲  |

### Infrastructure 🔲 IN PROGRESS
| Feature | Status |
|---|---|
| Tailscale VPN mesh between all laptops | 🔲 |
| Ollama installed and running on worker nodes | 🔲 |
| Skill files (`.skill`) per worker | 🔲 |
| Discovery server (returns current leader IP) | 🔲 |

---

## How to run the frontend

**Requirements:** Node.js installed

```bash
# 1. Go into the frontend folder
cd web-app

# 2. Install dependencies (only needed once after cloning)
npm install

#3. Create .env file
cp .env.example .env

# 4. Start the dev server
npm start
```

Opens at **http://localhost:3000**

> `node_modules/` is gitignored — always run `npm install` after pulling from GitHub.

---

## Logging in

| Role | Email | Password |
|---|---|---|
| Admin | `admin@cluster.local` | `admin` |
| Client user | `shan@example.com` | `password` |
| Server user | `abhin@example.com` | `password` |

All data is mocked locally — no backend needed to run the frontend.

---

## Connecting to the real backend

When the backend is ready, **only one file needs to change:** `src/api/index.js`

Every function in that file has a comment showing the real endpoint:

```js
// Real: POST /auth/login  { email, password }
export async function login(email, password) {
  // swap this mock with:
  // const res = await axios.post(`${BASE_URL}/auth/login`, { email, password });
  // return res.data.user;
}
```

Also create your `.env` file with the leader's Tailscale IP:

```bash
# in web-app/
cp .env.example .env
# then edit .env and set REACT_APP_LEADER_IP to the leader's actual Tailscale IP
```

`.env` is gitignored — each teammate has their own copy with the current leader IP.
When the leader changes (re-election), only `.env` needs to be updated, not any code.

---
