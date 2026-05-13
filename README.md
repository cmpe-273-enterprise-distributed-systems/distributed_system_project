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
<!--
### Frontend — `client/` ✅ DONE
| Feature | Status |
|---|---|
| Login / Signup screens | ✅ Done |
| Role-based routing (admin / client / server) | ✅ Done |
| Admin dashboard — Overview, Nodes, Users, Requests tabs | ✅ Done |
| Client chat screen (send prompts, show responses) | ✅ Done |
| Server node screen (register, heartbeat, activity log) | ✅ Done |
| API layer (`src/api/index.js`) wired up with mock data | ✅ Done |
| Auth context (user persisted to localStorage) | ✅ Done |
| Connect to real backend (swap mocks in `api/index.js`) | 🔲 Waiting on backend -->

## Check the Project Completion Checklist in `project_completion_checklist.md` for more details.

<!-- 
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
| Discovery server (returns current leader IP) | 🔲 | -->

---

## How to Run the Full Pipeline Locally

To run the complete system (React -> FastAPI -> Cassandra), you will need to open **two or three separate terminal windows** depending on your database mode.

### 1. The Database

The leader supports two modes, selected by the `USE_ASTRA` env var in
`server/leader/.env`:

#### Mode A — Astra DataStax (default; required for multi-laptop demo)

User data and request history live in a managed cloud Cassandra so they
survive leader failover. Set up once per database, then every leader
connects to the same instance.

1. Sign up at [astra.datastax.com](https://astra.datastax.com), create a
   serverless DB, and create a keyspace named `web_app`.
2. Open the Astra console's CQL editor against `web_app` and run
   `client/db/002_tables.cql` followed by `client/db/003_seed_data.cql`.
   Skip `001_keyspace.cql` — Astra creates the keyspace itself.
3. Generate an application token (Database Administrator role); copy
   `clientId` + `clientSecret`.
4. Download the **Secure Connect Bundle** (.zip) and place it at
   `secrets/secure-connect-web-app.zip` (or wherever
   `ASTRA_BUNDLE_PATH` points). The .zip is gitignored.
5. In `server/leader/.env`:
   ```
   USE_ASTRA=true
   ASTRA_BUNDLE_PATH=./secrets/secure-connect-web-app.zip
   ASTRA_CLIENT_ID=<from token>
   ASTRA_CLIENT_SECRET=<from token>
   ASTRA_KEYSPACE=web_app
   ```

No "database terminal" is needed in Astra mode.

#### Mode B — Local docker Cassandra (offline-dev fallback only)

Data is leader-local and does **not** survive failover. Useful for
working without internet or before Astra is provisioned.

**Requirements:** Docker and Python 3 installed.

In `server/leader/.env`, set `USE_ASTRA=false` (then run in a dedicated terminal):
```bash
cd client

# Start the container
make db-up

# Wait for boot and apply schemas
make db-setup
```
*(To reset the database, run `make db-reset`. To view data, run `make db-shell`)*

### 2. The FastAPI Backend
```bash
cd server/leader
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

python3 -m uvicorn main:app --reload
```

### 3. The React Frontend
**Requirements:** Node.js installed
```bash
cd client

# Install dependencies (only needed once)
npm install

# Create .env file (only needed once)
cp .env.example .env

# Start the dev server
npm start
```

Opens automatically at **http://localhost:3000**

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
# in client/
cp .env.example .env
# then edit .env and set REACT_APP_LEADER_IP to the leader's actual Tailscale IP
```

`.env` is gitignored — each teammate has their own copy with the current leader IP.
When the leader changes (re-election), only `.env` needs to be updated, not any code.

---
