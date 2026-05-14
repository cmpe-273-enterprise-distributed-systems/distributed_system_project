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

uvicorn main:app --host 0.0.0.0 --port 8000
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

## Multi-laptop demo on Tailscale

This is the operational checklist for the multi-laptop demo (3
leader-eligible laptops on Tailscale, each running both `leader/main.py`
and `worker.py`). The local pipeline above is a strict subset — most of
this is one-time setup per laptop or per cluster.

### Phase 1 — Tailscale mesh

1. Install Tailscale on every laptop, sign into the same tailnet.
2. `tailscale ip -4` on each laptop — note the `100.x.x.x` IP.
3. From each laptop, ping every other laptop's Tailscale IP. All pairs
   must respond before you go further.

### Phase 2 — Per-machine prerequisites

On each laptop:

1. Docker installed (`docker --version`).
2. Same git branch on every machine.
3. Python venv set up:
   ```bash
   python -m venv .venv
   .venv/bin/pip install -r server/leader/requirements.txt -r server/worker/requirements.txt
   ```
4. Ollama installed and at least one model pulled (`ollama pull mistral`).
   The worker silently falls back to a mock response without Ollama,
   which is fine for failover testing but useless for an actual demo.

### Phase 3 — Shared infrastructure

#### Astra DataStax (one-time, by anyone on the team)

Steps 1–5 from the [Mode A — Astra DataStax](#mode-a--astra-datastax-default-required-for-multi-laptop-demo) section above. The same secure
connect bundle and credentials get shared with every leader-eligible
laptop privately (`secrets/` is gitignored). Apply the schema via the
Astra console once; every leader connects to the same DB so user data
and request history survive leader failover.

#### Kafka KRaft quorum (per-laptop broker)

On **each** of the 3 leader-eligible laptops:

1. Copy `server/kafka/.env.example` → `server/kafka/.env` and set:
   - `KAFKA_NODE_ID` — unique per laptop (1, 2, 3).
   - `KAFKA_ADVERTISED_HOST` — that laptop's Tailscale IP.
   - `KAFKA_CLUSTER_ID` — same value on all three. Generate once with
     `docker run --rm confluentinc/cp-kafka:7.5.0 kafka-storage random-uuid`.
   - `KAFKA_QUORUM_VOTERS` — all three peers, e.g.
     `1@A_TS_IP:9093,2@B_TS_IP:9093,3@C_TS_IP:9093`.
2. Start the broker: `docker compose -f server/kafka/docker-compose.yml up -d`
3. After all three brokers are up, run **once** from any laptop to
   pre-create topics with the right replication factor (default RF=1
   would break case-B failover):
   ```bash
   python server/scripts/ensure_kafka_topics.py --brokers <A_TS_IP>:9092,<B_TS_IP>:9092,<C_TS_IP>:9092
   ```
   The leader's lifespan also calls this on every boot (idempotent), so
   this script is for one-off ops or pre-seeding.

#### Discovery service (Upstash Redis)

Create one Upstash Redis instance for the cluster (or use an existing
one). Share the REST URL + token with every leader and worker. They
go in the per-laptop `.env` files (next phase).

### Phase 4 — Per-laptop config files

For each leader-eligible laptop:

1. **`server/leader/.env`**:
   - `KAFKA_BROKER` — comma-separated list of all three brokers'
     Tailscale IPs (e.g. `100.64.0.5:9092,100.64.0.6:9092,100.64.0.7:9092`).
     Required for case-D (broker-only death) failover.
   - `LEADER_ADVERTISE_HOST` — this laptop's Tailscale IP. **Critical.**
     Without it the leader publishes its WSL2/local hostname to discovery
     and to peers, which isn't routable.
   - `USE_ASTRA=true` plus `ASTRA_BUNDLE_PATH` / `ASTRA_CLIENT_ID` /
     `ASTRA_CLIENT_SECRET` (from Phase 3).
   - `UPSTASH_REDIS_REST_URL` and `UPSTASH_REDIS_REST_TOKEN`.

2. **`server/worker/.env`** on the same laptop:
   - `KAFKA_BROKER` — same comma-separated list.
   - `LEADER_URL=http://<this_laptop_TS_IP>:8000` (fallback only;
     discovery overrides at runtime).
   - `UPSTASH_REDIS_REST_URL` and `UPSTASH_REDIS_REST_READ_ONLY_TOKEN`.
   - `NODE_ID` — distinct from the leader process's node_id on the same
     machine (e.g. `worker-A`).

3. **`server/leader/config/node.yaml` priorities** — election is
   deterministic by priority (highest wins). Set:
   - Laptop A → priority 100 (initial leader)
   - Laptop B → priority 90 (first failover target)
   - Laptop C → priority 80 (second failover target)

   The file is auto-generated on first run with default priority 100.
   Edit it after first run, or pass explicit `priority` in
   `POST /cluster/create`.

### Phase 5 — Bootstrap order

1. **All laptops**: Kafka brokers — `docker compose -f server/kafka/docker-compose.yml up -d`.
2. **Laptop A** (highest priority): start `python server/leader/main.py`.
   Then bootstrap the cluster:
   ```bash
   curl -X POST http://localhost:8000/cluster/create \
     -H 'Content-Type: application/json' \
     -d '{"role":"both","priority":100,"port":8000}'
   ```
   Save the `cluster_id` and `join_token` from the response.
3. **Laptops B and C**: start `python server/leader/main.py`, then
   `POST /cluster/join` with B's (then C's) local node info plus the
   cluster_id and join_token from A.
4. **Each laptop**: in a second terminal, start
   `python server/worker/worker.py` with a `NODE_ID` distinct from the
   leader process's.

### Phase 6 — Verification before demoing

From any laptop:

- `curl http://<A_TS_IP>:8000/cluster/status` lists all 3 leaders +
  3 workers in `known_nodes`; `current_leader.node_id` is A's leader
  process node_id.
- `curl http://<A_TS_IP>:8000/discovery/leader` returns A's URL.
- `curl -H "Authorization: Bearer $UPSTASH_TOKEN" "$UPSTASH_URL/get/leader"`
  also resolves to A's URL.
- Send a test prompt:
  ```bash
  curl -X POST http://<A_TS_IP>:8000/ask \
    -H 'Content-Type: application/json' \
    -d '{"prompt":"hi"}'
  ```
  Completes within a few seconds; response includes a `worker`.
- `curl http://<A_TS_IP>:8000/cluster/skills` returns the 3 seed skills.

### Common gotchas

1. **`LEADER_ADVERTISE_HOST` is mandatory** on WSL2. Without it the
   election silently breaks because peers can't reach the published
   hostname.
2. **All three brokers in `KAFKA_BROKER`**, not just localhost. Case-D
   failover relies on this being the full list.
3. **Topic RF must be 3, not 1.** Auto-creation gives 1; pre-create
   with `ensure_kafka_topics.py` from Phase 3.
4. **Node priorities must be unique.** Equal priorities tie-break by
   lexicographic node_id, which makes failover order non-obvious.
5. **Don't forget to start workers.** Without them, `/ask` 504s and
   you'll mistake it for a failover bug.

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
