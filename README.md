# Distributed AI Gateway

A distributed system where laptops pool compute over a VPN to run local AI models. Users send prompts from any device — the cluster handles load balancing, leader election, and failover automatically.

---

## Group Contributions

**Conlyn:** Kafka management, worker and leader node consumer/producer functionality, external data store and service discovery integration, skills management  
**Shivansh**: Contributed in designing the system architecture, Implemented the frontends and the hardware profiling specifications (RAM info, models, and skills extraction) during worker node registration.   
**Divya:** Implemented the distributed leader election, API endpoints, failover system and the full observability stack (Prometheus metrics, Grafana dashboards, structured JSON logging).

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
│   ├── POST /auth/login       → authenticate user         │
│   ├── POST /auth/signup      → create account            │
│   ├── GET  /health           → status check              │
│   ├── GET  /metrics          → Prometheus scrape target  │
│   ├── POST /ask              → classify + queue prompt   │
│   ├── POST /register         → worker joins cluster      │
│   ├── POST /heartbeat        → worker keep-alive         │
│   ├── GET  /cluster/*        → cluster admin API         │
│   └── GET  /discovery/leader → current leader URL        │
│                                                           │
│   Astra DataStax (managed Cassandra)                      │
│     → users + request history; shared across all leaders │
│   Kafka KRaft (single broker on designated laptop)        │
│     topics: tasks-high-ram  tasks-low-ram  tasks-general │
│   Upstash Redis — service discovery (current leader URL) │
│   Prometheus + Grafana — metrics and dashboards          │
└──────────┬───────────────────────┬────────────────────────┘
           │                       │
           ▼                       ▼
┌──────────────────┐   ┌──────────────────┐
│   WORKER NODE 1  │   │   WORKER NODE 2  │   ...
│                  │   │                  │
│  worker.py       │   │  worker.py       │
│  Ollama (local)  │   │  Ollama (local)  │
│  llama3.2:3b     │   │  qwen3:4b        │
│  Skills:         │   │  Skills:         │
│   - coding       │   │   - summarization│
│                  │   │   - creative-    │
│                  │   │     writing      │
└──────────────────┘   └──────────────────┘
```

**VPN:** All nodes connect via Tailscale. Clients only need the leader's VPN IP.

---

## Prerequisites

| Tool | Required By | Install |
|---|---|---|
| Python 3.10+ | Leader & Worker | [python.org](https://www.python.org/downloads/) |
| Node.js 18+ | Frontend | [nodejs.org](https://nodejs.org/) |
| Docker | Kafka, Cassandra, Prometheus, Grafana | [docker.com](https://www.docker.com/) |
| Ollama | Worker | [ollama.com](https://ollama.com/) |
| Tailscale | All nodes (multi-laptop only) | [tailscale.com](https://tailscale.com/) |

After installing Ollama, pull at least one model:
```bash
ollama pull llama3.2:3b
```

---

## Project Structure

```
ai-gateway/
├── server/
│   ├── leader/               # Leader node (FastAPI)
│   │   ├── main.py           # Endpoints, lifespan, Prometheus middleware
│   │   ├── cluster_state.py  # Bully-election state machine
│   │   ├── leader_monitor.py # Heartbeat + re-election loop
│   │   ├── kafka_client.py   # Tier-routing producer + result consumer
│   │   ├── kafka_admin.py    # Topic pre-creation (idempotent)
│   │   ├── registry.py       # Live worker registry
│   │   ├── database.py       # Astra / local Cassandra access
│   │   ├── discovery.py      # Upstash Redis discovery client
│   │   ├── metrics.py        # Prometheus counters / histograms / gauges
│   │   ├── logging_config.py # JSON structured logging
│   │   ├── system_checks.py  # Pre-flight requirement checks
│   │   └── config/           # node.yaml (node_id, priority, port)
│   ├── worker/               # Worker node (Kafka consumer + Ollama)
│   │   ├── worker.py         # Registration, heartbeat, task loop
│   │   ├── kafka_client.py   # Tier-aware consumer + result publisher
│   │   ├── ollama_client.py  # Ollama generate wrapper
│   │   └── skills/           # Skill directories (each with SKILL.md)
│   │       ├── coding/
│   │       ├── summarization/
│   │       └── creative-writing/
│   ├── kafka/                # Kafka KRaft compose (runs on one designated laptop)
│   ├── ui/                   # Server Node browser UI (port 8001)
│   │   ├── app.py            # FastAPI + Jinja2: setup checks, join, status
│   │   └── templates/
│   └── scripts/              # One-off ops scripts
│       ├── ensure_kafka_topics.py
│       ├── apply_schema_to_astra.py
│       └── start_local_cluster.py
├── client/                   # React frontend (port 3000)
│   ├── src/
│   │   ├── api/index.js      # API layer
│   │   ├── context/          # Auth context (localStorage)
│   │   └── screens/          # Login, Signup, Chat, Admin, Server
│   ├── db/                   # Cassandra CQL schemas + seed data
│   └── .env.example
├── grafana/
│   └── provisioning/         # Auto-provisioned datasources + dashboards
├── docker-compose.yml        # Local dev: Kafka + Cassandra + Prometheus + Grafana
├── prometheus.yml            # Scrape config (leader :8000/metrics every 5 s)
└── README.md
```

---

## Local Development

Run everything on a single machine with `docker compose` for infrastructure and separate terminals for the leader, worker, and frontend.

### Terminal 1 — Infrastructure (Docker)

```bash
docker compose up
```

Brings up Kafka (`:9092`), Cassandra (`:9042`), Prometheus (`:9090`), and Grafana (`:3001`). The `kafka-init` service automatically creates the four required topics (`tasks-high-ram`, `tasks-low-ram`, `tasks-general`, `completed-tasks`).

### Terminal 2 — Database setup (first run only)

**Astra (default, `USE_ASTRA=true`):** No terminal needed — the leader connects directly to the managed cloud database. See [Database Setup](#database-setup).

**Local docker fallback (`USE_ASTRA=false`):**
```bash
cd client
make db-setup   # waits for Cassandra to boot then applies schemas
```

> Local mode data is leader-local and does **not** survive leader failover.

### Terminal 3 — Leader Node

```bash
cd server/leader
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

python3 -m uvicorn main:app --reload
```

Leader starts on **http://localhost:8000**. Prometheus metrics at **http://localhost:8000/metrics**.

### Terminal 4 — Worker Node

```bash
cd server/worker
cp .env.example .env        # edit KAFKA_BROKER, MODEL, SKILLS as needed
pip install -r requirements.txt

python worker.py
```

### Terminal 5 — React Frontend

```bash
cd client
npm install
cp .env.example .env
npm start
```

Opens at **http://localhost:3000**.

---

## Observability

The leader exposes a Prometheus-compatible `/metrics` endpoint. The root `docker-compose.yml` brings up Prometheus and Grafana alongside Kafka and Cassandra.

| Service | URL | Notes |
|---|---|---|
| Prometheus | http://localhost:9090 | Scrapes leader at `host.docker.internal:8000` every 5 s |
| Grafana | http://localhost:3001 | Login: `admin` / `admin`; pre-provisioned AI Gateway dashboard |

### Tracked metrics

| Metric | Type | Description |
|---|---|---|
| `gateway_http_requests_total` | Counter | Requests by method, endpoint, status code |
| `gateway_http_request_duration_seconds` | Histogram | Per-endpoint latency |
| `gateway_ask_duration_seconds` | Histogram | End-to-end `/ask` latency including worker time |
| `gateway_tasks_dispatched_total` | Counter | Tasks sent to Kafka by topic |
| `gateway_tasks_by_tier_total` | Counter | Tasks dispatched by RAM tier |
| `gateway_tasks_completed_total` | Counter | Successfully completed tasks |
| `gateway_tasks_failed_total` | Counter | Timed-out or errored tasks |
| `gateway_tasks_completed_by_worker_total` | Counter | Per-worker completion count |
| `gateway_worker_ram_gb` | Gauge | RAM (GB) per registered worker |
| `gateway_nodes_active` | Gauge | Live (non-offline) worker count |
| `gateway_nodes_total` | Gauge | Total registered workers |
| `gateway_prompt_tokens_estimated` | Histogram | Estimated token count per prompt |

### Structured logging

All leader output is JSON (see `logging_config.py`). Set `LOG_LEVEL=DEBUG` in `server/leader/.env` for verbose output.

```json
{"ts": "2026-05-13T14:00:00Z", "level": "INFO", "logger": "main", "msg": "Task dispatched", "tier": "low-ram"}
```

---

## Skills System

Workers advertise specializations via `SKILL.md` files under `server/worker/skills/<skill-name>/`. Three skills ship with the repo:

| Skill | Description |
|---|---|
| `coding` | Clean Python with type hints and comments |
| `summarization` | Concise key-point extraction |
| `creative-writing` | Stories, poems, screenplays |

The leader auto-classifies prompts by keyword (e.g., "code", "summarize", "story") and routes to a worker advertising that skill. The `/ask` body also accepts an explicit `skill` field to force routing.

**The SKILL.md content is injected as Ollama's `system` prompt** when the task reaches the worker, tuning model behavior for that task type.

**RAM tier routing:** Before skill routing, the leader estimates prompt token count (`len(prompt) // 4`) and assigns a tier:
- `> 2000 tokens` → `tasks-high-ram` (workers with ≥ 16 GB RAM)
- `> 500 tokens` → `tasks-low-ram` (workers with ≥ 8 GB RAM)
- Otherwise → `tasks-general`

The leader walks down tiers if the preferred tier has no eligible workers.

To add a skill: create `server/worker/skills/<name>/SKILL.md`. The worker loads all skill directories on boot.

---

## Server Node UI

A lightweight server-side dashboard for cluster management, separate from the leader API:

```bash
cd server/ui
pip install -r requirements.txt
python app.py --port 8001 --leader http://localhost:8000
```

| Path | Purpose |
|---|---|
| `http://localhost:8001/` | Setup Check — verifies Tailscale, Ollama, Kafka, Cassandra |
| `http://localhost:8001/servers` | Cluster Status — live leader + known nodes |
| `http://localhost:8001/join` | Join Cluster — enter bootstrap URL + join code |

---

## Database Setup

### Astra DataStax (default; required for multi-laptop demo)

User data and request history live in managed cloud Cassandra and survive leader failover because every leader connects to the same instance.

1. Sign up at [astra.datastax.com](https://astra.datastax.com), create a serverless DB, and create a keyspace named `web_app`.
2. In the Astra CQL editor run `client/db/002_tables.cql` then `client/db/003_seed_data.cql`. Skip `001_keyspace.cql` — Astra creates the keyspace.
3. Generate an application token (Database Administrator role); copy `clientId` + `clientSecret`.
4. Download the **Secure Connect Bundle** (.zip) to `secrets/secure-connect-web-app.zip` (gitignored).
5. Set in `server/leader/.env`:
   ```
   USE_ASTRA=true
   ASTRA_BUNDLE_PATH=./secrets/secure-connect-web-app.zip
   ASTRA_CLIENT_ID=<clientId>
   ASTRA_CLIENT_SECRET=<clientSecret>
   ASTRA_KEYSPACE=web_app
   ```

### Local Docker Cassandra (offline-dev fallback)

Data is leader-local and does **not** survive failover. Useful for offline development before Astra is provisioned.

```bash
cd client
make db-up      # start the Cassandra container
make db-setup   # wait for boot + apply schemas
```

> To reset: `make db-reset` · To inspect: `make db-shell`

---

## Multi-laptop Production Demo

Three leader-eligible laptops on Tailscale, each running a Kafka broker, a leader process, and a worker process.

### Phase 1 — Tailscale mesh

1. Install Tailscale on every laptop; sign into the same tailnet.
2. `tailscale ip -4` on each laptop — note each `100.x.x.x` IP.
3. Ping every other laptop's Tailscale IP from each machine. All pairs must respond before proceeding.

### Phase 2 — Per-machine prerequisites

On each laptop:
1. Docker installed (`docker --version`).
2. Same git branch on every machine.
3. Python venv set up:
   ```bash
   python -m venv .venv
   .venv/bin/pip install -r server/leader/requirements.txt -r server/worker/requirements.txt
   ```
4. Ollama installed and at least one model pulled (`ollama pull llama3.2:3b`).

### Phase 3 — Shared infrastructure

#### Astra DataStax (one-time, any team member)

Follow the [Astra DataStax](#astra-datastax-default-required-for-multi-laptop-demo) steps above. Share the secure connect bundle and credentials privately with each laptop via `secrets/` (gitignored). Apply the schema once via the Astra console — all leaders connect to the same instance.

#### Kafka KRaft broker (one designated laptop)

Pick one laptop to host the Kafka broker (call it **Laptop K** — it can be the same as the initial leader or a dedicated machine):

1. Copy `server/kafka/.env.example` → `server/kafka/.env` and set:
   - `KAFKA_ADVERTISED_HOST` — Laptop K's Tailscale IP.
   - `KAFKA_CLUSTER_ID` — a base64-encoded UUID. Generate once:
     ```bash
     docker run --rm confluentinc/cp-kafka:7.5.0 kafka-storage random-uuid
     ```
2. Start the broker on Laptop K:
   ```bash
   docker compose -f server/kafka/docker-compose.yml up -d
   ```
3. Topics are created automatically by the `kafka-init` service. You can also run the provisioner manually (e.g., to verify RF):
   ```bash
   python server/scripts/ensure_kafka_topics.py --brokers <K_IP>:9092
   ```
   The leader also calls this idempotently on every boot.

> **Note:** A single broker is a single point of failure — if Laptop K goes down, in-flight tasks will time out until it recovers. All other laptops (leaders, workers) must point `KAFKA_BROKER` at Laptop K's Tailscale IP.

#### Discovery service (Upstash Redis)

Create one Upstash Redis instance for the cluster. Share the REST URL and both tokens (full + read-only) with every laptop. They go in each machine's `.env` files.

### Phase 4 — Per-laptop config files

**`server/leader/.env`** on each laptop:
```
KAFKA_BROKER=<K_IP>:9092
LEADER_ADVERTISE_HOST=<this laptop's Tailscale IP>
USE_ASTRA=true
ASTRA_BUNDLE_PATH=./secrets/secure-connect-web-app.zip
ASTRA_CLIENT_ID=<clientId>
ASTRA_CLIENT_SECRET=<clientSecret>
ASTRA_KEYSPACE=web_app
UPSTASH_REDIS_REST_URL=<url>
UPSTASH_REDIS_REST_TOKEN=<full token>
```

**`server/worker/.env`** on each laptop:
```
KAFKA_BROKER=<K_IP>:9092
LEADER_URL=http://<this laptop's Tailscale IP>:8000
UPSTASH_REDIS_REST_URL=<url>
UPSTASH_REDIS_REST_READ_ONLY_TOKEN=<read-only token>
NODE_ID=worker-<A|B|C>
```

**`server/leader/config/node.yaml`** — election priority (highest wins ties):
- Laptop A → `priority: 100` (initial leader)
- Laptop B → `priority: 90`
- Laptop C → `priority: 80`

The file is auto-generated on first run with priority 100. Edit it after first run, or pass `priority` in `POST /cluster/create`.

### Phase 5 — Bootstrap order

1. **Laptop K**: Start the Kafka broker.
   ```bash
   docker compose -f server/kafka/docker-compose.yml up -d
   ```
2. **Laptop A** (highest priority): Start the leader, then bootstrap the cluster:
   ```bash
   cd server/leader && source ../../.venv/bin/activate
   python main.py
   
   # In another terminal:
   curl -X POST http://localhost:8000/cluster/create \
     -H 'Content-Type: application/json' \
     -d '{"role":"both","priority":100,"port":8000}'
   ```
   Save `cluster_id` and `join_token` from the response.

3. **Laptops B and C**: Start their leaders, then join:
   ```bash
   python main.py
   
   curl -X POST http://localhost:8000/cluster/join \
     -H 'Content-Type: application/json' \
     -d '{"bootstrap_url":"http://<A_IP>:8000","join_token":"<token>"}'
   ```

4. **Each laptop**: In a second terminal, start the worker:
   ```bash
   cd server/worker && python worker.py
   ```

### Phase 6 — Verification before demoing

From any laptop:

```bash
# All 3 leaders + 3 workers listed; current_leader is Laptop A's node_id
curl http://<A_IP>:8000/cluster/status

# Discovery resolves to Laptop A
curl http://<A_IP>:8000/discovery/leader

# Send a test prompt
curl -X POST http://<A_IP>:8000/ask \
  -H 'Content-Type: application/json' \
  -d '{"prompt": "Say hi"}'

# Available skills
curl http://<A_IP>:8000/cluster/skills

# Prometheus metrics
curl http://<A_IP>:8000/metrics
```

### Common gotchas

1. **`LEADER_ADVERTISE_HOST` is mandatory** on WSL2. Without it, peers can't reach the published hostname and election silently breaks.
2. **`KAFKA_BROKER` must be Laptop K's Tailscale IP**, not `localhost`. Every laptop points at the same single broker.
3. **Kafka broker laptop must stay up.** A single broker is a single point of failure — if Laptop K goes down, `/ask` will time out until the broker recovers.
4. **Node priorities must be unique.** Equal priorities tie-break lexicographically, making failover order non-obvious.
5. **Don't forget to start workers.** Without them, `/ask` 504s and looks like a leader bug.

---

## Logging In

| Role | Email | Password |
|---|---|---|
| Admin | `admin@cluster.local` | `admin` |
| Client | `shan@example.com` | `password` |
| Server | `abhin@example.com` | `password` |

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `Cannot reach Leader` | Confirm `main.py` is running on port 8000 |
| `Models: (none)` | Run `ollama serve` and pull a model: `ollama pull llama3.2:3b` |
| `ModuleNotFoundError: cassandra` | Activate venv and reinstall: `pip install -r server/leader/requirements.txt` |
| `Read timed out` from Ollama | Use a smaller model (`llama3.2:3b`) or raise `TASK_TIMEOUT` in `.env` |
| `Skills: (none)` | Ensure `server/worker/skills/` contains subdirectories with `SKILL.md` |
| `RuntimeError: USE_ASTRA=true but missing env vars` | Fill in Astra credentials in `server/leader/.env` or set `USE_ASTRA=false` |
| `Database not connected` (local mode) | `cd client && make db-up && make db-setup` |
| `secure connect bundle does not exist` | Download the .zip from astra.datastax.com and place it at `ASTRA_BUNDLE_PATH` |
| `Frontend won't start` | Run `npm install` in `client/` |
| Grafana shows no data | Confirm leader is running; check `http://localhost:9090/targets` in Prometheus |
| `/ask` returns 503 immediately | No eligible workers online — start at least one worker |
--
