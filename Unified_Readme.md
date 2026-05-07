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
│   ├── POST /auth/login       → authenticate user         │
│   ├── POST /auth/signup      → create account            │
│   ├── GET  /health           → status check              │
│   ├── POST /ask              → queue prompt for workers   │
│   ├── POST /register         → worker joins cluster      │
│   ├── POST /heartbeat        → worker keep-alive         │
│   ├── GET  /task/{node_id}   → worker polls for tasks    │
│   ├── POST /task/complete    → worker returns AI result   │
│   └── GET  /task/result/{id} → fetch completed response  │
│                                                           │
│   Cassandra DB → stores users and request history         │
│   Kafka (planned) → distributes tasks to workers          │
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
│   - coding       │   │   - general      │
└──────────────────┘   └──────────────────┘
```

**VPN:** All nodes connect via Tailscale. Clients only need the leader's VPN IP.

---

## Prerequisites

| Tool | Required By | Install |
|---|---|---|
| Python 3.10+ | Backend & Worker | [python.org](https://www.python.org/downloads/) |
| Node.js 18+ | Frontend | [nodejs.org](https://nodejs.org/) |
| Docker | Database | [docker.com](https://www.docker.com/) |
| Ollama | Worker | [ollama.com](https://ollama.com/) |

After installing Ollama, pull at least one model:
```bash
ollama pull llama3.2:3b
```

---

## Project Structure

```
distributed_system_project/
├── server/
│   ├── main.py              # Leader node (FastAPI)
│   ├── worker.py            # Worker node (polls leader, runs Ollama)
│   ├── requirements.txt     # Python dependencies
│   ├── coding.skill         # Example skill file
│   └── README.md            # Worker-specific docs
├── web-app/
│   ├── src/
│   │   ├── api/index.js     # API layer (mock + real calls)
│   │   ├── context/         # Auth context (localStorage)
│   │   └── screens/         # Login, Signup, Chat, Admin, Server
│   ├── db/                  # Cassandra CQL schemas
│   ├── scripts/             # DB setup script
│   ├── docker-compose.yml   # Cassandra container
│   ├── Makefile             # DB shortcuts
│   └── .env.example         # Leader IP config template
├── architecture_scenarios.md
├── project_completion_checklist.md
└── README.md                # ← You are here
```

---

## How to Run (Step by Step)

You will need **5 terminal windows**. Start each one in order.

### Terminal 1 — Ollama

```bash
ollama serve
```

Starts the local AI model server on `http://localhost:11434`.

### Terminal 2 — Cassandra Database

```bash
cd web-app

# Start the container
make db-up

# Wait ~30 seconds for boot, then apply schemas
make db-setup
```

> To reset: `make db-reset` · To inspect: `make db-shell`

### Terminal 3 — FastAPI Backend (Leader Node)

```bash
cd server
python3 -m venv venv          # only needed once
source venv/bin/activate
pip install -r requirements.txt   # only needed once

python3 -m uvicorn main:app --reload
```

Leader starts on **http://localhost:8000**.

### Terminal 4 — Worker Node

```bash
cd server
source venv/bin/activate

python3 worker.py --leader-ip 127.0.0.1
```

> **Multi-laptop:** Replace `127.0.0.1` with the Leader's Tailscale IP (e.g., `100.64.0.5`).

### Terminal 5 — React Frontend

```bash
cd web-app
npm install                   # only needed once
cp .env.example .env          # only needed once

npm start
```

Opens automatically at **http://localhost:3000**.

> `node_modules/` is gitignored — always run `npm install` after pulling.

---

## Expected Output

### Worker (Terminal 4)

```
==================================================
  Worker Node Profile
==================================================
  Node ID : node_a1b2c3
  RAM     : 8.0 GB
  Models  : ['qwen3:4b', 'llama3.2:3b']
  Skills  : ['coding']
==================================================

Connecting to Leader at http://127.0.0.1:8000...
[✓] Registered with Leader at http://127.0.0.1:8000
[♥] Starting heartbeat + task polling loop (every 5s)...

[♥] Heartbeat sent — status: idle | tasks done: 0
```

### Leader (Terminal 3)

```
[REGISTER] node_a1b2c3 | RAM: 8.0GB | Models: ['qwen3:4b', 'llama3.2:3b'] | Skills: ['coding']
[HEARTBEAT] node_a1b2c3 | Status: idle | Tasks: 0
```

---

## Testing the AI Pipeline

With all 5 terminals running, open a **new terminal** and send a prompt:

### 1. Send a prompt

```bash
curl -X POST http://localhost:8000/ask \
  -H "Content-Type: application/json" \
  -d '{"prompt": "Say hi", "model": "llama3.2:3b"}'
```

**Response:**
```json
{"task_id": "task_dbb44622", "status": "queued"}
```

> `model` is optional. If omitted, the worker uses its first available model.

### 2. Watch the worker pick it up (Terminal 4)

```
[📥] Got task task_dbb44622!
[🤖] Executing prompt on model 'llama3.2:3b'...
[🤖] Prompt: "Say hi"
[🤖] Done. Response length: 95 chars
[✓] Task task_dbb44622 completed and sent back to Leader
```

### 3. Retrieve the result

Wait ~15-30 seconds, then:

```bash
curl http://localhost:8000/task/result/task_dbb44622
```

**Response:**
```json
{
  "status": "completed",
  "response": "Hello! It's nice to meet you. Is there something I can help you with?"
}
```

> **Performance note:** On an 8GB M1 MacBook, `llama3.2:3b` responds in ~15-30s. Larger models like `qwen3:4b` may take 1-2 minutes.

---

## Logging In (Frontend)

| Role | Email | Password |
|---|---|---|
| Admin | `admin@cluster.local` | `admin` |
| Client | `shan@example.com` | `password` |
| Server | `abhin@example.com` | `password` |

> Login/signup now use the real backend (`POST /auth/login`, `POST /auth/signup`). Other screens still use mock data until the remaining backend endpoints are built.

---

## Skill Files

Workers advertise specializations via `.skill` files in the `server/` directory:

```bash
touch server/coding.skill       # can handle coding tasks
touch server/general.skill      # can handle general chat
```

The worker scans for `*.skill` files on boot and reports them to the Leader. No files = empty skills list.

---

## Connecting to the Real Backend

When the backend is fully ready, **only one file changes:** `web-app/src/api/index.js`

Every function has a comment showing the real endpoint:

```js
// Real: GET /cluster/stats
export async function getClusterStats() {
  // swap the mock with:
  // const res = await axios.get(`${BASE}/cluster/stats`);
  // return res.data;
}
```

The `.env` file holds the Leader's IP:

```bash
# in web-app/
cp .env.example .env
# edit .env → set REACT_APP_LEADER_IP to the Leader's Tailscale IP
```

`.env` is gitignored — each teammate has their own copy.

## Troubleshooting

| Problem | Fix |
|---|---|
| `Cannot reach Leader` | Make sure `main.py` is running (Terminal 3) |
| `Models: (none)` | Make sure `ollama serve` is running (Terminal 1) |
| `ModuleNotFoundError: cassandra` | Activate the venv: `source venv/bin/activate` |
| `Read timed out` from Ollama | Use a smaller model (`llama3.2:3b`) or just wait longer |
| `Skills: (none)` | Create a skill file: `touch server/coding.skill` |
| `Database not connected` | Start Cassandra first: `cd web-app && make db-up && make db-setup` |
| `Frontend won't start` | Run `npm install` in `web-app/` |

---
