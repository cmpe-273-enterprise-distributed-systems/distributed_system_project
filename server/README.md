# Backend (`server/`)

The backend for **Distributed AI Gateway** is split into two Python programs:

- **`main.py` (Leader Node)**: FastAPI server that handles auth, health checks, and a demo task-dispatch pipeline.
- **`worker.py` (Worker Node)**: a worker process that registers with the Leader, sends heartbeats, polls for tasks, and runs prompts on a local **Ollama** instance.

---

## What’s implemented right now

### Leader (`main.py`)
- **Cassandra connection on startup** (expects Cassandra on `127.0.0.1:9042`, keyspace `web_app`)
- **Auth backed by Cassandra**
  - `POST /auth/signup` inserts a user into `web_app.users`
  - `POST /auth/login` validates credentials against `web_app.users`
  - Note: passwords are currently stored/compared as plain text in the `password_hash` column (no JWT yet)
- **Health**
  - `GET /health` → `{ "status": "ok" }`
- **Worker integration (currently just logging)**
  - `POST /register`
  - `POST /heartbeat`
- **Demo task pipeline (in-memory queue; placeholder for Kafka)**
  - `POST /ask` queues a task
  - `GET /task/{node_id}` worker polls for next task
  - `POST /task/complete` worker posts result
  - `GET /task/result/{task_id}` fetch result

### Worker (`worker.py`)
- Profiles host machine (RAM via `psutil`)
- Discovers local Ollama models via `GET http://localhost:11434/api/tags`
- Discovers skills via `server/*.skill`
- Registers + heartbeats + task polling loop
- Executes prompts via `POST http://localhost:11434/api/generate` (non-streaming)

---

## Prerequisites (local dev)

| Dependency | Install |
|---|---|
| Python 3.10+ | [python.org](https://www.python.org/downloads/) |
| Docker | [docker.com](https://www.docker.com/) |
| Ollama | [ollama.com](https://ollama.com/) |
| At least one Ollama model | `ollama pull llama3.2:3b` |

---

## Setup (one time)

### PowerShell (Windows)

```powershell
cd server
py -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

---

## How to run locally (Windows-friendly)

You will run **Cassandra + Leader + Worker + Ollama** in separate terminals.

### Terminal 1 — Start Ollama

```powershell
ollama serve
```

Ollama starts on `http://localhost:11434`.

### Terminal 2 — Start Cassandra (Docker)

```powershell
cd ..\web-app
docker compose up -d cassandra
python scripts\setup_cassandra.py
```

This creates keyspace/tables in Cassandra (see `web-app/db/*.cql`).

### Terminal 3 — Start the Leader (FastAPI)

```powershell
cd ..\server
.\venv\Scripts\Activate.ps1
py -m uvicorn main:app --reload
```

Leader starts on `http://localhost:8000`.

### Terminal 4 — Start the Worker

```powershell
cd ..\server
.\venv\Scripts\Activate.ps1
py worker.py --leader-ip 127.0.0.1
```

> Multi-laptop: replace `127.0.0.1` with the Leader’s Tailscale VPN IP (a `100.x.x.x` address).

---

## API reference (current)

### Auth (Cassandra-backed)
- `POST /auth/signup`

```json
{ "name": "Alice", "email": "alice@example.com", "password": "password", "role": "client" }
```

- `POST /auth/login`

```json
{ "email": "alice@example.com", "password": "password" }
```

### Health
- `GET /health`

### Worker ↔ Leader (currently logs only)
- `POST /register`
- `POST /heartbeat`

### Demo task pipeline (in-memory)
- `POST /ask`

```json
{ "prompt": "Say hi", "model": "llama3.2:3b" }
```

- `GET /task/{node_id}`
- `POST /task/complete`
- `GET /task/result/{task_id}`

---

## Expected Output

### Worker terminal (Terminal 4)
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
[♥] Heartbeat sent — status: idle | tasks done: 0
```

### Leader terminal (Terminal 3)
```
[REGISTER] node_a1b2c3 | RAM: 8.0GB | Models: ['qwen3:4b', 'llama3.2:3b'] | Skills: ['coding']
[HEARTBEAT] node_a1b2c3 | Status: idle | Tasks: 0
```

---

## Testing the Full Pipeline

### 1. Send a prompt (new terminal)

```powershell
curl -Method Post http://localhost:8000/ask `
  -ContentType "application/json" `
  -Body '{ "prompt": "Say hi", "model": "llama3.2:3b" }'
```

**Response:**
```json
{"task_id": "task_dbb44622", "status": "queued"}
```

> The `model` field is optional. If omitted, the worker uses the first available model.

### 2. Worker picks up the task

You will see this in the worker terminal:
```
[📥] Got task task_dbb44622!
[🤖] Executing prompt on model 'llama3.2:3b'...
[🤖] Prompt: "Say hi"
[🤖] Done. Response length: 95 chars
[✓] Task task_dbb44622 completed and sent back to Leader
```

### 3. Retrieve the result

```powershell
curl http://localhost:8000/task/result/task_dbb44622
```

**Response:**
```json
{
  "status": "completed",
  "response": "Hello! It's nice to meet you. Is there something I can help you with or would you like to chat?"
}
```

> **Note:** On an 8GB M1 MacBook, smaller models like `llama3.2:3b` respond in ~10-30 seconds. Larger models like `qwen3:4b` may take 1-2 minutes.

---

## Skill Files

Workers can advertise their specializations using `.skill` files. Place empty files in the `server/` directory:

```powershell
# This worker can handle coding tasks
New-Item -ItemType File coding.skill

# This worker can handle general chat
New-Item -ItemType File general.skill
```

The worker scans for `*.skill` files on boot and reports them to the Leader during registration. If no `.skill` files are found, the skills list will be empty.

---

## Architecture

```
┌──────────────┐       POST /register        ┌──────────────┐
│              │ ──────────────────────────►  │              │
│  worker.py   │       POST /heartbeat        │   main.py    │
│  (Worker)    │ ──────────────────────────►  │   (Leader)   │
│              │       GET  /task/{id}         │              │
│              │ ──────────────────────────►  │              │
│              │  ◄──── task payload ────────  │              │
│              │                              │              │
│   ┌────────┐ │       POST /task/complete     │              │
│   │ Ollama │ │ ──────────────────────────►  │              │
│   └────────┘ │                              │              │
└──────────────┘                              └──────────────┘
  localhost:11434                               localhost:8000
```

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `Cannot reach Leader` | Make sure `main.py` is running (Terminal 3) |
| `Database not connected` (signup/login fails) | Start Cassandra first (Terminal 2) and run `python scripts\setup_cassandra.py` |
| `Models: (none)` | Make sure `ollama serve` is running (Terminal 1) and you pulled a model (`ollama pull llama3.2:3b`) |
| `ModuleNotFoundError: cassandra` | Activate venv and `pip install -r requirements.txt` |
| `Read timed out` from Ollama | Your machine is slow — the timeout is set to 5 minutes, just wait longer or use a smaller model |
| `Skills: (none)` | Create a `.skill` file (PowerShell: `New-Item -ItemType File coding.skill`) |
