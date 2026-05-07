# Worker Node (`worker.py`)

A standalone Python script that runs on each worker laptop in the Distributed AI Gateway cluster. It profiles the host machine, registers with the Leader, maintains a heartbeat, and executes AI prompts using a local Ollama instance.

---

## Prerequisites

| Dependency | Install |
|---|---|
| Python 3.10+ | [python.org](https://www.python.org/downloads/) |
| Ollama | [ollama.com](https://ollama.com/) |
| At least one Ollama model | `ollama pull llama3.2:3b` |

---

## Setup (one time)

```bash
cd server
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

---

## How to Run (3 terminals)

### Terminal 1 — Start Ollama
```bash
ollama serve
```
Ollama will start on `http://localhost:11434`.

### Terminal 2 — Start the Leader (FastAPI)
```bash
cd server
source venv/bin/activate
python3 -m uvicorn main:app --reload
```
Leader will start on `http://localhost:8000`.

### Terminal 3 — Start the Worker
```bash
cd server
source venv/bin/activate
python3 worker.py --leader-ip 127.0.0.1
```

> **Multi-laptop setup:** Replace `127.0.0.1` with the Leader's Tailscale VPN IP (e.g., `100.64.0.5`).

---

## Expected Output

### Worker terminal (Terminal 3)
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

### Leader terminal (Terminal 2)
```
[REGISTER] node_a1b2c3 | RAM: 8.0GB | Models: ['qwen3:4b', 'llama3.2:3b'] | Skills: ['coding']
[HEARTBEAT] node_a1b2c3 | Status: idle | Tasks: 0
```

---

## Testing the Full Pipeline

### 1. Send a prompt (Terminal 4)

```bash
curl -X POST http://localhost:8000/ask \
  -H "Content-Type: application/json" \
  -d '{"prompt": "Say hi", "model": "llama3.2:3b"}'
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

```bash
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

```bash
# This worker can handle coding tasks
touch coding.skill

# This worker can handle general chat
touch general.skill
```

The worker scans for `*.skill` files on boot and reports them to the Leader during registration. If no `.skill` files are found, the skills list will be empty.

---

## API Endpoints (Leader)

These endpoints are currently **placeholder stubs** in `main.py` for testing. They will be replaced with real Cassandra-backed logic by Divya.

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/register` | Worker registers with its profile (node_id, RAM, models, skills) |
| `POST` | `/heartbeat` | Worker sends keep-alive ping every 5 seconds |
| `POST` | `/ask` | Queue a prompt for the next available worker |
| `GET` | `/task/{node_id}` | Worker polls for the next available task |
| `POST` | `/task/complete` | Worker submits completed Ollama response |
| `GET` | `/task/result/{task_id}` | Check if a task is completed and get the response |

### JSON Schemas

**POST /register**
```json
{
  "node_id": "node_a1b2c3",
  "ram_gb": 8.0,
  "models": ["qwen3:4b", "llama3.2:3b"],
  "skills": ["coding"]
}
```

**POST /heartbeat**
```json
{
  "node_id": "node_a1b2c3",
  "status": "idle",
  "tasks_completed": 2
}
```

**POST /ask**
```json
{
  "prompt": "What is recursion?",
  "model": "llama3.2:3b"
}
```

**POST /task/complete**
```json
{
  "task_id": "task_dbb44622",
  "node_id": "node_a1b2c3",
  "response": "Recursion is when a function calls itself..."
}
```

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
| `Cannot reach Leader` | Make sure `main.py` is running in Terminal 2 |
| `Models: (none)` | Make sure `ollama serve` is running in Terminal 1 |
| `ModuleNotFoundError: cassandra` | You forgot to activate the venv: `source venv/bin/activate` |
| `Read timed out` from Ollama | Your machine is slow — the timeout is set to 5 minutes, just wait longer or use a smaller model |
| `Skills: (none)` | Create a `.skill` file: `touch coding.skill` in the `server/` directory |
