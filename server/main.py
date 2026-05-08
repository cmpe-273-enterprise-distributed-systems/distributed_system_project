import uuid
import time
import subprocess
import json
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(title="Distributed AI Gateway - Leader Node")

# Allow React app to communicate with FastAPI
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # In production, restrict this to the frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Connect to Cassandra
cluster = None
session = None
db_last_error = None
DB_CQLSH_CONTAINER = "web-app-cassandra"


def _cqlsh(query: str) -> str:
    """
    Execute a CQL query using cqlsh inside the Cassandra container.
    This avoids relying on the Python cassandra-driver (which is not compatible
    with Python 3.12+ in some environments).
    """
    try:
        # -e executes statement and exits.
        res = subprocess.run(
            ["docker", "exec", DB_CQLSH_CONTAINER, "cqlsh", "-e", query],
            capture_output=True,
            text=True,
            timeout=15,
            check=True,
        )
        return res.stdout
    except Exception as e:
        raise RuntimeError(f"cqlsh failed: {e}") from e


def _cql_string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _parse_cqlsh_table_first_row(stdout: str):
    """
    Parse the first data row from cqlsh's default table output.
    This is a lightweight parser intended for simple SELECTs that return 0/1 rows.
    """
    lines = [ln.rstrip("\r") for ln in stdout.splitlines() if ln.strip()]
    # Typical format:
    #  col1 | col2
    # ------+------
    #  v1   | v2
    # (1 rows)
    data_rows = []
    for ln in lines:
        if ln.startswith("(") and ln.endswith("rows)"):
            break
        if "|" in ln and not set(ln.strip()).issubset(set("-+")):
            data_rows.append(ln)
    if len(data_rows) < 2:
        return None
    header = [h.strip() for h in data_rows[0].split("|")]
    row = [c.strip() for c in data_rows[1].split("|")]
    if len(row) != len(header):
        return None
    return dict(zip(header, row))

@app.on_event("startup")
def startup_event():
    global cluster, session, db_last_error
    print("Connecting to Cassandra...")
    for attempt in range(10):
        try:
            # Use cqlsh via docker exec as the "session".
            _cqlsh("DESCRIBE KEYSPACES;")
            session = True
            db_last_error = None
            print("Successfully connected to Cassandra via cqlsh")
            return
        except Exception as e:
            db_last_error = repr(e)
            print(f"Waiting for Cassandra (attempt {attempt + 1}/10)...")
            time.sleep(2)
    print("WARNING: Could not connect to Cassandra on startup.")


@app.get("/db/health")
async def db_health():
    return {
        "connected": bool(session),
        "last_error": db_last_error,
    }

@app.on_event("shutdown")
def shutdown_event():
    if cluster:
        cluster.shutdown()

class SignupRequest(BaseModel):
    name: str
    email: str
    password: str
    role: str

class LoginRequest(BaseModel):
    email: str
    password: str

@app.post("/auth/signup")
async def signup(req: SignupRequest):
    if not session:
        raise HTTPException(status_code=500, detail="Database not connected")
    
    # Check if user already exists
    q = f"SELECT email FROM web_app.users WHERE email = {_cql_string_literal(req.email)};"
    existing = _parse_cqlsh_table_first_row(_cqlsh(q))
    
    if existing:
        raise HTTPException(status_code=400, detail="An account with that email already exists.")
    
    user_id = uuid.uuid4()
    
    try:
        insert_q = (
            "INSERT INTO web_app.users (user_id, username, email, password_hash, role, created_at) "
            f"VALUES ({user_id}, {_cql_string_literal(req.name)}, {_cql_string_literal(req.email)}, "
            f"{_cql_string_literal(req.password)}, {_cql_string_literal(req.role)}, toTimestamp(now()));"
        )
        _cqlsh(insert_q)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        
    return {
        "id": str(user_id),
        "name": req.name,
        "email": req.email,
        "role": req.role
    }

@app.post("/auth/login")
async def login(req: LoginRequest):
    if not session:
        raise HTTPException(status_code=500, detail="Database not connected")
        
    # Real app would check password hash, this uses plain text for now
    q = (
        "SELECT user_id, username, email, password_hash, role "
        f"FROM web_app.users WHERE email = {_cql_string_literal(req.email)};"
    )
    user = _parse_cqlsh_table_first_row(_cqlsh(q))
    
    if not user:
        raise HTTPException(status_code=400, detail="Invalid email or password.")
        
    if user.get("password_hash") != req.password:
        raise HTTPException(status_code=400, detail="Invalid email or password.")
        
    return {
        "id": str(user.get("user_id")),
        "name": user.get("username"),
        "email": user.get("email"),
        "role": user.get("role"),
    }

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/debug/source")
async def debug_source():
    return {"file": __file__}

# ── Dummy endpoints for worker.py testing ──────────────
# These will be replaced by Divya with real registry logic.

class RegisterRequest(BaseModel):
    node_id: str
    ram_gb: float
    models: list
    skills: list

class HeartbeatRequest(BaseModel):
    node_id: str
    status: str
    tasks_completed: int

@app.post("/register")
async def register_node(req: RegisterRequest):
    print(f"[REGISTER] {req.node_id} | RAM: {req.ram_gb}GB | Models: {req.models} | Skills: {req.skills}")
    return {"status": "registered", "assigned_queue": f"worker_{req.node_id}"}

@app.post("/heartbeat")
async def heartbeat(req: HeartbeatRequest):
    print(f"[HEARTBEAT] {req.node_id} | Status: {req.status} | Tasks: {req.tasks_completed}")
    return {"status": "ok"}

# ── Simple task queue (placeholder for Kafka) ──────────
# This lets us test the full pipeline: user prompt → worker → Ollama → response

from collections import deque

task_queue = deque()       # pending tasks
completed_tasks = {}       # task_id → response

class AskRequest(BaseModel):
    prompt: str
    model: str = None      # optional: which model to use

class TaskCompleteRequest(BaseModel):
    task_id: str
    node_id: str
    response: str

@app.post("/ask")
async def ask(req: AskRequest):
    """User sends a prompt. We queue it for the next available worker."""
    task_id = f"task_{uuid.uuid4().hex[:8]}"
    task_queue.append({"task_id": task_id, "prompt": req.prompt, "model": req.model})
    print(f"[ASK] Queued task {task_id}: \"{req.prompt[:60]}\"")
    return {"task_id": task_id, "status": "queued"}

@app.get("/task/{node_id}")
async def get_task(node_id: str):
    """Worker polls this to pick up the next task."""
    if task_queue:
        task = task_queue.popleft()
        print(f"[DISPATCH] Task {task['task_id']} → {node_id}")
        return task
    raise HTTPException(status_code=204, detail="No tasks available")

@app.post("/task/complete")
async def complete_task(req: TaskCompleteRequest):
    """Worker reports back with the Ollama response."""
    completed_tasks[req.task_id] = req.response
    print(f"[COMPLETE] Task {req.task_id} from {req.node_id} — {len(req.response)} chars")
    return {"status": "ok"}

@app.get("/task/result/{task_id}")
async def get_result(task_id: str):
    """Check if a task has been completed and get the response."""
    if task_id in completed_tasks:
        return {"status": "completed", "response": completed_tasks[task_id]}
    return {"status": "pending"}

