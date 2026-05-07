import uuid
import time
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from cassandra.cluster import Cluster

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

@app.on_event("startup")
def startup_event():
    global cluster, session
    print("Connecting to Cassandra...")
    for attempt in range(10):
        try:
            cluster = Cluster(["127.0.0.1"], port=9042)
            session = cluster.connect('web_app')
            print("Successfully connected to Cassandra keyspace 'web_app'")
            return
        except Exception as e:
            print(f"Waiting for Cassandra (attempt {attempt + 1}/10)...")
            time.sleep(2)
    print("WARNING: Could not connect to Cassandra on startup.")

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
    query = "SELECT email FROM users WHERE email = %s"
    existing = session.execute(query, (req.email,)).one()
    
    if existing:
        raise HTTPException(status_code=400, detail="An account with that email already exists.")
    
    user_id = uuid.uuid4()
    
    insert_query = """
        INSERT INTO users (user_id, username, email, password_hash, role, created_at)
        VALUES (%s, %s, %s, %s, %s, toTimestamp(now()))
    """
    try:
        session.execute(insert_query, (user_id, req.name, req.email, req.password, req.role))
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
    query = "SELECT user_id, username, email, password_hash, role FROM users WHERE email = %s"
    user = session.execute(query, (req.email,)).one()
    
    if not user:
        raise HTTPException(status_code=400, detail="Invalid email or password.")
        
    if user.password_hash != req.password:
        raise HTTPException(status_code=400, detail="Invalid email or password.")
        
    return {
        "id": str(user.user_id),
        "name": user.username,
        "email": user.email,
        "role": user.role
    }

@app.get("/health")
async def health():
    return {"status": "ok"}

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

