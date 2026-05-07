import asyncio
import os
import uuid
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

load_dotenv()

from database import (
    _hash,
    create_user,
    delete_user,
    get_all_requests,
    get_all_users,
    get_user_by_email,
    init_db,
    save_request,
    update_user_role,
)
from kafka_client import ResultConsumer, TaskProducer
from registry import Registry

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TASK_TIMEOUT = int(os.getenv("TASK_TIMEOUT", "60"))

registry = Registry()
producer = TaskProducer(KAFKA_BROKER)
result_consumer = ResultConsumer(KAFKA_BROKER)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    result_consumer.start(asyncio.get_event_loop())
    asyncio.create_task(registry.check_timeouts())
    yield


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Request models ────────────────────────────────────────────────────────────

class LoginBody(BaseModel):
    email: str
    password: str

class SignupBody(BaseModel):
    name: str
    email: str
    password: str
    role: str = "client"

class AskBody(BaseModel):
    prompt: str
    user_id: int = 0
    user_name: str = "anonymous"

class RegisterBody(BaseModel):
    node_id: str
    ram_gb: int
    model: str
    skills: list[str]

class HeartbeatBody(BaseModel):
    node_id: str
    status: str
    tasks_completed: int

class UpdateRoleBody(BaseModel):
    role: str


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok"}


# ── Auth ──────────────────────────────────────────────────────────────────────

@app.post("/auth/login")
async def login(body: LoginBody):
    user = await get_user_by_email(body.email)
    if not user or user["password_hash"] != _hash(body.password):
        raise HTTPException(401, "Invalid email or password.")
    return {k: v for k, v in user.items() if k != "password_hash"}


@app.post("/auth/signup")
async def signup(body: SignupBody):
    if await get_user_by_email(body.email):
        raise HTTPException(409, "An account with that email already exists.")
    user_id = await create_user(body.name, body.email, body.password, body.role)
    return {"id": user_id, "name": body.name, "email": body.email, "role": body.role}


# ── Chat ──────────────────────────────────────────────────────────────────────

@app.post("/ask")
async def ask(body: AskBody):
    request_id = str(uuid.uuid4())

    # Register the event BEFORE publishing so the consumer thread can't race ahead
    event = result_consumer.register(request_id)
    producer.publish(request_id, body.prompt, body.user_id, body.user_name)

    try:
        await asyncio.wait_for(event.wait(), timeout=TASK_TIMEOUT)
    except asyncio.TimeoutError:
        result_consumer.cancel(request_id)
        raise HTTPException(504, "No worker responded in time.")

    result = result_consumer.pop_result(request_id)
    if not result:
        raise HTTPException(500, "Result missing after event fired.")

    if result.get("error"):
        raise HTTPException(500, result["error"])

    duration_ms = result.get("duration_ms", 0)
    worker_id = result.get("worker_id", "unknown")

    await save_request(
        request_id, body.user_id, body.user_name,
        body.prompt, worker_id, duration_ms, "completed",
    )

    return {
        "response": result.get("response", ""),
        "worker": worker_id,
        "duration": f"{duration_ms / 1000:.1f}s",
    }


# ── Node registration & heartbeat ────────────────────────────────────────────

@app.post("/register")
async def register_node(body: RegisterBody, request: Request):
    ip = request.client.host
    await registry.register(body.node_id, ip, body.ram_gb, body.model, body.skills)
    return {
        "status": "registered",
        "kafka_broker": KAFKA_BROKER,
        "assigned_queues": ["tasks-high-ram", "tasks-low-ram", "tasks-general"],
    }


@app.post("/heartbeat")
async def heartbeat(body: HeartbeatBody):
    known = await registry.heartbeat(body.node_id, body.status, body.tasks_completed)
    return {"status": "ok", "is_leader": False, "new_skill": None, "reregister": not known, "kafka_broker": KAFKA_BROKER}


# ── Cluster stats ─────────────────────────────────────────────────────────────

@app.get("/cluster/stats")
async def cluster_stats():
    nodes = await registry.get_all()
    requests = await get_all_requests()
    users = await get_all_users()

    online = [n for n in nodes if n.status != "offline"]
    completed = [r for r in requests if r["status"] == "completed" and r["duration_ms"]]
    avg_ms = sum(r["duration_ms"] for r in completed) / len(completed) if completed else 0

    return {
        "nodesOnline": len(online),
        "nodesTotal": len(nodes),
        "tasksCompleted": sum(n.tasks_completed for n in nodes),
        "avgResponseTime": f"{avg_ms / 1000:.1f}s",
        "activeUsers": len([u for u in users if u["role"] == "client"]),
    }


@app.get("/cluster/nodes")
async def cluster_nodes():
    nodes = await registry.get_all()
    return [
        {
            "id": n.node_id,
            "ip": n.ip,
            "status": n.status,
            "model": n.model,
            "skills": n.skills,
            "tasksCompleted": n.tasks_completed,
            "lastSeen": int(n.last_seen * 1000),
            "ram_gb": n.ram_gb,
        }
        for n in nodes
    ]


@app.get("/cluster/requests")
async def cluster_requests():
    rows = await get_all_requests()
    return [
        {
            "id": r["id"],
            "userId": r["user_id"],
            "userName": r["user_name"],
            "prompt": r["prompt"],
            "worker": r["worker_id"] or "unknown",
            "duration": f"{(r['duration_ms'] or 0) / 1000:.1f}s",
            "status": r["status"],
            "time": r["created_at"],
        }
        for r in rows
    ]


# ── Admin users ───────────────────────────────────────────────────────────────

@app.get("/admin/users")
async def admin_get_users():
    return await get_all_users()


@app.patch("/admin/users/{user_id}")
async def admin_update_role(user_id: int, body: UpdateRoleBody):
    await update_user_role(user_id, body.role)
    return {"success": True}


@app.delete("/admin/users/{user_id}")
async def admin_delete_user(user_id: int):
    await delete_user(user_id)
    return {"success": True}
