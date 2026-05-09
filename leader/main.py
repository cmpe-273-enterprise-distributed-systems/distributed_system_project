import asyncio
import os
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

load_dotenv()

from cluster_state import (
    add_or_update_node,
    create_cluster,
    elect_leader,
    generate_join_code,
    get_cluster_status,
    get_leader,
    load_cluster_state,
    merge_cluster_state,
    save_cluster_state,
    update_heartbeat,
    validate_join_token,
)
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
from discovery import DiscoveryClient
from kafka_client import ResultConsumer, TaskProducer
from leader_monitor import LeaderMonitor
from node_config import load_or_create_node_config
from registry import Registry
from tailscale_utils import get_advertise_host

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TASK_TIMEOUT = int(os.getenv("TASK_TIMEOUT", "60"))

registry = Registry()
producer = TaskProducer(KAFKA_BROKER)
result_consumer = ResultConsumer(KAFKA_BROKER)
discovery = DiscoveryClient()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    result_consumer.start(asyncio.get_event_loop())
    asyncio.create_task(registry.check_timeouts())

    cfg = load_or_create_node_config()
    host = get_advertise_host()
    port = int(cfg.get("port") or 8000)
    node_url = f"http://{host}:{port}"

    # Ensure this node is always present in known_nodes on startup.
    add_or_update_node({
        "node_id": cfg["node_id"],
        "role": cfg.get("role", "both"),
        "priority": int(cfg.get("priority", 100)),
        "host": host,
        "port": port,
        "url": node_url,
        "status": "alive",
        "last_heartbeat": int(time.time()),
    })
    elect_leader()
    save_cluster_state()

    monitor = LeaderMonitor(cfg["node_id"], node_url, discovery)
    asyncio.create_task(monitor.run())
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
    user_id: str = ""
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

class ClusterCreateRequest(BaseModel):
    role: str = "both"
    priority: int = 100
    port: int = 8000

class NodeInfo(BaseModel):
    node_id: str
    role: str = "worker"
    priority: int = 10
    host: str
    port: int = 8000
    url: str
    ram_gb: Optional[float] = None
    models: List[str] = []
    skills: List[str] = []
    status: str = "alive"
    last_heartbeat: int = 0
    tasks_completed: int = 0

class JoinClusterRequest(BaseModel):
    cluster_id: str
    join_token: str
    node: NodeInfo

class SyncRequest(BaseModel):
    cluster_id: str
    known_nodes: List[Dict[str, Any]] = []
    current_leader: Optional[Dict[str, Any]] = None


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
        response=result.get("response", ""),
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
    node = {
        "node_id": body.node_id,
        "role": "worker",
        "priority": 10,
        "host": ip,
        "port": 8000,
        "url": f"http://{ip}:8000",
        "ram_gb": body.ram_gb,
        "models": [body.model],
        "skills": body.skills,
        "status": "alive",
        "last_heartbeat": int(time.time()),
        "tasks_completed": 0,
    }
    add_or_update_node(node)
    elect_leader()
    save_cluster_state()
    return {
        "status": "registered",
        "kafka_broker": KAFKA_BROKER,
        "assigned_queues": ["tasks-high-ram", "tasks-low-ram", "tasks-general"],
    }


@app.post("/heartbeat")
async def heartbeat(body: HeartbeatBody):
    known = await registry.heartbeat(body.node_id, body.status, body.tasks_completed)
    update_heartbeat(body.node_id, body.status, body.tasks_completed)
    elect_leader()
    save_cluster_state()
    return {"status": "ok", "is_leader": False, "new_skill": None, "reregister": not known, "kafka_broker": KAFKA_BROKER}


# ── Cluster bootstrap & membership ───────────────────────────────────────────

@app.post("/cluster/create")
async def cluster_create(req: ClusterCreateRequest):
    cfg = load_or_create_node_config(role=req.role, priority=req.priority, port=req.port)
    host = get_advertise_host()
    port = int(cfg.get("port") or req.port)
    node = {
        "node_id": cfg["node_id"],
        "role": cfg.get("role") or req.role,
        "priority": int(cfg.get("priority") or req.priority),
        "host": host,
        "port": port,
        "url": f"http://{host}:{port}",
        "status": "alive",
        "last_heartbeat": int(time.time()),
        "tasks_completed": 0,
        "ram_gb": None,
        "models": [],
        "skills": [],
    }
    create_cluster(node)
    join_code = generate_join_code()
    st = load_cluster_state()
    return {
        "status": "created",
        "cluster_id": st.cluster_id,
        "join_code": {
            **join_code,
            "cluster_id": st.cluster_id,
            "seed_nodes": join_code.get("seed_nodes") or [node["url"]],
        },
    }


@app.post("/cluster/join")
async def cluster_join(req: JoinClusterRequest):
    st = load_cluster_state()
    if st.cluster_id and req.cluster_id != st.cluster_id:
        raise HTTPException(400, "Cluster ID mismatch")
    if not validate_join_token(req.join_token):
        raise HTTPException(401, "Invalid or expired join token")
    add_or_update_node(req.node.model_dump())
    elect_leader()
    save_cluster_state()
    status = get_cluster_status()
    return {
        "status": "joined",
        "cluster_id": status.get("cluster_id"),
        "current_leader": status.get("current_leader"),
        "known_nodes": status.get("known_nodes"),
        "heartbeat_interval_seconds": 5,
    }


@app.post("/cluster/sync")
async def cluster_sync(req: SyncRequest):
    st = load_cluster_state()
    if st.cluster_id and req.cluster_id != st.cluster_id:
        raise HTTPException(400, "Cluster ID mismatch")
    merge_cluster_state(req.model_dump())
    return get_cluster_status()


@app.get("/cluster/status")
async def cluster_status():
    return get_cluster_status()


@app.get("/leader")
async def leader_info():
    elect_leader()
    leader = get_leader()
    if not leader:
        raise HTTPException(404, "No leader known")
    return leader


@app.get("/discovery/leader")
async def discovery_leader():
    url = await discovery.resolve()
    if not url:
        leader = get_leader()
        url = leader.get("url") if leader else None
    if not url:
        raise HTTPException(404, "No leader known")
    return {"leader_url": url}


@app.get("/.well-known/ai-gateway")
async def well_known():
    st = load_cluster_state()
    cfg = load_or_create_node_config()
    return {
        "service": "distributed-ai-gateway",
        "cluster_id": st.cluster_id,
        "node_id": cfg.get("node_id"),
        "join_supported": True,
    }


# ── Cluster stats ─────────────────────────────────────────────────────────────

@app.get("/cluster/stats")
async def cluster_stats():
    nodes = await registry.get_all()
    requests = await get_all_requests()
    users = await get_all_users()

    def _parse_duration_s(d: str) -> float:
        try:
            return float(d.rstrip("s"))
        except (ValueError, AttributeError):
            return 0.0

    online = [n for n in nodes if n.status != "offline"]
    completed = [r for r in requests if r["status"] == "completed" and r.get("duration")]
    avg_s = sum(_parse_duration_s(r["duration"]) for r in completed) / len(completed) if completed else 0

    return {
        "nodesOnline": len(online),
        "nodesTotal": len(nodes),
        "tasksCompleted": sum(n.tasks_completed for n in nodes),
        "avgResponseTime": f"{avg_s:.1f}s",
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
            "duration": r["duration"],
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
async def admin_update_role(user_id: str, body: UpdateRoleBody):
    await update_user_role(user_id, body.role)
    return {"success": True}


@app.delete("/admin/users/{user_id}")
async def admin_delete_user(user_id: str):
    await delete_user(user_id)
    return {"success": True}
