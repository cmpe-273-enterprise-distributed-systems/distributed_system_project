import asyncio
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, Response, StreamingResponse
from pydantic import BaseModel, Field

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
    join_token_issue,
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
from kafka_admin import ensure_topics
from kafka_client import NoEligibleWorker, ResultConsumer, TaskProducer
from leader_monitor import LeaderMonitor
from node_config import load_or_create_node_config
from registry import Registry
from tailscale_utils import get_advertise_host
from system_checks import run_all_requirements_checks
from logging_config import setup_logging
from metrics import (
    ask_duration_seconds,
    http_request_duration_seconds,
    http_requests_total,
    tasks_completed_total,
    tasks_completed_by_worker_total,
    tasks_failed_total,
)
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

setup_logging(os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TASK_TIMEOUT = int(os.getenv("TASK_TIMEOUT", "60"))
_raw_cors = os.getenv("CORS_ORIGINS", "*")
CORS_ORIGINS: list[str] = [o.strip() for o in _raw_cors.split(",") if o.strip()] or ["*"]
# Browser dashboard (server/ui) — shown when someone opens /join or /servers on the leader by mistake.
_SERVER_UI_BASE = os.getenv("SERVER_UI_BASE_URL", "http://127.0.0.1:8001").rstrip("/")


def _leader_only_browser_hint(*, path: str, title: str, leader_base: str) -> HTMLResponse:
    ui = _SERVER_UI_BASE
    html = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>{title}</title>
<style>
  body {{ font-family: system-ui, sans-serif; margin: 2rem; max-width: 42rem; line-height: 1.5; color: #1a1a1a; }}
  code {{ background: #f0f0f0; padding: 0.15rem 0.4rem; border-radius: 4px; font-size: 0.9em; }}
  a {{ color: #0b57d0; }}
</style></head><body>
<h1>{title}</h1>
<p>You opened <code>{path}</code> on this machine&rsquo;s <strong>leader API</strong> (FastAPI). That service exposes JSON routes such as
<code>/health</code>, <code>/cluster/join</code> (POST), etc. It does <strong>not</strong> serve the HTML dashboard.</p>
<p>The <strong>Server Node UI</strong> is a separate process. Start it from the repo, pointing <code>--leader</code> at this API (including port):</p>
<pre style="background:#f6f8fa;padding:12px;border-radius:8px;overflow:auto">cd server/ui
python app.py --port 8001 --leader {leader_base}</pre>
<p>Then open the matching page in the UI (default base <a href="{ui}">{ui}</a>):</p>
<ul>
  <li><a href="{ui}/join">{ui}/join</a> &mdash; join cluster</li>
  <li><a href="{ui}/servers">{ui}/servers</a> &mdash; cluster status</li>
  <li><a href="{ui}/">{ui}/</a> &mdash; setup checks</li>
</ul>
<p>You can set <code>SERVER_UI_BASE_URL</code> if the UI runs somewhere other than <code>{ui}</code>.</p>
</body></html>"""
    return HTMLResponse(content=html)


registry = Registry()
producer: Optional[TaskProducer] = None
discovery = DiscoveryClient()
# Both producer and result_consumer are constructed inside lifespan after
# Kafka is confirmed reachable. result_consumer also needs node_id.
result_consumer: Optional[ResultConsumer] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer, result_consumer

    await init_db()
    asyncio.create_task(registry.check_timeouts())

    # Hydrate cluster state from disk before we touch known_nodes / save.
    # Otherwise join_token, cluster_id, and peers from cluster_state.yaml are
    # dropped on every restart when save_cluster_state() runs below.
    load_cluster_state()

    cfg = load_or_create_node_config()
    host = get_advertise_host()
    port = int(cfg.get("port") or 8000)
    node_url = f"http://{host}:{port}"

    # Pre-create cluster topics with the right replication factor before any
    # producer/consumer auto-creates them at RF=1 (which would silently break
    # case-B failover). Idempotent; logs each topic as 'created' / 'exists' /
    # 'rf_mismatch'. Raises RuntimeError if no broker is reachable, which we
    # let propagate — a leader without Kafka can't function anyway.
    try:
        topic_status = ensure_topics(KAFKA_BROKER)
        print(f"Topic provisioning: {topic_status}")
    except Exception as exc:
        print(f"WARNING: ensure_topics failed: {exc}. Topics will auto-create at RF=1 — case-B failover may hang.")

    producer = TaskProducer(KAFKA_BROKER)
    result_consumer = ResultConsumer(KAFKA_BROKER, cfg["node_id"])
    result_consumer.start(asyncio.get_running_loop())

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
    # Stash on app.state so require_leader() can consult it without globals.
    app.state.monitor = monitor
    asyncio.create_task(monitor.run())
    yield


app = FastAPI(lifespan=lifespan)


def require_leader(request: Request) -> None:
    """
    Dependency that gates user-facing endpoints to the elected, quorum-holding
    leader. Returns 503 with the current leader URL (when known) so workers
    and the React client can re-resolve via discovery and retry.

    Apply to /ask, /register, /heartbeat, /auth/*, /admin/*, and the
    cluster_state/database read endpoints whose answers are leader-local.
    Do NOT apply to /health, /cluster/{join,sync,status}, /leader,
    /discovery/leader, /server/*, or the well-known endpoint — those are part
    of the election machinery or are local-machine introspection that any
    node can answer.
    """
    monitor: LeaderMonitor | None = getattr(request.app.state, "monitor", None)
    if monitor is not None and monitor.is_local_leader:
        return
    leader = get_leader()
    leader_url = (leader or {}).get("url")
    raise HTTPException(
        status_code=503,
        detail={
            "code": "not_leader",
            "message": "Not the leader. Re-resolve via discovery.",
            "leader_url": leader_url,
        },
        headers={"X-Leader-URL": leader_url} if leader_url else None,
    )

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def _metrics_middleware(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    elapsed = time.perf_counter() - start
    route = request.scope.get("route")
    endpoint = route.path if route else request.url.path
    http_requests_total.labels(
        method=request.method,
        endpoint=endpoint,
        status_code=str(response.status_code),
    ).inc()
    http_request_duration_seconds.labels(endpoint=endpoint).observe(elapsed)
    return response


@app.get("/join", response_class=HTMLResponse, include_in_schema=False)
@app.get("/join/", response_class=HTMLResponse, include_in_schema=False)
async def browser_hint_join(request: Request):
    """Avoid JSON 404 when the leader is opened on the same port users expect for the UI."""
    leader_base = str(request.base_url).rstrip("/")
    return _leader_only_browser_hint(path="/join", title="Wrong service — use Server Node UI for /join", leader_base=leader_base)


@app.get("/servers", response_class=HTMLResponse, include_in_schema=False)
@app.get("/servers/", response_class=HTMLResponse, include_in_schema=False)
async def browser_hint_servers(request: Request):
    leader_base = str(request.base_url).rstrip("/")
    return _leader_only_browser_hint(path="/servers", title="Wrong service — use Server Node UI for /servers", leader_base=leader_base)


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
    prompt: str = Field(min_length=1, max_length=10000)
    user_id: str = ""
    user_name: str = "anonymous"
    tier: Optional[str] = None
    skill: Optional[str] = None

class RegisterBody(BaseModel):
    node_id: str = Field(min_length=1)
    ram_gb: int = Field(ge=1)
    model: str = Field(min_length=1)
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


@app.get("/metrics", include_in_schema=False)
async def metrics_endpoint():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


# ── Server / system readiness checks ──────────────────────────────────────────

@app.get("/server/requirements")
async def server_requirements():
    """
    Returns local machine readiness checks (Tailscale + Ollama + Kafka + Cassandra).
    Intended for the server UI role.
    """
    return await run_all_requirements_checks()


def _status_row_from_cluster_node(n: Dict[str, Any], *, node_role: str) -> Dict[str, Any]:
    models = n.get("models") or []
    first_model = models[0] if isinstance(models, list) and models else None
    return {
        "id": n.get("node_id"),
        "ip": n.get("host"),
        "status": n.get("status", "alive"),
        "model": n.get("model") or first_model,
        "skills": n.get("skills") or [],
        "tasksCompleted": int(n.get("tasks_completed") or 0),
        "lastSeen": int(n.get("last_heartbeat") or 0) * 1000,
        "ram_gb": n.get("ram_gb"),
        "node_role": node_role,
    }


@app.get("/server/status")
async def server_status():
    """
    Returns cluster/server status visible from the leader.
    Each node includes node_role: "leader" | "worker" (elected leader vs task workers).
    """
    cluster = get_cluster_status()
    leader_obj = cluster.get("current_leader") or {}
    leader_id = (leader_obj.get("node_id") or "").strip() or None

    nodes_from_registry = await registry.get_all()
    nodes_list: List[Dict[str, Any]] = []
    for n in nodes_from_registry:
        node_role = "leader" if leader_id and n.node_id == leader_id else "worker"
        nodes_list.append(
            {
                "id": n.node_id,
                "ip": n.ip,
                "status": n.status,
                "model": n.model,
                "skills": n.skills,
                "tasksCompleted": n.tasks_completed,
                "lastSeen": int(n.last_seen * 1000),
                "ram_gb": n.ram_gb,
                "node_role": node_role,
            }
        )

    # Show elected leader even when it does not appear in the worker registry.
    if leader_id and not any(r.get("id") == leader_id for r in nodes_list):
        nodes_list.insert(0, _status_row_from_cluster_node(leader_obj, node_role="leader"))

    # Some flows populate cluster_state (known_nodes) but not registry.
    if not nodes_list:
        cluster_known = cluster.get("known_nodes") or []
        for n in cluster_known:
            if not n.get("node_id"):
                continue
            nid = n.get("node_id")
            nr = "leader" if leader_id and nid == leader_id else "worker"
            nodes_list.append(_status_row_from_cluster_node(n, node_role=nr))

    nodes_list.sort(
        key=lambda r: (0 if r.get("node_role") == "leader" else 1, str(r.get("id") or "")),
    )

    return {"cluster": cluster, "nodes": nodes_list, "kafka_broker": KAFKA_BROKER}


@app.get("/server/local-node")
async def server_local_node():
    """
    Identity of this running node (leader process) for building POST /cluster/join bodies.
    """
    cfg = load_or_create_node_config()
    host = get_advertise_host()
    port = int(cfg.get("port") or 8000)
    now = int(time.time())
    return {
        "node_id": cfg["node_id"],
        "role": cfg.get("role") or "both",
        "priority": int(cfg.get("priority") or 100),
        "host": host,
        "port": port,
        "url": f"http://{host}:{port}",
        "ram_gb": None,
        "models": [],
        "skills": [],
        "status": "alive",
        "last_heartbeat": now,
        "tasks_completed": 0,
    }


# ── Auth ──────────────────────────────────────────────────────────────────────

@app.post("/auth/login", dependencies=[Depends(require_leader)])
async def login(body: LoginBody):
    user = await get_user_by_email(body.email)
    if not user or user["password_hash"] != _hash(body.password):
        logger.warning("auth.login.failed", extra={"email": body.email})
        raise HTTPException(401, "Invalid email or password.")
    logger.info("auth.login.ok", extra={"user_id": user["id"], "email": body.email, "role": user["role"]})
    return {k: v for k, v in user.items() if k != "password_hash"}


@app.post("/auth/signup", dependencies=[Depends(require_leader)])
async def signup(body: SignupBody):
    if await get_user_by_email(body.email):
        logger.warning("auth.signup.conflict", extra={"email": body.email})
        raise HTTPException(409, "An account with that email already exists.")
    user_id = await create_user(body.name, body.email, body.password, body.role)
    logger.info("auth.signup.ok", extra={"user_id": user_id, "email": body.email, "role": body.role})
    return {"id": user_id, "name": body.name, "email": body.email, "role": body.role}


# ── Chat ──────────────────────────────────────────────────────────────────────

@app.post("/ask", dependencies=[Depends(require_leader)])
async def ask(body: AskBody):
    request_id = str(uuid.uuid4())
    _t0 = time.perf_counter()

    # Register the event BEFORE publishing so the consumer thread can't race ahead
    event = result_consumer.register(request_id)
    explicit_skill = (body.skill or "").strip() or None
    try:
        chosen_tier, used_skill = await producer.publish(
            request_id, body.prompt, body.user_id, body.user_name,
            tier_override=body.tier,
            skill=explicit_skill,
            skill_strict=explicit_skill is not None,
            registry=registry,
        )
    except NoEligibleWorker as e:
        result_consumer.cancel(request_id)
        tasks_failed_total.inc()
        raise HTTPException(503, str(e))

    try:
        await asyncio.wait_for(event.wait(), timeout=TASK_TIMEOUT)
    except asyncio.TimeoutError:
        result_consumer.cancel(request_id)
        tasks_failed_total.inc()
        ask_duration_seconds.observe(time.perf_counter() - _t0)
        raise HTTPException(504, "No worker responded in time.")

    result = result_consumer.pop_result(request_id)
    if not result:
        tasks_failed_total.inc()
        ask_duration_seconds.observe(time.perf_counter() - _t0)
        raise HTTPException(500, "Result missing after event fired.")

    if result.get("error"):
        tasks_failed_total.inc()
        ask_duration_seconds.observe(time.perf_counter() - _t0)
        raise HTTPException(500, result["error"])

    duration_ms = result.get("duration_ms", 0)
    worker_id = result.get("worker_id", "unknown")

    tasks_completed_total.inc()
    tasks_completed_by_worker_total.labels(worker_id=worker_id).inc()
    ask_duration_seconds.observe(time.perf_counter() - _t0)

    await save_request(
        request_id, body.user_id, body.user_name,
        body.prompt, worker_id, duration_ms, "completed",
        response=result.get("response", ""),
    )

    return {
        "request_id": request_id,
        "response": result.get("response", ""),
        "worker": worker_id,
        "duration": f"{duration_ms / 1000:.1f}s",
        "tier": chosen_tier,
        "skill": used_skill,
    }


@app.post("/ask/stream", dependencies=[Depends(require_leader)])
async def ask_stream(body: AskBody):
    """
    Server-Sent Events (SSE) endpoint for chat responses.
    Since worker execution is currently single-shot (Kafka roundtrip),
    this streams status updates + the final response as soon as it arrives.
    """
    request_id = str(uuid.uuid4())

    # Register BEFORE publishing so the consumer thread can't race ahead.
    event = result_consumer.register(request_id)
    explicit_skill = (body.skill or "").strip() or None
    try:
        chosen_tier, used_skill = await producer.publish(
            request_id, body.prompt, body.user_id, body.user_name,
            tier_override=body.tier,
            skill=explicit_skill,
            skill_strict=explicit_skill is not None,
            registry=registry,
        )
    except NoEligibleWorker as e:
        result_consumer.cancel(request_id)
        raise HTTPException(503, str(e))

    async def _gen():
        # Initial event
        yield "event: status\ndata: queued\n\n"
        yield f"event: routing\ndata: tier={chosen_tier};skill={used_skill or '-'}\n\n"

        start = time.time()
        while True:
            try:
                await asyncio.wait_for(event.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                # Keep-alive + lightweight progress
                elapsed = time.time() - start
                yield f"event: status\ndata: running:{elapsed:.1f}\n\n"
                continue

            result = result_consumer.pop_result(request_id) or {}
            if result.get("error"):
                msg = str(result.get("error") or "Unknown error")
                yield f"event: error\ndata: {msg}\n\n"
                return

            duration_ms = int(result.get("duration_ms") or 0)
            worker_id = result.get("worker_id") or "unknown"
            response = (result.get("response") or "").replace("\r", "")
            # SSE data must not contain raw newlines unless split across multiple data: lines.
            data = response.replace("\n", "\\n")
            yield f"event: result\ndata: {data}\n\n"
            yield f"event: meta\ndata: request_id={request_id};worker={worker_id};duration_ms={duration_ms};tier={chosen_tier};skill={used_skill or '-'}\n\n"
            return

    return StreamingResponse(_gen(), media_type="text/event-stream")


# ── Node registration & heartbeat ────────────────────────────────────────────

@app.post("/register", dependencies=[Depends(require_leader)])
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


@app.post("/heartbeat", dependencies=[Depends(require_leader)])
async def heartbeat(body: HeartbeatBody):
    known = await registry.heartbeat(body.node_id, body.status, body.tasks_completed)
    update_heartbeat(body.node_id, body.status, body.tasks_completed)
    elect_leader()
    save_cluster_state()
    return {"status": "ok", "is_leader": False, "new_skill": None, "reregister": not known, "kafka_broker": KAFKA_BROKER}


# ── Cluster bootstrap & membership ───────────────────────────────────────────

@app.post("/cluster/create")
async def cluster_create(req: ClusterCreateRequest):
    load_cluster_state()
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
    issue = join_token_issue(req.join_token)
    if issue:
        raise HTTPException(
            status_code=401,
            detail={
                "code": issue,
                "message": "Join token rejected (wrong, expired, or server has no token).",
                "token_expires_at_epoch": st.join_token_expires_at,
            },
        )
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

@app.get("/cluster/stats", dependencies=[Depends(require_leader)])
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


@app.get("/cluster/nodes", dependencies=[Depends(require_leader)])
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


@app.get("/cluster/skills", dependencies=[Depends(require_leader)])
async def cluster_skills():
    """
    Aggregate skills advertised by alive workers across the cluster. Lets
    clients (and any future React skill picker) discover what skill values
    are accepted by /ask. Skill names come from worker SKILL.md directory
    names — see server/worker/skills/ and the load_skills() loader.
    """
    nodes = await registry.get_all()
    skills: set[str] = set()
    for n in nodes:
        if n.status == "offline":
            continue
        skills.update(n.skills or [])
    return {"skills": sorted(skills)}


@app.get("/cluster/requests", dependencies=[Depends(require_leader)])
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

@app.get("/admin/users", dependencies=[Depends(require_leader)])
async def admin_get_users():
    return await get_all_users()


@app.patch("/admin/users/{user_id}", dependencies=[Depends(require_leader)])
async def admin_update_role(user_id: str, body: UpdateRoleBody):
    await update_user_role(user_id, body.role)
    return {"success": True}


@app.delete("/admin/users/{user_id}", dependencies=[Depends(require_leader)])
async def admin_delete_user(user_id: str):
    await delete_user(user_id)
    return {"success": True}
