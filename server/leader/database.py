"""
Cassandra access for the leader process.

Two connection modes, selected by USE_ASTRA env var:
  USE_ASTRA=true  -> Astra DataStax (cloud-managed). Required for
                     production / multi-laptop demo so user data and
                     request history survive leader failover. All leaders
                     read from the same shared DB.
  USE_ASTRA=false -> Local docker Cassandra at LOCAL_CASSANDRA_HOST.
                     Offline-dev fallback only — data is leader-local and
                     does NOT survive failover.

All queries go through the official cassandra-driver with prepared
statements; the previous docker-exec subprocess + cqlsh-text-parsing
approach is gone, which also closes the CQL injection vector that
existed via user-supplied prompts and names.

Driver calls are sync; the existing async API is preserved by wrapping
each call in loop.run_in_executor. Function signatures and return shapes
match the previous module so main.py needs no changes.
"""

import asyncio
import hashlib
import os
import uuid
from datetime import datetime, timezone
from typing import Optional

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, Session
from cassandra.query import PreparedStatement


def _bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name, "")
    if not raw:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


USE_ASTRA = _bool_env("USE_ASTRA", default=True)
ASTRA_BUNDLE_PATH = os.getenv("ASTRA_BUNDLE_PATH", "").strip()
ASTRA_CLIENT_ID = os.getenv("ASTRA_CLIENT_ID", "").strip()
ASTRA_CLIENT_SECRET = os.getenv("ASTRA_CLIENT_SECRET", "").strip()
ASTRA_KEYSPACE = os.getenv("ASTRA_KEYSPACE", "web_app").strip() or "web_app"

LOCAL_CASSANDRA_HOST = os.getenv("LOCAL_CASSANDRA_HOST", "127.0.0.1").strip() or "127.0.0.1"
LOCAL_CASSANDRA_PORT = int(os.getenv("LOCAL_CASSANDRA_PORT", "9042"))
LOCAL_CASSANDRA_KEYSPACE = os.getenv("LOCAL_CASSANDRA_KEYSPACE", "web_app").strip() or "web_app"


def _hash(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()


# Module-level singleton. Connection is lazily established on first use,
# then reused for the lifetime of the process. The driver's connection
# pool handles per-host failover within the cluster.
_cluster: Optional[Cluster] = None
_session: Optional[Session] = None
_prepared: dict[str, PreparedStatement] = {}


def _build_cluster() -> Cluster:
    if USE_ASTRA:
        missing = [
            name for name, val in (
                ("ASTRA_BUNDLE_PATH", ASTRA_BUNDLE_PATH),
                ("ASTRA_CLIENT_ID", ASTRA_CLIENT_ID),
                ("ASTRA_CLIENT_SECRET", ASTRA_CLIENT_SECRET),
            )
            if not val
        ]
        if missing:
            raise RuntimeError(
                f"USE_ASTRA=true but missing env vars: {', '.join(missing)}. "
                "Set them or switch to USE_ASTRA=false for the local docker fallback."
            )
        if not os.path.exists(ASTRA_BUNDLE_PATH):
            raise RuntimeError(
                f"ASTRA_BUNDLE_PATH does not point to an existing file: {ASTRA_BUNDLE_PATH}. "
                "Download the Secure Connect Bundle from astra.datastax.com and update the env var."
            )
        return Cluster(
            cloud={"secure_connect_bundle": ASTRA_BUNDLE_PATH},
            auth_provider=PlainTextAuthProvider(ASTRA_CLIENT_ID, ASTRA_CLIENT_SECRET),
        )
    return Cluster([LOCAL_CASSANDRA_HOST], port=LOCAL_CASSANDRA_PORT)


def _keyspace() -> str:
    return ASTRA_KEYSPACE if USE_ASTRA else LOCAL_CASSANDRA_KEYSPACE


def _get_session() -> Session:
    global _cluster, _session
    if _session is None:
        _cluster = _build_cluster()
        _session = _cluster.connect(_keyspace())
    return _session


def _prep(key: str, cql: str) -> PreparedStatement:
    if key not in _prepared:
        _prepared[key] = _get_session().prepare(cql)
    return _prepared[key]


async def _exec(stmt: PreparedStatement, params: tuple = ()):
    """Run a prepared statement with bound params off the event loop."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _get_session().execute, stmt, params)


async def init_db() -> None:
    """Connect (with retry) and confirm the keyspace is reachable."""
    loop = asyncio.get_event_loop()
    for attempt in range(10):
        try:
            session = await loop.run_in_executor(None, _get_session)
            await loop.run_in_executor(
                None, session.execute, "SELECT release_version FROM system.local"
            )
            mode = "Astra" if USE_ASTRA else f"local ({LOCAL_CASSANDRA_HOST}:{LOCAL_CASSANDRA_PORT})"
            print(f"Cassandra ready ({mode}, keyspace={_keyspace()}).")
            return
        except Exception as exc:
            print(f"Waiting for Cassandra (attempt {attempt + 1}/10): {exc}")
            await asyncio.sleep(2)
    print("WARNING: Could not connect to Cassandra on startup.")


# ── User queries ─────────────────────────────────────────────────────────────

async def get_user_by_email(email: str) -> dict | None:
    stmt = _prep(
        "user_by_email",
        "SELECT user_id, username, email, password_hash, role "
        "FROM users WHERE email = ?",
    )
    rows = await _exec(stmt, (email,))
    row = rows.one()
    if not row:
        return None
    return {
        "id": str(row.user_id),
        "name": row.username,
        "email": row.email,
        "password_hash": row.password_hash,
        "role": row.role,
    }


async def create_user(name: str, email: str, password: str, role: str) -> str:
    user_id = uuid.uuid4()
    stmt = _prep(
        "create_user",
        "INSERT INTO users (user_id, username, email, password_hash, role, created_at) "
        "VALUES (?, ?, ?, ?, ?, toTimestamp(now()))",
    )
    await _exec(stmt, (user_id, name, email, _hash(password), role))
    return str(user_id)


async def get_all_users() -> list[dict]:
    stmt = _prep(
        "all_users",
        "SELECT user_id, username, email, role, created_at FROM users",
    )
    rows = await _exec(stmt, ())
    out: list[dict] = []
    for r in rows:
        if r.role == "admin":
            continue
        joined = ""
        if r.created_at:
            try:
                joined = r.created_at.strftime("%Y-%m-%d")
            except Exception:
                joined = str(r.created_at)[:10]
        out.append({
            "id": str(r.user_id),
            "name": r.username,
            "email": r.email,
            "role": r.role,
            "joinedAt": joined,
        })
    return out


async def update_user_role(user_id: str, role: str) -> None:
    stmt = _prep("update_role", "UPDATE users SET role = ? WHERE user_id = ?")
    await _exec(stmt, (role, uuid.UUID(user_id)))


async def delete_user(user_id: str) -> None:
    stmt = _prep("delete_user", "DELETE FROM users WHERE user_id = ?")
    await _exec(stmt, (uuid.UUID(user_id),))


# ── Request history ─────────────────────────────────────────────────────────

async def save_request(
    req_id: str,
    user_id: str,
    user_name: str,
    prompt: str,
    worker_id: str,
    duration_ms: int,
    status: str,
    response: str = "",
) -> None:
    duration = f"{duration_ms / 1000:.1f}s"
    time_bucket = datetime.now(timezone.utc).strftime("%Y-%m")

    # Anonymous requests get the nil UUID so the per-user history table still
    # accepts them (user_id is part of the partition key).
    try:
        uid = uuid.UUID(user_id)
    except (ValueError, AttributeError):
        uid = uuid.UUID("00000000-0000-0000-0000-000000000000")

    try:
        rid = uuid.UUID(req_id)
    except (ValueError, AttributeError):
        rid = uuid.uuid4()

    user_stmt = _prep(
        "save_request_user",
        "INSERT INTO requests "
        "(user_id, created_at, request_id, prompt, response, worker_node, duration, status) "
        "VALUES (?, toTimestamp(now()), ?, ?, ?, ?, ?, ?)",
    )
    admin_stmt = _prep(
        "save_request_admin",
        "INSERT INTO cluster_requests_by_month "
        "(time_bucket, created_at, request_id, user_id, username, prompt, worker_node, duration, status) "
        "VALUES (?, toTimestamp(now()), ?, ?, ?, ?, ?, ?, ?)",
    )
    await _exec(user_stmt, (uid, rid, prompt, response, worker_id, duration, status))
    await _exec(
        admin_stmt,
        (time_bucket, rid, uid, user_name, prompt, worker_id, duration, status),
    )


async def get_all_requests() -> list[dict]:
    now = datetime.now(timezone.utc)
    buckets: list[str] = []
    for i in range(3):
        month = now.month - i
        year = now.year
        while month <= 0:
            month += 12
            year -= 1
        buckets.append(f"{year}-{month:02d}")

    stmt = _prep(
        "requests_by_bucket",
        "SELECT request_id, user_id, username, prompt, worker_node, duration, status, created_at "
        "FROM cluster_requests_by_month WHERE time_bucket = ? LIMIT 200",
    )

    out: list[dict] = []
    for bucket in buckets:
        try:
            rows = await _exec(stmt, (bucket,))
            for r in rows:
                created_at = ""
                if r.created_at:
                    try:
                        created_at = r.created_at.isoformat()
                    except Exception:
                        created_at = str(r.created_at)
                out.append({
                    "id": str(r.request_id),
                    "user_id": str(r.user_id),
                    "user_name": r.username,
                    "prompt": r.prompt,
                    "worker_id": r.worker_node,
                    "duration": r.duration,
                    "status": r.status,
                    "created_at": created_at,
                })
        except Exception:
            pass
    return out
