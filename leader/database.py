import asyncio
import hashlib
import os
import subprocess
import time
import uuid
from datetime import datetime, timezone

DB_CQLSH_CONTAINER = os.getenv("DB_CQLSH_CONTAINER", "web-app-cassandra")


def _hash(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()


def _cqlsh(query: str) -> str:
    res = subprocess.run(
        ["docker", "exec", DB_CQLSH_CONTAINER, "cqlsh", "-e", query],
        capture_output=True, text=True, timeout=15, check=True,
    )
    return res.stdout


def _cql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _parse_rows(stdout: str) -> list[dict]:
    lines = [ln.rstrip("\r") for ln in stdout.splitlines() if ln.strip()]
    data_rows = []
    for ln in lines:
        if ln.startswith("(") and ln.endswith("rows)"):
            break
        if "|" in ln and not set(ln.strip()).issubset(set("-+")):
            data_rows.append(ln)
    if len(data_rows) < 2:
        return []
    header = [h.strip() for h in data_rows[0].split("|")]
    result = []
    for ln in data_rows[1:]:
        values = [c.strip() for c in ln.split("|")]
        if len(values) == len(header):
            result.append(dict(zip(header, values)))
    return result


async def _run(query: str) -> str:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _cqlsh, query)


async def init_db():
    for attempt in range(10):
        try:
            await _run("DESCRIBE KEYSPACES;")
            print("Cassandra is ready.")
            return
        except Exception:
            print(f"Waiting for Cassandra (attempt {attempt + 1}/10)…")
            await asyncio.sleep(2)
    print("WARNING: Could not connect to Cassandra on startup.")


async def get_user_by_email(email: str) -> dict | None:
    q = (
        "SELECT user_id, username, email, password_hash, role "
        f"FROM web_app.users WHERE email = {_cql_str(email)};"
    )
    rows = _parse_rows(await _run(q))
    if not rows:
        return None
    r = rows[0]
    return {
        "id": r["user_id"],
        "name": r["username"],
        "email": r["email"],
        "password_hash": r["password_hash"],
        "role": r["role"],
    }


async def create_user(name: str, email: str, password: str, role: str) -> str:
    user_id = str(uuid.uuid4())
    q = (
        "INSERT INTO web_app.users (user_id, username, email, password_hash, role, created_at) "
        f"VALUES ({user_id}, {_cql_str(name)}, {_cql_str(email)}, "
        f"{_cql_str(_hash(password))}, {_cql_str(role)}, toTimestamp(now()));"
    )
    await _run(q)
    return user_id


async def get_all_users() -> list[dict]:
    q = "SELECT user_id, username, email, role, created_at FROM web_app.users ALLOW FILTERING;"
    rows = _parse_rows(await _run(q))
    return [
        {
            "id": r["user_id"],
            "name": r["username"],
            "email": r["email"],
            "role": r["role"],
            "joinedAt": r.get("created_at", "")[:10],
        }
        for r in rows
        if r.get("role") != "admin"
    ]


async def update_user_role(user_id: str, role: str):
    q = f"UPDATE web_app.users SET role = {_cql_str(role)} WHERE user_id = {user_id};"
    await _run(q)


async def delete_user(user_id: str):
    q = f"DELETE FROM web_app.users WHERE user_id = {user_id};"
    await _run(q)


async def save_request(
    req_id: str,
    user_id: str,
    user_name: str,
    prompt: str,
    worker_id: str,
    duration_ms: int,
    status: str,
    response: str = "",
):
    duration = f"{duration_ms / 1000:.1f}s"
    time_bucket = datetime.now(timezone.utc).strftime("%Y-%m")

    # Validate user_id is a UUID; fall back to nil UUID for anonymous requests
    try:
        uuid.UUID(user_id)
        uid_cql = user_id
    except (ValueError, AttributeError):
        uid_cql = "00000000-0000-0000-0000-000000000000"

    # Validate req_id is a UUID
    try:
        uuid.UUID(req_id)
        rid_cql = req_id
    except (ValueError, AttributeError):
        rid_cql = str(uuid.uuid4())

    q_user = (
        "INSERT INTO web_app.requests "
        "(user_id, created_at, request_id, prompt, response, worker_node, duration, status) "
        f"VALUES ({uid_cql}, toTimestamp(now()), {rid_cql}, {_cql_str(prompt)}, "
        f"{_cql_str(response)}, {_cql_str(worker_id)}, {_cql_str(duration)}, {_cql_str(status)});"
    )
    q_admin = (
        "INSERT INTO web_app.cluster_requests_by_month "
        "(time_bucket, created_at, request_id, user_id, username, prompt, worker_node, duration, status) "
        f"VALUES ({_cql_str(time_bucket)}, toTimestamp(now()), {rid_cql}, {uid_cql}, "
        f"{_cql_str(user_name)}, {_cql_str(prompt)}, {_cql_str(worker_id)}, {_cql_str(duration)}, {_cql_str(status)});"
    )
    await _run(q_user)
    await _run(q_admin)


async def get_all_requests() -> list[dict]:
    now = datetime.now(timezone.utc)
    buckets = []
    for i in range(3):
        month = now.month - i
        year = now.year
        while month <= 0:
            month += 12
            year -= 1
        buckets.append(f"{year}-{month:02d}")

    all_rows = []
    for bucket in buckets:
        q = (
            "SELECT request_id, user_id, username, prompt, worker_node, duration, status, created_at "
            f"FROM web_app.cluster_requests_by_month WHERE time_bucket = {_cql_str(bucket)} LIMIT 200;"
        )
        try:
            rows = _parse_rows(await _run(q))
            all_rows.extend(rows)
        except Exception:
            pass

    return [
        {
            "id": r.get("request_id", ""),
            "user_id": r.get("user_id", ""),
            "user_name": r.get("username", ""),
            "prompt": r.get("prompt", ""),
            "worker_id": r.get("worker_node", ""),
            "duration": r.get("duration", "0.0s"),
            "status": r.get("status", ""),
            "created_at": r.get("created_at", ""),
        }
        for r in all_rows
    ]
