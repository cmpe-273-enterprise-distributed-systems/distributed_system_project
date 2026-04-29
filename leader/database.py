import hashlib
import os
import time
from datetime import date

import aiosqlite

DB_PATH = os.getenv("DB_PATH", "leader.db")


def _hash(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                name          TEXT NOT NULL,
                email         TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role          TEXT NOT NULL DEFAULT 'client',
                joined_at     TEXT NOT NULL
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS requests (
                id          TEXT PRIMARY KEY,
                user_id     INTEGER,
                user_name   TEXT,
                prompt      TEXT NOT NULL,
                worker_id   TEXT,
                duration_ms INTEGER,
                status      TEXT DEFAULT 'pending',
                created_at  INTEGER NOT NULL
            )
        """)
        # Seed the admin account on first run
        await db.execute(
            "INSERT OR IGNORE INTO users (name, email, password_hash, role, joined_at) VALUES (?, ?, ?, ?, ?)",
            ("Admin", "admin@cluster.local", _hash("admin"), "admin", str(date.today())),
        )
        await db.commit()


async def get_user_by_email(email: str) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM users WHERE email = ?", (email,)) as cur:
            row = await cur.fetchone()
            return dict(row) if row else None


async def create_user(name: str, email: str, password: str, role: str) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "INSERT INTO users (name, email, password_hash, role, joined_at) VALUES (?, ?, ?, ?, ?)",
            (name, email, _hash(password), role, str(date.today())),
        )
        await db.commit()
        return cur.lastrowid


async def get_all_users() -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT id, name, email, role, joined_at FROM users WHERE role != 'admin' ORDER BY id"
        ) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]


async def update_user_role(user_id: int, role: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET role = ? WHERE id = ?", (role, user_id))
        await db.commit()


async def delete_user(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM users WHERE id = ?", (user_id,))
        await db.commit()


async def save_request(
    req_id: str,
    user_id: int,
    user_name: str,
    prompt: str,
    worker_id: str,
    duration_ms: int,
    status: str,
):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT OR IGNORE INTO requests
               (id, user_id, user_name, prompt, worker_id, duration_ms, status, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (req_id, user_id, user_name, prompt, worker_id, duration_ms, status, int(time.time() * 1000)),
        )
        await db.commit()


async def get_all_requests() -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM requests ORDER BY created_at DESC LIMIT 200"
        ) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]
