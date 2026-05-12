"""
Worker node entry point.

Startup sequence:
  1. Read hardware info (RAM via psutil) and config from .env
  2. POST /register to the leader — retries until the leader is reachable
  3. Start a heartbeat thread (every 5 s)
  4. Enter the Kafka consume loop — pull tasks, call Ollama, publish results

Run:
  cd server/worker
  cp .env.example .env   # edit LEADER_URL, KAFKA_BROKER, MODEL, SKILLS
  pip install -r requirements.txt
  python worker.py
"""

import glob
import json
import os
import signal
import sys
import threading
import time

import httpx
import psutil
from dotenv import load_dotenv

load_dotenv()

from kafka_client import WorkerKafka
from ollama_client import generate

# ── Config ────────────────────────────────────────────────────────────────────

# LEADER_URL from .env is used as the fallback bootstrap address. The actual
# leader URL is resolved at runtime — first from the Upstash discovery service
# (if configured), then from this fallback. The cached value is updated again
# after HEARTBEAT_FAILURE_THRESHOLD consecutive heartbeat failures so workers
# can follow the leader to its new home after a failover.
FALLBACK_LEADER_URL = os.getenv("LEADER_URL", "http://localhost:8000")
UPSTASH_URL = os.getenv("UPSTASH_REDIS_REST_URL", "").strip().rstrip("/")
UPSTASH_TOKEN = os.getenv("UPSTASH_REDIS_REST_READ_ONLY_TOKEN", "").strip()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
MODEL = os.getenv("MODEL", "mistral")
SKILLS = [s.strip() for s in os.getenv("SKILLS", "general").split(",")]
NODE_ID = os.getenv("NODE_ID") or f"node_{os.urandom(3).hex()}"
RAM_GB = int(os.getenv("RAM_GB") or round(psutil.virtual_memory().total / (1024 ** 3)))
HEARTBEAT_INTERVAL = 5
HEARTBEAT_FAILURE_THRESHOLD = 3

_leader_url = FALLBACK_LEADER_URL
_leader_lock = threading.Lock()


def _get_leader_url() -> str:
    with _leader_lock:
        return _leader_url


def _set_leader_url(url: str) -> None:
    global _leader_url
    with _leader_lock:
        _leader_url = url


def resolve_leader_from_discovery() -> str | None:
    """Return the current leader URL from Upstash, or None if unavailable."""
    if not UPSTASH_URL or not UPSTASH_TOKEN:
        return None
    try:
        r = httpx.get(
            f"{UPSTASH_URL}/get/leader",
            headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
            timeout=5,
        )
        r.raise_for_status()
        raw = r.json().get("result")
        if not raw:
            return None
        return (json.loads(raw).get("leader_url") or "").strip() or None
    except Exception as exc:
        print(f"[{NODE_ID}] Discovery resolve failed: {exc}")
        return None


# ── Hardware profiling ────────────────────────────────────────────────────────

def get_ollama_models() -> list[str]:
    try:
        resp = httpx.get(f"{OLLAMA_URL}/api/tags", timeout=3)
        resp.raise_for_status()
        return [m["name"] for m in resp.json().get("models", [])]
    except Exception:
        return []


def get_skills() -> list[str]:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    skill_files = glob.glob(os.path.join(script_dir, "*.skill"))
    return [os.path.splitext(os.path.basename(f))[0] for f in skill_files]

# ── Shared state (only written from main thread or heartbeat thread) ───────────

_tasks_completed = 0
_current_status = "idle"
_shutdown = threading.Event()

# Resolved once at startup (first register() call) and reused for every task.
# Reading MODEL directly in process_task was a bug: registration reported the
# first detected Ollama tag while execution silently fell back to whatever
# MODEL happened to be, even when those didn't match.
_effective_model: str = MODEL


# ── Registration ──────────────────────────────────────────────────────────────

def register() -> str:
    """Block until the leader accepts registration. Returns the Kafka broker address.

    Re-resolves the leader URL from the discovery service on every retry attempt
    so a worker started before any leader is up will pick up the leader as soon
    as one publishes itself, and a worker rejoining after a failover will target
    the new leader.
    """
    global _effective_model
    detected_models = get_ollama_models()
    _effective_model = detected_models[0] if detected_models else MODEL
    effective_skills = get_skills() or SKILLS

    print(f"[{NODE_ID}] RAM: {RAM_GB} GB  Model: {_effective_model}  Skills: {effective_skills}")

    while not _shutdown.is_set():
        if discovered := resolve_leader_from_discovery():
            _set_leader_url(discovered)
        url = _get_leader_url()
        try:
            r = httpx.post(
                f"{url}/register",
                json={"node_id": NODE_ID, "ram_gb": RAM_GB, "model": _effective_model, "skills": effective_skills},
                timeout=5,
            )
            r.raise_for_status()
            data = r.json()
            broker = data.get("kafka_broker", KAFKA_BROKER)
            print(f"[{NODE_ID}] Registered with leader at {url}. Kafka broker: {broker}")
            return broker
        except Exception as e:
            print(f"[{NODE_ID}] Registration failed against {url} ({e}). Retrying in 5 s…")
            time.sleep(5)
    return KAFKA_BROKER


# ── Heartbeat loop ────────────────────────────────────────────────────────────

def heartbeat_loop(kafka: WorkerKafka):
    global _current_status
    failures = 0
    while not _shutdown.is_set():
        url = _get_leader_url()
        try:
            r = httpx.post(
                f"{url}/heartbeat",
                json={"node_id": NODE_ID, "status": _current_status, "tasks_completed": _tasks_completed},
                timeout=5,
            )
            r.raise_for_status()
            data = r.json()
            failures = 0
            if data.get("reregister"):
                print(f"[{NODE_ID}] Leader asked us to re-register.")
                new_broker = register()
                kafka.request_reconnect(new_broker)
            elif (new_broker := data.get("kafka_broker")) and new_broker != kafka.bootstrap_servers:
                print(f"[{NODE_ID}] Kafka broker changed to {new_broker}.")
                kafka.request_reconnect(new_broker)
        except Exception as e:
            failures += 1
            print(f"[{NODE_ID}] Heartbeat to {url} failed ({failures}/{HEARTBEAT_FAILURE_THRESHOLD}): {e}")
            if failures >= HEARTBEAT_FAILURE_THRESHOLD:
                discovered = resolve_leader_from_discovery()
                if discovered and discovered != url:
                    print(f"[{NODE_ID}] Discovery reports new leader: {discovered}. Re-registering.")
                    _set_leader_url(discovered)
                    new_broker = register()
                    kafka.request_reconnect(new_broker)
                    failures = 0
                else:
                    print(f"[{NODE_ID}] Discovery has no new leader yet; will keep trying.")
        time.sleep(HEARTBEAT_INTERVAL)


# ── Task processing ───────────────────────────────────────────────────────────

def process_task(kafka: WorkerKafka, task: dict):
    global _tasks_completed, _current_status

    request_id = task.get("request_id", "unknown")
    prompt = task.get("prompt", "")
    print(f"[{NODE_ID}] Processing {request_id}: {prompt[:80]}…")

    _current_status = "busy"
    start = time.time()
    try:
        response = generate(_effective_model, prompt)
        duration_ms = int((time.time() - start) * 1000)
        kafka.publish_result(request_id, response, duration_ms)
        _tasks_completed += 1
        print(f"[{NODE_ID}] Done {request_id} in {duration_ms} ms")
    except Exception as e:
        duration_ms = int((time.time() - start) * 1000)
        print(f"[{NODE_ID}] Error on {request_id}: {e}")
        kafka.publish_result(request_id, "", duration_ms, error=str(e))
    finally:
        _current_status = "idle"


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print(f"[{NODE_ID}] Starting — RAM: {RAM_GB} GB  Model: {MODEL}  Skills: {SKILLS}")

    broker = register()
    kafka = WorkerKafka(broker, NODE_ID, RAM_GB)
    print(f"[{NODE_ID}] Subscribed to topics: {kafka.topics}")

    # Heartbeat thread needs the kafka reference so it can signal broker changes.
    hb = threading.Thread(target=heartbeat_loop, args=(kafka,), daemon=True, name="heartbeat")
    hb.start()

    def on_signal(sig, frame):
        print(f"\n[{NODE_ID}] Shutting down…")
        _shutdown.set()
        kafka.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    print(f"[{NODE_ID}] Listening for tasks on Kafka broker {broker}…")
    while not _shutdown.is_set():
        try:
            for task in kafka.poll():
                if _shutdown.is_set():
                    break
                process_task(kafka, task)
                # Commit only after process_task returns. If the worker crashes
                # mid-task, the offset stays uncommitted and Kafka redelivers
                # to another worker on rebalance.
                kafka.commit()
            kafka.apply_reconnect_if_needed()
        except Exception as e:
            print(f"[{NODE_ID}] Consumer error: {e}. Retrying in 2 s…")
            time.sleep(2)

    kafka.close()


if __name__ == "__main__":
    main()
